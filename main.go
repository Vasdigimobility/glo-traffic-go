package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	ListenAddr              string
	ForwardURL              string
	ForwardTimeout          time.Duration
	MaxRetries              int
	RetryDelay              time.Duration
	WorkerCount             int
	QueueSize               int
	RetryQueueSize          int
	RetryWorkerCount        int
	MaxBodySize             int64
	RequestTTL              time.Duration
	EnableDedupe            bool
	DedupeWindow            time.Duration
	EnablePersistence       bool
	PersistencePath         string
	RedisAddr               string
	RedisPassword           string
	RedisDB                 int
	RedisKeyPrefix          string
	EnableDLQ               bool
	DLQPath                 string
	DLQMaxSize              int
	CircuitBreakerEnabled   bool
	CircuitBreakerThreshold int
	CircuitBreakerTimeout   time.Duration
	MemoryAlertThreshold    int64
}

type ForwardRequest struct {
	Method     string
	Path       string
	Headers    http.Header
	Body       []byte
	Query      string
	ReceivedAt time.Time
	ExpiresAt  time.Time
	RequestID  string
	RetryCount int
}

type DedupeCache struct {
	seen map[string]time.Time
	mu   sync.RWMutex
	ttl  time.Duration
}

func NewDedupeCache(ttl time.Duration) *DedupeCache {
	return &DedupeCache{
		seen: make(map[string]time.Time),
		ttl:  ttl,
	}
}

func (dc *DedupeCache) IsDuplicate(requestID string) bool {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	if firstSeen, exists := dc.seen[requestID]; exists {
		if time.Since(firstSeen) < dc.ttl {
			return true
		}
	}
	return false
}

func (dc *DedupeCache) Add(requestID string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.seen[requestID] = time.Now()
}

func (dc *DedupeCache) Cleanup() {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	for id, timestamp := range dc.seen {
		if now.Sub(timestamp) > dc.ttl {
			delete(dc.seen, id)
		}
	}
}

func (dc *DedupeCache) Size() int {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	return len(dc.seen)
}

type CircuitBreakerState int

const (
	StateClosed CircuitBreakerState = iota
	StateOpen
	StateHalfOpen
)

type CircuitBreaker struct {
	state           CircuitBreakerState
	failureCount    int64
	successCount    int64
	lastFailureTime time.Time
	threshold       int
	timeout         time.Duration
	mu              sync.RWMutex
}

func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:     StateClosed,
		threshold: threshold,
		timeout:   timeout,
	}
}

func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.Lock()
	state := cb.state

	if state == StateOpen {
		if time.Since(cb.lastFailureTime) > cb.timeout {
			cb.state = StateHalfOpen
			cb.successCount = 0
			state = StateHalfOpen
			log.Printf("Circuit breaker entering half-open state")
		} else {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker is open")
		}
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failureCount++
		cb.lastFailureTime = time.Now()

		if state == StateHalfOpen {
			cb.state = StateOpen
			log.Printf("Circuit breaker reopened after failure in half-open state")
		} else if cb.failureCount >= int64(cb.threshold) {
			cb.state = StateOpen
			log.Printf("Circuit breaker opened after %d failures", cb.failureCount)
		}
		return err
	}

	if state == StateHalfOpen {
		cb.successCount++
		if cb.successCount >= 3 {
			cb.state = StateClosed
			cb.failureCount = 0
			log.Printf("Circuit breaker closed after successful recovery")
		}
	} else {
		cb.failureCount = 0
	}

	return nil
}

func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *CircuitBreaker) GetStateString() string {
	state := cb.GetState()
	switch state {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

func (cb *CircuitBreaker) GetFailureCount() int64 {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.failureCount
}

type QueuePersistence struct {
	path          string
	enabled       bool
	mu            sync.Mutex
	disabled      bool
	lastErrorTime time.Time
	errorCount    int
}

func NewQueuePersistence(path string, enabled bool) *QueuePersistence {
	qp := &QueuePersistence{
		path:    path,
		enabled: enabled,
	}

	if enabled {
		if err := qp.validatePath(); err != nil {
			log.Printf("WARNING: Queue persistence disabled - %v", err)
			qp.disabled = true
		}
	}

	return qp
}

func (qp *QueuePersistence) validatePath() error {
	dir := filepath.Dir(qp.path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("cannot create directory %s: %w", dir, err)
	}

	testFile := filepath.Join(dir, ".write_test")
	if err := os.WriteFile(testFile, []byte("test"), 0600); err != nil {
		if os.IsPermission(err) || strings.Contains(err.Error(), "read-only file system") {
			return fmt.Errorf("path %s is not writable (read-only file system or permission denied)", dir)
		}
		return fmt.Errorf("cannot write to %s: %w", dir, err)
	}
	os.Remove(testFile)

	return nil
}

func (qp *QueuePersistence) Save(requests []ForwardRequest) error {
	if !qp.enabled || qp.disabled {
		return nil
	}

	qp.mu.Lock()
	defer qp.mu.Unlock()

	data, err := json.Marshal(requests)
	if err != nil {
		return fmt.Errorf("failed to marshal queue: %w", err)
	}

	err = os.WriteFile(qp.path, data, 0600)
	if err != nil {
		if os.IsPermission(err) || strings.Contains(err.Error(), "read-only file system") {
			if !qp.disabled {
				log.Printf("WARNING: Disabling queue persistence - path %s is not writable: %v", qp.path, err)
				qp.disabled = true
			}
			return nil
		}

		qp.errorCount++
		qp.lastErrorTime = time.Now()

		if qp.errorCount >= 10 {
			log.Printf("WARNING: Disabling queue persistence after %d consecutive errors. Last error: %v", qp.errorCount, err)
			qp.disabled = true
			return nil
		}

		return fmt.Errorf("failed to write queue to disk: %w", err)
	}

	qp.errorCount = 0
	return nil
}

func (qp *QueuePersistence) Load() ([]ForwardRequest, error) {
	if !qp.enabled || qp.disabled {
		return nil, nil
	}

	qp.mu.Lock()
	defer qp.mu.Unlock()

	data, err := os.ReadFile(qp.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read queue from disk: %w", err)
	}

	var requests []ForwardRequest
	err = json.Unmarshal(data, &requests)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal queue: %w", err)
	}

	return requests, nil
}

func (qp *QueuePersistence) Clear() error {
	if !qp.enabled || qp.disabled {
		return nil
	}

	qp.mu.Lock()
	defer qp.mu.Unlock()

	return os.Remove(qp.path)
}

type RedisPersistence struct {
	client    *redis.Client
	enabled   bool
	keyPrefix string
	mu        sync.Mutex
	ctx       context.Context
}

func NewRedisPersistence(addr, password string, db int, keyPrefix string, enabled bool) *RedisPersistence {
	if !enabled || addr == "" {
		log.Printf("Redis persistence disabled")
		return &RedisPersistence{
			enabled: false,
			ctx:     context.Background(),
		}
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("WARNING: Redis connection failed - %v. Persistence disabled.", err)
		return &RedisPersistence{
			enabled: false,
			ctx:     ctx,
		}
	}

	log.Printf("Redis persistence enabled at %s (DB: %d, prefix: %s)", addr, db, keyPrefix)
	return &RedisPersistence{
		client:    client,
		enabled:   true,
		keyPrefix: keyPrefix,
		ctx:       ctx,
	}
}

func (rp *RedisPersistence) Save(requests []ForwardRequest) error {
	if !rp.enabled || rp.client == nil {
		return nil
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	data, err := json.Marshal(requests)
	if err != nil {
		return fmt.Errorf("failed to marshal queue: %w", err)
	}

	key := rp.keyPrefix + ":queue"
	err = rp.client.Set(rp.ctx, key, data, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to save queue to Redis: %w", err)
	}

	return nil
}

func (rp *RedisPersistence) Load() ([]ForwardRequest, error) {
	if !rp.enabled || rp.client == nil {
		return nil, nil
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	key := rp.keyPrefix + ":queue"
	data, err := rp.client.Get(rp.ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read queue from Redis: %w", err)
	}

	var requests []ForwardRequest
	err = json.Unmarshal(data, &requests)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal queue: %w", err)
	}

	return requests, nil
}

func (rp *RedisPersistence) Clear() error {
	if !rp.enabled || rp.client == nil {
		return nil
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	key := rp.keyPrefix + ":queue"
	return rp.client.Del(rp.ctx, key).Err()
}

func (rp *RedisPersistence) Close() error {
	if rp.client != nil {
		return rp.client.Close()
	}
	return nil
}

type Persistence interface {
	Save(requests []ForwardRequest) error
	Load() ([]ForwardRequest, error)
	Clear() error
}

type DLQEntry struct {
	Request       ForwardRequest `json:"request"`
	FailureReason string         `json:"failure_reason"`
	FailureTime   time.Time      `json:"failure_time"`
	ErrorType     string         `json:"error_type"`
	AttemptCount  int            `json:"attempt_count"`
}

type DLQ interface {
	Add(entry DLQEntry) error
	GetAll() ([]DLQEntry, error)
	GetCount() (int, error)
	Clear() error
	Replay(limit int) ([]ForwardRequest, error)
}

type DeadLetterQueue struct {
	path    string
	enabled bool
	maxSize int
	mu      sync.Mutex
}

func NewDeadLetterQueue(path string, enabled bool, maxSize int) *DeadLetterQueue {
	return &DeadLetterQueue{
		path:    path,
		enabled: enabled,
		maxSize: maxSize,
	}
}

func (dlq *DeadLetterQueue) Add(entry DLQEntry) error {
	if !dlq.enabled {
		return nil
	}

	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	entries, err := dlq.loadEntries()
	if err != nil {
		return fmt.Errorf("failed to load DLQ: %w", err)
	}

	entries = append(entries, entry)

	if dlq.maxSize > 0 && len(entries) > dlq.maxSize {
		entries = entries[len(entries)-dlq.maxSize:]
	}

	return dlq.saveEntries(entries)
}

func (dlq *DeadLetterQueue) GetAll() ([]DLQEntry, error) {
	if !dlq.enabled {
		return nil, nil
	}

	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	return dlq.loadEntries()
}

func (dlq *DeadLetterQueue) GetCount() (int, error) {
	entries, err := dlq.GetAll()
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

func (dlq *DeadLetterQueue) Clear() error {
	if !dlq.enabled {
		return nil
	}

	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	return dlq.saveEntries([]DLQEntry{})
}

func (dlq *DeadLetterQueue) Replay(limit int) ([]ForwardRequest, error) {
	if !dlq.enabled {
		return nil, nil
	}

	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	entries, err := dlq.loadEntries()
	if err != nil {
		return nil, err
	}

	if limit > 0 && limit < len(entries) {
		entries = entries[:limit]
	}

	requests := make([]ForwardRequest, len(entries))
	for i, entry := range entries {
		req := entry.Request
		req.RetryCount = 0
		req.ReceivedAt = time.Now()
		requests[i] = req
	}

	remaining := []DLQEntry{}
	if limit > 0 && limit < len(entries) {
		remaining = entries[limit:]
	}
	if err := dlq.saveEntries(remaining); err != nil {
		return nil, err
	}

	return requests, nil
}

func (dlq *DeadLetterQueue) loadEntries() ([]DLQEntry, error) {
	data, err := os.ReadFile(dlq.path)
	if err != nil {
		if os.IsNotExist(err) {
			return []DLQEntry{}, nil
		}
		return nil, err
	}

	var entries []DLQEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	return entries, nil
}

func (dlq *DeadLetterQueue) saveEntries(entries []DLQEntry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}

	return os.WriteFile(dlq.path, data, 0600)
}

type RedisDLQ struct {
	client    *redis.Client
	enabled   bool
	maxSize   int
	keyPrefix string
	mu        sync.Mutex
	ctx       context.Context
}

func NewRedisDLQ(client *redis.Client, keyPrefix string, enabled bool, maxSize int) *RedisDLQ {
	if !enabled || client == nil {
		return &RedisDLQ{
			enabled: false,
			ctx:     context.Background(),
		}
	}

	return &RedisDLQ{
		client:    client,
		enabled:   true,
		maxSize:   maxSize,
		keyPrefix: keyPrefix,
		ctx:       context.Background(),
	}
}

func (rdlq *RedisDLQ) Add(entry DLQEntry) error {
	if !rdlq.enabled || rdlq.client == nil {
		return nil
	}

	rdlq.mu.Lock()
	defer rdlq.mu.Unlock()

	entries, err := rdlq.loadEntries()
	if err != nil {
		return fmt.Errorf("failed to load DLQ: %w", err)
	}

	entries = append(entries, entry)

	if rdlq.maxSize > 0 && len(entries) > rdlq.maxSize {
		entries = entries[len(entries)-rdlq.maxSize:]
	}

	return rdlq.saveEntries(entries)
}

func (rdlq *RedisDLQ) GetAll() ([]DLQEntry, error) {
	if !rdlq.enabled || rdlq.client == nil {
		return nil, nil
	}

	rdlq.mu.Lock()
	defer rdlq.mu.Unlock()

	return rdlq.loadEntries()
}

func (rdlq *RedisDLQ) GetCount() (int, error) {
	entries, err := rdlq.GetAll()
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

func (rdlq *RedisDLQ) Clear() error {
	if !rdlq.enabled || rdlq.client == nil {
		return nil
	}

	rdlq.mu.Lock()
	defer rdlq.mu.Unlock()

	return rdlq.saveEntries([]DLQEntry{})
}

func (rdlq *RedisDLQ) Replay(limit int) ([]ForwardRequest, error) {
	if !rdlq.enabled || rdlq.client == nil {
		return nil, nil
	}

	rdlq.mu.Lock()
	defer rdlq.mu.Unlock()

	entries, err := rdlq.loadEntries()
	if err != nil {
		return nil, err
	}

	if limit > 0 && limit < len(entries) {
		entries = entries[:limit]
	}

	requests := make([]ForwardRequest, len(entries))
	for i, entry := range entries {
		req := entry.Request
		req.RetryCount = 0
		req.ReceivedAt = time.Now()
		requests[i] = req
	}

	remaining := []DLQEntry{}
	if limit > 0 && limit < len(entries) {
		remaining = entries[limit:]
	}
	if err := rdlq.saveEntries(remaining); err != nil {
		return nil, err
	}

	return requests, nil
}

func (rdlq *RedisDLQ) loadEntries() ([]DLQEntry, error) {
	key := rdlq.keyPrefix + ":dlq"
	data, err := rdlq.client.Get(rdlq.ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return []DLQEntry{}, nil
		}
		return nil, err
	}

	var entries []DLQEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	return entries, nil
}

func (rdlq *RedisDLQ) saveEntries(entries []DLQEntry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}

	key := rdlq.keyPrefix + ":dlq"
	if len(entries) == 0 {
		return rdlq.client.Del(rdlq.ctx, key).Err()
	}
	return rdlq.client.Set(rdlq.ctx, key, data, 0).Err()
}

type ErrorRecord struct {
	Timestamp time.Time `json:"timestamp"`
	Method    string    `json:"method"`
	Path      string    `json:"path"`
	Error     string    `json:"error"`
	ErrorType string    `json:"error_type"`
	Attempts  int       `json:"attempts"`
}

type TimeWindow struct {
	Received  int64
	Forwarded int64
	Failed    int64
	Dropped   int64
	StartTime time.Time
}

type Metrics struct {
	StartTime         time.Time
	TotalReceived     int64
	TotalForwarded    int64
	TotalFailed       int64
	TotalDropped      int64
	TotalDeduplicated int64
	TotalBodyTooLarge int64
	TotalExpired      int64
	TotalDLQ          int64
	MemoryAlerts      int64
	LastForwardMs     int64
	AvgForwardMs      int64
	MaxForwardMs      int64
	MinForwardMs      int64
	forwardCount      int64
	forwardSumMs      int64

	LastSuccessTime time.Time
	LastFailureTime time.Time
	LastFailureMsg  string
	LastMemoryAlert time.Time

	RecentErrors    []ErrorRecord
	maxRecentErrors int

	Window1Min  TimeWindow
	Window5Min  TimeWindow
	Window15Min TimeWindow
	Window1Hour TimeWindow

	QueueWaitTimes []int64
	ForwardTimes   []int64
	maxHistorySize int

	CircuitBreakerState     CircuitBreakerState
	CircuitBreakerFailures  int64
	CircuitBreakerSuccesses int64
	CircuitBreakerTimeouts  int64

	PersistenceErrors int64
	PersistenceWrites int64
	PersistenceReads  int64

	mu sync.RWMutex
}

func (m *Metrics) RecordForward(duration time.Duration) {
	ms := duration.Milliseconds()
	atomic.AddInt64(&m.TotalForwarded, 1)
	atomic.StoreInt64(&m.LastForwardMs, ms)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastSuccessTime = time.Now()
	m.forwardCount++
	m.forwardSumMs += ms
	m.AvgForwardMs = m.forwardSumMs / m.forwardCount
	if ms > m.MaxForwardMs {
		m.MaxForwardMs = ms
	}
	if m.MinForwardMs == 0 || ms < m.MinForwardMs {
		m.MinForwardMs = ms
	}

	m.ForwardTimes = append(m.ForwardTimes, ms)
	if len(m.ForwardTimes) > m.maxHistorySize {
		m.ForwardTimes = m.ForwardTimes[1:]
	}
}

func (m *Metrics) RecordQueueWait(duration time.Duration) {
	ms := duration.Milliseconds()
	m.mu.Lock()
	defer m.mu.Unlock()

	m.QueueWaitTimes = append(m.QueueWaitTimes, ms)
	if len(m.QueueWaitTimes) > m.maxHistorySize {
		m.QueueWaitTimes = m.QueueWaitTimes[1:]
	}
}

func (m *Metrics) RecordError(method, path, errMsg, errType string, attempts int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastFailureTime = time.Now()
	m.LastFailureMsg = errMsg

	errRecord := ErrorRecord{
		Timestamp: time.Now(),
		Method:    method,
		Path:      path,
		Error:     errMsg,
		ErrorType: errType,
		Attempts:  attempts,
	}

	m.RecentErrors = append(m.RecentErrors, errRecord)
	if len(m.RecentErrors) > m.maxRecentErrors {
		m.RecentErrors = m.RecentErrors[1:]
	}
}

func (m *Metrics) UpdateTimeWindows() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	totalReceived := atomic.LoadInt64(&m.TotalReceived)
	totalForwarded := atomic.LoadInt64(&m.TotalForwarded)
	totalFailed := atomic.LoadInt64(&m.TotalFailed)
	totalDropped := atomic.LoadInt64(&m.TotalDropped)

	if now.Sub(m.Window1Min.StartTime) >= time.Minute {
		m.Window1Min = TimeWindow{
			Received:  totalReceived,
			Forwarded: totalForwarded,
			Failed:    totalFailed,
			Dropped:   totalDropped,
			StartTime: now,
		}
	}

	if now.Sub(m.Window5Min.StartTime) >= 5*time.Minute {
		m.Window5Min = TimeWindow{
			Received:  totalReceived,
			Forwarded: totalForwarded,
			Failed:    totalFailed,
			Dropped:   totalDropped,
			StartTime: now,
		}
	}

	if now.Sub(m.Window15Min.StartTime) >= 15*time.Minute {
		m.Window15Min = TimeWindow{
			Received:  totalReceived,
			Forwarded: totalForwarded,
			Failed:    totalFailed,
			Dropped:   totalDropped,
			StartTime: now,
		}
	}

	if now.Sub(m.Window1Hour.StartTime) >= time.Hour {
		m.Window1Hour = TimeWindow{
			Received:  totalReceived,
			Forwarded: totalForwarded,
			Failed:    totalFailed,
			Dropped:   totalDropped,
			StartTime: now,
		}
	}
}

func (m *Metrics) CalculatePercentile(values []int64, percentile float64) int64 {
	if len(values) == 0 {
		return 0
	}

	sorted := make([]int64, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	index := int(float64(len(sorted)) * percentile)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func (m *Metrics) GetThroughput(window TimeWindow) map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if window.StartTime.IsZero() {
		return map[string]float64{
			"received_per_sec":  0,
			"forwarded_per_sec": 0,
			"failed_per_sec":    0,
			"dropped_per_sec":   0,
		}
	}

	duration := time.Since(window.StartTime).Seconds()
	if duration == 0 {
		duration = 1
	}

	totalReceived := atomic.LoadInt64(&m.TotalReceived)
	totalForwarded := atomic.LoadInt64(&m.TotalForwarded)
	totalFailed := atomic.LoadInt64(&m.TotalFailed)
	totalDropped := atomic.LoadInt64(&m.TotalDropped)

	return map[string]float64{
		"received_per_sec":  float64(totalReceived-window.Received) / duration,
		"forwarded_per_sec": float64(totalForwarded-window.Forwarded) / duration,
		"failed_per_sec":    float64(totalFailed-window.Failed) / duration,
		"dropped_per_sec":   float64(totalDropped-window.Dropped) / duration,
	}
}

func formatWithCommas(n int64) string {
	str := strconv.FormatInt(n, 10)
	if len(str) <= 3 {
		return str
	}
	var result []byte
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func formatUptime(d time.Duration) string {
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm %ds", days, hours, minutes, seconds)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

func getThroughputStatus(incoming, processing float64) string {
	if incoming == 0 {
		return "idle"
	}
	if processing >= incoming*0.95 {
		return "keeping up"
	}
	if processing >= incoming*0.7 {
		return "falling behind"
	}
	return "critically behind"
}

func getStatusIcon(successRate float64, queueUtilization float64) string {
	if successRate >= 99.0 && queueUtilization < 0.8 {
		return "🟢"
	}
	if successRate >= 95.0 && queueUtilization < 0.95 {
		return "🟡"
	}
	return "🔴"
}

func (m *Metrics) GetHealthStatus(queueSize, queueCap int) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	queueUtilization := float64(queueSize) / float64(queueCap)
	totalReceived := atomic.LoadInt64(&m.TotalReceived)
	totalFailed := atomic.LoadInt64(&m.TotalFailed)

	var errorRate float64
	if totalReceived > 0 {
		errorRate = float64(totalFailed) / float64(totalReceived)
	}

	if queueUtilization >= 0.95 || errorRate >= 0.5 {
		return "fail"
	}

	if queueUtilization >= 0.8 || errorRate >= 0.2 {
		return "warn"
	}

	return "pass"
}

type CallbackProxy struct {
	config         Config
	client         *http.Client
	queue          chan ForwardRequest
	retryQueue     chan ForwardRequest
	wg             sync.WaitGroup
	shutdownCh     chan struct{}
	metrics        *Metrics
	dedupeCache    *DedupeCache
	circuitBreaker *CircuitBreaker
	persistence    Persistence
	dlq            DLQ
}

func NewCallbackProxy(config Config) *CallbackProxy {
	now := time.Now()

	var dedupeCache *DedupeCache
	if config.EnableDedupe {
		dedupeCache = NewDedupeCache(config.DedupeWindow)
	}

	var circuitBreaker *CircuitBreaker
	if config.CircuitBreakerEnabled {
		circuitBreaker = NewCircuitBreaker(config.CircuitBreakerThreshold, config.CircuitBreakerTimeout)
	}

	var persistence Persistence
	var redisClient *redis.Client
	if config.RedisAddr != "" {
		redisPersistence := NewRedisPersistence(config.RedisAddr, config.RedisPassword, config.RedisDB, config.RedisKeyPrefix, config.EnablePersistence)
		persistence = redisPersistence
		if redisPersistence.enabled && redisPersistence.client != nil {
			redisClient = redisPersistence.client
		}
	} else {
		persistence = NewQueuePersistence(config.PersistencePath, config.EnablePersistence)
	}

	var dlq DLQ
	if redisClient != nil {
		dlq = NewRedisDLQ(redisClient, config.RedisKeyPrefix, config.EnableDLQ, config.DLQMaxSize)
		log.Printf("Using Redis DLQ (prefix: %s)", config.RedisKeyPrefix)
	} else {
		dlq = NewDeadLetterQueue(config.DLQPath, config.EnableDLQ, config.DLQMaxSize)
		log.Printf("Using file-based DLQ (path: %s)", config.DLQPath)
	}

	retryQueueSize := config.RetryQueueSize
	if retryQueueSize == 0 {
		retryQueueSize = config.QueueSize / 2
	}

	cp := &CallbackProxy{
		config: config,
		client: &http.Client{
			Timeout: config.ForwardTimeout,
		},
		queue:          make(chan ForwardRequest, config.QueueSize),
		retryQueue:     make(chan ForwardRequest, retryQueueSize),
		shutdownCh:     make(chan struct{}),
		dedupeCache:    dedupeCache,
		circuitBreaker: circuitBreaker,
		persistence:    persistence,
		dlq:            dlq,
		metrics: &Metrics{
			StartTime:       now,
			maxRecentErrors: 100,
			maxHistorySize:  1000,
			Window1Min:      TimeWindow{StartTime: now},
			Window5Min:      TimeWindow{StartTime: now},
			Window15Min:     TimeWindow{StartTime: now},
			Window1Hour:     TimeWindow{StartTime: now},
		},
	}

	if config.EnablePersistence {
		if requests, err := persistence.Load(); err == nil && len(requests) > 0 {
			log.Printf("Loaded %d requests from persistence", len(requests))
			go func() {
				for _, req := range requests {
					select {
					case cp.queue <- req:
						atomic.AddInt64(&cp.metrics.PersistenceReads, 1)
					default:
						log.Printf("Queue full, cannot restore persisted request")
						break
					}
				}
				persistence.Clear()
			}()
		} else if err != nil {
			log.Printf("Failed to load persisted queue: %v", err)
			atomic.AddInt64(&cp.metrics.PersistenceErrors, 1)
		}
	}

	return cp
}

func (cp *CallbackProxy) Start() {
	for i := 0; i < cp.config.WorkerCount; i++ {
		cp.wg.Add(1)
		go cp.worker(i)
	}

	retryWorkerCount := cp.config.RetryWorkerCount
	if retryWorkerCount == 0 {
		retryWorkerCount = cp.config.WorkerCount / 2
		if retryWorkerCount < 1 {
			retryWorkerCount = 1
		}
	}
	for i := 0; i < retryWorkerCount; i++ {
		cp.wg.Add(1)
		go cp.retryWorker(i)
	}

	cp.wg.Add(1)
	go cp.metricsUpdater()

	if cp.config.EnableDedupe {
		cp.wg.Add(1)
		go cp.dedupeCleanup()
	}

	if cp.config.EnablePersistence {
		cp.wg.Add(1)
		go cp.persistenceWorker()
	}

	if cp.config.MemoryAlertThreshold > 0 {
		cp.wg.Add(1)
		go cp.memoryMonitor()
	}

	log.Printf("Started %d forwarding workers and %d retry workers", cp.config.WorkerCount, retryWorkerCount)
}

func (cp *CallbackProxy) metricsUpdater() {
	defer cp.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cp.metrics.UpdateTimeWindows()
		case <-cp.shutdownCh:
			return
		}
	}
}

func (cp *CallbackProxy) dedupeCleanup() {
	defer cp.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cp.dedupeCache.Cleanup()
		case <-cp.shutdownCh:
			return
		}
	}
}

func (cp *CallbackProxy) persistenceWorker() {
	defer cp.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cp.saveQueue()
		case <-cp.shutdownCh:
			cp.saveQueue()
			return
		}
	}
}

func (cp *CallbackProxy) saveQueue() {
	queueSize := len(cp.queue)
	if queueSize == 0 {
		return
	}

	requests := make([]ForwardRequest, 0, queueSize)
	for i := 0; i < queueSize; i++ {
		select {
		case req := <-cp.queue:
			requests = append(requests, req)
			cp.queue <- req
		default:
			break
		}
	}

	if len(requests) > 0 {
		if err := cp.persistence.Save(requests); err != nil {
			log.Printf("Failed to persist queue: %v", err)
			atomic.AddInt64(&cp.metrics.PersistenceErrors, 1)
		} else {
			atomic.AddInt64(&cp.metrics.PersistenceWrites, 1)
		}
	}
}

func (cp *CallbackProxy) memoryMonitor() {
	defer cp.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			if int64(m.Alloc) > cp.config.MemoryAlertThreshold {
				cp.metrics.mu.Lock()
				cp.metrics.LastMemoryAlert = time.Now()
				cp.metrics.mu.Unlock()
				atomic.AddInt64(&cp.metrics.MemoryAlerts, 1)

				log.Printf("MEMORY ALERT: Current allocation %d MB exceeds threshold %d MB",
					m.Alloc/1024/1024, cp.config.MemoryAlertThreshold/1024/1024)
			}
		case <-cp.shutdownCh:
			return
		}
	}
}

func (cp *CallbackProxy) Stop() {
	close(cp.shutdownCh)
	close(cp.queue)
	close(cp.retryQueue)
	cp.wg.Wait()

	if rp, ok := cp.persistence.(*RedisPersistence); ok {
		if err := rp.Close(); err != nil {
			log.Printf("Error closing Redis connection: %v", err)
		}
	}

	log.Println("All workers stopped")
}

func (cp *CallbackProxy) worker(id int) {
	defer cp.wg.Done()
	log.Printf("Worker %d started", id)

	for req := range cp.queue {
		if !req.ExpiresAt.IsZero() && time.Now().After(req.ExpiresAt) {
			atomic.AddInt64(&cp.metrics.TotalExpired, 1)
			log.Printf("Request expired: %s %s (age: %s)", req.Method, req.Path, time.Since(req.ReceivedAt))
			continue
		}
		cp.processRequest(req, false)
	}

	log.Printf("Worker %d stopped", id)
}

func (cp *CallbackProxy) retryWorker(id int) {
	defer cp.wg.Done()
	log.Printf("Retry worker %d started", id)

	for req := range cp.retryQueue {
		if !req.ExpiresAt.IsZero() && time.Now().After(req.ExpiresAt) {
			atomic.AddInt64(&cp.metrics.TotalExpired, 1)
			log.Printf("Retry request expired: %s %s (age: %s)", req.Method, req.Path, time.Since(req.ReceivedAt))
			continue
		}
		cp.processRequest(req, true)
	}

	log.Printf("Retry worker %d stopped", id)
}

func (cp *CallbackProxy) processRequest(fwdReq ForwardRequest, isRetry bool) {
	start := time.Now()
	err := cp.forward(fwdReq)
	forwardDuration := time.Since(start)
	queueWait := start.Sub(fwdReq.ReceivedAt)

	if err == nil {
		cp.metrics.RecordForward(forwardDuration)
		cp.metrics.RecordQueueWait(queueWait)

		// Log full payload for reconciliation
		payload := string(fwdReq.Body)

		log.Printf("Successfully forwarded: %s %s (queue_wait: %dms, forward: %dms, retry_count: %d) payload: %s",
			fwdReq.Method, fwdReq.Path, queueWait.Milliseconds(), forwardDuration.Milliseconds(), fwdReq.RetryCount, payload)
		return
	}

	errType := categorizeError(err)
	fwdReq.RetryCount++

	if fwdReq.RetryCount <= cp.config.MaxRetries {
		select {
		case cp.retryQueue <- fwdReq:
			log.Printf("Queued for retry (%d/%d): %s %s - %s", fwdReq.RetryCount, cp.config.MaxRetries, fwdReq.Method, fwdReq.Path, err.Error())
		default:
			log.Printf("Retry queue full, sending to DLQ: %s %s", fwdReq.Method, fwdReq.Path)
			cp.sendToDLQ(fwdReq, err.Error(), errType, fwdReq.RetryCount)
		}
		return
	}

	atomic.AddInt64(&cp.metrics.TotalFailed, 1)
	cp.metrics.RecordError(fwdReq.Method, fwdReq.Path, err.Error(), errType, fwdReq.RetryCount)
	cp.sendToDLQ(fwdReq, err.Error(), errType, fwdReq.RetryCount)
	log.Printf("Request failed after %d attempts: %s %s - %s", fwdReq.RetryCount, fwdReq.Method, fwdReq.Path, err.Error())
}

func (cp *CallbackProxy) sendToDLQ(req ForwardRequest, reason string, errType string, attempts int) {
	if cp.dlq == nil || !cp.config.EnableDLQ {
		return
	}

	entry := DLQEntry{
		Request:       req,
		FailureReason: reason,
		FailureTime:   time.Now(),
		ErrorType:     errType,
		AttemptCount:  attempts,
	}

	if err := cp.dlq.Add(entry); err != nil {
		log.Printf("Failed to add to DLQ: %v", err)
	} else {
		atomic.AddInt64(&cp.metrics.TotalDLQ, 1)
		log.Printf("Added to DLQ: %s %s (attempts: %d, reason: %s)", req.Method, req.Path, attempts, reason)
	}
}

func (cp *CallbackProxy) forwardWithRetry(fwdReq ForwardRequest) {
	var lastErr error
	var errType string

	for attempt := 0; attempt <= cp.config.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(cp.config.RetryDelay * time.Duration(attempt))
			log.Printf("Retry attempt %d for %s %s", attempt, fwdReq.Method, fwdReq.Path)
		}

		start := time.Now()
		err := cp.forward(fwdReq)
		forwardDuration := time.Since(start)
		queueWait := start.Sub(fwdReq.ReceivedAt)
		if err == nil {
			cp.metrics.RecordForward(forwardDuration)
			cp.metrics.RecordQueueWait(queueWait)
			log.Printf("Successfully forwarded: %s %s (queue_wait: %dms, forward: %dms)",
				fwdReq.Method, fwdReq.Path, queueWait.Milliseconds(), forwardDuration.Milliseconds())
			return
		}
		lastErr = err
		errType = categorizeError(err)
	}

	atomic.AddInt64(&cp.metrics.TotalFailed, 1)
	cp.metrics.RecordError(fwdReq.Method, fwdReq.Path, lastErr.Error(), errType, cp.config.MaxRetries+1)
	log.Printf("Failed to forward after %d retries: %s %s - Error: %v",
		cp.config.MaxRetries, fwdReq.Method, fwdReq.Path, lastErr)
}

func categorizeError(err error) string {
	if err == nil {
		return "none"
	}

	errMsg := err.Error()
	if contains(errMsg, "timeout") || contains(errMsg, "deadline exceeded") {
		return "timeout"
	}
	if contains(errMsg, "connection refused") || contains(errMsg, "no such host") {
		return "network"
	}
	if contains(errMsg, "status 4") {
		return "client_error_4xx"
	}
	if contains(errMsg, "status 5") {
		return "server_error_5xx"
	}
	return "other"
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func (cp *CallbackProxy) forward(fwdReq ForwardRequest) error {
	forwardFunc := func() error {
		targetURL := cp.config.ForwardURL + fwdReq.Path
		if fwdReq.Query != "" {
			targetURL += "?" + fwdReq.Query
		}

		req, err := http.NewRequest(fwdReq.Method, targetURL, bytes.NewReader(fwdReq.Body))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		for key, values := range fwdReq.Headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}

		req.Header.Del("Host")
		req.Header.Set("X-Forwarded-By", "glo-traffic-proxy")

		resp, err := cp.client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to forward request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			return fmt.Errorf("forward target returned status %d: %s", resp.StatusCode, string(body))
		}

		return nil
	}

	if cp.config.CircuitBreakerEnabled && cp.circuitBreaker != nil {
		err := cp.circuitBreaker.Call(forwardFunc)
		if err != nil {
			if strings.Contains(err.Error(), "circuit breaker is open") {
				atomic.AddInt64(&cp.metrics.CircuitBreakerTimeouts, 1)
			}
		}
		return err
	}

	return forwardFunc()
}

func (cp *CallbackProxy) HandleCallback(w http.ResponseWriter, r *http.Request) {
	requestStart := time.Now()

	var bodyReader io.Reader = r.Body
	if cp.config.MaxBodySize > 0 {
		bodyReader = io.LimitReader(r.Body, cp.config.MaxBodySize)
	}

	body, err := io.ReadAll(bodyReader)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if cp.config.MaxBodySize > 0 && int64(len(body)) >= cp.config.MaxBodySize {
		atomic.AddInt64(&cp.metrics.TotalBodyTooLarge, 1)
		log.Printf("Request body too large: %s %s (%d bytes)", r.Method, r.URL.Path, len(body))
		http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	receivedAt := time.Now()

	requestID := cp.generateRequestID(r, body)

	if cp.config.EnableDedupe && cp.dedupeCache != nil {
		if cp.dedupeCache.IsDuplicate(requestID) {
			atomic.AddInt64(&cp.metrics.TotalDeduplicated, 1)
			log.Printf("Duplicate request detected: %s %s (ID: %s)", r.Method, r.URL.Path, requestID[:16])
			w.WriteHeader(http.StatusNoContent)
			return
		}
		cp.dedupeCache.Add(requestID)
	}

	var expiresAt time.Time
	if cp.config.RequestTTL > 0 {
		expiresAt = receivedAt.Add(cp.config.RequestTTL)
	}

	fwdReq := ForwardRequest{
		Method:     r.Method,
		Path:       r.URL.Path,
		Headers:    r.Header.Clone(),
		Body:       body,
		Query:      r.URL.RawQuery,
		ReceivedAt: receivedAt,
		ExpiresAt:  expiresAt,
		RequestID:  requestID,
		RetryCount: 0,
	}

	select {
	case cp.queue <- fwdReq:
		atomic.AddInt64(&cp.metrics.TotalReceived, 1)
		responseTime := time.Since(requestStart)

		// Log full payload for reconciliation
		payload := string(body)

		log.Printf("Queued request: %s %s (queue size: %d, response_time: %dµs) payload: %s",
			r.Method, r.URL.Path, len(cp.queue), responseTime.Microseconds(), payload)
	default:
		atomic.AddInt64(&cp.metrics.TotalDropped, 1)
		log.Printf("Queue full, dropping request: %s %s", r.Method, r.URL.Path)
		http.Error(w, "Server busy", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (cp *CallbackProxy) generateRequestID(r *http.Request, body []byte) string {
	if reqID := r.Header.Get("X-Request-ID"); reqID != "" {
		return reqID
	}
	if reqID := r.Header.Get("X-Idempotency-Key"); reqID != "" {
		return reqID
	}
	if reqID := r.Header.Get("X-Webhook-ID"); reqID != "" {
		return reqID
	}

	h := sha256.New()
	h.Write([]byte(r.Method))
	h.Write([]byte(r.URL.Path))
	h.Write([]byte(r.URL.RawQuery))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func (cp *CallbackProxy) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	cp.metrics.mu.RLock()
	defer cp.metrics.mu.RUnlock()

	queueSize := len(cp.queue)
	queueCap := cap(cp.queue)
	retryQueueSize := len(cp.retryQueue)
	retryQueueCap := cap(cp.retryQueue)
	uptime := time.Since(cp.metrics.StartTime)
	queueUtilization := float64(queueSize) / float64(queueCap)
	if queueCap == 0 {
		queueUtilization = 0
	}

	totalReceived := atomic.LoadInt64(&cp.metrics.TotalReceived)
	totalForwarded := atomic.LoadInt64(&cp.metrics.TotalForwarded)
	totalFailed := atomic.LoadInt64(&cp.metrics.TotalFailed)
	totalDropped := atomic.LoadInt64(&cp.metrics.TotalDropped)
	persistenceWrites := atomic.LoadInt64(&cp.metrics.PersistenceWrites)

	var successRate float64
	if totalReceived > 0 {
		successRate = float64(totalForwarded) / float64(totalReceived) * 100
	}

	avgLatency := atomic.LoadInt64(&cp.metrics.AvgForwardMs)
	minLatency := atomic.LoadInt64(&cp.metrics.MinForwardMs)
	maxLatency := atomic.LoadInt64(&cp.metrics.MaxForwardMs)

	p50 := cp.metrics.CalculatePercentile(cp.metrics.QueueWaitTimes, 0.50)
	p95 := cp.metrics.CalculatePercentile(cp.metrics.QueueWaitTimes, 0.95)
	p99 := cp.metrics.CalculatePercentile(cp.metrics.QueueWaitTimes, 0.99)

	throughput1Min := cp.metrics.GetThroughput(cp.metrics.Window1Min)
	incomingRate := throughput1Min["received_per_sec"]
	processingRate := throughput1Min["forwarded_per_sec"]
	lifetimeRate := float64(totalForwarded) / uptime.Seconds()

	throughputStatus := getThroughputStatus(incomingRate, processingRate)
	statusIcon := getStatusIcon(successRate, queueUtilization)

	var circuitBreakerState string
	if cp.circuitBreaker != nil {
		circuitBreakerState = cp.circuitBreaker.GetStateString()
	} else {
		circuitBreakerState = "closed"
	}

	queueUsage := "0.0%"
	if queueCap > 0 {
		queueUsage = fmt.Sprintf("%.1f%%", queueUtilization*100)
	} else {
		queueUsage = "NaN%"
	}

	overallStatus := cp.metrics.GetHealthStatus(queueSize, queueCap)

	w.Header().Set("Content-Type", "application/json")
	if overallStatus == "fail" {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	response := map[string]interface{}{
		"apps": map[string]interface{}{
			"GLO": map[string]interface{}{
				"avg_latency":     fmt.Sprintf("%dms", avgLatency),
				"circuit_breaker": circuitBreakerState,
				"dropped":         formatWithCommas(totalDropped),
				"expired":         "0",
				"failed":          formatWithCommas(totalFailed),
				"forwarded":       formatWithCommas(totalForwarded),
				"persisted":       formatWithCommas(persistenceWrites),
				"queue": map[string]interface{}{
					"capacity": queueCap,
					"size":     queueSize,
					"usage":    queueUsage,
				},
				"queue_wait_ms": map[string]interface{}{
					"p50": p50,
					"p95": p95,
					"p99": p99,
				},
				"received": formatWithCommas(totalReceived),
				"status":   statusIcon,
				"success":  fmt.Sprintf("%.1f%%", successRate),
				"throughput": map[string]interface{}{
					"incoming":   fmt.Sprintf("%.1f/sec", incomingRate),
					"lifetime":   fmt.Sprintf("%.1f/sec", lifetimeRate),
					"processing": fmt.Sprintf("%.1f/sec", processingRate),
					"status":     throughputStatus,
				},
			},
		},
		"icon": statusIcon,
		"persistent_queue": map[string]interface{}{
			"current_depth":    persistenceWrites,
			"expired_this_run": 0,
			"gauge_in_memory":  persistenceWrites,
			"redis_status":     cp.getRedisStatus(),
			"total_expired":    0,
			"ttl":              "48h0m0s",
		},
		"queue": map[string]interface{}{
			"retry_queue": fmt.Sprintf("%d/%d", retryQueueSize, retryQueueCap),
			"total":       fmt.Sprintf("%d/%d (%s)", queueSize, queueCap, queueUsage),
		},
		"started_at": cp.metrics.StartTime.Format(time.RFC3339),
		"status":     overallStatus,
		"traffic": map[string]interface{}{
			"dropped":   formatWithCommas(totalDropped),
			"expired":   "0",
			"failed":    formatWithCommas(totalFailed),
			"forwarded": formatWithCommas(totalForwarded),
			"latency_ms": map[string]interface{}{
				"avg": fmt.Sprintf("%dms", avgLatency),
				"max": fmt.Sprintf("%dms", maxLatency),
				"min": fmt.Sprintf("%dms", minLatency),
			},
			"persisted": formatWithCommas(persistenceWrites),
			"queue_wait_ms": map[string]interface{}{
				"p50": p50,
				"p95": p95,
				"p99": p99,
			},
			"received": formatWithCommas(totalReceived),
			"success":  fmt.Sprintf("%.1f%%", successRate),
			"throughput": map[string]interface{}{
				"incoming":   fmt.Sprintf("%.1f/sec", incomingRate),
				"lifetime":   fmt.Sprintf("%.1f/sec", lifetimeRate),
				"processing": fmt.Sprintf("%.1f/sec", processingRate),
				"status":     throughputStatus,
			},
		},
		"uptime":  formatUptime(uptime),
		"workers": cp.config.WorkerCount,
	}

	json.NewEncoder(w).Encode(response)
}

func (cp *CallbackProxy) HealthDetailed(w http.ResponseWriter, r *http.Request) {
	cp.metrics.mu.RLock()
	defer cp.metrics.mu.RUnlock()

	queueSize := len(cp.queue)
	queueCap := cap(cp.queue)
	status := cp.metrics.GetHealthStatus(queueSize, queueCap)
	uptime := time.Since(cp.metrics.StartTime)
	queueUtilization := float64(queueSize) / float64(queueCap)

	totalReceived := atomic.LoadInt64(&cp.metrics.TotalReceived)
	totalForwarded := atomic.LoadInt64(&cp.metrics.TotalForwarded)
	totalFailed := atomic.LoadInt64(&cp.metrics.TotalFailed)
	totalDropped := atomic.LoadInt64(&cp.metrics.TotalDropped)

	var successRate, errorRate float64
	if totalReceived > 0 {
		successRate = float64(totalForwarded) / float64(totalReceived) * 100
		errorRate = float64(totalFailed) / float64(totalReceived) * 100
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	w.Header().Set("Content-Type", "application/json")
	if status == "fail" {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	response := map[string]interface{}{
		"status":         status,
		"uptime_seconds": int64(uptime.Seconds()),
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"queue": map[string]interface{}{
			"size":        queueSize,
			"capacity":    queueCap,
			"utilization": fmt.Sprintf("%.2f%%", queueUtilization*100),
		},
		"counters": map[string]interface{}{
			"total_received":       totalReceived,
			"total_forwarded":      totalForwarded,
			"total_failed":         totalFailed,
			"total_dropped":        totalDropped,
			"total_deduplicated":   atomic.LoadInt64(&cp.metrics.TotalDeduplicated),
			"total_body_too_large": atomic.LoadInt64(&cp.metrics.TotalBodyTooLarge),
			"success_rate":         fmt.Sprintf("%.2f%%", successRate),
			"error_rate":           fmt.Sprintf("%.2f%%", errorRate),
		},
		"latency": map[string]interface{}{
			"forward": map[string]interface{}{
				"last_ms": atomic.LoadInt64(&cp.metrics.LastForwardMs),
				"avg_ms":  cp.metrics.AvgForwardMs,
				"min_ms":  cp.metrics.MinForwardMs,
				"max_ms":  cp.metrics.MaxForwardMs,
				"p50_ms":  cp.metrics.CalculatePercentile(cp.metrics.ForwardTimes, 0.50),
				"p95_ms":  cp.metrics.CalculatePercentile(cp.metrics.ForwardTimes, 0.95),
				"p99_ms":  cp.metrics.CalculatePercentile(cp.metrics.ForwardTimes, 0.99),
			},
			"queue_wait": map[string]interface{}{
				"avg_ms": calculateAvg(cp.metrics.QueueWaitTimes),
				"min_ms": calculateMin(cp.metrics.QueueWaitTimes),
				"max_ms": calculateMax(cp.metrics.QueueWaitTimes),
				"p50_ms": cp.metrics.CalculatePercentile(cp.metrics.QueueWaitTimes, 0.50),
				"p95_ms": cp.metrics.CalculatePercentile(cp.metrics.QueueWaitTimes, 0.95),
				"p99_ms": cp.metrics.CalculatePercentile(cp.metrics.QueueWaitTimes, 0.99),
			},
		},
		"throughput": map[string]interface{}{
			"1min":  cp.metrics.GetThroughput(cp.metrics.Window1Min),
			"5min":  cp.metrics.GetThroughput(cp.metrics.Window5Min),
			"15min": cp.metrics.GetThroughput(cp.metrics.Window15Min),
			"1hour": cp.metrics.GetThroughput(cp.metrics.Window1Hour),
		},
		"resources": map[string]interface{}{
			"goroutines": runtime.NumGoroutine(),
			"memory": map[string]interface{}{
				"alloc_bytes":       m.Alloc,
				"alloc_mb":          fmt.Sprintf("%.2f", float64(m.Alloc)/1024/1024),
				"total_alloc_bytes": m.TotalAlloc,
				"total_alloc_mb":    fmt.Sprintf("%.2f", float64(m.TotalAlloc)/1024/1024),
				"sys_bytes":         m.Sys,
				"sys_mb":            fmt.Sprintf("%.2f", float64(m.Sys)/1024/1024),
				"num_gc":            m.NumGC,
				"gc_pause_ns":       m.PauseNs[(m.NumGC+255)%256],
			},
		},
		"workers": map[string]interface{}{
			"count": cp.config.WorkerCount,
		},
		"configuration": map[string]interface{}{
			"forward_url":     cp.config.ForwardURL,
			"forward_timeout": cp.config.ForwardTimeout.String(),
			"max_retries":     cp.config.MaxRetries,
			"retry_delay":     cp.config.RetryDelay.String(),
			"worker_count":    cp.config.WorkerCount,
			"queue_size":      cp.config.QueueSize,
		},
	}

	if !cp.metrics.LastSuccessTime.IsZero() {
		response["last_success"] = cp.metrics.LastSuccessTime.UTC().Format(time.RFC3339)
	}

	if !cp.metrics.LastFailureTime.IsZero() {
		response["last_failure"] = map[string]interface{}{
			"timestamp": cp.metrics.LastFailureTime.UTC().Format(time.RFC3339),
			"message":   cp.metrics.LastFailureMsg,
		}
	}

	if len(cp.metrics.RecentErrors) > 0 {
		errorBreakdown := make(map[string]int)
		for _, err := range cp.metrics.RecentErrors {
			errorBreakdown[err.ErrorType]++
		}
		response["error_breakdown"] = errorBreakdown
		response["recent_errors"] = cp.metrics.RecentErrors[max(0, len(cp.metrics.RecentErrors)-10):]
	}

	if cp.config.CircuitBreakerEnabled && cp.circuitBreaker != nil {
		response["circuit_breaker"] = map[string]interface{}{
			"state":    cp.circuitBreaker.GetStateString(),
			"failures": cp.circuitBreaker.GetFailureCount(),
			"timeouts": atomic.LoadInt64(&cp.metrics.CircuitBreakerTimeouts),
		}
	}

	if cp.config.EnableDedupe && cp.dedupeCache != nil {
		response["deduplication"] = map[string]interface{}{
			"enabled":     true,
			"cache_size":  cp.dedupeCache.Size(),
			"window":      cp.config.DedupeWindow.String(),
			"total_dupes": atomic.LoadInt64(&cp.metrics.TotalDeduplicated),
		}
	}

	if cp.config.EnablePersistence {
		persistenceInfo := map[string]interface{}{
			"enabled": true,
			"writes":  atomic.LoadInt64(&cp.metrics.PersistenceWrites),
			"reads":   atomic.LoadInt64(&cp.metrics.PersistenceReads),
			"errors":  atomic.LoadInt64(&cp.metrics.PersistenceErrors),
		}
		if cp.config.RedisAddr != "" {
			persistenceInfo["type"] = "redis"
			persistenceInfo["redis_addr"] = cp.config.RedisAddr
			persistenceInfo["redis_db"] = cp.config.RedisDB
		} else {
			persistenceInfo["type"] = "file"
			persistenceInfo["path"] = cp.config.PersistencePath
		}
		response["persistence"] = persistenceInfo
	}

	if cp.config.MemoryAlertThreshold > 0 {
		response["memory_monitoring"] = map[string]interface{}{
			"threshold_mb": cp.config.MemoryAlertThreshold / 1024 / 1024,
			"current_mb":   m.Alloc / 1024 / 1024,
			"alert_count":  atomic.LoadInt64(&cp.metrics.MemoryAlerts),
			"last_alert":   cp.metrics.LastMemoryAlert,
		}
	}

	json.NewEncoder(w).Encode(response)
}

func (cp *CallbackProxy) HealthLive(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (cp *CallbackProxy) HealthReady(w http.ResponseWriter, r *http.Request) {
	queueSize := len(cp.queue)
	queueCap := cap(cp.queue)
	status := cp.metrics.GetHealthStatus(queueSize, queueCap)

	if status == "fail" {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("NOT READY"))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("READY"))
}

func (cp *CallbackProxy) MetricsPrometheus(w http.ResponseWriter, r *http.Request) {
	cp.metrics.mu.RLock()
	defer cp.metrics.mu.RUnlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)

	totalReceived := atomic.LoadInt64(&cp.metrics.TotalReceived)
	totalForwarded := atomic.LoadInt64(&cp.metrics.TotalForwarded)
	totalFailed := atomic.LoadInt64(&cp.metrics.TotalFailed)
	totalDropped := atomic.LoadInt64(&cp.metrics.TotalDropped)
	queueSize := len(cp.queue)
	uptime := time.Since(cp.metrics.StartTime).Seconds()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Fprintf(w, "# HELP glo_traffic_requests_total Total number of requests received\n")
	fmt.Fprintf(w, "# TYPE glo_traffic_requests_total counter\n")
	fmt.Fprintf(w, "glo_traffic_requests_total{status=\"received\"} %d\n", totalReceived)
	fmt.Fprintf(w, "glo_traffic_requests_total{status=\"forwarded\"} %d\n", totalForwarded)
	fmt.Fprintf(w, "glo_traffic_requests_total{status=\"failed\"} %d\n", totalFailed)
	fmt.Fprintf(w, "glo_traffic_requests_total{status=\"dropped\"} %d\n", totalDropped)

	fmt.Fprintf(w, "\n# HELP glo_traffic_queue_size Current queue size\n")
	fmt.Fprintf(w, "# TYPE glo_traffic_queue_size gauge\n")
	fmt.Fprintf(w, "glo_traffic_queue_size %d\n", queueSize)

	fmt.Fprintf(w, "\n# HELP glo_traffic_queue_capacity Queue capacity\n")
	fmt.Fprintf(w, "# TYPE glo_traffic_queue_capacity gauge\n")
	fmt.Fprintf(w, "glo_traffic_queue_capacity %d\n", cap(cp.queue))

	fmt.Fprintf(w, "\n# HELP glo_traffic_forward_duration_ms Forward request duration in milliseconds\n")
	fmt.Fprintf(w, "# TYPE glo_traffic_forward_duration_ms summary\n")
	fmt.Fprintf(w, "glo_traffic_forward_duration_ms{quantile=\"0.5\"} %d\n", cp.metrics.CalculatePercentile(cp.metrics.ForwardTimes, 0.50))
	fmt.Fprintf(w, "glo_traffic_forward_duration_ms{quantile=\"0.95\"} %d\n", cp.metrics.CalculatePercentile(cp.metrics.ForwardTimes, 0.95))
	fmt.Fprintf(w, "glo_traffic_forward_duration_ms{quantile=\"0.99\"} %d\n", cp.metrics.CalculatePercentile(cp.metrics.ForwardTimes, 0.99))
	fmt.Fprintf(w, "glo_traffic_forward_duration_ms_sum %d\n", cp.metrics.forwardSumMs)
	fmt.Fprintf(w, "glo_traffic_forward_duration_ms_count %d\n", cp.metrics.forwardCount)

	fmt.Fprintf(w, "\n# HELP glo_traffic_uptime_seconds Service uptime in seconds\n")
	fmt.Fprintf(w, "# TYPE glo_traffic_uptime_seconds gauge\n")
	fmt.Fprintf(w, "glo_traffic_uptime_seconds %.0f\n", uptime)

	fmt.Fprintf(w, "\n# HELP glo_traffic_goroutines Number of goroutines\n")
	fmt.Fprintf(w, "# TYPE glo_traffic_goroutines gauge\n")
	fmt.Fprintf(w, "glo_traffic_goroutines %d\n", runtime.NumGoroutine())

	fmt.Fprintf(w, "\n# HELP glo_traffic_memory_alloc_bytes Allocated memory in bytes\n")
	fmt.Fprintf(w, "# TYPE glo_traffic_memory_alloc_bytes gauge\n")
	fmt.Fprintf(w, "glo_traffic_memory_alloc_bytes %d\n", m.Alloc)

	fmt.Fprintf(w, "\n# HELP glo_traffic_gc_runs_total Total number of GC runs\n")
	fmt.Fprintf(w, "# TYPE glo_traffic_gc_runs_total counter\n")
	fmt.Fprintf(w, "glo_traffic_gc_runs_total %d\n", m.NumGC)
}

func calculateAvg(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	var sum int64
	for _, v := range values {
		sum += v
	}
	return sum / int64(len(values))
}

func calculateMin(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
	}
	return min
}

func calculateMax(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (cp *CallbackProxy) ClearQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cleared := 0
	for {
		select {
		case <-cp.queue:
			cleared++
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "cleared",
				"cleared": cleared,
			})
			log.Printf("Queue cleared: %d requests dropped", cleared)
			return
		}
	}
}

func (cp *CallbackProxy) ViewDLQ(w http.ResponseWriter, r *http.Request) {
	if !cp.config.EnableDLQ {
		http.Error(w, "DLQ not enabled", http.StatusNotFound)
		return
	}

	entries, err := cp.dlq.GetAll()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get DLQ entries: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"count":   len(entries),
		"entries": entries,
	})
}

func (cp *CallbackProxy) ReplayDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !cp.config.EnableDLQ {
		http.Error(w, "DLQ not enabled", http.StatusNotFound)
		return
	}

	limitStr := r.URL.Query().Get("limit")
	limit := 0
	if limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
			return
		}
	}

	requests, err := cp.dlq.Replay(limit)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to replay DLQ: %v", err), http.StatusInternalServerError)
		return
	}

	replayed := 0
	failed := 0
	for _, req := range requests {
		select {
		case cp.queue <- req:
			replayed++
		default:
			failed++
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "replayed",
		"replayed": replayed,
		"failed":   failed,
		"total":    len(requests),
	})
	log.Printf("DLQ replay: %d replayed, %d failed", replayed, failed)
}

func (cp *CallbackProxy) ClearDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !cp.config.EnableDLQ {
		http.Error(w, "DLQ not enabled", http.StatusNotFound)
		return
	}

	count, _ := cp.dlq.GetCount()
	if err := cp.dlq.Clear(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to clear DLQ: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "cleared",
		"cleared": count,
	})
	log.Printf("DLQ cleared: %d entries removed", count)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var i int
		if _, err := fmt.Sscanf(value, "%d", &i); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

func (cp *CallbackProxy) getRedisStatus() string {
	if cp.config.RedisAddr == "" {
		return "not_configured"
	}
	if rp, ok := cp.persistence.(*RedisPersistence); ok {
		if rp.enabled && rp.client != nil {
			if err := rp.client.Ping(rp.ctx).Err(); err == nil {
				return "connected"
			}
			return "disconnected"
		}
	}
	return "disabled"
}

func main() {
	config := Config{
		ListenAddr:              getEnv("LISTEN_ADDR", ":8080"),
		ForwardURL:              getEnv("FORWARD_URL", ""),
		ForwardTimeout:          getEnvDuration("FORWARD_TIMEOUT", 30*time.Second),
		MaxRetries:              getEnvInt("MAX_RETRIES", 3),
		RetryDelay:              getEnvDuration("RETRY_DELAY", 1*time.Second),
		WorkerCount:             getEnvInt("WORKER_COUNT", 10),
		QueueSize:               getEnvInt("QUEUE_SIZE", 10000),
		RetryQueueSize:          getEnvInt("RETRY_QUEUE_SIZE", 5000),
		RetryWorkerCount:        getEnvInt("RETRY_WORKER_COUNT", 5),
		MaxBodySize:             getEnvInt64("MAX_BODY_SIZE", 10*1024*1024),
		RequestTTL:              getEnvDuration("REQUEST_TTL", 5*time.Minute),
		EnableDedupe:            getEnvBool("ENABLE_DEDUPE", true),
		DedupeWindow:            getEnvDuration("DEDUPE_WINDOW", 5*time.Minute),
		EnablePersistence:       getEnvBool("ENABLE_PERSISTENCE", true),
		PersistencePath:         getEnv("PERSISTENCE_PATH", "/tmp/glo-traffic-queue.json"),
		RedisAddr:               getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:           getEnv("REDIS_PASSWORD", ""),
		RedisDB:                 getEnvInt("REDIS_DB", 0),
		RedisKeyPrefix:          getEnv("REDIS_KEY_PREFIX", "glo-traffic"),
		EnableDLQ:               getEnvBool("ENABLE_DLQ", true),
		DLQPath:                 getEnv("DLQ_PATH", "/tmp/glo-traffic-dlq.json"),
		DLQMaxSize:              getEnvInt("DLQ_MAX_SIZE", 10000),
		CircuitBreakerEnabled:   getEnvBool("CIRCUIT_BREAKER_ENABLED", true),
		CircuitBreakerThreshold: getEnvInt("CIRCUIT_BREAKER_THRESHOLD", 5),
		CircuitBreakerTimeout:   getEnvDuration("CIRCUIT_BREAKER_TIMEOUT", 60*time.Second),
		MemoryAlertThreshold:    getEnvInt64("MEMORY_ALERT_THRESHOLD", 500*1024*1024),
	}

	if config.ForwardURL == "" {
		log.Fatal("FORWARD_URL environment variable is required")
	}

	log.Printf("Configuration:")
	log.Printf("  Listen Address: %s", config.ListenAddr)
	log.Printf("  Forward URL: %s", config.ForwardURL)
	log.Printf("  Forward Timeout: %s", config.ForwardTimeout)
	log.Printf("  Max Retries: %d", config.MaxRetries)
	log.Printf("  Retry Delay: %s", config.RetryDelay)
	log.Printf("  Worker Count: %d", config.WorkerCount)
	log.Printf("  Retry Worker Count: %d", config.RetryWorkerCount)
	log.Printf("  Queue Size: %d", config.QueueSize)
	log.Printf("  Retry Queue Size: %d", config.RetryQueueSize)
	log.Printf("  Request TTL: %s", config.RequestTTL)
	log.Printf("  Max Body Size: %d MB", config.MaxBodySize/1024/1024)
	log.Printf("  Deduplication: %v (window: %s)", config.EnableDedupe, config.DedupeWindow)
	if config.RedisAddr != "" {
		log.Printf("  Persistence: %v (type: Redis, addr: %s, db: %d, prefix: %s)", config.EnablePersistence, config.RedisAddr, config.RedisDB, config.RedisKeyPrefix)
	} else {
		log.Printf("  Persistence: %v (type: File, path: %s)", config.EnablePersistence, config.PersistencePath)
	}
	log.Printf("  DLQ: %v (path: %s, max size: %d)", config.EnableDLQ, config.DLQPath, config.DLQMaxSize)
	log.Printf("  Circuit Breaker: %v (threshold: %d, timeout: %s)", config.CircuitBreakerEnabled, config.CircuitBreakerThreshold, config.CircuitBreakerTimeout)
	log.Printf("  Memory Alert Threshold: %d MB", config.MemoryAlertThreshold/1024/1024)

	proxy := NewCallbackProxy(config)
	proxy.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", proxy.HealthCheck)
	mux.HandleFunc("/health/detailed", proxy.HealthDetailed)
	mux.HandleFunc("/health/live", proxy.HealthLive)
	mux.HandleFunc("/health/ready", proxy.HealthReady)
	mux.HandleFunc("/metrics", proxy.MetricsPrometheus)
	mux.HandleFunc("/clear-queue", proxy.ClearQueue)
	mux.HandleFunc("/dlq", proxy.ViewDLQ)
	mux.HandleFunc("/dlq/replay", proxy.ReplayDLQ)
	mux.HandleFunc("/dlq/clear", proxy.ClearDLQ)
	mux.HandleFunc("/", proxy.HandleCallback)

	server := &http.Server{
		Addr:         config.ListenAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("Starting callback proxy server on %s", config.ListenAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	proxy.Stop()
	log.Println("Server exited")
}
