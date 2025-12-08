package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	ListenAddr     string
	ForwardURL     string
	ForwardTimeout time.Duration
	MaxRetries     int
	RetryDelay     time.Duration
	WorkerCount    int
	QueueSize      int
}

type ForwardRequest struct {
	Method  string
	Path    string
	Headers http.Header
	Body    []byte
	Query   string
}

type CallbackProxy struct {
	config     Config
	client     *http.Client
	queue      chan ForwardRequest
	wg         sync.WaitGroup
	shutdownCh chan struct{}
}

func NewCallbackProxy(config Config) *CallbackProxy {
	return &CallbackProxy{
		config: config,
		client: &http.Client{
			Timeout: config.ForwardTimeout,
		},
		queue:      make(chan ForwardRequest, config.QueueSize),
		shutdownCh: make(chan struct{}),
	}
}

func (cp *CallbackProxy) Start() {
	for i := 0; i < cp.config.WorkerCount; i++ {
		cp.wg.Add(1)
		go cp.worker(i)
	}
	log.Printf("Started %d forwarding workers", cp.config.WorkerCount)
}

func (cp *CallbackProxy) Stop() {
	close(cp.shutdownCh)
	close(cp.queue)
	cp.wg.Wait()
	log.Println("All workers stopped")
}

func (cp *CallbackProxy) worker(id int) {
	defer cp.wg.Done()
	log.Printf("Worker %d started", id)

	for req := range cp.queue {
		cp.forwardWithRetry(req)
	}

	log.Printf("Worker %d stopped", id)
}

func (cp *CallbackProxy) forwardWithRetry(fwdReq ForwardRequest) {
	var lastErr error

	for attempt := 0; attempt <= cp.config.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(cp.config.RetryDelay * time.Duration(attempt))
			log.Printf("Retry attempt %d for %s %s", attempt, fwdReq.Method, fwdReq.Path)
		}

		err := cp.forward(fwdReq)
		if err == nil {
			log.Printf("Successfully forwarded: %s %s", fwdReq.Method, fwdReq.Path)
			return
		}
		lastErr = err
	}

	log.Printf("Failed to forward after %d retries: %s %s - Error: %v",
		cp.config.MaxRetries, fwdReq.Method, fwdReq.Path, lastErr)
}

func (cp *CallbackProxy) forward(fwdReq ForwardRequest) error {
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

func (cp *CallbackProxy) HandleCallback(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	fwdReq := ForwardRequest{
		Method:  r.Method,
		Path:    r.URL.Path,
		Headers: r.Header.Clone(),
		Body:    body,
		Query:   r.URL.RawQuery,
	}

	select {
	case cp.queue <- fwdReq:
		log.Printf("Queued request: %s %s (queue size: %d)", r.Method, r.URL.Path, len(cp.queue))
	default:
		log.Printf("Queue full, dropping request: %s %s", r.Method, r.URL.Path)
		http.Error(w, "Server busy", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (cp *CallbackProxy) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "healthy",
		"queue_size": len(cp.queue),
		"queue_cap":  cap(cp.queue),
	})
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

func main() {
	config := Config{
		ListenAddr:     getEnv("LISTEN_ADDR", ":8080"),
		ForwardURL:     getEnv("FORWARD_URL", ""),
		ForwardTimeout: getEnvDuration("FORWARD_TIMEOUT", 30*time.Second),
		MaxRetries:     getEnvInt("MAX_RETRIES", 3),
		RetryDelay:     getEnvDuration("RETRY_DELAY", 1*time.Second),
		WorkerCount:    getEnvInt("WORKER_COUNT", 10),
		QueueSize:      getEnvInt("QUEUE_SIZE", 10000),
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
	log.Printf("  Queue Size: %d", config.QueueSize)

	proxy := NewCallbackProxy(config)
	proxy.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", proxy.HealthCheck)
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
