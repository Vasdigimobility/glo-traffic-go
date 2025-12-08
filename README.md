# GLO Traffic Callback Proxy

A high-performance Go application that accepts URL callbacks and immediately responds while asynchronously forwarding requests to another endpoint. This reduces latency for callback providers while ensuring reliable delivery to your backend.

## Features

- **Instant Response**: Returns immediately to callback providers, reducing timeout issues
- **Async Forwarding**: Queues and forwards requests asynchronously using worker goroutines
- **Retry Logic**: Automatic retries with exponential backoff for failed forwards
- **Configurable**: All settings via environment variables
- **Health Check**: Built-in `/health` endpoint for monitoring
- **Graceful Shutdown**: Properly drains queue on shutdown
- **Production Ready**: Systemd service file included

## Architecture

```
[Callback Provider] → [GLO Traffic Proxy] → (immediate 200 OK response)
                              ↓
                        [Queue + Workers]
                              ↓
                      [Your Backend API]
```

## Quick Start

### Local Development

```bash
# Clone and build
git clone https://github.com/vasdigimobility/glo-traffic-go.git
cd glo-traffic-go
go build -o glo-traffic-proxy .

# Run with required config
export FORWARD_URL="https://your-backend-api.com"
./glo-traffic-proxy
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `FORWARD_URL` | *required* | Target URL to forward callbacks to |
| `LISTEN_ADDR` | `:8080` | Address and port to listen on |
| `FORWARD_TIMEOUT` | `30s` | Timeout for forwarding requests |
| `MAX_RETRIES` | `3` | Maximum retry attempts for failed forwards |
| `RETRY_DELAY` | `1s` | Base delay between retries |
| `WORKER_COUNT` | `10` | Number of worker goroutines |
| `QUEUE_SIZE` | `10000` | Maximum queue size |

## API Endpoints

### `POST/GET/PUT/*` `/` (and all paths)
Accepts any HTTP method and path. Returns immediately with HTTP `204 No Content`.

### `GET /health`
Health check endpoint:
```json
{
  "status": "healthy",
  "queue_size": 5,
  "queue_cap": 10000
}
```

---

# Deployment Guide: Ubuntu 24.04+ on Digital Ocean

## Prerequisites

- Digital Ocean account
- SSH key added to your Digital Ocean account

## Step 1: Create a Droplet

1. Log in to Digital Ocean
2. Click **Create** → **Droplets**
3. Choose:
   - **Region**: Select closest to your callback providers
   - **Image**: Ubuntu 24.04 LTS
   - **Size**: Basic → Regular → $6/mo (1 GB RAM) for light traffic, or $12/mo (2 GB RAM) for higher loads
   - **Authentication**: SSH Key (recommended)
4. Click **Create Droplet**

## Step 2: Initial Server Setup

SSH into your droplet:

```bash
ssh root@YOUR_DROPLET_IP
```

Update the system:

```bash
apt update && apt upgrade -y
```

Create a dedicated user for the service:

```bash
useradd --system --no-create-home --shell /bin/false glo-traffic
```

Install required packages:

```bash
apt install -y ufw fail2ban
```

## Step 3: Configure Firewall

```bash
# Allow SSH
ufw allow 22/tcp

# Allow HTTP traffic (or your custom port)
ufw allow 8080/tcp

# Enable firewall
ufw --force enable
```

## Step 4: Install Go and Build the Application

### Option A: Build on Server

```bash
# Install Go
wget https://go.dev/dl/go1.22.0.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# Create application directory
mkdir -p /opt/glo-traffic-proxy
cd /opt/glo-traffic-proxy

# Clone repository (or copy files)
git clone https://github.com/vasdigimobility/glo-traffic-go.git .

# Build
go build -ldflags="-w -s" -o glo-traffic-proxy .

# Clean up Go installation (optional, saves space)
rm -rf /usr/local/go ~/go1.22.0.linux-amd64.tar.gz
```

### Option B: Build Locally and Upload

On your local machine:

```bash
# Cross-compile for Linux
cd glo-traffic-go
GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o glo-traffic-proxy .

# Upload to server
scp glo-traffic-proxy root@YOUR_DROPLET_IP:/opt/glo-traffic-proxy/
```

## Step 5: Configure the Application

Create the environment file:

```bash
cat > /opt/glo-traffic-proxy/.env << 'EOF'
FORWARD_URL=https://your-backend-api.com
LISTEN_ADDR=:8080
FORWARD_TIMEOUT=30s
MAX_RETRIES=3
RETRY_DELAY=1s
WORKER_COUNT=10
QUEUE_SIZE=10000
EOF
```

Set permissions:

```bash
chown -R glo-traffic:glo-traffic /opt/glo-traffic-proxy
chmod 600 /opt/glo-traffic-proxy/.env
chmod 755 /opt/glo-traffic-proxy/glo-traffic-proxy
```

## Step 6: Install Systemd Service

```bash
# Copy service file
cp /opt/glo-traffic-proxy/glo-traffic-proxy.service /etc/systemd/system/

# Or create it manually:
cat > /etc/systemd/system/glo-traffic-proxy.service << 'EOF'
[Unit]
Description=GLO Traffic Callback Proxy
After=network.target

[Service]
Type=simple
User=glo-traffic
Group=glo-traffic
WorkingDirectory=/opt/glo-traffic-proxy
ExecStart=/opt/glo-traffic-proxy/glo-traffic-proxy
Restart=always
RestartSec=5
EnvironmentFile=/opt/glo-traffic-proxy/.env
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
RestrictNamespaces=true
StandardOutput=journal
StandardError=journal
SyslogIdentifier=glo-traffic-proxy

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and enable service
systemctl daemon-reload
systemctl enable glo-traffic-proxy
systemctl start glo-traffic-proxy
```

## Step 7: Verify Installation

Check service status:

```bash
systemctl status glo-traffic-proxy
```

View logs:

```bash
journalctl -u glo-traffic-proxy -f
```

Test the health endpoint:

```bash
curl http://localhost:8080/health
```

Test from external (replace with your droplet IP):

```bash
curl http://YOUR_DROPLET_IP:8080/health
```

## Step 8: (Optional) Setup Nginx Reverse Proxy with SSL

Install Nginx and Certbot:

```bash
apt install -y nginx certbot python3-certbot-nginx
```

Create Nginx configuration:

```bash
cat > /etc/nginx/sites-available/glo-traffic-proxy << 'EOF'
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_connect_timeout 5s;
        proxy_send_timeout 10s;
        proxy_read_timeout 10s;
    }
}
EOF

ln -s /etc/nginx/sites-available/glo-traffic-proxy /etc/nginx/sites-enabled/
nginx -t && systemctl reload nginx
```

Get SSL certificate:

```bash
certbot --nginx -d your-domain.com
```

Update firewall:

```bash
ufw allow 80/tcp
ufw allow 443/tcp
```

## Maintenance Commands

```bash
# View logs
journalctl -u glo-traffic-proxy -f

# Restart service
systemctl restart glo-traffic-proxy

# Stop service
systemctl stop glo-traffic-proxy

# Check status
systemctl status glo-traffic-proxy

# Update application
cd /opt/glo-traffic-proxy
git pull
go build -ldflags="-w -s" -o glo-traffic-proxy .
systemctl restart glo-traffic-proxy
```

## Monitoring

For production, consider adding:

1. **Prometheus metrics** - Add `/metrics` endpoint
2. **Log aggregation** - Ship logs to a central location
3. **Uptime monitoring** - Use Digital Ocean's monitoring or external services

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Service won't start | Check logs: `journalctl -u glo-traffic-proxy -n 50` |
| Permission denied | Verify ownership: `chown -R glo-traffic:glo-traffic /opt/glo-traffic-proxy` |
| Port already in use | Check: `ss -tlnp | grep 8080` |
| Callbacks not forwarding | Verify `FORWARD_URL` in `.env` and check logs |

## License

MIT