# DINIS

DINIS is an ICMP network monitoring daemon with an embedded web interface and REST API. It discovers active hosts across CIDR ranges, runs periodic ICMP probes to track latency and packet loss, and can export metrics to InfluxDB 3.

## Prerequisites

- **Go**: 1.26 or later (for building from source)
- **Linux Network Permissions**: ICMP socket access via unprivileged ping sockets or raw socket capabilities:
  - *Option 1 (Unprivileged ICMP)*:
    ```bash
    sudo sysctl -w net.ipv4.ping_group_range="0 2147483647"
    ```
  - *Option 2 (`CAP_NET_RAW` capability)*:
    ```bash
    sudo setcap cap_net_raw+ep ./dinis
    ```
  *(If neither socket permission is available, DINIS falls back to executing the system `ping` utility.)*
- **Docker & Docker Compose** (optional, for containerized deployment)

## Installation & Setup

### Build from Source

```bash
go build -o dinis main.go
```

### Run with Docker Compose

```bash
docker compose up -d
```

Once running, access the web dashboard at `http://localhost` (or `http://<server-ip>`). Remote traffic is routed through the Nginx reverse proxy, which also exposes InfluxDB 3 at `http://<server-ip>:8181` for external Grafana access.

## Usage / Quickstart

### Start the Daemon (Standalone Binary)

```bash
./dinis -port 8080 -data data/dinis.json
```

Once running, access the web dashboard at `http://localhost:8080`.

### REST API Examples

Add a CIDR range for discovery and monitoring:
```bash
curl -X POST http://localhost:8080/api/cidrs \
  -H "Content-Type: application/json" \
  -d '{"cidr": "192.168.1.0/24", "description": "LAN"}'
```

Trigger an immediate discovery scan:
```bash
curl -X POST http://localhost:8080/api/discovery/run
```

Get system summary metrics:
```bash
curl http://localhost:8080/api/summary
```

List monitored hosts:
```bash
curl "http://localhost:8080/api/hosts?page=1&limit=50&status=UP"
```

Send an on-demand ping probe to a specific host:
```bash
curl -X POST http://localhost:8080/api/hosts/192.168.1.1/ping
```

Listen to real-time events via Server-Sent Events (SSE):
```bash
curl -N http://localhost:8080/api/stream
```

### API Endpoints Overview

| Method | Path | Description |
|---|---|---|
| `GET` | `/health`, `/api/health` | Health check endpoint |
| `GET` | `/api/summary` | Aggregate host counts, latency averages, and scan status |
| `GET` | `/api/stream` | Server-Sent Events (SSE) event stream |
| `GET`, `POST` | `/api/cidrs` | List or add monitored CIDR blocks |
| `DELETE` | `/api/cidrs/{cidr}` | Remove a monitored CIDR block |
| `GET` | `/api/discovery/status` | Current discovery scan status |
| `POST` | `/api/discovery/run` | Trigger an asynchronous discovery sweep |
| `GET` | `/api/hosts` | Paginated host list (supports `status`, `search`, `sort`) |
| `GET`, `PUT`, `DELETE` | `/api/hosts/{ip}` | Get host detail, update alias/notes, or un-enroll |
| `POST` | `/api/hosts/{ip}/ping` | Immediate single-host probe (rate-limited) |
| `POST` | `/api/hosts/{ip}/promote` | Mark discovered host as static monitored target |
| `GET`, `POST` | `/api/exclusions` | List or create exclusion rules |
| `DELETE` | `/api/exclusions/{id}` | Delete an exclusion rule |
| `GET` | `/api/alerts` | Active incident alerts |
| `POST` | `/api/alerts/ack` | Acknowledge active alert |
| `GET` | `/api/alerts/history` | Historical resolved alerts |
| `GET`, `PUT` | `/api/settings` | Read or update runtime engine settings |
| `GET` | `/api/matrix` | Subnet heatmap matrix |
| `GET` | `/api/export/csv` | Export current host inventory as CSV |

## Configuration

### Command-Line Flags

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `-port` | `DINIS_PORT` | `8080` | HTTP listen port for API and web dashboard |
| `-host` | `DINIS_HOST` | `0.0.0.0` | HTTP listen address |
| `-data` | `DINIS_DATA` | `data/dinis.json` | Path to persistent JSON database file |
| `-static` | `DINIS_STATIC` | `""` | Directory path for static web assets (overrides embedded assets) |
| `-api-token` | `DINIS_API_TOKEN` | `""` | Optional API authentication token required for REST endpoints |
| `-allowed-hosts` | `DINIS_ALLOWED_HOSTS` | `""` | Comma-separated list of allowed Host header values (DNS rebinding protection) |
| `-allowed-client-ips` | `DINIS_ALLOWED_CLIENT_IPS` | `""` | Comma-separated list of allowed client IPs/CIDRs |
| `-trusted-proxies` | `DINIS_TRUSTED_PROXIES` | `""` | Comma-separated trusted proxy IPs/CIDRs or preset ('docker'/'private') |
| `-allowed-origins` | `DINIS_ALLOWED_ORIGINS` | `""` | Comma-separated list of allowed CORS origins |
| `-max-metric-hosts` | `DINIS_MAX_METRIC_HOSTS` | `10000` | In-memory time-series host retention capacity before LRU eviction |
| `-influxdb-url` | `INFLUXDB3_URL` | `""` | InfluxDB 3 Core URL (e.g. `http://localhost:8181`). Empty disables export |
| `-influxdb-bucket` | `INFLUXDB3_BUCKET` | `dinis` | InfluxDB database/bucket name |
| `-influxdb-token` | `INFLUXDB3_TOKEN` | `""` | InfluxDB authentication token (must start with `apiv3_` if set) |
| `-version` | — | `false` | Print version and exit |

### Environment Variables (Docker Compose)

| Variable | Default | Description |
|---|---|---|
| `NGINX_HTTP_PORT` | `80` | Host port mapped to Nginx for the DINIS dashboard & REST API |
| `INFLUXDB3_PORT` | `8181` | Host port mapped directly to InfluxDB 3 Core (HTTP API & Flight SQL) |
| `DINIS_PORT` | `8080` | Internal container port for DINIS |
| `DINIS_DATA` | `/data/dinis.json` | Storage file path inside the container |
| `DINIS_API_TOKEN` | `""` | Optional API authentication token required for REST endpoints |
| `DINIS_ALLOWED_HOSTS` | `""` | Allowed Host header values for DNS rebinding defense |
| `DINIS_ALLOWED_CLIENT_IPS`| `""` | Client IP/CIDR whitelist for dashboard & API access |
| `DINIS_TRUSTED_PROXIES` | `docker` | Trusted reverse proxy IPs/CIDRs permitted to provide `X-Forwarded-For`/`Host` |
| `DINIS_ALLOWED_ORIGINS` | `""` | Allowed CORS origins |
| `DINIS_MAX_METRIC_HOSTS` | `10000` | In-memory time-series host retention limit before LRU rollup eviction |
| `INFLUXDB3_URL` | `http://influxdb3:8181` | InfluxDB 3 Core endpoint |
| `INFLUXDB3_BUCKET` | `dinis` | InfluxDB bucket name |
| `INFLUXDB3_TOKEN` | `""` | InfluxDB API token (must start with `apiv3_` if set) |
| `INFLUXDB3_NODE_ID` | `dinis-node` | InfluxDB 3 node identifier |

## Connecting an External Grafana Instance

To visualize DINIS probe metrics in your existing Grafana installation:

### Option A: Native InfluxDB 3 / Flight SQL (Recommended)
1. In Grafana, navigate to **Connections** > **Data Sources** > **Add data source** and select **InfluxDB**.
2. Configure settings:
   - **Query Language:** `SQL`
   - **URL:** `http://<dinis-host-ip>:8181`
   - **Database:** `dinis` (or the value of `INFLUXDB3_BUCKET`)
   - **Token:** Your `INFLUXDB3_TOKEN` (starts with `apiv3_`)
   - **Insecure Connection:** Toggle **ON** (required for cleartext HTTP/2 `h2c` without TLS)
3. Click **Save & test**.

### Option B: InfluxQL Compatibility Mode
1. In Grafana, select **InfluxDB**.
2. Configure settings:
   - **Query Language:** `InfluxQL`
   - **URL:** `http://<dinis-host-ip>:8181`
   - **Database:** `dinis` (or the value of `INFLUXDB3_BUCKET`)
   - **HTTP Method:** `POST`
3. If authentication is enabled (`INFLUXDB3_TOKEN` is set):
   - Under **Custom HTTP Headers**, click **Add header**:
     - **Header:** `Authorization`
     - **Value:** `Bearer <your_INFLUXDB3_TOKEN>`
4. Click **Save & test**.

## Architecture / Project Structure

```
.
├── main.go               # Program entrypoint, CLI flag configuration, and service coordinator
├── Dockerfile            # Multi-stage build definition for alpine-based container
├── docker-compose.yml    # Service definition for DINIS, InfluxDB 3 Core, and Nginx reverse proxy
├── docker/
│   └── nginx/            # Nginx reverse proxy configuration and virtual host definitions
├── verify_e2e.py         # End-to-end integration and API verification test script
└── pkg/
    ├── alerts/           # Alert evaluation, state transitions, and acknowledgments
    ├── influxdb/         # InfluxDB 3 line protocol metric exporter
    ├── network/          # CIDR parsing, IP expansion, and exclusion matching
    ├── pinger/           # ICMP raw/datagram sockets, packet pacing, and probe engine
    ├── server/           # HTTP handlers, REST API endpoints, and SSE event streaming
    │   └── web_dist/     # Embedded web dashboard frontend assets
    ├── store/            # JSON persistence layer with atomic writes
    └── timeseries/       # In-memory metric ring buffers, rollups, and outlier detection
```
