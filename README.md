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

## Usage / Quickstart

### Start the Daemon

```bash
./dinis -port 8080 -data data/dinis.json
```

Once running, access the web dashboard at `http://localhost:8080`.

### REST API Examples

Add a CIDR range for discovery and monitoring:
```bash
curl -X POST http://localhost:8080/api/cidrs \
  -H "Content-Type: application/json" \
  -d '{"cidr": "192.168.1.0/24", "label": "LAN"}'
```

Trigger an immediate discovery scan:
```bash
curl -X POST http://localhost:8080/api/discovery/run
```

Get system summary metrics:
```bash
curl http://localhost:8080/api/summary
```

Send an on-demand ping probe to a specific host:
```bash
curl -X POST http://localhost:8080/api/hosts/192.168.1.1/ping
```

## Configuration

### Command-Line Flags

| Flag | Default | Description |
|---|---|---|
| `-port` | `8080` | HTTP listen port for API and web dashboard |
| `-host` | `0.0.0.0` | HTTP listen address |
| `-data` | `data/dinis.json` | Path to persistent JSON database file |
| `-static` | `""` | Directory path for static web assets (overrides embedded assets) |
| `-influxdb-url` | `""` | InfluxDB 3 Core URL (e.g. `http://localhost:8181`). Empty disables export |
| `-influxdb-bucket` | `dinis` | InfluxDB database/bucket name |
| `-influxdb-token` | `""` | InfluxDB authentication token |
| `-version` | `false` | Print version and exit |

### Environment Variables (Docker Compose)

| Variable | Default | Description |
|---|---|---|
| `DINIS_PORT` | `8080` | Host port mapped to the DINIS container |
| `DINIS_DATA` | `/data/dinis.json` | Storage file path inside the container |
| `INFLUXDB3_URL` | `http://influxdb3:8181` | InfluxDB 3 Core endpoint |
| `INFLUXDB3_BUCKET` | `dinis` | InfluxDB bucket name |
| `INFLUXDB3_TOKEN` | `""` | InfluxDB API token |
| `INFLUXDB3_NODE_ID` | `dinis-node` | InfluxDB 3 node identifier |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | Initial admin password for Grafana |

## Architecture / Project Structure

```
.
├── main.go               # Program entrypoint, CLI flag configuration, and service coordinator
├── Dockerfile            # Multi-stage build definition for alpine-based container
├── docker-compose.yml    # Service definition for DINIS, InfluxDB 3 Core, and Grafana
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
