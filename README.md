# DINIS

ICMP network monitoring daemon with embedded web dashboard. Written in Go, single binary, no external dependencies.

Scans CIDR ranges for responsive hosts, monitors them with paced ICMP probes, tracks latency/loss metrics with in-memory time-series rollups, and fires outage alerts.

## Requirements

- Go 1.26+
- Linux with unprivileged ICMP sockets or `CAP_NET_RAW`

### ICMP Socket Permissions

Option A — unprivileged ICMP (`SOCK_DGRAM`):
```bash
sudo sysctl -w net.ipv4.ping_group_range="0 2147483647"
```

Persist across reboots:
```bash
echo "net.ipv4.ping_group_range = 0 2147483647" | sudo tee /etc/sysctl.d/99-ping-group.conf
sudo sysctl --system
```

Option B — capabilities on the binary:
```bash
sudo setcap cap_net_raw+ep dinis
```

## Build & Run

```bash
go build -o dinis main.go
./dinis -port 8080 -data data/dinis.json
```

## CLI Flags

| Flag | Default | Description |
|---|---|---|
| `-port` | `8080` | HTTP listen port |
| `-host` | `0.0.0.0` | HTTP listen address |
| `-data` | `data/dinis.json` | Path to JSON persistence file |
| `-static` | `""` | Filesystem path for web assets (overrides embedded) |
| `-influxdb-url` | `""` | InfluxDB 3 Core URL (e.g. `http://localhost:8181`). Empty disables export |
| `-influxdb-bucket` | `dinis` | InfluxDB database/bucket name |
| `-influxdb-token` | `""` | InfluxDB authentication token (optional) |
| `-version` | `false` | Print version and exit |

## Docker & Enterprise Deployment

Run the complete observability stack (DINIS + InfluxDB 3 Core + Grafana) via Docker Compose:

```bash
docker compose up -d
```

- **DINIS Web Dashboard**: [http://localhost:8080](http://localhost:8080)
- **InfluxDB 3 Core API**: [http://localhost:8181](http://localhost:8181)
- **Grafana**: [http://localhost:3000](http://localhost:3000) (default credentials: `admin` / `admin`)

## Settings

Configurable at runtime via `PUT /api/settings`.

| Setting | Default | Description |
|---|---|---|
| `intervalSec` | `60.0` | Probe interval (seconds) |
| `discoveryIntervalMin` | `240` | Discovery sweep interval (minutes, `0` disables) |
| `timeoutMs` | `1000` | ICMP reply timeout (ms) |
| `failThreshold` | `2` | Consecutive failures before marking host `DOWN` |
| `concurrency` | `100` | Max parallel probe/discovery goroutines |
| `autoDiscovery` | `true` | Run discovery sweeps on schedule |

## REST API

### Hosts & Monitoring

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/summary` | Aggregate metrics and alert counts |
| `GET` | `/api/hosts` | List hosts (supports `?page=&limit=&search=&status=&sort=&lightweight=true`) |
| `GET` | `/api/hosts/{ip}` | Single host detail |
| `GET` | `/api/hosts/{ip}/history?window={1h\|24h\|168h}` | Rollup history for a host |
| `POST` | `/api/hosts/{ip}/ping` | On-demand probe |
| `PUT` | `/api/hosts/{ip}/meta` | Update alias/notes |
| `DELETE` | `/api/hosts/{ip}/enrollment` | Remove host from monitoring |
| `POST` | `/api/hosts/{ip}/promote` | Mark host as static target |

### Subnets & Heatmap

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/subnets/matrix` | Subnet heatmap data |
| `GET` | `/api/outliers?limit={n}` | Top degraded hosts by loss/latency |

### CIDRs & Exclusions

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/cidrs` | List CIDR blocks |
| `POST` | `/api/cidrs` | Add/update CIDR block |
| `DELETE` | `/api/cidrs?cidr={cidr}` | Remove CIDR block |
| `GET` | `/api/exclusions` | List exclusions |
| `POST` | `/api/exclusions` | Add exclusion rule |
| `DELETE` | `/api/exclusions?rule={rule}` | Remove exclusion rule |

### Discovery & Alerts

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/discovery/status` | Discovery state and next run time |
| `POST` | `/api/discovery/run` | Trigger discovery sweep (optional `{"cidr": "..."}`) |
| `GET` | `/api/alerts` | Active alerts |
| `GET` | `/api/alerts/history` | Resolved alert history |
| `POST` | `/api/alerts/acknowledge` | Acknowledge alert |
| `POST` | `/api/alerts/acknowledge-all` | Acknowledge all firing alerts |

### Settings & Stream

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/settings` | Current settings |
| `PUT` | `/api/settings` | Update settings |
| `GET` | `/api/stream` | SSE stream for real-time updates |

## Project Structure

```
├── main.go                      # Entrypoint
├── go.mod
├── verify_e2e.py                # E2E test suite
├── pkg/
│   ├── network/                 # CIDR parsing, exclusion matching
│   ├── pinger/                  # ICMP sockets, probe engine, pacing
│   ├── timeseries/              # Ring buffer, 1m/1h rollups, outlier detection
│   ├── alerts/                  # Alert lifecycle and acknowledgements
│   ├── store/                   # Atomic JSON persistence
│   └── server/                  # HTTP server, REST API, SSE
│       └── web_dist/            # Embedded dashboard (HTML/CSS/JS)
└── data/                        # Persistent storage directory
```

## Testing

```bash
go test -v -race ./...
```

E2E tests against a running instance:
```bash
./dinis -port 8080 -data data/dinis_test.json &
python3 verify_e2e.py
```
