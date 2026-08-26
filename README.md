# DINIS — ICMP Network Monitor

DINIS is an ICMP network monitoring daemon and web dashboard written in Go. It performs automated subnet discovery across configured CIDR ranges, continuously monitors discovered and static targets with paced ICMP Echo requests, calculates time-series metrics and downsampling rollups, manages outage alerts, and serves an embedded web interface.

---

## Features

- **Subnet Discovery**: Scans configured CIDR blocks on a schedule and enrolls responsive hosts.
- **Paced Probing**: Distributes ICMP probes evenly across the monitoring interval to avoid network bursts.
- **In-Memory Time-Series & Rollups**: Retains raw probe history and computes 1-minute and 1-hour rollups with P50/P95/P99 latency, jitter, and loss.
- **Subnet Heatmaps**: Visualizes monitored hosts grouped by prefix with color-coded latency and loss.
- **Outlier Detection**: Surfaces hosts exhibiting packet loss, latency spikes, or outages.
- **Outage Alerting**: Tracks host state transitions (`UP`/`DOWN`), fires alerts, and supports operator acknowledgements.
- **Exclusion Rules**: Prevents specific IPs or subnets from being probed or alerting.
- **Embedded Web Dashboard**: Single-binary deployment with real-time SSE updates and no external frontend dependencies.
- **Atomic JSON Persistence**: Saves targets, metadata, exclusions, and settings to disk.

---

## Requirements

- Go 1.20+
- Linux kernel with unprivileged ICMP socket support (`net.ipv4.ping_group_range`) or `CAP_NET_RAW` capability.

### Socket Permissions

Enable unprivileged ICMP sockets (`SOCK_DGRAM`):
```bash
sudo sysctl -w net.ipv4.ping_group_range="0 2147483647"
```

To persist across reboots:
```bash
echo "net.ipv4.ping_group_range = 0 2147483647" | sudo tee /etc/sysctl.d/99-ping-group.conf
sudo sysctl --system
```

Alternatively, grant raw network capabilities to the compiled binary:
```bash
sudo setcap cap_net_raw+ep dinis
```

---

## Build & Run

### Build
```bash
go build -o dinis main.go
```

### Run
```bash
./dinis -port 8080 -data data/dinis.json
```

---

## CLI Flags

| Flag | Default | Description |
|---|---|---|
| `-port` | `8080` | HTTP listen port for the web dashboard and REST API |
| `-host` | `0.0.0.0` | HTTP listen address |
| `-data` | `data/dinis.json` | Path to persistent JSON storage file |
| `-static` | `""` | Optional filesystem directory for web assets (overrides embedded assets) |
| `-version` | `false` | Print application version and exit |

---

## Configuration Settings

| Setting | Default | Description |
|---|---|---|
| `intervalSec` | `60.0` | Probing frequency for monitored hosts (seconds) |
| `discoveryIntervalMin` | `240` | Periodic CIDR discovery interval (minutes; `0` disables auto-scan) |
| `timeoutMs` | `1000` | ICMP Echo Reply timeout (milliseconds) |
| `failThreshold` | `2` | Consecutive failed probes required to mark a host `DOWN` |
| `concurrency` | `100` | Maximum parallel worker goroutines for probing and discovery |
| `autoDiscovery` | `true` | Enable periodic discovery sweeps |

Settings can be updated at runtime via `PUT /api/settings`.

---

## REST API

### Monitoring & Hosts

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/summary` | Aggregated metrics, reachability rate, alert counts, and pacing rate |
| `GET` | `/api/hosts` | List monitored hosts. Unparameterized returns full array. Supports `?page=1&limit=50&search=...&status=...&sort=...&lightweight=true` |
| `GET` | `/api/hosts/{ip}` | Detailed host state, latency metrics, and metadata |
| `GET` | `/api/hosts/{ip}/history?window={1h\|24h\|168h}` | Historical rollup data points for a host |
| `POST` | `/api/hosts/{ip}/ping` | Run an immediate on-demand ICMP probe against a host |
| `PUT` | `/api/hosts/{ip}/meta` | Update alias and operator notes (`{"alias": "...", "notes": "..."}`) |
| `DELETE` | `/api/hosts/{ip}/enrollment` | Un-enroll a host from active monitoring |
| `POST` | `/api/hosts/{ip}/promote` | Mark a host as a static target |

### Heatmap & Outliers

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/subnets/matrix` | List subnet blocks and cell status for heatmap visualization |
| `GET` | `/api/outliers?limit={n}` | List top degraded hosts ranked by packet loss and P95 latency |

### Subnets & Exclusions

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/cidrs` | List configured CIDR blocks |
| `POST` | `/api/cidrs` | Add or update a CIDR block (`{"cidr": "192.168.1.0/24", "description": "LAN", ...}`) |
| `DELETE` | `/api/cidrs?cidr={cidr}` | Remove a CIDR block and prune associated dynamic hosts |
| `GET` | `/api/exclusions` | List active exclusion rules |
| `POST` | `/api/exclusions` | Add an exclusion rule (`{"rule": "192.168.1.1", "reason": "Gateway"}`) |
| `DELETE` | `/api/exclusions?rule={rule}` | Remove an exclusion rule |

### Discovery & Alerts

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/discovery/status` | Current discovery state, scan counts, capacity, and next run timestamp |
| `POST` | `/api/discovery/run` | Trigger an immediate discovery sweep (`{"cidr": "..."}` optional) |
| `GET` | `/api/alerts` | List active firing and acknowledged alerts |
| `GET` | `/api/alerts/history` | List resolved alert history |
| `POST` | `/api/alerts/acknowledge` | Acknowledge an alert (`{"ip": "...", "alertId": "...", "operator": "...", "note": "..."}`) |
| `POST` | `/api/alerts/acknowledge-all` | Acknowledge all active firing alerts (`{"operator": "...", "note": "..."}`) |

### Settings & Stream

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/settings` | Get current application settings |
| `PUT` | `/api/settings` | Update settings live without restart |
| `GET` | `/api/stream` | Server-Sent Events (SSE) stream for real-time state changes |

---

## Directory Structure

```
dinis/
├── main.go                     # Entrypoint & CLI orchestration
├── go.mod                      # Go module definition
├── verify_e2e.py               # E2E integration test suite
├── pkg/
│   ├── network/                # CIDR expansion and exclusion matching
│   │   ├── cidr.go
│   │   └── cidr_test.go
│   ├── pinger/                 # ICMP socket operations, pacing, and probe engine
│   │   ├── icmp.go
│   │   ├── icmp_test.go
│   │   ├── engine.go
│   │   └── engine_test.go
│   ├── timeseries/             # In-memory ring buffer, rollups, and outlier detector
│   │   ├── ring_buffer.go
│   │   ├── rollup.go
│   │   ├── store.go
│   │   └── store_test.go
│   ├── alerts/                 # Outage alert lifecycle, acknowledgements, and history
│   │   ├── manager.go
│   │   └── manager_test.go
│   ├── store/                  # Atomic JSON file persistence
│   │   ├── store.go
│   │   └── store_test.go
│   └── server/                 # HTTP routing, REST handlers, SSE, and web server
│       ├── server.go
│       ├── server_test.go
│       └── web_dist/           # Embedded dashboard assets
│           ├── index.html
│           ├── style.css
│           └── app.js
└── data/                       # Persistent JSON storage directory
```

---

## Testing

Run unit and benchmark tests:
```bash
go test -v -race ./...
```

Run E2E integration tests against a running server:
```bash
./dinis -port 8080 -data data/dinis_test.json &
python3 verify_e2e.py
```
