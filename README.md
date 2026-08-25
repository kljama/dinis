# DINIS — ICMP Network Monitor

DINIS is a lightweight ICMP Echo (ping) network monitoring daemon and web dashboard written in Go. It performs automated subnet discovery across configured CIDR ranges, continuously monitors discovered and static targets with paced ICMP requests, manages outage alerts with operator acknowledgement workflows, and serves a self-contained real-time web interface.

---

## Features

- **Paced Probing**: Distributes ICMP probes across the configured monitor interval to avoid burst packet storms on network switches and gateway routers.
- **CIDR Subnet Discovery**: Sweeps entire CIDR blocks on a configurable schedule (default: every 4 hours) and automatically enrolls responsive hosts into continuous monitoring.
- **Exclusion Rules**: Supports IP and CIDR exclusions with user-defined reasons to ignore broadcast domains, gateways, or non-monitored ranges.
- **Incident & Alert Lifecycle**: Tracks consecutive packet drops, triggers outage alerts when a host reaches the failure threshold, supports operator acknowledgements with notes, and automatically resolves alerts upon host recovery.
- **Embedded Web Dashboard**: Real-time updates via Server-Sent Events (SSE), live latency sparklines, grid and table views, on-demand manual pinging, and sound notifications. Single binary with all HTML, CSS, and JS assets embedded via `embed.FS`.
- **Atomic Persistence**: Persists configuration, discovered hosts, metadata, and settings to a JSON data store using atomic write operations.

---

## Requirements

- **Linux** with unprivileged ICMP datagram sockets enabled (`net.ipv4.ping_group_range`) or `CAP_NET_RAW` capabilities.
- **Go 1.20+** (for building from source).

To enable unprivileged ICMP sockets on Linux:
```bash
sudo sysctl -w net.ipv4.ping_group_range="0 2147483647"
```

---

## Building & Running

### Build

```bash
go build -o dinis main.go
```

### Run

```bash
./dinis -port 8080 -data data/dinis.json
```

Open `http://localhost:8080` in a browser to access the dashboard.

---

## CLI Flags

| Flag | Default | Description |
|---|---|---|
| `-port` | `8080` | HTTP listen port for the web dashboard and REST API |
| `-host` | `0.0.0.0` | HTTP listen address |
| `-data` | `data/dinis.json` | Path to persistent JSON storage file |
| `-static` | `""` | Optional filesystem path to web assets (overrides embedded assets for local development) |
| `-version` | `false` | Print application version and exit |

---

## Default Configuration

| Setting | Default | Description |
|---|---|---|
| `intervalSec` | `60.0` | Probing frequency for monitored hosts (seconds) |
| `discoveryIntervalMin` | `240` | Scheduled CIDR subnet discovery interval (minutes, `0` = manual only) |
| `timeoutMs` | `1000` | ICMP Echo Reply receive timeout (milliseconds) |
| `failThreshold` | `2` | Consecutive failed probes required to mark a host `DOWN` and trigger an alert |
| `concurrency` | `100` | Maximum parallel worker goroutines for probing and discovery |
| `autoDiscovery` | `true` | Enable automated periodic subnet discovery sweeps |
| `soundAlerts` | `true` | Enable browser audio chime on state transitions |

Settings can be changed at runtime via the Web UI Settings modal or via `PUT /api/settings`.

---

## REST API

### Monitoring & Hosts

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/summary` | Aggregated metrics, reachability rate, active alert counts, and pacing rates |
| `GET` | `/api/hosts` | List all monitored hosts with status, latency history, and packet loss |
| `GET` | `/api/hosts/{ip}` | Detailed host state, latency metrics, and metadata |
| `POST` | `/api/hosts/{ip}/ping` | Execute an immediate on-demand ICMP probe against a host |
| `PUT` | `/api/hosts/{ip}/meta` | Update custom alias and operator notes for a host |
| `DELETE` | `/api/hosts/{ip}/enrollment` | Un-enroll a host from active monitoring |
| `POST` | `/api/hosts/{ip}/promote` | Promote a discovered dynamic host to a static target |

### Subnets & Exclusions

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/cidrs` | List configured CIDR blocks |
| `POST` | `/api/cidrs` | Add or update a CIDR block (`{"cidr": "192.168.1.0/24", "description": "LAN", ...}`) |
| `DELETE` | `/api/cidrs?cidr={cidr}` | Remove a CIDR block and prune associated non-static hosts |
| `GET` | `/api/exclusions` | List active exclusion rules |
| `POST` | `/api/exclusions` | Add an exclusion rule (`{"rule": "192.168.1.1", "reason": "Gateway"}`) |
| `DELETE` | `/api/exclusions?rule={rule}` | Remove an exclusion rule |

### Discovery & Alerts

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/discovery/status` | Current discovery state, scan counts, capacity, and next run timestamp |
| `POST` | `/api/discovery/run` | Trigger an immediate discovery sweep (`?cidr={cidr}` optional) |
| `GET` | `/api/alerts` | List all active firing and acknowledged alerts |
| `GET` | `/api/alerts/history` | List resolved historical alert incidents |
| `POST` | `/api/alerts/acknowledge` | Acknowledge an outage alert (`{"ip": "...", "ackBy": "...", "note": "..."}`) |
| `POST` | `/api/alerts/acknowledge-all` | Acknowledge all active firing alerts |

### Configuration & Real-Time Stream

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/settings` | Get current application settings |
| `PUT` | `/api/settings` | Update settings live without restarting |
| `GET` | `/api/stream` | Server-Sent Events (SSE) stream for real-time state updates |

---

## Directory Structure

```
dinis/
├── main.go                     # Application entrypoint & CLI orchestration
├── go.mod                      # Go module definition
├── verify_e2e.py               # E2E integration test suite
├── pkg/
│   ├── network/                # CIDR calculation and exclusion matching
│   │   ├── cidr.go
│   │   └── cidr_test.go
│   ├── pinger/                 # Raw/DGRAM ICMP sockets, pacing, and probe engine
│   │   ├── icmp.go
│   │   ├── icmp_test.go
│   │   ├── engine.go
│   │   └── engine_test.go
│   ├── alerts/                 # Incident management, transitions, and history
│   │   ├── manager.go
│   │   └── manager_test.go
│   ├── store/                  # Atomic JSON file persistence
│   │   ├── store.go
│   │   └── store_test.go
│   └── server/                 # HTTP server, REST handlers, SSE, and embedded frontend
│       ├── server.go
│       └── web_dist/           # Embedded dashboard assets
│           ├── index.html
│           ├── style.css
│           └── app.js
└── data/                       # Persistent JSON storage directory
```

---

## Testing

Run the unit test suite with race detection:
```bash
go test -race -v ./...
```

Run the end-to-end integration test suite:
```bash
# Start server in background, then run:
python3 verify_e2e.py
```
