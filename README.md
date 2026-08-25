# DINIS — ICMP Network Monitor

DINIS is a lightweight ICMP Echo (ping) network monitoring daemon and web dashboard written in Go. It performs automated subnet discovery across configured CIDR ranges, continuously monitors discovered and static targets with paced ICMP requests, manages outage alerts with operator acknowledgement workflows, and serves a self-contained real-time web interface.

---

## Features

- **Subnet Discovery**: Scans configured CIDR ranges on a schedule and enrolls responsive hosts.
- **ICMP Monitoring**: Probes active hosts at regular intervals with distributed probe pacing.
- **Exclusions**: Excludes specific IPs or subnets from discovery and monitoring.
- **Outage Alerting**: Triggers alerts after consecutive failed probes and supports operator acknowledgements.
- **Web Dashboard**: Embedded real-time UI with live status updates (SSE), latency history, and manual pinging.
- **JSON Storage**: Persists targets, exclusions, host metadata, and settings to a local file.

---

## Requirements & Dependencies

### Required Packages

| Purpose | Debian / Ubuntu | RHEL / Fedora / Rocky | Arch Linux | Alpine Linux |
|---|---|---|---|---|
| **Build Compiler** | `golang-go` (1.20+) | `golang` | `go` | `go` |
| **ICMP Ping Fallback** | `iputils-ping` | `iputils` | `iputils` | `iputils` |
| **E2E Test Suite** | `python3` | `python3` | `python` | `python3` |

### Package Installation

**Debian / Ubuntu**:
```bash
sudo apt update && sudo apt install -y golang-go iputils-ping python3
```

**RHEL / Fedora / Rocky Linux**:
```bash
sudo dnf install -y golang iputils python3
```

**Arch Linux**:
```bash
sudo pacman -S --needed go iputils python
```

**Alpine Linux**:
```bash
apk add --no-cache go iputils python3
```

### Kernel Socket Permissions

DINIS uses native Linux unprivileged ICMP sockets (`SOCK_DGRAM`) and falls back to `SOCK_RAW` / system `ping`. Ensure unprivileged ICMP sockets are enabled:

```bash
# Enable for the current session:
sudo sysctl -w net.ipv4.ping_group_range="0 2147483647"

# Persist across reboots:
echo "net.ipv4.ping_group_range = 0 2147483647" | sudo tee /etc/sysctl.d/99-ping-group.conf
sudo sysctl --system
```

Alternatively, grant raw network capabilities to the compiled binary:
```bash
sudo setcap cap_net_raw+ep dinis
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
| `POST` | `/api/hosts/{ip}/promote` | Promote a dynamic host to a permanent static target (prevents pruning if parent CIDR is deleted) |

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
