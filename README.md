# DINIS — High-Performance ICMP Network Monitor

**DINIS** is a lightweight, blazing-fast, and real-time ICMP Echo (ping) network monitoring application built in Go. It enables DevOps engineers and NOC teams to monitor subnets via CIDR notation, exclude specific hosts or IP ranges, receive instant alerts for unreachable targets, acknowledge outages with operator notes, and visualize network health through a dark-mode web dashboard.

---

## Key Features

- ⚡ **Interval-Stretched Pacing & Scaling**:
  - Automatically spreads and staggers ICMP probes uniformly across the duration of the configured interval ($\Delta t = \text{Window} / N$).
  - Instead of sending burst spikes all at $t=0$, packets are dispatched in a continuous, smooth, low-impact trickle (e.g. 200 hosts across 5s = 1 probe every 20ms).
  - Eliminates network switch buffer overruns, ARP storming, and router rate-limiting on large subnets.
- 🔍 **Intelligent Subnet Discovery**:
  - Automatically sweeps configured CIDR subnets on a **configurable discovery interval** (e.g. every 5m, 15m, 1h, or manual on-demand).
  - Only adds hosts that are **actually online during discovery** to continuous live monitoring. Unallocated/unused IPs in the subnet are not continuously pinged and do not trigger false alerts!
  - Ability to trigger "Run Discovery Now" globally or per-subnet in the Web UI.
- 🌐 **CIDR Notation Support**: Add entire subnets (e.g. `192.168.1.0/24`, `10.0.0.0/16`) or single static IPs (`8.8.8.8`). Displays active hosts vs total subnet capacity.
- 🚫 **IP & Subnet Exclusions**: Exclude specific IPs or ranges from both discovery and live monitoring with custom reasons.
- 🚨 **Alert & Incident Management**:
  - Live outage detection when an active monitored device stops responding.
  - One-click or noted **Alert Acknowledgements** (track operator names, notes, and duration).
  - Automatic resolution when hosts recover.
  - Incident history logging.
- 📊 **Modern NOC Web Dashboard**:
  - Real-time updates via **Server-Sent Events (SSE)** with zero-polling latency.
  - Live micro-sparkline latency trend charts and packet loss gauges.
  - Dual view modes: Visual Cards and High-Density NOC Table.
  - Audio alert chime generator using Web Audio API.
  - Host inspection drawer with on-demand manual pinging and custom host aliases.
- 📦 **Single Standalone Binary**: Web assets (HTML, CSS, JS) are embedded directly into the Go binary (`embed.FS`) with zero external runtime dependencies.
- 💾 **Thread-Safe Persistence**: Atomically saved JSON data store across restarts.

---

## Quickstart

### 1. Build from Source
Ensure you have Go (1.20+) installed:

```bash
go build -o dinis main.go
```

### 2. Run the Application
Start the monitoring service on port `8080`:

```bash
./dinis -port 8080 -data data/dinis.json
```

Then open your browser and navigate to:
```
http://localhost:8080
```

---

## CLI Options

| Flag | Default | Description |
|---|---|---|
| `-port` | `8080` | HTTP Web UI and REST API listening port |
| `-host` | `0.0.0.0` | Listen host address |
| `-data` | `data/dinis.json` | Path to persistent storage JSON file |
| `-static` | `""` | Optional path to static web directory (for live UI development) |
| `-version` | `false` | Display version and exit |

---

## REST API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/summary` | Get aggregated monitoring metrics and health rates |
| `GET` | `/api/hosts` | List all monitored hosts with live latency & state |
| `GET` | `/api/hosts/{ip}` | Get detailed metrics and RTT history for a host |
| `POST` | `/api/hosts/{ip}/ping` | Trigger an immediate on-demand manual probe |
| `PUT` | `/api/hosts/{ip}/meta` | Update host alias or operator notes |
| `GET` | `/api/cidrs` | List all configured CIDR subnets |
| `POST` | `/api/cidrs` | Add or update a CIDR range |
| `DELETE` | `/api/cidrs?cidr=...` | Remove a CIDR subnet from monitoring |
| `GET` | `/api/exclusions` | List active host/subnet exclusion rules |
| `POST` | `/api/exclusions` | Add an exclusion rule |
| `DELETE` | `/api/exclusions?rule=...`| Remove an exclusion rule |
| `GET` | `/api/alerts` | List all active firing and acknowledged alerts |
| `GET` | `/api/alerts/history` | List resolved historical incidents |
| `POST` | `/api/alerts/acknowledge` | Acknowledge an outage alert with note |
| `POST` | `/api/alerts/acknowledge-all` | Bulk acknowledge all active outages |
| `GET` | `/api/settings` | Get probe intervals, timeouts, and concurrency |
| `PUT` | `/api/settings` | Update settings live without restarting |
| `GET` | `/api/stream` | Server-Sent Events (SSE) real-time event stream |

---

## Project Structure

```
dinis/
├── main.go                     # Entrypoint & CLI flag orchestration
├── go.mod                      # Go module definition
├── pkg/
│   ├── network/                # CIDR parsing, expansion, and exclusion engine
│   │   ├── cidr.go
│   │   └── cidr_test.go
│   ├── pinger/                 # High-throughput ICMP sockets & async worker engine
│   │   ├── icmp.go
│   │   ├── icmp_test.go
│   │   ├── engine.go
│   │   └── engine_test.go
│   ├── alerts/                 # Alert state transitions, incident logs & acknowledgements
│   │   ├── manager.go
│   │   └── manager_test.go
│   ├── store/                  # Thread-safe atomic persistent JSON store
│   │   ├── store.go
│   │   └── store_test.go
│   └── server/                 # REST API, SSE broadcaster, and embedded web server
│       ├── server.go
│       └── web_dist/           # Embedded dashboard assets
│           ├── index.html
│           ├── style.css
│           └── app.js
└── data/                       # Persistent JSON data directory
```

---

## Running Tests

Execute the unit test suite:
```bash
go test -v ./...
```

Execute end-to-end integration tests:
```bash
python3 verify_e2e.py
```
