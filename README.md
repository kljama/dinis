# DINIS

DINIS is an ICMP network monitoring daemon with an embedded web dashboard and REST API. You give it IPv4 CIDR ranges and single IPs. It finds the hosts that answer ping, keeps probing them, tracks latency and packet loss, raises alerts when hosts go down, and can export every probe result to InfluxDB 3.

## How it works

- **Discovery.** A configured CIDR is not pinged in full on every cycle. A discovery sweep pings every address in the range once (750 ms timeout), and only the hosts that answer are enrolled for continuous monitoring. Sweeps run:
  - 5 seconds after startup, if `autoDiscovery` is on
  - every `discoveryIntervalMin` minutes (default 240)
  - when a CIDR is added, if `autoDiscovery` is on
  - on demand
  
  A sweep requested while another one runs is queued and starts when that one finishes. The periodic schedule counts from the last *full* sweep, so sweeping a single range doesn't postpone it. Addresses matching an exclusion rule are skipped. Single-IP targets (`/32`) are always monitored, whether or not they answer.
- **Enrolled hosts stay monitored.** A host that stops answering is not removed: it goes DOWN and raises an alert. Monitoring stops when you delete or disable the host's CIDR, or add an exclusion for it. Un-enrolling a discovered host only lasts until the next sweep finds it again. See [Removing targets](#removing-targets).
- **Probing.** Each host is probed every `intervalSec` seconds (default 60; per-CIDR overrides down to 0.5 s). Every host has a fixed slot within its interval, so probes are spread evenly and each host is probed at a steady pace.
  - A host added while DINIS runs (new target, or found by discovery) is first probed within 5 seconds. Interval changes take effect immediately.
  - A host starts as `PENDING` and becomes `UP` on its first reply.
  - It becomes `DOWN` after `failThreshold` consecutive failures (default 2).
  - Packet loss shown per host is cumulative since DINIS started.
- **Probe capacity during outages.** A probe to a host that doesn't answer occupies a worker for the whole timeout. Two rules keep outages from slowing down the monitoring of everything else:
  - Probes of hosts whose last probe failed may use at most three quarters of the workers (`concurrency`); the rest stay free for hosts that answer.
  - Hosts that have been DOWN for longer than `downProbeIntervalSec` (default 300) are only probed that often. Their recovery is then noticed within that time instead of within one interval. Set it to `0` to probe DOWN hosts at their normal interval.
- **Alerts.** Every host that goes `DOWN` gets an alert, which resolves automatically when the host recovers. Operators can acknowledge alerts with a name and a note. Alerts are shown in the dashboard only; DINIS sends no email, webhook or other notifications.
- **What is stored where.**
  - The JSON data file holds CIDRs, exclusions, discovered hosts, host aliases and notes, and settings.
  - A second file next to it, `<data file name>.alerts.json` (e.g. `data/dinis.alerts.json`), holds active alerts with their acknowledgements and the last 500 resolved alerts. It is saved every 5 seconds while alerts change, and on shutdown.
  - After a restart, ongoing outages keep their alert, start time and acknowledgement. Hosts with an active alert restart as `DOWN`; all other hosts restart as `PENDING`.
  - Latency history lives in memory and is lost on restart: the last 128 raw samples, 1-minute rollups for 2 hours, and hourly rollups for 30 days per host. Rollups cover wall-clock minutes and hours. Use the InfluxDB export for long-term data.
  - A `<data file>.lock` file prevents two DINIS processes from sharing one data file.

### Limits and defaults to know about

- **IPv4 only.** An IPv6 address is accepted as a `/128` target but every probe of it fails, so it shows as DOWN.
- **At most a /16 per CIDR entry** (65,536 addresses). Larger ranges are rejected; add several entries instead.
- **Default targets.** On first start (no data file yet) DINIS adds three targets: `127.0.0.1`, `1.1.1.1` and `8.8.8.8`. Delete their CIDR entries if you don't want DINIS to ping public resolvers.
- **History limit.** Latency history is kept for at most `maxMetricHosts` hosts (default 10,000). Hosts beyond the limit are still probed and alerted on, but have no history charts.

## Prerequisites

- **Go** 1.26.5 or later (for building from source; see `go.mod`).
- **ICMP socket permission on Linux.** DINIS tries an unprivileged ping socket first, then a raw socket:
  - *Option 1 (unprivileged ICMP):*
    ```bash
    sudo sysctl -w net.ipv4.ping_group_range="0 2147483647"
    ```
    This setting is lost on reboot. To keep it, add `net.ipv4.ping_group_range = 0 2147483647` to a file in `/etc/sysctl.d/`.
  - *Option 2 (`CAP_NET_RAW` capability):*
    ```bash
    sudo setcap cap_net_raw+ep ./dinis
    ```
  
  If neither is available, DINIS falls back to running the system `ping` command once per probe. That needs `ping` installed and is much slower, so only use it for small setups.
- **Docker and Docker Compose** (optional, for the containerized stack).

## Installation & Deployment

### Method 1: Standalone binary

1. Build from source:
   ```bash
   go build -o dinis .
   ```

2. Start the daemon:
   ```bash
   ./dinis -port 8080 -data data/dinis.json
   ```

The dashboard is then at `http://localhost:8080`.

DINIS listens on all interfaces (`0.0.0.0`) by default and has no authentication unless `-api-token` is set. Read [Security notes](#security-notes) before exposing it.

In this mode, InfluxDB export is off unless you pass `-influxdb-url`.

### Method 2: Docker Compose

```bash
cp .env.example .env   # then edit .env, at least INFLUXDB3_TOKEN and DINIS_API_TOKEN
docker compose up -d
```

The stack runs four containers:

| Service | Reachable at | Notes |
|---|---|---|
| Nginx → DINIS | `http://<server-ip>` (port `NGINX_HTTP_PORT`, default 80) | Dashboard and REST API, via the reverse proxy |
| Nginx → InfluxDB 3 Explorer | `http://<server-ip>:8888` (`NGINX_EXPLORER_PORT`) | Runs in admin mode **with no login** |
| InfluxDB 3 Core | `http://<server-ip>:8181` (`INFLUXDB3_PORT`) | Published directly on the host, not through Nginx |
| DINIS | internal only (`dinis:8080`) | Has the `NET_RAW` capability for ICMP |

Notes:
- In the compose stack, InfluxDB export is always on (`INFLUXDB3_URL` defaults to `http://influxdb3:8181`).
- If `INFLUXDB3_TOKEN` is empty, InfluxDB runs with authentication disabled while still published on port 8181.
- `.env.example` contains a placeholder token (`apiv3_dinis_secret_token`). Replace it with your own value.

## Usage / Quickstart

### REST API examples

Add a CIDR range. If auto-discovery is on, adding it also starts a discovery sweep of that range:
```bash
curl -X POST http://localhost:8080/api/cidrs \
  -H "Content-Type: application/json" \
  -d '{"cidr": "192.168.1.0/24", "description": "LAN"}'
```

Start a discovery sweep of all enabled CIDRs. Send `{"cidr": "192.168.1.0/24"}` as the body to sweep a single range. If a sweep is already running, the request is queued and the answer is `202`. You can request at most one sweep every 30 seconds; extra requests get `429`:
```bash
curl -X POST http://localhost:8080/api/discovery/run
```

Get summary metrics:
```bash
curl http://localhost:8080/api/summary
```

List monitored hosts (paginated):
```bash
curl "http://localhost:8080/api/hosts?page=1&limit=50&status=up&sort=status"
```

Probe a host once, right now. This works for any IPv4 address and updates the host's state if it is monitored. Each target IP can be probed at most once per second (`429` otherwise):
```bash
curl -X POST http://localhost:8080/api/hosts/192.168.1.1/ping
```

Listen to real-time events (Server-Sent Events):
```bash
curl -N http://localhost:8080/api/stream
```

### Authentication

When `DINIS_API_TOKEN` is set, every `/api/*` request must send the token in a header, either `Authorization: Bearer <token>` or `X-API-Key: <token>`. Tokens in the query string (`?token=`) are not accepted, so they never end up in URLs or proxy access logs.

Two kinds of request stay public: the health checks (`/health`, `/api/health`) and the dashboard's static files. In the dashboard, enter the token with the key button in the header; it is stored in your browser.

Browsers can't set headers on `EventSource`, so the SSE stream uses a short-lived ticket instead. Request one with the token, then open the stream within 30 seconds. Each ticket works for one connection:
```bash
TICKET=$(curl -s -X POST -H "Authorization: Bearer $DINIS_API_TOKEN" http://localhost:8080/api/stream/ticket | jq -r .ticket)
curl -N "http://localhost:8080/api/stream?ticket=$TICKET"
```

### API endpoints overview

| Method | Path | Description |
|---|---|---|
| `GET` | `/health`, `/api/health` | Health check (always public; always returns `{"status":"ok"}`) |
| `GET` | `/api/summary` | Host counts by status, alert counts, average latency, probe rate (probes per second and the average gap between probes) and total subnet capacity |
| `GET` | `/api/stream` | Server-Sent Events stream (`?ticket=` when a token is configured) |
| `POST` | `/api/stream/ticket` | Issue a single-use, 30 s stream ticket (requires the API token header) |
| `GET` | `/api/cidrs` | List configured CIDRs |
| `POST` | `/api/cidrs` | Add (or overwrite) a CIDR: `cidr`, `description`, `enabled`, `includeNetAndBcast`, `intervalSec` |
| `PUT` | `/api/cidrs` | Update fields of an existing CIDR (only the fields you send) |
| `DELETE` | `/api/cidrs` | Remove a CIDR (`?cidr=...` or JSON body) and stop monitoring the hosts it enrolled (promoted hosts are kept) |
| `GET` | `/api/discovery/status` | Discovery state: running or not, last and next run, counts |
| `POST` | `/api/discovery/run` | Start a discovery sweep (optional `{"cidr": ...}`; `202` if queued behind a running sweep; max one request per 30 s) |
| `GET` | `/api/hosts` | Host list; see query parameters below |
| `GET` | `/api/hosts/{ip}` | Host detail, including alias and notes |
| `GET` | `/api/hosts/{ip}/history` | Latency and loss history (`?window=1h`) |
| `POST` | `/api/hosts/{ip}/ping` | Probe one IPv4 address now (max 1 per second per IP) |
| `POST` | `/api/hosts/{ip}/promote` | Make a host a static target, monitored even if the range it belongs to is deleted |
| `PUT`, `POST` | `/api/hosts/{ip}/meta` | Set alias and notes. Both are replaced, so an omitted field is cleared |
| `DELETE` | `/api/hosts/{ip}/enrollment` | Remove a discovered or promoted host from the monitored set; see [Removing targets](#removing-targets) |
| `GET` | `/api/subnets/matrix` | Subnet heatmap: hosts grouped by subnet (ranges larger than /24 are split into /24 blocks), with per-block health |
| `GET` | `/api/outliers` | Hosts with packet loss, high latency or high jitter (`?limit=50`, max 500) |
| `GET` | `/api/exclusions` | List exclusion rules |
| `POST` | `/api/exclusions` | Add (or overwrite) an exclusion: `rule` (IP or CIDR), `reason`, `enabled` |
| `DELETE` | `/api/exclusions` | Delete an exclusion rule (`?rule=...` or JSON body) |
| `GET` | `/api/alerts` | Active alerts |
| `POST` | `/api/alerts/acknowledge` | Acknowledge one alert: `ip` or `id`, plus `ackBy` and `note` |
| `POST` | `/api/alerts/acknowledge-all` | Acknowledge all active alerts: `ackBy`, `note` |
| `GET` | `/api/alerts/history` | Resolved alerts, newest first (`?limit=100`, max 500) |
| `GET`, `PUT`, `POST` | `/api/settings` | Read or update runtime settings; see below |

**`/api/hosts` query parameters.**
- With no parameters, the response is a plain JSON array of all hosts.
- With any parameter, it is `{"total", "page", "limit", "totalPages", "hosts"}`.
- Parameters:

  | Parameter | Values |
  |---|---|
  | `page` | 1-based page number |
  | `limit` | page size, default 50, max 500 |
  | `status` | `all`, `up`, `down` (unacknowledged), `ack` (acknowledged DOWN), `pending`, `excluded` |
  | `search` | matches IP, alias, CIDR, notes or exclusion reason |
  | `sort` | `ip-asc`, `ip-desc`, `status`, `latency-asc`, `latency-desc`, `loss` |
  | `lightweight` | `true` leaves out each host's recent latency list |

**History windows.**
- `window` takes a Go duration, e.g. `30m`, `1h`, `24h` or `168h`.
- Windows up to 2 hours return 1-minute rollups; longer windows return hourly rollups, up to 30 days. Each point is timestamped at the start of its minute or hour.
- History is in memory only. It starts empty after a restart, and hourly points first appear after an hour of uptime.

**Probe response.** `/api/hosts/{ip}/ping` returns `{"IP", "Success", "Latency", "LatencyMs", "Error", "Timestamp"}`. Note the capitalised keys, unlike the other endpoints.

### Removing targets

- **Single-IP target (a `/32` CIDR entry).**
  - Delete the CIDR entry to remove the target.
  - Disable it to pause monitoring while keeping its settings. If a larger enabled CIDR also contains the address, the host stays monitored as part of that range.
  - Un-enrolling has no effect while the CIDR entry exists.
- **Host discovered in a larger CIDR.**
  - Deleting or disabling the CIDR stops monitoring all hosts discovered in it.
  - To stop one host for good, add an exclusion for it.
  - Un-enrolling (`DELETE /api/hosts/{ip}/enrollment`) only lasts until the next discovery sweep finds the host again.
- **Promoted host.**
  - Promoted hosts are kept when the range they belong to is deleted or disabled.
  - Un-enroll the host to remove it. If it is inside a configured range and still answers, discovery enrolls it again as an ordinary host; add an exclusion to prevent that.
  - Promoting a single-IP target changes nothing: it is removed when its CIDR entry is deleted.

### Settings

`GET /api/settings` returns the current values. `PUT` or `POST` updates them: fields you leave out keep their current values. Out-of-range values are adjusted to the nearest allowed value. A zero or negative `timeoutMs`, `failThreshold`, `concurrency` or `maxMetricHosts` resets that field to its default.

| Field | Default | Range | Meaning |
|---|---|---|---|
| `intervalSec` | `60` | 0.5–3600 | Default probe interval per host; CIDRs can override it |
| `timeoutMs` | `1000` | up to 30000 | Probe timeout |
| `failThreshold` | `2` | 1–100 | Consecutive failed probes before a host is `DOWN` |
| `concurrency` | `100` | 1–1024 | Parallel probes; changes apply immediately |
| `discoveryIntervalMin` | `240` | ≥ 0 | Minutes between automatic discovery sweeps; `0` turns them off |
| `autoDiscovery` | `true` | — | Whether to sweep at startup and when a CIDR is added |
| `maxMetricHosts` | `10000` | 500–500000 | Hosts that keep latency history |
| `downProbeIntervalSec` | `300` | 0–86400 | Hosts DOWN for longer than this are only probed this often; `0` disables the back-off |

Settings are saved to the data file. `-max-metric-hosts` / `DINIS_MAX_METRIC_HOSTS`, if set, overrides `maxMetricHosts` at every start.

### Live events (SSE)

`/api/stream` sends these events:

| Event | When |
|---|---|
| `summary_update` | Every second; same content as `/api/summary` |
| `host_state_change` | A host changed status (e.g. UP → DOWN) |
| `host_update` | A host was updated by an alert acknowledgement |
| `alert_fired`, `alert_acknowledged`, `alert_resolved` | Alert lifecycle |
| `discovery_started`, `discovery_completed` | Discovery sweeps |
| `desync` | The client fell behind and should reload its data |

A keepalive comment is sent every 15 seconds.

## Configuration

### Command-line flags

| Flag | Environment variable | Default | Description |
|---|---|---|---|
| `-port` | `DINIS_PORT` | `8080` | HTTP listen port for the API and dashboard |
| `-host` | `DINIS_HOST` | `0.0.0.0` | HTTP listen address (all interfaces by default) |
| `-data` | `DINIS_DATA` | `data/dinis.json` | Path to the JSON data file (created with default targets if missing) |
| `-static` | `DINIS_STATIC` | `""` | Serve dashboard files from this directory instead of the built-in ones |
| `-api-token` | `DINIS_API_TOKEN` | `""` | API token; when empty, the API has no authentication |
| `-allowed-hosts` | `DINIS_ALLOWED_HOSTS` | `""` | Comma-separated allowed `Host` header values (DNS rebinding protection). `localhost`, `127.0.0.1` and `::1` are always allowed; `*` allows any |
| `-allowed-client-ips` | `DINIS_ALLOWED_CLIENT_IPS` | `""` | Comma-separated client IPs/CIDRs allowed to use the dashboard and API. Loopback is always allowed |
| `-trusted-proxies` | `DINIS_TRUSTED_PROXIES` | `""` | Comma-separated proxy IPs/CIDRs whose `X-Forwarded-For`, `X-Real-IP` and `X-Forwarded-Host` headers are trusted. The presets `docker` and `private` are identical: they trust all private and loopback ranges. See [Security notes](#security-notes) |
| `-allowed-origins` | `DINIS_ALLOWED_ORIGINS` | `""` | Comma-separated allowed CORS origins. Same-host origins and `localhost`/`127.0.0.1` origins on any port are always allowed |
| `-max-metric-hosts` | `DINIS_MAX_METRIC_HOSTS` | `0` | Hosts that keep latency history. `0` keeps the saved setting (default 10,000); any other value overrides it at start |
| `-influxdb-url` | `INFLUXDB3_URL` | `""` | InfluxDB 3 Core URL (e.g. `http://localhost:8181`); empty turns export off |
| `-influxdb-bucket` | `INFLUXDB3_BUCKET` | `dinis` | InfluxDB database name |
| `-influxdb-token` | `INFLUXDB3_TOKEN` | `""` | InfluxDB token, sent as `Authorization: Bearer` |
| `-version` | — | `false` | Print version and exit |

### Environment variables (Docker Compose)

| Variable | Default | Description |
|---|---|---|
| `NGINX_HTTP_PORT` | `80` | Host port for the DINIS dashboard and REST API (via Nginx) |
| `NGINX_EXPLORER_PORT` | `8888` | Host port for the InfluxDB 3 Explorer UI (via Nginx) |
| `INFLUXDB3_PORT` | `8181` | Host port mapped directly to InfluxDB 3 Core (HTTP API and Flight SQL) |
| `DINIS_PORT` | `8080` | DINIS port inside the container. Leave at `8080`: the Nginx config and health checks expect it |
| `DINIS_DATA` | `/data/dinis.json` | Data file path inside the container (on the `dinis-data` volume) |
| `DINIS_API_TOKEN` | `""` | API token; empty means no authentication |
| `DINIS_ALLOWED_HOSTS` | `""` | Allowed `Host` header values |
| `DINIS_ALLOWED_CLIENT_IPS` | `""` | Client IP/CIDR allow-list |
| `DINIS_TRUSTED_PROXIES` | `docker` | Trusted proxies (the `docker` preset trusts all private ranges) |
| `DINIS_ALLOWED_ORIGINS` | `""` | Allowed CORS origins |
| `DINIS_MAX_METRIC_HOSTS` | `""` | Hosts that keep latency history; empty keeps the saved setting |
| `INFLUXDB3_URL` | `http://influxdb3:8181` | InfluxDB endpoint; export is on by default in compose |
| `INFLUXDB3_BUCKET` | `dinis` | InfluxDB database name |
| `INFLUXDB3_TOKEN` | `""` | InfluxDB admin token; InfluxDB requires it to start with `apiv3_`. **If empty, InfluxDB runs without authentication** |
| `INFLUXDB3_NODE_ID` | `dinis-node` | InfluxDB 3 node identifier |

## InfluxDB data

Each probe result is written as one point:

| | Name | Type | Notes |
|---|---|---|---|
| Measurement | `icmp_probe` | | |
| Tag | `ip` | string | Probed IPv4 address |
| Tag | `subnet` | string | CIDR the host belongs to |
| Tag | `alias` | string | Host alias, or the CIDR description if no alias is set |
| Field | `latency_ms` | float | Round-trip time; `0` when the probe failed |
| Field | `success` | integer | `1` = reply received, `0` = failed |
| Time | | ns | When the probe completed |

Writes are batched every 5 seconds (or every 100 points). If InfluxDB is unreachable, up to 10 MB of points are buffered and retried.

Because failed probes have `latency_ms = 0`, filter on `success` when charting latency. Example SQL:
```sql
SELECT time, ip, latency_ms
FROM icmp_probe
WHERE success = 1 AND time > now() - INTERVAL '1 hour'
ORDER BY time
```
Packet loss per host is `1 - avg(success)` over a time bucket.

### InfluxDB 3 Explorer web UI

To browse tables and run ad-hoc SQL without Grafana:
* Open **`http://<dinis-host-ip>:8888`** in your browser.
* Connect with:
  - **Host URL:** `http://influxdb3:8181` (inside Docker) or `http://<dinis-host-ip>:8181`
  - **Database:** `dinis` (or `INFLUXDB3_BUCKET`)
  - **Token:** `INFLUXDB3_TOKEN`, if configured

The Explorer runs in admin mode and has no login of its own. Anyone who can reach port 8888 can use it.

### Connecting an external Grafana instance

#### Option A: Native InfluxDB 3 / Flight SQL (recommended)
1. In Grafana, go to **Connections** > **Data Sources** > **Add data source** and select **InfluxDB**.
2. Configure:
   - **Query Language:** `SQL`
   - **URL:** `http://<dinis-host-ip>:8181`
   - **Database:** `dinis` (or the value of `INFLUXDB3_BUCKET`)
   - **Token:** your `INFLUXDB3_TOKEN`
   - **Insecure Connection:** **ON** (needed for plain-text HTTP/2 without TLS)
3. Click **Save & test**.

#### Option B: InfluxQL compatibility mode
1. In Grafana, select **InfluxDB**.
2. Configure:
   - **Query Language:** `InfluxQL`
   - **URL:** `http://<dinis-host-ip>:8181`
   - **Database:** `dinis` (or the value of `INFLUXDB3_BUCKET`)
   - **HTTP Method:** `POST`
3. If `INFLUXDB3_TOKEN` is set, under **Custom HTTP Headers** click **Add header**:
   - **Header:** `Authorization`
   - **Value:** `Bearer <your_INFLUXDB3_TOKEN>`
4. Click **Save & test**.

## Security notes

The defaults are built for a trusted lab network. Before running DINIS anywhere else:

- **Set `DINIS_API_TOKEN`.** Without it, anyone who can reach the dashboard can change CIDRs, exclusions and settings, and can make DINIS ping arbitrary addresses.
- **Set `DINIS_ALLOWED_HOSTS`** to the hostnames you use to reach DINIS. This protects against DNS-rebinding attacks from web pages opened in an operator's browser.
- **Narrow `DINIS_TRUSTED_PROXIES`.** The `docker`/`private` preset trusts every private address. That lets any client on a private network set its own `X-Forwarded-For` header and get past `DINIS_ALLOWED_CLIENT_IPS`. In compose, set it to the compose network instead (`172.30.0.0/24`).
- **Protect InfluxDB.**
  - Set a strong `INFLUXDB3_TOKEN`; don't keep the placeholder from `.env.example`.
  - Port 8181 is published on all interfaces. Without a token, InfluxDB accepts unauthenticated reads and writes.
- **Restrict port 8888.** The InfluxDB Explorer has no login.

## Architecture / project structure

```
.
├── main.go               # Entry point: parses flags/env vars and wires the components together
├── Dockerfile            # Multi-stage build, Alpine runtime image
├── docker-compose.yml    # DINIS, InfluxDB 3 Core, InfluxDB 3 Explorer and Nginx
├── .env.example          # Template for docker compose settings (copy to .env)
├── docker/
│   └── nginx/            # Nginx reverse proxy configuration
├── data/                 # Default location of the JSON data file
├── verify_e2e.py         # End-to-end API test script (run in CI against the compose stack)
├── .github/workflows/    # CI: gofmt, go vet, race tests, config validation, e2e
└── pkg/
    ├── alerts/           # Alert lifecycle: firing, acknowledgement, resolution, history
    ├── influxdb/         # Batched InfluxDB 3 line-protocol exporter
    ├── network/          # CIDR parsing, IP expansion, exclusion matching
    ├── pinger/           # ICMP sockets, probe engine, pacing, UP/DOWN evaluation
    ├── server/           # Coordinator (ties engine, alerts, discovery and storage together), REST API, SSE
    │   └── web_dist/     # Embedded web dashboard
    ├── store/            # JSON persistence with atomic writes and a single-instance lock
    └── timeseries/       # In-memory latency history, rollups, outlier detection
```
