# DINIS

DINIS is a daemon that monitors network hosts with ICMP echo requests (ping). DINIS has a web dashboard and a REST API.

You give DINIS IPv4 CIDR ranges and single IPv4 addresses. DINIS then does these tasks:

- It finds the hosts that send an echo reply.
- It probes these hosts at a fixed interval.
- It records the latency and the packet loss of each host.
- It starts an alert when a host does not reply.
- It can export each probe result to InfluxDB 3.

## How DINIS operates

### Discovery

DINIS does not probe all addresses of a CIDR range at each interval. A discovery sweep sends one probe to each address in the range. The timeout of each probe is 750 ms. DINIS monitors only the hosts that reply.

DINIS starts a discovery sweep at these times:

- 5 seconds after DINIS starts, if `autoDiscovery` is `true`.
- Each `discoveryIntervalMin` minutes. The default is 240 minutes.
- When you add a CIDR, if `autoDiscovery` is `true`.
- When you start a sweep in the dashboard or through the API.

Rules for discovery sweeps:

- If a sweep runs and you start a new sweep, DINIS puts the new sweep in a queue. The new sweep starts when the current sweep is complete.
- DINIS calculates the periodic schedule from the last full sweep. A sweep of only one range does not change this schedule.
- If `autoDiscovery` is `false` and `discoveryIntervalMin` is more than 0, the first full sweep starts approximately 1 minute after DINIS starts.
- DINIS does not probe the addresses in an exclusion rule.
- DINIS always monitors single-IP targets (`/32`), also if they do not reply.

### Monitored hosts

DINIS does not remove a host that does not reply. The status of the host changes to `DOWN`, and DINIS starts an alert.

DINIS stops the probes to a host after one of these actions:

- You delete the CIDR of the host.
- You disable the CIDR of the host.
- You add an exclusion rule for the host.

If you un-enroll a discovered host, the next discovery sweep can find the host again. Refer to [Remove a target](#remove-a-target).

### Probe schedule

DINIS probes each host each `intervalSec` seconds. The default interval is 60 seconds. You can set a different interval for each CIDR. The minimum interval is 0.5 seconds.

Each host has a fixed time slot in its interval. DINIS distributes the probes equally across the interval. Each host has a constant probe rate.

- When DINIS adds a host, DINIS sends the first probe not more than 5 seconds later. This rule is for new targets and for hosts that a discovery sweep finds.
- When you change an interval, DINIS uses the new interval immediately.
- A new host has the status `PENDING`. After the first reply, the status changes to `UP`.
- After `failThreshold` failed probes in sequence, the status changes to `DOWN`. The default is 2.
- After `recoveryThreshold` replies in sequence, a `DOWN` host changes to `UP`. The default is 2. Thus a host with intermittent packet loss does not start a new alert for each lost probe.
- The packet loss of a host is the total loss since the start of DINIS.

### Probe capacity during outages

A probe to a host that does not reply uses a worker for the full timeout. Two rules keep the probes to all other hosts at their normal interval during an outage:

1. Probes to hosts with a failed last probe can use a maximum of three quarters of the workers (`concurrency`). The other workers are for the hosts that reply.
2. If a host is `DOWN` for more than `downProbeIntervalSec` seconds and its last probe failed, DINIS probes the host only each `downProbeIntervalSec` seconds. The default is 300 seconds. This rule is only for hosts with a shorter normal interval.

When a host of rule 2 replies again, DINIS finds this at the next probe. This can occur up to `downProbeIntervalSec` seconds later. After this reply, DINIS probes the host at its normal interval again. To probe `DOWN` hosts at their normal interval, set `downProbeIntervalSec` to `0`.

### Alerts

When the status of a host changes to `DOWN`, DINIS starts an alert. When the status changes to `UP` again, DINIS resolves the alert automatically. An operator can acknowledge an alert with a name and a note.

DINIS also closes the alert of a host when it stops the monitoring of the host. The field `resolveReason` of a resolved alert gives the reason:

| `resolveReason` | Reason |
|---|---|
| `recovered` | The host replies again. |
| `excluded` | An exclusion rule now applies to the host. |
| `removed` | DINIS does not monitor the host now. For example, you deleted or disabled its CIDR, or you un-enrolled the host. |

DINIS shows the alerts only in the dashboard. DINIS does not send email, webhook messages, or other notifications.

### Data storage

DINIS keeps its data in these locations:

- **Data file.** This JSON file contains the CIDRs, the exclusion rules, the discovered hosts, the host aliases and notes, and the settings.
- **Alert state file.** This file is in the same directory as the data file. Its name is the name of the data file with `.alerts.json` in place of the extension. For example, `data/dinis.json` gives `data/dinis.alerts.json`. The file contains:
  - The active alerts and their acknowledgements.
  - The last 500 resolved alerts. Each host has a maximum of 20 of these alerts. Thus a host with frequent outages cannot remove the alerts of all other hosts from the history.

  DINIS saves this file each 5 seconds while alerts change. DINIS also saves the file when it stops.
- **Memory.** DINIS keeps the latency history only in memory. After a restart, the history is empty. For each host, the history contains:
  - The last 128 raw samples.
  - 1-minute rollups for 2 hours.
  - 1-hour rollups for 30 days.

  Each rollup contains one full clock minute or one full clock hour. For long-term data, use the InfluxDB export. Refer to [Retention and query range](#retention-and-query-range).

  The history of one host uses approximately 44 KB of memory. For the default `maxMetricHosts` of 10,000 hosts, this is approximately 430 MB. At the start and when you change `maxMetricHosts`, DINIS writes an estimate to the log.
- **Lock file.** The file `<data file>.lock` makes sure that only one DINIS process uses a data file.

After a restart, each active alert keeps its ID, its start time, and its acknowledgement. A host with an active alert starts with the status `DOWN`. All other hosts start with the status `PENDING`.

### Limits

- DINIS operates only with IPv4. DINIS accepts an IPv6 address as a `/128` target. But each probe to this target fails, and its status is `DOWN`.
- A CIDR entry can contain a maximum of a /16 range (65,536 addresses). DINIS rejects larger ranges. For a larger range, add more than one CIDR entry.
- When DINIS starts without a data file, it adds three targets: `127.0.0.1`, `1.1.1.1`, and `8.8.8.8`. The last two targets are public DNS resolvers. To stop the pings to them, delete their CIDR entries.
- DINIS keeps the latency history for a maximum of `maxMetricHosts` hosts. The default is 10,000. DINIS also probes the other hosts and starts alerts for them. But these hosts have no history charts. Each host with a history uses approximately 44 KB of memory.

## Prerequisites

- **Go 1.26.5 or a later version.** You must have this version to build DINIS from the source code. Refer to `go.mod`.
- **Permission for ICMP sockets on Linux.** If possible, DINIS uses an unprivileged ping socket. If this is not possible, DINIS uses a raw socket. Use one of these options:
  - Option 1, unprivileged ICMP:
    ```bash
    sudo sysctl -w net.ipv4.ping_group_range="0 2147483647"
    ```
    Linux does not keep this setting after a reboot. To keep the setting, add the line `net.ipv4.ping_group_range = 0 2147483647` to a file in `/etc/sysctl.d/`.
  - Option 2, the `CAP_NET_RAW` capability:
    ```bash
    sudo setcap cap_net_raw+ep ./dinis
    ```

  If DINIS cannot open an ICMP socket, DINIS starts the system `ping` command for each probe. For this method, the system must have the `ping` command. This method is much slower. Use it only for small installations.
- **Docker and Docker Compose.** These are necessary only for the installation with containers.

## Installation

### Install the standalone binary

1. Build DINIS from the source code:
   ```bash
   go build -o dinis .
   ```
2. Start DINIS:
   ```bash
   ./dinis -port 8080 -data data/dinis.json
   ```
3. Open the dashboard at `http://localhost:8080`.

**CAUTION:** Read [Security](#security) before you connect DINIS to a network that is not safe. By default, DINIS listens on all interfaces (`0.0.0.0`) and does not use authentication.

**NOTE:** In this installation, the InfluxDB export is off. To set the export to on, use the `-influxdb-url` flag.

### Install with Docker Compose

1. Copy the example file for the environment variables:
   ```bash
   cp .env.example .env
   ```
2. Edit `.env`. Set `INFLUXDB3_TOKEN`, `DINIS_API_TOKEN`, and `INFLUXDB3_EXPLORER_SESSION_KEY`.
3. Start the containers:
   ```bash
   docker compose up -d
   ```

The stack has four containers:

| Service | Address | Notes |
|---|---|---|
| Nginx → DINIS | `http://<server-ip>` (port `NGINX_HTTP_PORT`, default 80) | Dashboard and REST API, through the reverse proxy |
| Nginx → InfluxDB 3 Explorer | `http://<server-ip>:8888` (`NGINX_EXPLORER_PORT`) | Admin mode with the configured connection `dinis`. **There is no login.** |
| InfluxDB 3 Core | `http://<server-ip>:8181` (`INFLUXDB3_PORT`) | Docker publishes this port directly on the host, not through Nginx. |
| DINIS | Only in the container network (`dinis:8080`) | Has the `NET_RAW` capability for ICMP |

**NOTES:**

- In the Compose stack, the InfluxDB export is always on. The default value of `INFLUXDB3_URL` is `http://influxdb3:8181`.
- If `INFLUXDB3_TOKEN` is empty, InfluxDB operates without authentication. Docker also publishes port 8181 on the host in this condition.
- `.env.example` contains a placeholder token (`apiv3_dinis_secret_token`). Replace this token with a new secret value.
- InfluxDB keeps the probe results for `INFLUXDB3_RETENTION_DAYS` days. The default is 60. Refer to [Retention and query range](#retention-and-query-range).
- Docker keeps a maximum of five log files of 10 MB for each container.
- When DINIS stops, it has 30 seconds to save the alerts and to send the last points to InfluxDB.

## Use the REST API

### Examples

Add a CIDR range. If `autoDiscovery` is `true`, DINIS also starts a discovery sweep of this range:
```bash
curl -X POST http://localhost:8080/api/cidrs \
  -H "Content-Type: application/json" \
  -d '{"cidr": "192.168.1.0/24", "description": "LAN"}'
```

Start a discovery sweep of all enabled CIDRs:
```bash
curl -X POST http://localhost:8080/api/discovery/run
```

- To sweep only one range, send `{"cidr": "192.168.1.0/24"}` as the request body.
- If a sweep runs, DINIS puts your sweep in a queue and returns `202`.
- You can send a maximum of one request each 30 seconds. DINIS returns `429` for more requests.

Show the summary values:
```bash
curl http://localhost:8080/api/summary
```

Show the monitored hosts, one page at a time:
```bash
curl "http://localhost:8080/api/hosts?page=1&limit=50&status=up&sort=status"
```

Probe a host one time, immediately:
```bash
curl -X POST http://localhost:8080/api/hosts/192.168.1.1/ping
```

- You can probe any IPv4 address. If DINIS monitors the host, DINIS updates the status of the host.
- If an exclusion rule applies to the host, DINIS only returns the result. The status of the host does not change.
- You can probe each address a maximum of one time each second. DINIS returns `429` for more probes.

Receive the real-time events (Server-Sent Events):
```bash
curl -N http://localhost:8080/api/stream
```

### Authentication

If you set `DINIS_API_TOKEN`, each `/api/*` request must send the token in a header. Use one of these headers:

- `Authorization: Bearer <token>`
- `X-API-Key: <token>`

DINIS does not accept a token in the query string (`?token=`). Because of this, tokens do not appear in URLs or in the access logs of proxies.

These requests do not use authentication:

- The health checks (`/health` and `/api/health`).
- The static files of the dashboard.

To use the token in the dashboard, click the key button in the header. Then enter the token. The browser keeps the token.

Browsers cannot set headers for an `EventSource`. Because of this, the SSE stream uses a ticket. To open the stream, do these steps:

1. Send a request for a ticket. Send the token with this request.
2. Open the stream not more than 30 seconds after you receive the ticket.

You can use each ticket for one connection only.

```bash
TICKET=$(curl -s -X POST -H "Authorization: Bearer $DINIS_API_TOKEN" http://localhost:8080/api/stream/ticket | jq -r .ticket)
curl -N "http://localhost:8080/api/stream?ticket=$TICKET"
```

### API endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health`, `/api/health` | Health check. It does not use authentication. It always returns `{"status":"ok"}`. |
| `GET` | `/api/summary` | Values for all hosts: the number of hosts for each status, the number of alerts, the average latency, the probe rate, and the subnet capacity. The probe rate gives the probes each second and the average time between two probes. |
| `GET` | `/api/stream` | Server-Sent Events stream. If you set a token, add a ticket with `?ticket=`. |
| `POST` | `/api/stream/ticket` | Gives a ticket for one stream connection. You can use the ticket for 30 seconds. The request must contain the API token header. |
| `GET` | `/api/cidrs` | Returns the configured CIDRs. |
| `POST` | `/api/cidrs` | Adds a CIDR, or replaces the CIDR with the same range. Fields: `cidr`, `description`, `enabled`, `includeNetAndBcast`, `intervalSec`. |
| `PUT` | `/api/cidrs` | Changes the fields of a CIDR. DINIS changes only the fields in the request. |
| `DELETE` | `/api/cidrs` | Deletes a CIDR (`?cidr=...` or JSON body). DINIS stops the probes to the hosts from this CIDR. DINIS keeps the promoted hosts. |
| `GET` | `/api/discovery/status` | Returns the discovery state: a sweep runs or not, the last run, the next run, and the counts. |
| `POST` | `/api/discovery/run` | Starts a discovery sweep. You can send `{"cidr": ...}` as the body. If a sweep runs, DINIS puts the new sweep in a queue and returns `202`. Maximum one request each 30 seconds. |
| `GET` | `/api/hosts` | Returns the hosts. Refer to the query parameters below. |
| `GET` | `/api/hosts/{ip}` | Returns the details of one host, with the alias and the notes. |
| `GET` | `/api/hosts/{ip}/history` | Returns the latency and loss history (`?window=1h`). Refer to [History windows](#history-windows). |
| `POST` | `/api/hosts/{ip}/ping` | Probes one IPv4 address immediately. Maximum one probe each second for each address. |
| `POST` | `/api/hosts/{ip}/promote` | Makes the host a static target. DINIS monitors a static target also after you delete its range. |
| `PUT`, `POST` | `/api/hosts/{ip}/meta` | Sets the alias and the notes. The request replaces both values. If a field is not in the request, its value becomes empty. |
| `DELETE` | `/api/hosts/{ip}/enrollment` | Removes a discovered host or a promoted host from the monitored hosts. For a single-IP CIDR target, DINIS returns `409`. Refer to [Remove a target](#remove-a-target). |
| `GET` | `/api/subnets/matrix` | Returns the subnet heatmap: the hosts in groups for each subnet, with the health of each block. DINIS divides ranges larger than /24 into /24 blocks. |
| `GET` | `/api/outliers` | Returns the hosts with packet loss, high latency, or high jitter (`?limit=50`, maximum 500). |
| `GET` | `/api/exclusions` | Returns the exclusion rules. |
| `POST` | `/api/exclusions` | Adds an exclusion rule, or replaces the rule with the same text. Fields: `rule` (IP or CIDR), `reason`, `enabled`. |
| `DELETE` | `/api/exclusions` | Deletes an exclusion rule (`?rule=...` or JSON body). If no rule has this text, DINIS returns `404`. |
| `GET` | `/api/alerts` | Returns the active alerts. |
| `POST` | `/api/alerts/acknowledge` | Acknowledges one alert. Fields: `ip` or `id`, and `ackBy` and `note`. |
| `POST` | `/api/alerts/acknowledge-all` | Acknowledges all active alerts. Fields: `ackBy`, `note`. |
| `GET` | `/api/alerts/history` | Returns the resolved alerts, the last alert first (`?limit=100`, maximum 500). The field `resolveReason` gives the reason. Refer to [Alerts](#alerts). |
| `GET`, `PUT`, `POST` | `/api/settings` | Reads or changes the runtime settings. Refer to [Settings](#settings). |

#### Query parameters for `/api/hosts`

- If you send no parameters, the response is a JSON array of all hosts.
- If you send one or more parameters, the response is a JSON object: `{"total", "page", "limit", "totalPages", "hosts"}`.

| Parameter | Values |
|---|---|
| `page` | The page number. The first page is 1. |
| `limit` | The number of hosts on each page. The default is 50. The maximum is 500. |
| `status` | `all`, `up`, `down` (not acknowledged), `ack` (acknowledged `DOWN` hosts), `pending`, `excluded` |
| `search` | The text to find in the IP, the alias, the CIDR, the notes, or the exclusion reason |
| `sort` | `ip-asc`, `ip-desc`, `status`, `latency-asc`, `latency-desc`, `loss` |
| `lightweight` | If `true`, DINIS prepares the response faster. DINIS adds the recent latency list only to the hosts on the page. |

#### History windows

- The `window` parameter is a Go duration, for example `30m`, `1h`, `24h`, or `168h`.
- For a window of 2 hours or less, DINIS returns 1-minute rollups. For a longer window, DINIS returns 1-hour rollups and the current hour. The maximum is 30 days.
- The point of the current hour has `"partial": true`. It contains the data up to the last full minute.
- The timestamp of each point is the start of its minute or its hour.
- `p50LatencyMs`, `p95LatencyMs`, and `p99LatencyMs` are the percentiles of all samples in the point. For a 1-hour point, the error is approximately 2 % of the value.
- `jitterMs` is the mean difference between two consecutive successful probes. The first difference starts at the last probe before the point. If the point has no such pair, the value is `null`.
- DINIS keeps the history only in memory. After a restart, the history is empty.
- The first 1-hour point is available shortly after the end of the first full clock hour. This point contains only the data after the start of DINIS.

#### Probe response

`/api/hosts/{ip}/ping` returns `{"IP", "Success", "Latency", "LatencyMs", "Error", "Timestamp"}`. These keys start with a capital letter. The keys of all other endpoints start with a lowercase letter.

### Remove a target

The procedure is different for each type of target.

**Single-IP target (a `/32` CIDR entry):**

- To remove the target, delete its CIDR entry.
- To stop the probes but keep the settings, disable the CIDR entry. If an enabled larger CIDR also contains the address, DINIS continues to monitor the host as part of that range.
- You cannot un-enroll this target. DINIS returns `409` and makes no change.

**Host that a discovery sweep found in a larger CIDR:**

- When you delete or disable the CIDR, DINIS stops the probes to all hosts from that CIDR.
- To stop the probes to one host permanently, add an exclusion rule for the host.
- If you un-enroll the host (`DELETE /api/hosts/{ip}/enrollment`), the next discovery sweep can find the host again.

**Promoted host:**

- DINIS keeps a promoted host when you delete or disable its range.
- To remove a promoted host, un-enroll it. If the host is in a configured range and replies, the next discovery sweep adds it again as a normal host. To prevent this, add an exclusion rule.
- If you promote a single-IP target, nothing changes. DINIS removes the target when you delete its CIDR entry.

### Settings

`GET /api/settings` returns the current values. `PUT` or `POST` changes the values. If a field is not in the request, DINIS keeps its current value.

If a value is out of range, DINIS uses the nearest value in the range. If `timeoutMs`, `failThreshold`, `concurrency`, or `maxMetricHosts` is zero or negative, DINIS uses the default value.

| Field | Default | Range | Description |
|---|---|---|---|
| `intervalSec` | `60` | 0.5–3600 | The default probe interval for each host. A CIDR can have a different interval. |
| `timeoutMs` | `1000` | 1–30000 | The probe timeout. |
| `failThreshold` | `2` | 1–100 | The number of failed probes in sequence before the status changes to `DOWN`. |
| `recoveryThreshold` | `2` | 1–100 | The number of replies in sequence before a `DOWN` host changes to `UP`. `1` resolves the alert at the first reply. |
| `concurrency` | `100` | 1–1024 | The number of probes at the same time. DINIS uses a new value immediately. |
| `discoveryIntervalMin` | `240` | 0 or more | The minutes between two automatic discovery sweeps. `0` stops the automatic sweeps. |
| `autoDiscovery` | `true` | — | If `true`, DINIS starts a sweep when DINIS starts and when you add a CIDR. |
| `maxMetricHosts` | `10000` | 500–500000 | The maximum number of hosts with a latency history. |
| `downProbeIntervalSec` | `300` | 0–86400 | The probe interval for hosts that are `DOWN` for longer than this time. `0` stops this function. |

DINIS saves the settings in the data file. If you set `-max-metric-hosts` or `DINIS_MAX_METRIC_HOSTS`, this value replaces `maxMetricHosts` at each start.

### Live events (SSE)

`/api/stream` sends these events:

| Event | Description |
|---|---|
| `summary_update` | Each second. The content is the same as `/api/summary`. |
| `host_state_change` | The status of a host changed, for example from `UP` to `DOWN`. |
| `host_update` | An alert acknowledgement changed a host. |
| `alert_fired`, `alert_acknowledged`, `alert_resolved` | An alert started, an operator acknowledged an alert, or DINIS resolved an alert. |
| `discovery_started`, `discovery_completed` | A discovery sweep started or stopped. |
| `desync` | The client did not receive some events. The client must load all data again. |

DINIS also sends a keepalive comment each 15 seconds.

## Configuration

### Command-line flags

| Flag | Environment variable | Default | Description |
|---|---|---|---|
| `-port` | `DINIS_PORT` | `8080` | The HTTP port for the API and the dashboard. |
| `-host` | `DINIS_HOST` | `0.0.0.0` | The HTTP listen address. The default is all interfaces. |
| `-data` | `DINIS_DATA` | `data/dinis.json` | The path of the JSON data file. If the file does not exist, DINIS makes the file and adds the default targets. |
| `-static` | `DINIS_STATIC` | `""` | A directory with dashboard files. DINIS uses these files in place of the built-in files. |
| `-api-token` | `DINIS_API_TOKEN` | `""` | The API token. If it is empty, the API does not use authentication. |
| `-allowed-hosts` | `DINIS_ALLOWED_HOSTS` | `""` | A list of accepted `Host` header values, with commas between the items. This gives protection against DNS rebinding. DINIS always accepts `localhost`, `127.0.0.1`, and `::1`. The value `*` accepts all hosts. |
| `-allowed-client-ips` | `DINIS_ALLOWED_CLIENT_IPS` | `""` | A list of client IPs and CIDRs that can use the dashboard and the API, with commas between the items. DINIS always accepts loopback addresses. |
| `-trusted-proxies` | `DINIS_TRUSTED_PROXIES` | `""` | A list of proxy IPs and CIDRs, with commas between the items. DINIS trusts the `X-Forwarded-For`, `X-Real-IP`, and `X-Forwarded-Host` headers from these proxies. The presets `docker` and `private` are the same. They trust all private ranges and loopback ranges. Refer to [Security](#security). |
| `-allowed-origins` | `DINIS_ALLOWED_ORIGINS` | `""` | A list of accepted CORS origins, with commas between the items. DINIS always accepts origins with the same host. DINIS also accepts `localhost` and `127.0.0.1` origins on all ports. |
| `-max-metric-hosts` | `DINIS_MAX_METRIC_HOSTS` | `0` | The maximum number of hosts with a latency history. `0` keeps the saved setting. The default of the saved setting is 10,000. A different value replaces the saved setting at each start. |
| `-influxdb-url` | `INFLUXDB3_URL` | `""` | The URL of InfluxDB 3 Core, for example `http://localhost:8181`. If it is empty, the export is off. |
| `-influxdb-bucket` | `INFLUXDB3_BUCKET` | `dinis` | The name of the InfluxDB database. |
| `-influxdb-token` | `INFLUXDB3_TOKEN` | `""` | The InfluxDB token. DINIS sends it as `Authorization: Bearer`. |
| `-influxdb-retention-days` | `INFLUXDB3_RETENTION_DAYS` | `60` | The retention period in days that DINIS sets on the InfluxDB database. `0` does not change the database. Refer to [Retention and query range](#retention-and-query-range). |
| `-version` | — | `false` | Shows the version, then DINIS stops. |

### Environment variables for Docker Compose

| Variable | Default | Description |
|---|---|---|
| `NGINX_HTTP_PORT` | `80` | The host port for the dashboard and the REST API, through Nginx. |
| `NGINX_EXPLORER_PORT` | `8888` | The host port for InfluxDB 3 Explorer, through Nginx. |
| `INFLUXDB3_PORT` | `8181` | The host port for InfluxDB 3 Core (HTTP API and Flight SQL). Docker publishes this port directly. |
| `DINIS_PORT` | `8080` | The DINIS port in the container. Do not change this value. The Nginx configuration and the health checks use port 8080. |
| `DINIS_DATA` | `/data/dinis.json` | The path of the data file in the container, on the `dinis-data` volume. |
| `DINIS_API_TOKEN` | `""` | The API token. If it is empty, the API does not use authentication. |
| `DINIS_ALLOWED_HOSTS` | `""` | The accepted `Host` header values. |
| `DINIS_ALLOWED_CLIENT_IPS` | `""` | The client IPs and CIDRs that can use DINIS. |
| `DINIS_TRUSTED_PROXIES` | `docker` | The trusted proxies. The `docker` preset trusts all private ranges. |
| `DINIS_ALLOWED_ORIGINS` | `""` | The accepted CORS origins. |
| `DINIS_MAX_METRIC_HOSTS` | `""` | The maximum number of hosts with a latency history. If it is empty, DINIS keeps the saved setting. |
| `INFLUXDB3_URL` | `http://influxdb3:8181` | The InfluxDB endpoint. In Docker Compose, the export is on by default. The Explorer connection also uses this value. |
| `INFLUXDB3_BUCKET` | `dinis` | The name of the InfluxDB database. The Explorer connection also uses this value. |
| `INFLUXDB3_TOKEN` | `""` | The InfluxDB admin token. InfluxDB accepts only tokens that start with `apiv3_`. The Explorer connection also uses this value. **If the token is empty, InfluxDB operates without authentication.** |
| `INFLUXDB3_NODE_ID` | `dinis-node` | The node identifier of InfluxDB 3. |
| `INFLUXDB3_RETENTION_DAYS` | `60` | The number of days that InfluxDB keeps the probe results. All of these days stay available for queries. Use a whole number. `0` does not change the database. Refer to [Retention and query range](#retention-and-query-range). |
| `INFLUXDB3_EXPLORER_SESSION_KEY` | `""` | The session key of InfluxDB 3 Explorer. Explorer keeps its saved settings only while this key does not change. If it is empty, Docker Compose uses a fixed default key. |

## InfluxDB data

DINIS writes each probe result as one point:

| Element | Name | Type | Description |
|---|---|---|---|
| Measurement | `icmp_probe` | — | — |
| Tag | `ip` | string | The IPv4 address of the host. |
| Tag | `subnet` | string | The CIDR of the host. |
| Tag | `alias` | string | The alias of the host. If the host has no alias, the description of the CIDR. |
| Field | `latency_ms` | float | The round-trip time. If the probe failed, the value is `0`. |
| Field | `success` | integer | `1`: DINIS received a reply. `0`: the probe failed. |
| Time | — | ns | The time when the probe was complete. |

DINIS sends the points to InfluxDB in batches. DINIS sends a batch each 5 seconds or after 100 points. If InfluxDB is not available, DINIS keeps a maximum of 10 MB of points and sends them again later. If this buffer is full, DINIS deletes the oldest points first.

A failed probe has `latency_ms = 0`. Because of this, use only the points with `success = 1` for latency charts. Example SQL query:

```sql
SELECT time, ip, latency_ms
FROM icmp_probe
WHERE success = 1 AND time > now() - INTERVAL '1 hour'
ORDER BY time
```

The packet loss of a host in a time bucket is `1 - avg(success)`.

### Retention and query range

Before DINIS writes the first point, it sets a retention period on the database. The period is `INFLUXDB3_RETENTION_DAYS` days. The default is 60 days.

- If the database does not exist, DINIS creates it with this retention period.
- If the database exists, DINIS changes its retention period to this value. This also applies to a database from an earlier version of DINIS. InfluxDB then deletes the data that is older than the retention period.
- If InfluxDB is not available, DINIS keeps the points in its buffer and tries again at the next batch. DINIS sends no point before the retention period is set.
- If InfluxDB refuses the retention period, DINIS writes a warning to the log. Then DINIS writes the points, and the database keeps its current retention period. For example, an old InfluxDB version cannot change the retention period of a database.
- If `INFLUXDB3_RETENTION_DAYS` is `0`, DINIS does not change the database. If InfluxDB creates the database at the first write, the data does not expire.

InfluxDB 3 Core can query a time range only if two settings are large enough. In the Docker Compose stack, the `influxdb3` service sets them from `INFLUXDB3_RETENTION_DAYS`:

| InfluxDB setting | Value in the Compose stack | InfluxDB default |
|---|---|---|
| `--gen1-lookback-duration` | The retention period | 1 month. At the start, InfluxDB loads the index of the data files only for this time. |
| `--query-file-limit` | 288 × the number of days (two files for each 10-minute block) | 432 files, approximately 72 hours |

Thus you can query all data in the retention period, also after a restart of InfluxDB. If you use InfluxDB without the Compose stack, set these two options yourself.

**NOTE:** A query over many days reads many files. For example, a query over 60 days reads up to 17,280 files. Such a query is slow and uses much memory in the InfluxDB container.

### InfluxDB 3 Explorer

Use InfluxDB 3 Explorer to examine the tables and to run SQL queries without Grafana. Open `http://<dinis-host-ip>:8888` in your browser.

In the Docker Compose stack, Explorer starts with a configured connection named `dinis`. It is not necessary to configure this connection manually. The connection uses these values from `.env`:

| Explorer value | Variable in `.env` | Default |
|---|---|---|
| Server name | — | `dinis` |
| Server URL | `INFLUXDB3_URL` | `http://influxdb3:8181` |
| Token | `INFLUXDB3_TOKEN` | empty |
| Database | `INFLUXDB3_BUCKET` | `dinis` |

Docker Compose gives these values to the Explorer container as the environment variables `DEFAULT_SERVER_NAME`, `DEFAULT_INFLUX_SERVER`, `DEFAULT_API_TOKEN`, and `DEFAULT_INFLUX_DATABASE`. After you change a value in `.env`, run `docker compose up -d` again.

Explorer keeps its settings and saved connections on the `influxdb-explorer-data` volume. After a restart, Explorer can read these settings only with the same session key. Set `INFLUXDB3_EXPLORER_SESSION_KEY` in `.env` one time, and do not change it. To make a key, use `openssl rand -hex 32`. If the variable is empty, Docker Compose uses a fixed default key.

**CAUTION:** Limit the access to port 8888. The Explorer operates in admin mode and has no login. The configured connection uses the InfluxDB admin token. Because of this, all persons who can connect to port 8888 have admin access to InfluxDB.

### Connect an external Grafana instance

Use option A if possible.

#### Option A: Native InfluxDB 3 with Flight SQL

1. In Grafana, go to **Connections** > **Data Sources** > **Add data source**.
2. Select **InfluxDB**.
3. Set these values:
   - **Query Language:** `SQL`
   - **URL:** `http://<dinis-host-ip>:8181`
   - **Database:** `dinis`, or the value of `INFLUXDB3_BUCKET`
   - **Token:** the value of `INFLUXDB3_TOKEN`
   - **Insecure Connection:** **ON**. This value is necessary for HTTP/2 without TLS.
4. Click **Save & test**.

#### Option B: InfluxQL compatibility mode

1. In Grafana, select **InfluxDB**.
2. Set these values:
   - **Query Language:** `InfluxQL`
   - **URL:** `http://<dinis-host-ip>:8181`
   - **Database:** `dinis`, or the value of `INFLUXDB3_BUCKET`
   - **HTTP Method:** `POST`
3. If you set `INFLUXDB3_TOKEN`, go to **Custom HTTP Headers** and click **Add header**.
4. Set these values for the header:
   - **Header:** `Authorization`
   - **Value:** `Bearer <your_INFLUXDB3_TOKEN>`
5. Click **Save & test**.

## Security

The default settings are for a safe test network. Before you use DINIS on a different network, do these steps:

- **Set `DINIS_API_TOKEN`.** If you do not set a token, all persons who can connect to the dashboard can change the CIDRs, the exclusions, and the settings. They can also make DINIS ping any address.
- **Set `DINIS_ALLOWED_HOSTS` to the host names that you use for DINIS.** This gives protection against DNS rebinding attacks from web pages in the browser of an operator.
- **Set `DINIS_TRUSTED_PROXIES` to a smaller range.** The `docker` and `private` presets trust all private addresses. Because of this, each client on a private network can send a false `X-Forwarded-For` header. With this header, the client can pass the `DINIS_ALLOWED_CLIENT_IPS` filter. In Docker Compose, set this variable to the Compose network (`172.30.0.0/24`).
- **Protect InfluxDB.**
  - Set a strong `INFLUXDB3_TOKEN`. Do not use the placeholder from `.env.example`.
  - Docker publishes port 8181 on all interfaces. If you do not set a token, InfluxDB accepts reads and writes without authentication.
- **Limit the access to port 8888.** The InfluxDB Explorer has no login, and its configured connection uses the admin token.

## Project structure

```
.
├── main.go               # Entry point. Reads the flags and the environment variables, and connects the components.
├── Dockerfile            # Multi-stage build with an Alpine runtime image
├── docker-compose.yml    # DINIS, InfluxDB 3 Core, InfluxDB 3 Explorer, and Nginx
├── .env.example          # Template for the Docker Compose settings. Copy it to .env.
├── docker/
│   └── nginx/            # Configuration of the Nginx reverse proxy
├── data/                 # Default directory for the data file and the alert state file
├── verify_e2e.py         # End-to-end API test script. CI runs it on the Compose stack.
├── .github/workflows/    # CI: gofmt, go vet, tests with the race detector, configuration checks, end-to-end test
└── pkg/
    ├── alerts/           # Alert life cycle: start, acknowledgement, resolution, history, export for restarts
    ├── influxdb/         # InfluxDB 3 exporter for line protocol, with batches
    ├── network/          # CIDR parser, IP expansion, exclusion rules
    ├── pinger/           # ICMP sockets, probe engine, probe schedule for each host, UP and DOWN status
    ├── server/           # Coordinator for the engine, the alerts, the discovery, and the storage. REST API and SSE.
    │   └── web_dist/     # Built-in web dashboard
    ├── store/            # Data file and alert state file with atomic writes, lock for one process
    └── timeseries/       # Latency history in memory, rollups, outlier detection
```
