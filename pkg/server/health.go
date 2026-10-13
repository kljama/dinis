package server

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"
)

// Health check thresholds
const (
	// healthLagFail: probes of hosts that reply are this late. DINIS cannot keep its schedule
	// (engine stuck, or too few workers for the probe rate).
	healthLagFail = 2 * time.Minute
	// healthSuspectLagDegraded: probes of failing hosts are this late (all suspect slots are
	// busy during a large outage).
	healthSuspectLagDegraded = 5 * time.Minute
)

// healthInfluxStale: data waits for InfluxDB and no write succeeded for this long.
var healthInfluxStale = 2 * time.Minute

// Health states, worst last
const (
	healthOK       = "ok"
	healthDegraded = "degraded"
	healthFail     = "fail"
)

func worse(a, b string) string {
	rank := map[string]int{healthOK: 0, healthDegraded: 1, healthFail: 2}
	if rank[b] > rank[a] {
		return b
	}
	return a
}

// componentHealth is the state of one part of DINIS.
type componentHealth struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type probingHealth struct {
	componentHealth
	ScheduledHosts int        `json:"scheduledHosts"`
	ScheduleLagSec float64    `json:"scheduleLagSec"`
	LastProbe      *time.Time `json:"lastProbe,omitempty"`
}

type influxHealth struct {
	componentHealth
	BufferedBytes int        `json:"bufferedBytes"`
	LastWrite     *time.Time `json:"lastWrite,omitempty"`
}

// HealthReport is the answer of /health.
type HealthReport struct {
	Status   string        `json:"status"`
	Probing  probingHealth `json:"probing"`
	InfluxDB *influxHealth `json:"influxdb,omitempty"` // only if the export is on
}

// Health checks whether DINIS probes on schedule and whether the InfluxDB export works.
// "fail" means that the monitoring does not work; "degraded" means that it works with a
// problem (for example InfluxDB is not available).
func (c *Coordinator) Health() HealthReport {
	st := c.pinger.Stats()
	p := probingHealth{
		componentHealth: componentHealth{Status: healthOK},
		ScheduledHosts:  st.ScheduledHosts,
		ScheduleLagSec:  roundSec(st.HealthyLag),
	}
	if !st.LastProbe.IsZero() {
		t := st.LastProbe
		p.LastProbe = &t
	}
	switch {
	case !st.Running:
		p.Status, p.Message = healthFail, "the probe engine is not running"
	case st.ScheduledHosts > 0 && st.HealthyLag > healthLagFail:
		p.Status = healthFail
		p.Message = fmt.Sprintf("probes are %.0f s behind schedule", st.HealthyLag.Seconds())
	case st.SuspectLag > healthSuspectLagDegraded:
		p.Status = healthDegraded
		p.Message = fmt.Sprintf("probes of failing hosts are %.0f s behind schedule", st.SuspectLag.Seconds())
	}
	report := HealthReport{Status: p.Status, Probing: p}

	if c.influxWriter != nil {
		ws := c.influxWriter.Stats()
		ih := &influxHealth{componentHealth: componentHealth{Status: healthOK}, BufferedBytes: ws.BufferedBytes}
		since := ws.Started
		if !ws.LastSuccess.IsZero() {
			t := ws.LastSuccess
			ih.LastWrite = &t
			since = t
		}
		if ws.BufferedBytes > 0 && time.Since(since) > healthInfluxStale {
			ih.Status = healthDegraded
			ih.Message = fmt.Sprintf("no successful write for %.0f s; %d bytes wait in the buffer", time.Since(since).Seconds(), ws.BufferedBytes)
		}
		report.InfluxDB = ih
		report.Status = worse(report.Status, ih.Status)
	}
	return report
}

func roundSec(d time.Duration) float64 {
	return float64(d.Milliseconds()) / 1000
}

// handleHealth answers 200 for "ok" and "degraded" (DINIS monitors), 503 for "fail".
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	report := s.coord.Health()
	status := http.StatusOK
	if report.Status == healthFail {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, report)
}

// handleMetrics writes the Prometheus text format (version 0.0.4).
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	var b strings.Builder
	c := s.coord
	m := metricWriter{&b}

	m.gauge("dinis_build_info", "Version of DINIS.", 1, "version", s.version)
	m.gauge("dinis_start_time_seconds", "Start time of DINIS (Unix time).", float64(c.startedAt.Unix()))

	sum := c.pinger.GetSummary()
	m.header("dinis_hosts", "gauge", "Monitored hosts by status.")
	m.value("dinis_hosts", float64(sum.UpCount), "status", "up")
	m.value("dinis_hosts", float64(sum.DownCount), "status", "down")
	m.value("dinis_hosts", float64(sum.PendingCount), "status", "pending")
	m.value("dinis_hosts", float64(sum.ExcludedCount), "status", "excluded")
	m.header("dinis_alerts", "gauge", "Active alerts by state.")
	m.value("dinis_alerts", float64(sum.AlertsUnack), "state", "firing")
	m.value("dinis_alerts", float64(sum.AlertsActive-sum.AlertsUnack), "state", "acknowledged")
	m.gauge("dinis_probe_rate_per_second", "Planned probes per second for all scheduled hosts.", sum.PacketsPerSec)

	st := c.pinger.Stats()
	m.counter("dinis_probes_total", "Scheduled probes completed since the start.", float64(st.ProbesTotal))
	m.counter("dinis_probe_failures_total", "Scheduled probes without a reply since the start.", float64(st.FailuresTotal))
	m.gauge("dinis_probe_workers", "Probe workers.", float64(st.Workers))
	m.gauge("dinis_probe_workers_busy", "Probes in flight.", float64(st.BusyWorkers))
	m.header("dinis_probe_schedule_lag_seconds", "gauge", "How long the most overdue probe waits for a worker (healthy: hosts that reply, suspect: hosts whose last probe failed).")
	m.value("dinis_probe_schedule_lag_seconds", st.HealthyLag.Seconds(), "queue", "healthy")
	m.value("dinis_probe_schedule_lag_seconds", st.SuspectLag.Seconds(), "queue", "suspect")
	if !st.LastProbe.IsZero() {
		m.gauge("dinis_last_probe_timestamp_seconds", "Completion of the last scheduled probe (Unix time).", float64(st.LastProbe.UnixNano())/1e9)
	}

	ts := c.pinger.GetTimeseriesStore().Stats()
	m.gauge("dinis_history_hosts", "Hosts with a latency history in memory.", float64(ts.HostsWithHistory))
	m.gauge("dinis_history_hosts_max", "Maximum number of hosts with a latency history (maxMetricHosts).", float64(ts.MaxHosts))
	m.counter("dinis_history_samples_dropped_total", "Probe results without history because maxMetricHosts was reached.", float64(ts.DroppedSamplesTotal))

	disc := c.GetDiscoveryStatus()
	m.gauge("dinis_discovery_running", "1 while a discovery sweep runs.", boolFloat(disc.IsScanning))
	if disc.LastRun != nil {
		m.gauge("dinis_discovery_last_run_timestamp_seconds", "End of the last discovery sweep (Unix time).", float64(disc.LastRun.Unix()))
	}

	c.clientsMu.RLock()
	sse := len(c.sseClients)
	c.clientsMu.RUnlock()
	m.gauge("dinis_sse_clients", "Open dashboard event streams.", float64(sse))

	if c.influxWriter != nil {
		ws := c.influxWriter.Stats()
		m.gauge("dinis_influxdb_buffer_bytes", "Probe data waiting for the next InfluxDB write.", float64(ws.BufferedBytes))
		m.counter("dinis_influxdb_write_errors_total", "InfluxDB writes and database setups that failed after their retries.", float64(ws.WriteErrors))
		m.counter("dinis_influxdb_lines_dropped_total", "Probe results not written to InfluxDB (buffer full or refused).", float64(ws.DroppedLines))
		m.gauge("dinis_influxdb_database_ready", "1 when the database has its retention period.", boolFloat(ws.DatabaseReady))
		if !ws.LastSuccess.IsZero() {
			m.gauge("dinis_influxdb_last_write_timestamp_seconds", "Last successful InfluxDB write (Unix time).", float64(ws.LastSuccess.Unix()))
		}
	}

	m.gauge("dinis_health_status", "Health check result: 0 ok, 1 degraded, 2 fail.", map[string]float64{healthOK: 0, healthDegraded: 1, healthFail: 2}[c.Health().Status])

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.gauge("go_goroutines", "Number of goroutines.", float64(runtime.NumGoroutine()))
	m.gauge("go_memstats_heap_alloc_bytes", "Bytes of allocated heap objects.", float64(ms.HeapAlloc))
	m.gauge("go_memstats_sys_bytes", "Bytes of memory obtained from the operating system.", float64(ms.Sys))

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, b.String())
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// metricWriter writes metrics in the Prometheus text format.
type metricWriter struct{ b *strings.Builder }

func (m metricWriter) header(name, kind, help string) {
	fmt.Fprintf(m.b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
}

// value writes one sample; labels are name/value pairs.
func (m metricWriter) value(name string, v float64, labels ...string) {
	m.b.WriteString(name)
	if len(labels) > 0 {
		m.b.WriteByte('{')
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				m.b.WriteByte(',')
			}
			fmt.Fprintf(m.b, "%s=%q", labels[i], labels[i+1])
		}
		m.b.WriteByte('}')
	}
	fmt.Fprintf(m.b, " %g\n", v)
}

func (m metricWriter) gauge(name, help string, v float64, labels ...string) {
	m.header(name, "gauge", help)
	m.value(name, v, labels...)
}

func (m metricWriter) counter(name, help string, v float64) {
	m.header(name, "counter", help)
	m.value(name, v)
}
