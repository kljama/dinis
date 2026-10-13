package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dinis/pkg/influxdb"
	"dinis/pkg/store"
	"dinis/pkg/timeseries"
)

func TestHealthReportsEngineAndInfluxState(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()

	// Not started: the monitoring does not work
	rec := serve(srv, http.MethodGet, "/health", "")
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not running") {
		t.Fatalf("expected 503 for a stopped engine, got %d %s", rec.Code, rec.Body.String())
	}

	// InfluxDB that refuses writes: degraded, but DINIS still monitors (200)
	influx := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer influx.Close()
	wr := influxdb.NewWriter(influxdb.Config{URL: influx.URL, Bucket: "dinis", FlushInterval: time.Hour})
	defer wr.Stop()
	coord.SetInfluxDB(wr, nil, 60)
	coord.Start()

	rec = serve(srv, http.MethodGet, "/health", "")
	var report HealthReport
	_ = json.Unmarshal(rec.Body.Bytes(), &report)
	if rec.Code != http.StatusOK || report.Status != healthOK || report.InfluxDB == nil || report.InfluxDB.Status != healthOK {
		t.Fatalf("expected ok before data waits, got %d %s", rec.Code, rec.Body.String())
	}

	old := healthInfluxStale
	healthInfluxStale = 0
	defer func() { healthInfluxStale = old }()
	wr.WriteProbe("10.0.0.1", "", "", 1, true, time.Now())
	rec = serve(srv, http.MethodGet, "/health", "")
	_ = json.Unmarshal(rec.Body.Bytes(), &report)
	if rec.Code != http.StatusOK || report.Status != healthDegraded || report.InfluxDB.Status != healthDegraded {
		t.Fatalf("expected degraded (200) while InfluxDB does not accept data, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()
	srv.SetVersion("9.9.9")
	coord.Start()

	rec := serve(srv, http.MethodGet, "/metrics", "")
	body := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain; version=0.0.4") {
		t.Fatalf("unexpected answer %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	for _, want := range []string{
		`dinis_build_info{version="9.9.9"} 1`,
		`dinis_hosts{status="pending"}`,
		`dinis_alerts{state="firing"} 0`,
		"# TYPE dinis_probes_total counter",
		`dinis_probe_schedule_lag_seconds{queue="healthy"}`,
		"dinis_history_hosts_max 10000",
		"dinis_health_status 0",
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics are missing %q", want)
		}
	}

	// With an API token, /metrics needs it
	srv.SetAPIToken("secret")
	if rec := serve(srv, http.MethodGet, "/metrics", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without the token, got %d", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with the token, got %d", rec.Code)
	}
}

func TestHistoryFromInfluxDB(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()
	var queries atomic.Int32
	var lastQuery atomic.Value
	influx := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries.Add(1)
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		lastQuery.Store(req["q"])
		start := time.Now().Add(-3 * time.Hour).Truncate(time.Hour).Unix()
		_, _ = w.Write([]byte(`[{"bucket_s": ` + itoa(start) + `, "probes": 60, "replies": 57, "min_ms": 1, "max_ms": 30,
			"avg_ms": 2.5, "p50_ms": 2, "p95_ms": 12.5, "p99_ms": 29, "jitter_ms": 1.25}]`))
	}))
	defer influx.Close()
	coord.SetInfluxDB(nil, influxdb.NewQuerier(influxdb.Config{URL: influx.URL, Bucket: "dinis"}), 60)
	if coord.pinger.GetTimeseriesStore().Stats().HourHistory {
		t.Fatal("with InfluxDB, the in-memory hour history must be off")
	}

	rec := serve(srv, http.MethodGet, "/api/hosts/127.0.0.1/history?window=24h", "")
	var pts []timeseries.RollupPoint
	if err := json.Unmarshal(rec.Body.Bytes(), &pts); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("unexpected answer %d %s", rec.Code, rec.Body.String())
	}
	if len(pts) != 1 || pts[0].SampleCount != 60 || pts[0].PacketLossPct != 5 || pts[0].UpRatio != 0.95 ||
		pts[0].P95LatencyMs != 12.5 || pts[0].JitterMs == nil || *pts[0].JitterMs != 1.25 || pts[0].BucketDuration != time.Hour {
		t.Fatalf("unexpected point %+v", pts)
	}

	// The answer is cached; windows up to 2 hours come from memory
	serve(srv, http.MethodGet, "/api/hosts/127.0.0.1/history?window=24h", "")
	serve(srv, http.MethodGet, "/api/hosts/127.0.0.1/history?window=1h", "")
	if n := queries.Load(); n != 1 {
		t.Fatalf("expected 1 InfluxDB query, got %d", n)
	}

	// Long windows use larger buckets and are capped at the retention period
	serve(srv, http.MethodGet, "/api/hosts/127.0.0.1/history?window=2000h", "")
	q, _ := lastQuery.Load().(string)
	if !strings.Contains(q, "INTERVAL '5184000 seconds'") || !strings.Contains(q, "INTERVAL '7200 seconds'") {
		t.Fatalf("expected a 60-day window in 2-hour buckets, got %s", q)
	}
}

func itoa(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestHistoryFromInfluxDBError(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()
	coord.SetInfluxDB(nil, influxdb.NewQuerier(influxdb.Config{URL: "http://127.0.0.1:1", Bucket: "dinis"}), 60)
	rec := serve(srv, http.MethodGet, "/api/hosts/127.0.0.1/history?window=168h", "")
	if rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), "InfluxDB") {
		t.Fatalf("expected 502 with an InfluxDB error, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestLastReplySurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dinis.json")
	st, err := store.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	coord := NewCoordinator(st)
	coord.pinger.PingSingle(context.Background(), "127.0.0.1")
	h, _ := coord.pinger.GetHost("127.0.0.1")
	if h.LastSeen == nil {
		t.Fatal("expected a reply from 127.0.0.1")
	}
	coord.Stop() // saves the last reply times
	_ = st.Close()

	st2, err := store.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	coord2 := NewCoordinator(st2)
	defer coord2.Stop()
	h2, _ := coord2.pinger.GetHost("127.0.0.1")
	if h2.LastSeen == nil || !h2.LastSeen.Equal(h.LastSeen.Truncate(0)) {
		t.Fatalf("expected lastSeen %v after the restart, got %v", h.LastSeen, h2.LastSeen)
	}
}
