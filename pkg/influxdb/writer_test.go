package influxdb

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestEscapeTag(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"with space", "with\\ space"},
		{"with,comma", "with\\,comma"},
		{"with=equals", "with\\=equals"},
		{"a b,c=d", "a\\ b\\,c\\=d"},
		{"line\nbreak", "line break"},
		{"carriage\rreturn", "carriage return"},
	}
	for _, tc := range tests {
		got := escapeTag(tc.input)
		if got != tc.want {
			t.Errorf("escapeTag(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestWriteProbeLineProtocol(t *testing.T) {
	receivedCh := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(strings.Builder)
		io.Copy(buf, r.Body)
		receivedCh <- buf.String()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := Config{
		URL:           srv.URL,
		Bucket:        "testdb",
		FlushInterval: time.Hour, // prevent auto-flush
		BatchSize:     1,         // flush immediately on first write
	}
	w := NewWriter(cfg)
	defer w.Stop()

	ts := time.Unix(0, 1000000000)
	w.WriteProbe("1.2.3.4", "myhost", "10.0.0.0/24", 12.3456, true, ts)

	var received string
	select {
	case received = <-receivedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for flush")
	}

	if !strings.Contains(received, "icmp_probe,ip=1.2.3.4,subnet=10.0.0.0/24,alias=myhost") {
		t.Errorf("unexpected line-protocol output: %q", received)
	}
	if !strings.Contains(received, "latency_ms=12.35") {
		t.Errorf("missing latency_ms field: %q", received)
	}
	if !strings.Contains(received, "success=1i") {
		t.Errorf("missing success field: %q", received)
	}
	if !strings.Contains(received, "1000000000") {
		t.Errorf("missing timestamp: %q", received)
	}
}

func TestWriteProbeNoSubnetNoAlias(t *testing.T) {
	receivedCh := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(strings.Builder)
		io.Copy(buf, r.Body)
		receivedCh <- buf.String()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := Config{
		URL:           srv.URL,
		Bucket:        "testdb",
		FlushInterval: time.Hour,
		BatchSize:     1,
	}
	w := NewWriter(cfg)
	defer w.Stop()

	ts := time.Unix(0, 2000000000)
	w.WriteProbe("5.6.7.8", "", "", 0.5, false, ts)

	var received string
	select {
	case received = <-receivedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for flush")
	}

	if !strings.HasPrefix(received, "icmp_probe,ip=5.6.7.8 ") {
		t.Errorf("unexpected measurement/tags: %q", received)
	}
	if strings.Contains(received, "subnet=") || strings.Contains(received, "alias=") {
		t.Errorf("unexpected subnet/alias tags in output: %q", received)
	}
	if !strings.Contains(received, "success=0i") {
		t.Errorf("missing success=0i: %q", received)
	}
}

func TestWriteProbeEndpoint(t *testing.T) {
	type reqInfo struct {
		path, query string
	}
	infoCh := make(chan reqInfo, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		infoCh <- reqInfo{r.URL.Path, r.URL.RawQuery}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := Config{
		URL:           srv.URL,
		Bucket:        "mydb",
		FlushInterval: time.Hour,
		BatchSize:     1,
	}
	w := NewWriter(cfg)
	defer w.Stop()

	w.WriteProbe("1.1.1.1", "", "", 1.0, true, time.Now())

	var info reqInfo
	select {
	case info = <-infoCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for flush")
	}

	if info.path != "/api/v3/write_lp" {
		t.Errorf("expected path /api/v3/write_lp, got %q", info.path)
	}
	if info.query != "db=mydb" {
		t.Errorf("expected query db=mydb, got %q", info.query)
	}
}

func TestWriterRetryOnTransientFailure(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt32(&attempts, 1)
		if att < 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := Config{
		URL:           srv.URL,
		Bucket:        "retrydb",
		FlushInterval: time.Hour,
		BatchSize:     1,
	}
	w := NewWriter(cfg)
	defer w.Stop()

	w.WriteProbe("10.0.0.1", "", "", 2.5, true, time.Now())

	// Wait for worker to flush and retry
	time.Sleep(300 * time.Millisecond)

	if atomic.LoadInt32(&attempts) < 2 {
		t.Errorf("expected at least 2 attempts due to retry, got %d", atomic.LoadInt32(&attempts))
	}
}

func TestWriterGracefulShutdownDrain(t *testing.T) {
	receivedCh := make(chan string, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(strings.Builder)
		io.Copy(buf, r.Body)
		receivedCh <- buf.String()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := Config{
		URL:           srv.URL,
		Bucket:        "drainingdb",
		FlushInterval: time.Hour, // no auto ticker flush
		BatchSize:     1000,      // high batch size so it won't auto flush
	}
	w := NewWriter(cfg)

	// Write items that shouldn't flush immediately
	w.WriteProbe("10.0.0.1", "host1", "", 1.0, true, time.Now())
	w.WriteProbe("10.0.0.2", "host2", "", 2.0, true, time.Now())

	// Calling Stop must trigger a final flush
	w.Stop()

	var received string
	select {
	case received = <-receivedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for final flush on Stop()")
	}

	if !strings.Contains(received, "10.0.0.1") || !strings.Contains(received, "10.0.0.2") {
		t.Errorf("expected both hosts in flushed output on Stop, got: %q", received)
	}
}

func BenchmarkWriteProbe(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := Config{
		URL:           srv.URL,
		Bucket:        "benchdb",
		FlushInterval: 100 * time.Millisecond,
		BatchSize:     1000,
	}
	w := NewWriter(cfg)
	defer w.Stop()

	ts := time.Now()
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w.WriteProbe("192.168.1.100", "core-switch-01", "192.168.1.0/24", 1.452, true, ts)
		}
	})
}

func TestWriterRetainFailedPayload(t *testing.T) {
	var shouldFail int32 = 1
	receivedCh := make(chan string, 10)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&shouldFail) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		buf := new(strings.Builder)
		io.Copy(buf, r.Body)
		receivedCh <- buf.String()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := Config{
		URL:           srv.URL,
		Bucket:        "retryretaindb",
		FlushInterval: 100 * time.Millisecond,
		BatchSize:     1,
	}
	w := NewWriter(cfg)

	// Write probe while server is returning 500 InternalServerError
	w.WriteProbe("10.99.99.1", "failing-host", "", 5.5, true, time.Now())

	// Allow initial attempts to fail and retain
	time.Sleep(350 * time.Millisecond)

	// Recover the server
	atomic.StoreInt32(&shouldFail, 0)

	// Enqueue another probe to trigger a successful flush of retained data
	w.WriteProbe("10.99.99.2", "succeeding-host", "", 2.2, true, time.Now())

	var received string
	select {
	case received = <-receivedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for flush after server recovery")
	}

	w.Stop()

	// The flushed data must contain the previously failed probe payload
	if !strings.Contains(received, "10.99.99.1") {
		t.Errorf("expected previously failed probe 10.99.99.1 to be retained and flushed, got: %q", received)
	}
}
