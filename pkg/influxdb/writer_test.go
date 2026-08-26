package influxdb

import (
	"net/http"
	"net/http/httptest"
	"io"
	"strings"
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
	}
	for _, tc := range tests {
		got := escapeTag(tc.input)
		if got != tc.want {
			t.Errorf("escapeTag(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestWriteProbeLineProtocol(t *testing.T) {
	var received string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(strings.Builder)
		io.Copy(buf, r.Body)
		received = buf.String()
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
	w.WriteProbe("1.2.3.4", "10.0.0.0/24", "myhost", 12.3456, true, ts)

	// Allow the async flush goroutine to complete.
	time.Sleep(100 * time.Millisecond)

	if !strings.Contains(received, "icmp_probe,ip=1.2.3.4,subnet=10.0.0.0/24,alias=myhost") {
		t.Errorf("unexpected line-protocol output: %q", received)
	}
	if !strings.Contains(received, "latency_ms=12.3456") {
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
	var received string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(strings.Builder)
		io.Copy(buf, r.Body)
		received = buf.String()
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

	time.Sleep(100 * time.Millisecond)

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
	var gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
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
	time.Sleep(100 * time.Millisecond)

	if gotPath != "/api/v3/write_lp" {
		t.Errorf("expected path /api/v3/write_lp, got %q", gotPath)
	}
	if gotQuery != "db=mydb" {
		t.Errorf("expected query db=mydb, got %q", gotQuery)
	}
}
