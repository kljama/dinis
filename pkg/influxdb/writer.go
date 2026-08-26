// Package influxdb provides a lightweight writer that sends ICMP probe results
// to InfluxDB 3 Core using the line-protocol HTTP write endpoint.
package influxdb

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Writer batches and sends line-protocol data to InfluxDB 3 Core.
type Writer struct {
	url    string
	bucket string
	token  string
	client *http.Client

	mu    sync.Mutex
	buf   bytes.Buffer
	count int

	flushInterval time.Duration
	batchSize     int
	stopChan      chan struct{}
	wg            sync.WaitGroup
}

// Config holds InfluxDB writer configuration.
type Config struct {
	URL           string
	Bucket        string
	Token         string // optional auth token
	FlushInterval time.Duration
	BatchSize     int
}

// NewWriter creates a new InfluxDB line-protocol writer.
func NewWriter(cfg Config) *Writer {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}

	w := &Writer{
		url:           strings.TrimRight(cfg.URL, "/"),
		bucket:        cfg.Bucket,
		token:         cfg.Token,
		client:        &http.Client{Timeout: 10 * time.Second},
		flushInterval: cfg.FlushInterval,
		batchSize:     cfg.BatchSize,
		stopChan:      make(chan struct{}),
	}

	w.wg.Add(1)
	go w.flushLoop()
	return w
}

// WriteProbe enqueues a single ICMP probe result for batch writing.
func (w *Writer) WriteProbe(ip string, subnet string, alias string, latencyMs float64, success bool, ts time.Time) {
	// Build line-protocol line:
	// icmp_probe,ip=x.x.x.x,subnet=y.y.y.y/z latency_ms=1.23,success=1i <timestamp_ns>
	var line strings.Builder
	line.WriteString("icmp_probe,ip=")
	line.WriteString(escapeTag(ip))
	if subnet != "" {
		line.WriteString(",subnet=")
		line.WriteString(escapeTag(subnet))
	}
	if alias != "" {
		line.WriteString(",alias=")
		line.WriteString(escapeTag(alias))
	}
	line.WriteString(" latency_ms=")
	line.WriteString(fmt.Sprintf("%.4f", latencyMs))
	successVal := 0
	if success {
		successVal = 1
	}
	line.WriteString(fmt.Sprintf(",success=%di", successVal))
	line.WriteString(fmt.Sprintf(" %d\n", ts.UnixNano()))

	w.mu.Lock()
	w.buf.WriteString(line.String())
	w.count++
	shouldFlush := w.count >= w.batchSize
	w.mu.Unlock()

	if shouldFlush {
		go w.flush()
	}
}

// Stop gracefully flushes remaining data and stops the background loop.
func (w *Writer) Stop() {
	close(w.stopChan)
	w.wg.Wait()
	w.flush()
}

func (w *Writer) flushLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			return
		case <-ticker.C:
			w.flush()
		}
	}
}

func (w *Writer) flush() {
	w.mu.Lock()
	if w.buf.Len() == 0 {
		w.mu.Unlock()
		return
	}
	payload := make([]byte, w.buf.Len())
	copy(payload, w.buf.Bytes())
	w.buf.Reset()
	w.count = 0
	w.mu.Unlock()

	endpoint := fmt.Sprintf("%s/api/v3/write_lp?db=%s", w.url, w.bucket)
	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(payload))
	if err != nil {
		log.Printf("[INFLUXDB] Failed to create request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if w.token != "" {
		req.Header.Set("Authorization", "Bearer "+w.token)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		log.Printf("[INFLUXDB] Write failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("[INFLUXDB] Write returned status %d", resp.StatusCode)
	}
}

// escapeTag escapes special characters in tag values per line-protocol spec.
func escapeTag(s string) string {
	s = strings.ReplaceAll(s, " ", "\\ ")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "=", "\\=")
	return s
}
