// Package influxdb provides a lightweight writer that sends ICMP probe results
// to InfluxDB 3 Core using the line-protocol HTTP write endpoint.
package influxdb

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
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
	flushSignal   chan struct{}
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

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}

	w := &Writer{
		url:    cfg.URL,
		bucket: cfg.Bucket,
		token:  cfg.Token,
		client: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
		flushInterval: cfg.FlushInterval,
		batchSize:     cfg.BatchSize,
		flushSignal:   make(chan struct{}, 1),
		stopChan:      make(chan struct{}),
	}

	w.wg.Add(1)
	go w.flushLoop()
	return w
}

// WriteProbe enqueues a single ICMP probe result for batch writing.
func (w *Writer) WriteProbe(ip string, alias string, subnet string, latencyMs float64, success bool, ts time.Time) {
	// Pre-format line protocol into a stack-allocated buffer without heap allocations:
	// icmp_probe,ip=x.x.x.x[,subnet=...][,alias=...] latency_ms=1.23,success=1i <timestamp_ns>\n
	var line [256]byte
	b := line[:0]
	b = append(b, "icmp_probe,ip="...)
	b = appendEscapedTag(b, ip)
	if subnet != "" {
		b = append(b, ",subnet="...)
		b = appendEscapedTag(b, subnet)
	}
	if alias != "" {
		b = append(b, ",alias="...)
		b = appendEscapedTag(b, alias)
	}
	b = append(b, " latency_ms="...)
	b = strconv.AppendFloat(b, latencyMs, 'f', 4, 64)
	if success {
		b = append(b, ",success=1i "...)
	} else {
		b = append(b, ",success=0i "...)
	}
	b = strconv.AppendInt(b, ts.UnixNano(), 10)
	b = append(b, '\n')

	w.mu.Lock()
	w.buf.Write(b)
	w.count++
	shouldSignal := w.count >= w.batchSize
	w.mu.Unlock()

	if shouldSignal {
		select {
		case w.flushSignal <- struct{}{}:
		default:
		}
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
		case <-w.flushSignal:
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

	// Attempt write with up to 2 retries for transient connection errors
	for attempt := 0; attempt < 3; attempt++ {
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
			if attempt < 2 {
				time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
				continue
			}
			log.Printf("[INFLUXDB] Write failed after retries: %v", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return
		}

		// Transient server error: retry
		if (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500) && attempt < 2 {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
			continue
		}

		log.Printf("[INFLUXDB] Write returned status %d", resp.StatusCode)
		return
	}
}

// appendEscapedTag appends tag string s to dst, escaping '\', ' ', ',', and '=' per line-protocol spec.
func appendEscapedTag(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\', ' ', ',', '=':
			dst = append(dst, '\\', c)
		default:
			dst = append(dst, c)
		}
	}
	return dst
}

// escapeTag escapes special characters in tag values per line-protocol spec.
func escapeTag(s string) string {
	var buf []byte
	return string(appendEscapedTag(buf, s))
}
