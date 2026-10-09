// Package influxdb provides a lightweight writer that sends ICMP probe results
// to InfluxDB 3 Core using the line-protocol HTTP write endpoint.
package influxdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const maxBufferSize = 10 << 20 // 10 MB maximum buffer ceiling

// Writer batches and sends line-protocol data to InfluxDB 3 Core.
type Writer struct {
	url       string
	bucket    string
	token     string
	retention string
	client    *http.Client

	// dbReady is set once the database is set up (see ensureDatabase); no write is sent
	// before, so InfluxDB cannot create the database without its retention period.
	dbReady     atomic.Bool
	lastDBError time.Time

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
	URL    string
	Bucket string
	Token  string // optional auth token
	// RetentionPeriod (for example "60d") is set on the database before the first write: the
	// database is created with it, or an existing database is updated. Empty leaves the
	// database as it is, and InfluxDB creates a missing database on the first write.
	RetentionPeriod string
	FlushInterval   time.Duration
	BatchSize       int
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
		url:       cfg.URL,
		bucket:    cfg.Bucket,
		token:     cfg.Token,
		retention: cfg.RetentionPeriod,
		client: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
		flushInterval: cfg.FlushInterval,
		batchSize:     cfg.BatchSize,
		flushSignal:   make(chan struct{}, 1),
		stopChan:      make(chan struct{}),
	}
	if w.retention == "" {
		w.dbReady.Store(true)
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
	b = strconv.AppendFloat(b, latencyMs, 'f', 2, 64)
	if success {
		b = append(b, ",success=1i "...)
	} else {
		b = append(b, ",success=0i "...)
	}
	b = strconv.AppendInt(b, ts.UnixNano(), 10)
	b = append(b, '\n')

	w.mu.Lock()
	if w.buf.Len()+len(b) <= maxBufferSize {
		w.buf.Write(b)
		w.count++
	} else {
		log.Printf("[INFLUXDB] Buffer ceiling reached (%d bytes); dropping newest probe metric to prevent OOM", maxBufferSize)
	}
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

// retainFailedPayload puts a payload that could not be written back in front of the buffer.
// If both do not fit in maxBufferSize, the oldest lines of the payload are dropped.
func (w *Writer) retainFailedPayload(payload []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if excess := w.buf.Len() + len(payload) - maxBufferSize; excess > 0 {
		dropped := len(payload)
		if excess < len(payload) {
			// Cut at the end of the line that contains byte excess, so only whole lines go
			if i := bytes.IndexByte(payload[excess-1:], '\n'); i >= 0 {
				dropped = excess + i
			}
		}
		log.Printf("[INFLUXDB] Buffer ceiling reached; dropping the %d oldest buffered lines to prevent OOM", bytes.Count(payload[:dropped], []byte{'\n'}))
		payload = payload[dropped:]
	}
	var newBuf bytes.Buffer
	newBuf.Grow(len(payload) + w.buf.Len())
	newBuf.Write(payload)
	newBuf.Write(w.buf.Bytes())
	w.buf = newBuf
}

// ensureDatabase creates the database with the retention period, or sets the retention period
// of an existing database. It returns an error only if InfluxDB is not reachable or fails, so
// the call is repeated; other problems are logged and writes start anyway.
func (w *Writer) ensureDatabase() error {
	body, _ := json.Marshal(map[string]string{"db": w.bucket, "retention_period": w.retention})
	status, msg, err := w.configureDatabase(http.MethodPost, body)
	if err != nil {
		return err
	}
	switch {
	case status >= 200 && status < 300:
		log.Printf("[INFLUXDB] Created database %q with retention period %s", w.bucket, w.retention)
		return nil
	case status == http.StatusConflict:
		// The database exists: apply the retention period to it
		status, msg, err = w.configureDatabase(http.MethodPut, body)
		if err != nil {
			return err
		}
		if status >= 200 && status < 300 {
			log.Printf("[INFLUXDB] Set retention period of database %q to %s", w.bucket, w.retention)
			return nil
		}
	}
	if status >= 500 {
		return fmt.Errorf("status %d: %s", status, msg)
	}
	log.Printf("[INFLUXDB] Warning: could not set retention period %s on database %q (status %d: %s); writing without it", w.retention, w.bucket, status, msg)
	return nil
}

func (w *Writer) configureDatabase(method string, body []byte) (int, string, error) {
	req, err := http.NewRequest(method, w.url+"/api/v3/configure/database", bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if w.token != "" {
		req.Header.Set("Authorization", "Bearer "+w.token)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return resp.StatusCode, string(bytes.TrimSpace(msg)), nil
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

	if !w.dbReady.Load() {
		if err := w.ensureDatabase(); err != nil {
			if time.Since(w.lastDBError) >= time.Minute {
				log.Printf("[INFLUXDB] Database setup failed, data stays buffered: %v", err)
				w.lastDBError = time.Now()
			}
			w.retainFailedPayload(payload)
			return
		}
		w.dbReady.Store(true)
	}

	endpoint := fmt.Sprintf("%s/api/v3/write_lp?db=%s", w.url, w.bucket)

	// Attempt write with up to 2 retries for transient connection errors
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest("POST", endpoint, bytes.NewReader(payload))
		if err != nil {
			log.Printf("[INFLUXDB] Failed to create request: %v", err)
			w.retainFailedPayload(payload)
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
			w.retainFailedPayload(payload)
			return
		}

		// Drain and close body for this attempt so the HTTP connection can be reused
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return
		}

		// Transient server error: retry
		if (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500) && attempt < 2 {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
			continue
		}

		log.Printf("[INFLUXDB] Write returned status %d", resp.StatusCode)
		if resp.StatusCode >= 500 {
			w.retainFailedPayload(payload)
		}
		return
	}
}

// appendEscapedTag appends tag string s to dst, escaping '\', ' ', ',', and '=' per line-protocol spec,
// and sanitizing newlines to spaces so record boundaries remain intact.
func appendEscapedTag(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\', ' ', ',', '=':
			dst = append(dst, '\\', c)
		case '\n', '\r':
			dst = append(dst, ' ')
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
