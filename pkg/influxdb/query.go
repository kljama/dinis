package influxdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Querier runs SQL queries against InfluxDB 3 (POST /api/v3/query_sql).
type Querier struct {
	url    string
	db     string
	token  string
	client *http.Client
}

// NewQuerier creates a query client for the database in cfg.Bucket.
func NewQuerier(cfg Config) *Querier {
	return &Querier{
		url:    strings.TrimRight(cfg.URL, "/"),
		db:     cfg.Bucket,
		token:  cfg.Token,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Query runs sql and returns the rows. Numbers are json.Number. A query on a table or database
// that does not exist yet (no data written) returns no rows and no error.
func (q *Querier) Query(ctx context.Context, sql string) ([]map[string]any, error) {
	body, _ := json.Marshal(map[string]string{"db": q.db, "q": sql, "format": "json"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, q.url+"/api/v3/query_sql", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if q.token != "" {
		req.Header.Set("Authorization", "Bearer "+q.token)
	}
	resp, err := q.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		text := strings.TrimSpace(string(msg))
		if strings.Contains(text, "not found") {
			return nil, nil // no data written yet
		}
		return nil, fmt.Errorf("InfluxDB returned status %d: %s", resp.StatusCode, text)
	}

	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	var rows []map[string]any
	if err := dec.Decode(&rows); err != nil && err != io.EOF {
		return nil, fmt.Errorf("invalid InfluxDB response: %w", err)
	}
	return rows, nil
}

// HistoryPoint is one time bucket of the latency history of a host.
type HistoryPoint struct {
	Start   time.Time
	Probes  int
	Replies int
	// Latency values of the successful probes; zero if the bucket has no reply.
	MinMs, MaxMs, AvgMs, P50Ms, P95Ms, P99Ms float64
	// JitterMs is the mean difference between consecutive successful probes (the first step
	// may start in the previous bucket); nil if the bucket has no such pair.
	JitterMs *float64
}

// HostHistory returns the history of one host for the last window, in buckets of the given
// size. ip must be a valid IP address.
func (q *Querier) HostHistory(ctx context.Context, ip string, window, bucket time.Duration) ([]HistoryPoint, error) {
	ip = strings.ReplaceAll(ip, "'", "''")
	win := int64(window / time.Second)
	bin := int64(bucket / time.Second)
	// approx_percentile_cont(expr, p) is the form that all InfluxDB 3 versions accept.
	sql := fmt.Sprintf(`
WITH replies AS (
  SELECT time, latency_ms,
         abs(latency_ms - lag(latency_ms) OVER (ORDER BY time)) AS step_ms
  FROM icmp_probe
  WHERE ip = '%[1]s' AND success = 1 AND time >= now() - INTERVAL '%[2]d seconds'
),
latency AS (
  SELECT date_bin(INTERVAL '%[3]d seconds', time) AS bucket,
         min(latency_ms) AS min_ms, max(latency_ms) AS max_ms, avg(latency_ms) AS avg_ms,
         approx_percentile_cont(latency_ms, 0.5)  AS p50_ms,
         approx_percentile_cont(latency_ms, 0.95) AS p95_ms,
         approx_percentile_cont(latency_ms, 0.99) AS p99_ms,
         avg(step_ms) AS jitter_ms
  FROM replies
  GROUP BY date_bin(INTERVAL '%[3]d seconds', time)
),
probes AS (
  SELECT date_bin(INTERVAL '%[3]d seconds', time) AS bucket, count(*) AS probes, sum(success) AS replies
  FROM icmp_probe
  WHERE ip = '%[1]s' AND time >= now() - INTERVAL '%[2]d seconds'
  GROUP BY date_bin(INTERVAL '%[3]d seconds', time)
)
SELECT CAST(p.bucket AS BIGINT) / 1000000000 AS bucket_s, p.probes, p.replies,
       l.min_ms, l.max_ms, l.avg_ms, l.p50_ms, l.p95_ms, l.p99_ms, l.jitter_ms
FROM probes p LEFT JOIN latency l ON p.bucket = l.bucket
ORDER BY p.bucket`, ip, win, bin)

	rows, err := q.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	points := make([]HistoryPoint, 0, len(rows))
	for _, r := range rows {
		p := HistoryPoint{
			Start:   time.Unix(int64(num(r["bucket_s"])), 0),
			Probes:  int(num(r["probes"])),
			Replies: int(num(r["replies"])),
			MinMs:   num(r["min_ms"]),
			MaxMs:   num(r["max_ms"]),
			AvgMs:   num(r["avg_ms"]),
			P50Ms:   num(r["p50_ms"]),
			P95Ms:   num(r["p95_ms"]),
			P99Ms:   num(r["p99_ms"]),
		}
		if v, ok := r["jitter_ms"].(json.Number); ok {
			if f, err := v.Float64(); err == nil {
				p.JitterMs = &f
			}
		}
		points = append(points, p)
	}
	return points, nil
}

// num converts a JSON number (or null) to float64; null and invalid values give 0.
func num(v any) float64 {
	if n, ok := v.(json.Number); ok {
		f, _ := n.Float64()
		return f
	}
	return 0
}
