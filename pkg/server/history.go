package server

import (
	"context"
	"math"
	"time"

	"dinis/pkg/timeseries"
)

const (
	// historyCacheTTL: the host modal refreshes every few seconds; an InfluxDB query over
	// many days reads many files, so its answer is kept for this long.
	historyCacheTTL = time.Minute
	// historyCacheMax bounds the cache (one entry per host and window).
	historyCacheMax = 512
	// historyMaxPoints: windows longer than this many hours use buckets of several hours.
	historyMaxPoints = 720
)

type cachedHistory struct {
	points  []timeseries.RollupPoint
	expires time.Time
}

// historyBucket returns the bucket size for a window: 1 hour, or more for windows over 30
// days, so that a chart has at most historyMaxPoints points.
func historyBucket(window time.Duration) time.Duration {
	hours := int64(math.Ceil(window.Hours() / historyMaxPoints))
	if hours < 1 {
		hours = 1
	}
	return time.Duration(hours) * time.Hour
}

// maxHistoryWindow is the longest window that InfluxDB can answer: its retention period
// (at least 30 days).
func (c *Coordinator) maxHistoryWindow() time.Duration {
	days := c.retentionDays
	if days < 30 {
		days = 30
	}
	return time.Duration(days) * 24 * time.Hour
}

// influxHistory returns the history of a host from InfluxDB, in the format of the in-memory
// rollups. The first and the last bucket can be partial: they are marked with Partial.
func (c *Coordinator) influxHistory(ctx context.Context, ip string, window time.Duration) ([]timeseries.RollupPoint, error) {
	key := ip + "|" + window.String()
	now := time.Now()
	c.histMu.Lock()
	if e, ok := c.histCache[key]; ok && now.Before(e.expires) {
		c.histMu.Unlock()
		return e.points, nil
	}
	c.histMu.Unlock()

	bucket := historyBucket(window)
	rows, err := c.influxQuery.HostHistory(ctx, ip, window, bucket)
	if err != nil {
		return nil, err
	}
	start := now.Add(-window)
	points := make([]timeseries.RollupPoint, 0, len(rows))
	for _, r := range rows {
		p := timeseries.RollupPoint{
			Timestamp:         r.Start,
			BucketDuration:    bucket,
			BucketDurationSec: bucket.Seconds(),
			BucketDurationStr: bucket.String(),
			SampleCount:       r.Probes,
			Partial:           r.Start.Before(start) || r.Start.Add(bucket).After(now),
		}
		if r.Probes > 0 {
			p.PacketLossPct = round3f(100 * float64(r.Probes-r.Replies) / float64(r.Probes))
			p.UpRatio = round3f(float64(r.Replies) / float64(r.Probes))
		}
		if r.Replies > 0 {
			p.MinLatencyMs = round3f(r.MinMs)
			p.MaxLatencyMs = round3f(r.MaxMs)
			p.AvgLatencyMs = round3f(r.AvgMs)
			p.P50LatencyMs = round3f(r.P50Ms)
			p.P95LatencyMs = round3f(r.P95Ms)
			p.P99LatencyMs = round3f(r.P99Ms)
		}
		if r.JitterMs != nil {
			j := round3f(*r.JitterMs)
			p.JitterMs = &j
		}
		points = append(points, p)
	}

	c.histMu.Lock()
	if len(c.histCache) >= historyCacheMax {
		clear(c.histCache)
	}
	c.histCache[key] = cachedHistory{points: points, expires: now.Add(historyCacheTTL)}
	c.histMu.Unlock()
	return points, nil
}

func round3f(v float64) float64 {
	return math.Round(v*1000) / 1000
}
