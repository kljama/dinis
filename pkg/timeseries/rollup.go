package timeseries

import (
	"math"
	"sort"
	"sync"
	"time"
)

// RollupPoint represents downsampled aggregate metrics over a time bucket (e.g. 1m, 1h).
type RollupPoint struct {
	Timestamp         time.Time     `json:"timestamp"`
	BucketDuration    time.Duration `json:"bucketDuration"`
	BucketDurationSec float64       `json:"bucketDurationSec,omitempty"`
	BucketDurationStr string        `json:"bucketDurationStr,omitempty"`
	MinLatencyMs      float64       `json:"minLatencyMs"`
	MaxLatencyMs      float64       `json:"maxLatencyMs"`
	AvgLatencyMs      float64       `json:"avgLatencyMs"`
	P50LatencyMs      float64       `json:"p50LatencyMs"`
	P95LatencyMs      float64       `json:"p95LatencyMs"`
	P99LatencyMs      float64       `json:"p99LatencyMs"`
	PacketLossPct     float64       `json:"packetLossPct"`
	SampleCount       int           `json:"sampleCount"`
	UpRatio           float64       `json:"upRatio"`
	JitterMs          float64       `json:"jitterMs"` // Mean consecutive latency variance
}

// ComputeRollup aggregates raw samples into a single statistical RollupPoint.
func ComputeRollup(bucketTime time.Time, duration time.Duration, samples []RawSample) RollupPoint {
	if len(samples) == 0 {
		return RollupPoint{
			Timestamp:         bucketTime,
			BucketDuration:    duration,
			BucketDurationSec: duration.Seconds(),
			BucketDurationStr: duration.String(),
		}
	}

	var validLatencies []float64
	var sumLatency float64
	var failCount int
	minLat := math.MaxFloat64
	maxLat := 0.0

	for _, s := range samples {
		if s.Success && s.LatencyMs >= 0 {
			validLatencies = append(validLatencies, s.LatencyMs)
			sumLatency += s.LatencyMs
			if s.LatencyMs < minLat {
				minLat = s.LatencyMs
			}
			if s.LatencyMs > maxLat {
				maxLat = s.LatencyMs
			}
		} else {
			failCount++
		}
	}

	total := len(samples)
	lossPct := (float64(failCount) / float64(total)) * 100.0
	upRatio := float64(len(validLatencies)) / float64(total)

	rp := RollupPoint{
		Timestamp:         bucketTime,
		BucketDuration:    duration,
		BucketDurationSec: duration.Seconds(),
		BucketDurationStr: duration.String(),
		PacketLossPct:     lossPct,
		SampleCount:       total,
		UpRatio:           upRatio,
	}

	if len(validLatencies) > 0 {
		rp.MinLatencyMs = minLat
		rp.MaxLatencyMs = maxLat
		rp.AvgLatencyMs = sumLatency / float64(len(validLatencies))

		// Calculate jitter (RFC 3550 standard mean consecutive variance) over chronological samples
		if len(validLatencies) > 1 {
			var sumDiff float64
			for i := 1; i < len(validLatencies); i++ {
				sumDiff += math.Abs(validLatencies[i] - validLatencies[i-1])
			}
			rp.JitterMs = sumDiff / float64(len(validLatencies)-1)
		}

		// Sort latencies only after temporal metrics like jitter are calculated
		sort.Float64s(validLatencies)
		rp.P50LatencyMs = getPercentile(validLatencies, 0.50)
		rp.P95LatencyMs = getPercentile(validLatencies, 0.95)
		rp.P99LatencyMs = getPercentile(validLatencies, 0.99)
	}

	return rp
}

func getPercentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(pct*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// AggregateRollups aggregates multiple smaller RollupPoints (e.g. 1-minute rollups)
// into a higher-tier RollupPoint (e.g. 1-hour rollup) using sample-weighted statistics and percentiles.
func AggregateRollups(bucketTime time.Time, duration time.Duration, points []RollupPoint) RollupPoint {
	if len(points) == 0 {
		return RollupPoint{
			Timestamp:         bucketTime,
			BucketDuration:    duration,
			BucketDurationSec: duration.Seconds(),
			BucketDurationStr: duration.String(),
		}
	}

	var totalSamples int
	var totalValidSamples float64
	var totalFailedSamples float64

	var weightedLatencySum float64
	var weightedJitterSum float64
	var weightedP50Sum float64
	var weightedP95Sum float64
	var weightedP99Sum float64

	minLat := math.MaxFloat64
	maxLat := 0.0

	for _, pt := range points {
		if pt.SampleCount <= 0 {
			continue
		}
		totalSamples += pt.SampleCount
		validCount := float64(pt.SampleCount) * pt.UpRatio
		failCount := float64(pt.SampleCount) * (pt.PacketLossPct / 100.0)

		totalValidSamples += validCount
		totalFailedSamples += failCount

		if validCount > 0 {
			weightedLatencySum += pt.AvgLatencyMs * validCount
			weightedJitterSum += pt.JitterMs * validCount
			weightedP50Sum += pt.P50LatencyMs * validCount
			weightedP95Sum += pt.P95LatencyMs * validCount
			weightedP99Sum += pt.P99LatencyMs * validCount

			if pt.MinLatencyMs > 0 && pt.MinLatencyMs < minLat {
				minLat = pt.MinLatencyMs
			}
			if pt.MaxLatencyMs > maxLat {
				maxLat = pt.MaxLatencyMs
			}
		}
	}

	if totalSamples == 0 {
		return RollupPoint{
			Timestamp:         bucketTime,
			BucketDuration:    duration,
			BucketDurationSec: duration.Seconds(),
			BucketDurationStr: duration.String(),
		}
	}

	rp := RollupPoint{
		Timestamp:         bucketTime,
		BucketDuration:    duration,
		BucketDurationSec: duration.Seconds(),
		BucketDurationStr: duration.String(),
		SampleCount:       totalSamples,
		PacketLossPct:     (totalFailedSamples / float64(totalSamples)) * 100.0,
		UpRatio:           totalValidSamples / float64(totalSamples),
	}

	if totalValidSamples > 0 {
		if minLat != math.MaxFloat64 {
			rp.MinLatencyMs = minLat
		}
		rp.MaxLatencyMs = maxLat
		rp.AvgLatencyMs = math.Round((weightedLatencySum/totalValidSamples)*100) / 100
		rp.JitterMs = math.Round((weightedJitterSum/totalValidSamples)*100) / 100
		rp.P50LatencyMs = math.Round((weightedP50Sum/totalValidSamples)*100) / 100
		rp.P95LatencyMs = math.Round((weightedP95Sum/totalValidSamples)*100) / 100
		rp.P99LatencyMs = math.Round((weightedP99Sum/totalValidSamples)*100) / 100
	}

	return rp
}

// RollupSeries holds a circular buffer of RollupPoints for a single host.
type RollupSeries struct {
	mu       sync.RWMutex
	points   []RollupPoint
	capacity int
	head     int
	count    int
}

// NewRollupSeries creates a series buffer with a fixed capacity.
func NewRollupSeries(capacity int) *RollupSeries {
	if capacity <= 0 {
		capacity = 1440 // e.g. 24 hours of 1-minute rollups
	}
	return &RollupSeries{
		points:   make([]RollupPoint, 0, 8),
		capacity: capacity,
	}
}

// Append adds a new RollupPoint to the series.
func (rs *RollupSeries) Append(point RollupPoint) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if len(rs.points) < rs.capacity {
		rs.points = append(rs.points, point)
		rs.count = len(rs.points)
		rs.head = (rs.head + 1) % rs.capacity
		return
	}

	rs.points[rs.head] = point
	rs.head = (rs.head + 1) % rs.capacity
	if rs.count < rs.capacity {
		rs.count++
	}
}

// GetAll returns all recorded rollup points in chronological order.
func (rs *RollupSeries) GetAll() []RollupPoint {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if len(rs.points) == 0 {
		return nil
	}

	result := make([]RollupPoint, len(rs.points))
	if len(rs.points) < rs.capacity {
		copy(result, rs.points)
		return result
	}

	tailLen := rs.capacity - rs.head
	copy(result[:tailLen], rs.points[rs.head:])
	copy(result[tailLen:], rs.points[:rs.head])
	return result
}

// GetSince returns rollups recorded at or after the cutoff timestamp.
// Robust against clock skew and non-monotonic timestamps.
func (rs *RollupSeries) GetSince(cutoff time.Time) []RollupPoint {
	all := rs.GetAll()
	if len(all) == 0 {
		return nil
	}

	result := make([]RollupPoint, 0, len(all))
	for _, p := range all {
		if !p.Timestamp.Before(cutoff) {
			result = append(result, p)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
