package timeseries

import (
	"math"
	"sort"
	"sync"
	"time"
)

// RollupPoint represents downsampled aggregate metrics over a time bucket (e.g. 1m, 1h).
type RollupPoint struct {
	Timestamp      time.Time     `json:"timestamp"`
	BucketDuration time.Duration `json:"bucketDuration"`
	MinLatencyMs   float64       `json:"minLatencyMs"`
	MaxLatencyMs   float64       `json:"maxLatencyMs"`
	AvgLatencyMs   float64       `json:"avgLatencyMs"`
	P50LatencyMs   float64       `json:"p50LatencyMs"`
	P95LatencyMs   float64       `json:"p95LatencyMs"`
	P99LatencyMs   float64       `json:"p99LatencyMs"`
	PacketLossPct  float64       `json:"packetLossPct"`
	SampleCount    int           `json:"sampleCount"`
	UpRatio        float64       `json:"upRatio"`
	JitterMs       float64       `json:"jitterMs"` // Mean consecutive latency variance
}

// ComputeRollup aggregates raw samples into a single statistical RollupPoint.
func ComputeRollup(bucketTime time.Time, duration time.Duration, samples []RawSample) RollupPoint {
	if len(samples) == 0 {
		return RollupPoint{
			Timestamp:      bucketTime,
			BucketDuration: duration,
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
		Timestamp:      bucketTime,
		BucketDuration: duration,
		PacketLossPct:  lossPct,
		SampleCount:    total,
		UpRatio:        upRatio,
	}

	if len(validLatencies) > 0 {
		rp.MinLatencyMs = minLat
		rp.MaxLatencyMs = maxLat
		rp.AvgLatencyMs = sumLatency / float64(len(validLatencies))

		sort.Float64s(validLatencies)
		rp.P50LatencyMs = getPercentile(validLatencies, 0.50)
		rp.P95LatencyMs = getPercentile(validLatencies, 0.95)
		rp.P99LatencyMs = getPercentile(validLatencies, 0.99)

		// Calculate jitter (RFC 3550 standard approximation)
		if len(validLatencies) > 1 {
			var sumDiff float64
			for i := 1; i < len(validLatencies); i++ {
				sumDiff += math.Abs(validLatencies[i] - validLatencies[i-1])
			}
			rp.JitterMs = sumDiff / float64(len(validLatencies)-1)
		}
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
		points:   make([]RollupPoint, capacity),
		capacity: capacity,
	}
}

// Append adds a new RollupPoint to the series.
func (rs *RollupSeries) Append(point RollupPoint) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

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

	if rs.count == 0 {
		return nil
	}

	result := make([]RollupPoint, rs.count)
	if rs.count < rs.capacity {
		copy(result, rs.points[:rs.count])
		return result
	}

	tailLen := rs.capacity - rs.head
	copy(result[:tailLen], rs.points[rs.head:])
	copy(result[tailLen:], rs.points[:rs.head])
	return result
}

// GetSince returns rollups recorded at or after the cutoff timestamp.
func (rs *RollupSeries) GetSince(cutoff time.Time) []RollupPoint {
	all := rs.GetAll()
	if len(all) == 0 {
		return nil
	}

	idx := sort.Search(len(all), func(i int) bool {
		return !all[i].Timestamp.Before(cutoff)
	})

	if idx >= len(all) {
		return nil
	}
	return all[idx:]
}
