package timeseries

import (
	"math"
	"sort"
	"sync"
	"time"
)

// RawSample represents a single ICMP probe result stored in memory.
type RawSample struct {
	Timestamp time.Time
	LatencyMs float64
	Success   bool
}

// HostRingBuffer is a fixed-size circular buffer storing the most recent raw probe samples for a single IP.
type HostRingBuffer struct {
	mu       sync.RWMutex
	samples  []RawSample
	capacity int
	head     int
	count    int
}

// NewHostRingBuffer creates a new ring buffer with the given capacity.
func NewHostRingBuffer(capacity int) *HostRingBuffer {
	if capacity <= 0 {
		capacity = 120 // Default ~10-20 minutes of samples at standard probe intervals
	}
	return &HostRingBuffer{
		samples:  make([]RawSample, 0, 8),
		capacity: capacity,
	}
}

// Push adds a new raw sample to the ring buffer in O(1) time.
func (rb *HostRingBuffer) Push(timestamp time.Time, latencyMs float64, success bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	sample := RawSample{
		Timestamp: timestamp,
		LatencyMs: latencyMs,
		Success:   success,
	}

	if len(rb.samples) < rb.capacity {
		rb.samples = append(rb.samples, sample)
		rb.count = len(rb.samples)
		rb.head = (rb.head + 1) % rb.capacity
		return
	}

	rb.samples[rb.head] = sample
	rb.head = (rb.head + 1) % rb.capacity
	if rb.count < rb.capacity {
		rb.count++
	}
}

// GetAll returns a chronological slice of all recorded raw samples.
func (rb *HostRingBuffer) GetAll() []RawSample {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if len(rb.samples) == 0 {
		return nil
	}

	result := make([]RawSample, len(rb.samples))
	if len(rb.samples) < rb.capacity {
		copy(result, rb.samples)
		return result
	}

	// Buffer wrapped around: head points to the oldest sample
	tailLen := rb.capacity - rb.head
	copy(result[:tailLen], rb.samples[rb.head:])
	copy(result[tailLen:], rb.samples[:rb.head])
	return result
}

// GetSince returns samples recorded at or after the given cutoff time.
// Robust against clock skew and non-monotonic timestamps.
func (rb *HostRingBuffer) GetSince(cutoff time.Time) []RawSample {
	all := rb.GetAll()
	if len(all) == 0 {
		return nil
	}

	result := make([]RawSample, 0, len(all))
	for _, s := range all {
		if !s.Timestamp.Before(cutoff) {
			result = append(result, s)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// ComputeSummary calculates quick statistical metrics over the current ring buffer window.
func (rb *HostRingBuffer) ComputeSummary() (avgLatency float64, minLatency float64, maxLatency float64, p95Latency float64, lossRatio float64, jitter float64, totalCount int) {
	samples := rb.GetAll()
	if len(samples) == 0 {
		return 0, 0, 0, 0, 0, 0, 0
	}

	totalCount = len(samples)
	var successfulLatencies []float64
	var sumLatency float64
	var failCount int
	minLatency = math.MaxFloat64
	maxLatency = 0

	for _, s := range samples {
		if s.Success && s.LatencyMs >= 0 {
			successfulLatencies = append(successfulLatencies, s.LatencyMs)
			sumLatency += s.LatencyMs
			if s.LatencyMs < minLatency {
				minLatency = s.LatencyMs
			}
			if s.LatencyMs > maxLatency {
				maxLatency = s.LatencyMs
			}
		} else {
			failCount++
		}
	}

	lossRatio = float64(failCount) / float64(totalCount)

	if len(successfulLatencies) > 0 {
		avgLatency = sumLatency / float64(len(successfulLatencies))

		// RFC 3550 standard mean consecutive latency variance calculated before sorting
		if len(successfulLatencies) > 1 {
			var sumDiff float64
			for i := 1; i < len(successfulLatencies); i++ {
				sumDiff += math.Abs(successfulLatencies[i] - successfulLatencies[i-1])
			}
			jitter = math.Round((sumDiff/float64(len(successfulLatencies)-1))*100) / 100
		}

		sort.Float64s(successfulLatencies)
		p95Idx := int(math.Ceil(0.95*float64(len(successfulLatencies)))) - 1
		if p95Idx < 0 {
			p95Idx = 0
		}
		if p95Idx >= len(successfulLatencies) {
			p95Idx = len(successfulLatencies) - 1
		}
		p95Latency = successfulLatencies[p95Idx]
	} else {
		minLatency = 0
	}

	return avgLatency, minLatency, maxLatency, p95Latency, lossRatio, jitter, totalCount
}
