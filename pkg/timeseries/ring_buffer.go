package timeseries

import (
	"math"
	"sort"
	"sync"
	"time"
)

// RawSample represents a single ICMP probe result.
type RawSample struct {
	Timestamp time.Time
	LatencyMs float64
	Success   bool
}

// packedSample is the in-memory form of a RawSample. It is 16 bytes and pointer-free (a
// time.Time holds a pointer), so ring buffers are small and the GC does not scan them.
type packedSample struct {
	ts  int64 // UnixNano
	lat float32
	ok  bool
}

func (p packedSample) unpack() RawSample {
	return RawSample{Timestamp: time.Unix(0, p.ts), LatencyMs: round3(p.lat), Success: p.ok}
}

// grownCap returns the new capacity for a full slice of cur elements that may hold at most
// limit elements. It doubles but never exceeds limit, so a buffer at its limit has no slack.
func grownCap(cur, limit int) int {
	n := cur * 2
	if n < 8 {
		n = 8
	}
	if n > limit {
		n = limit
	}
	return n
}

// HostRingBuffer is a fixed-size circular buffer storing the most recent raw probe samples for a single IP.
type HostRingBuffer struct {
	mu       sync.RWMutex
	samples  []packedSample
	capacity int
	head     int // position of the oldest sample once the buffer is full
}

// NewHostRingBuffer creates a new ring buffer with the given capacity.
func NewHostRingBuffer(capacity int) *HostRingBuffer {
	if capacity <= 0 {
		capacity = RawSampleRetention
	}
	return &HostRingBuffer{
		samples:  make([]packedSample, 0, min(8, capacity)),
		capacity: capacity,
	}
}

// Push adds a new raw sample to the ring buffer in O(1) time.
func (rb *HostRingBuffer) Push(timestamp time.Time, latencyMs float64, success bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	s := packedSample{ts: timestamp.UnixNano(), lat: float32(latencyMs), ok: success}
	if len(rb.samples) < rb.capacity {
		if len(rb.samples) == cap(rb.samples) {
			grown := make([]packedSample, len(rb.samples), grownCap(cap(rb.samples), rb.capacity))
			copy(grown, rb.samples)
			rb.samples = grown
		}
		rb.samples = append(rb.samples, s)
		rb.head = len(rb.samples) % rb.capacity
		return
	}
	rb.samples[rb.head] = s
	rb.head = (rb.head + 1) % rb.capacity
}

// partsUnsafe returns the samples, oldest first, as two slices of the buffer (the second is
// empty until the buffer wraps). The caller must hold rb.mu.
func (rb *HostRingBuffer) partsUnsafe() (older, newer []packedSample) {
	if len(rb.samples) < rb.capacity {
		return rb.samples, nil
	}
	return rb.samples[rb.head:], rb.samples[:rb.head]
}

// GetAll returns a chronological slice of all recorded raw samples.
func (rb *HostRingBuffer) GetAll() []RawSample {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if len(rb.samples) == 0 {
		return nil
	}
	older, newer := rb.partsUnsafe()
	result := make([]RawSample, 0, len(rb.samples))
	for _, part := range [2][]packedSample{older, newer} {
		for _, s := range part {
			result = append(result, s.unpack())
		}
	}
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

// GetRange returns the samples timestamped in [start, end), in buffer order.
func (rb *HostRingBuffer) GetRange(start, end time.Time) []RawSample {
	all := rb.GetAll()
	var result []RawSample
	for _, s := range all {
		if !s.Timestamp.Before(start) && s.Timestamp.Before(end) {
			result = append(result, s)
		}
	}
	return result
}

// ComputeSummary calculates quick statistical metrics over the current ring buffer window.
func (rb *HostRingBuffer) ComputeSummary() (avgLatency float64, minLatency float64, maxLatency float64, p95Latency float64, lossRatio float64, jitter float64, totalCount int) {
	avgLatency, minLatency, maxLatency, p95Latency, lossRatio, jitter, totalCount, _ = rb.computeSummary(nil)
	return
}

// computeSummary is ComputeSummary without copying the buffer. It collects the successful
// latencies in scratch (reused across calls) and returns it for the next call.
func (rb *HostRingBuffer) computeSummary(scratch []float64) (avgLatency, minLatency, maxLatency, p95Latency, lossRatio, jitter float64, totalCount int, out []float64) {
	valid := scratch[:0]
	var sumLatency, sumDiff float64
	var failCount int
	minLatency = math.MaxFloat64

	rb.mu.RLock()
	older, newer := rb.partsUnsafe()
	totalCount = len(older) + len(newer)
	for _, part := range [2][]packedSample{older, newer} {
		for _, s := range part {
			if !s.ok || s.lat < 0 {
				failCount++
				continue
			}
			lat := round3(s.lat)
			// RFC 3550 style mean consecutive latency variance, in time order
			if len(valid) > 0 {
				sumDiff += math.Abs(lat - valid[len(valid)-1])
			}
			valid = append(valid, lat)
			sumLatency += lat
			if lat < minLatency {
				minLatency = lat
			}
			if lat > maxLatency {
				maxLatency = lat
			}
		}
	}
	rb.mu.RUnlock()

	if totalCount == 0 {
		return 0, 0, 0, 0, 0, 0, 0, valid
	}
	lossRatio = float64(failCount) / float64(totalCount)

	if len(valid) == 0 {
		return 0, 0, 0, 0, lossRatio, 0, totalCount, valid
	}
	avgLatency = sumLatency / float64(len(valid))
	if len(valid) > 1 {
		jitter = math.Round((sumDiff/float64(len(valid)-1))*100) / 100
	}
	sort.Float64s(valid)
	p95Latency = getPercentile(valid, 0.95)
	return avgLatency, minLatency, maxLatency, p95Latency, lossRatio, jitter, totalCount, valid
}
