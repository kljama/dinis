package timeseries

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Retention per host. Minute rollups only serve history windows up to 2h; longer windows read
// the hourly series.
const (
	MinuteRollupRetention = 120 // 2 hours of 1-minute rollups
	HourRollupRetention   = 720 // 30 days of 1-hour rollups

	// RawSampleRetention is the number of raw samples kept per host. At the minimum probe
	// interval (0.5 s) it covers 64 s: a full minute bucket plus the few seconds that can
	// pass before the bucket is rolled up.
	RawSampleRetention = 128
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
	// JitterMs is the mean difference between consecutive successful probes, including the
	// step from the last probe before the bucket. nil (JSON null) if there is no such pair.
	JitterMs *float64 `json:"jitterMs"`
	// Partial marks the bucket in progress (the current hour up to the last full minute).
	Partial bool `json:"partial,omitempty"`
}

// jitterState carries the last successful latency of a host across buckets.
type jitterState struct {
	last float64
	has  bool
}

// bucketStats accumulates the samples of one bucket.
type bucketStats struct {
	total, fails int
	sum          float64
	min, max     float64
	valid        []float64 // successful latencies
	jitterSum    float64
	jitterN      int
}

func (b *bucketStats) reset() {
	*b = bucketStats{valid: b.valid[:0], min: math.MaxFloat64}
}

func (b *bucketStats) add(lat float64, ok bool, js *jitterState) {
	b.total++
	if !ok || lat < 0 {
		b.fails++
		return
	}
	b.valid = append(b.valid, lat)
	b.sum += lat
	if lat < b.min {
		b.min = lat
	}
	if lat > b.max {
		b.max = lat
	}
	if js.has {
		b.jitterSum += math.Abs(lat - js.last)
		b.jitterN++
	}
	js.last, js.has = lat, true
}

func emptyPoint(bucketTime time.Time, duration time.Duration) RollupPoint {
	return RollupPoint{
		Timestamp:         bucketTime,
		BucketDuration:    duration,
		BucketDurationSec: duration.Seconds(),
		BucketDurationStr: duration.String(),
	}
}

// point returns the rollup of the bucket. It sorts b.valid.
func (b *bucketStats) point(bucketTime time.Time, duration time.Duration) RollupPoint {
	rp := emptyPoint(bucketTime, duration)
	if b.total == 0 {
		return rp
	}
	rp.SampleCount = b.total
	rp.PacketLossPct = float64(b.fails) / float64(b.total) * 100.0
	rp.UpRatio = float64(len(b.valid)) / float64(b.total)
	if len(b.valid) > 0 {
		rp.MinLatencyMs = b.min
		rp.MaxLatencyMs = b.max
		rp.AvgLatencyMs = b.sum / float64(len(b.valid))
		sort.Float64s(b.valid)
		rp.P50LatencyMs = getPercentile(b.valid, 0.50)
		rp.P95LatencyMs = getPercentile(b.valid, 0.95)
		rp.P99LatencyMs = getPercentile(b.valid, 0.99)
	}
	if b.jitterN > 0 {
		j := b.jitterSum / float64(b.jitterN)
		rp.JitterMs = &j
	}
	return rp
}

// ComputeRollup aggregates raw samples into a single statistical RollupPoint. Jitter only
// uses the pairs of consecutive samples inside the bucket.
func ComputeRollup(bucketTime time.Time, duration time.Duration, samples []RawSample) RollupPoint {
	b := bucketStats{min: math.MaxFloat64}
	var js jitterState
	for _, s := range samples {
		b.add(s.LatencyMs, s.Success, &js)
	}
	return b.point(bucketTime, duration)
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

// Latency histogram of the hour in progress, from which the hourly percentiles are computed.
// Buckets are log-scaled: each covers a factor of histGamma, so a percentile is exact within
// about ±2 %. They span histMinMs to histMinMs × histGamma^histBuckets (about 65 s).
const (
	histMinMs   = 0.01
	histGamma   = 1.04
	histBuckets = 400
)

var histLnGamma = math.Log(histGamma)

func histIndex(v float64) int {
	if v <= histMinMs {
		return 0
	}
	i := int(math.Log(v/histMinMs) / histLnGamma)
	if i >= histBuckets {
		i = histBuckets - 1
	}
	return i
}

// histValue returns the value that represents bucket i (its geometric middle).
func histValue(i int) float64 {
	return histMinMs * math.Pow(histGamma, float64(i)+0.5)
}

// hourStats accumulates every sample of one hour.
type hourStats struct {
	total, fails, valid uint32
	sum                 float64
	min, max            float64
	jitterSum           float64
	jitterN             uint32
	hist                []uint16 // allocated on first use, histBuckets entries
}

func (h *hourStats) reset() {
	hist := h.hist
	clear(hist)
	*h = hourStats{hist: hist}
}

func (h *hourStats) addBucket(b *bucketStats) {
	if h.total == 0 {
		h.min = math.MaxFloat64
	}
	h.total += uint32(b.total)
	h.fails += uint32(b.fails)
	h.valid += uint32(len(b.valid))
	h.sum += b.sum
	h.jitterSum += b.jitterSum
	h.jitterN += uint32(b.jitterN)
	if len(b.valid) == 0 {
		return
	}
	if b.min < h.min {
		h.min = b.min
	}
	if b.max > h.max {
		h.max = b.max
	}
	if h.hist == nil {
		h.hist = make([]uint16, histBuckets)
	}
	for _, v := range b.valid {
		if i := histIndex(v); h.hist[i] < math.MaxUint16 {
			h.hist[i]++
		}
	}
}

func (h *hourStats) quantile(q float64) float64 {
	rank := uint32(math.Ceil(q * float64(h.valid)))
	if rank < 1 {
		rank = 1
	}
	var cum uint32
	v := h.max
	for i, n := range h.hist {
		cum += uint32(n)
		if cum >= rank {
			v = histValue(i)
			break
		}
	}
	return math.Min(math.Max(v, h.min), h.max)
}

func (h *hourStats) point(start time.Time, partial bool) RollupPoint {
	rp := emptyPoint(start, time.Hour)
	rp.Partial = partial
	if h.total == 0 {
		return rp
	}
	rp.SampleCount = int(h.total)
	rp.PacketLossPct = float64(h.fails) / float64(h.total) * 100.0
	rp.UpRatio = float64(h.valid) / float64(h.total)
	if h.valid > 0 {
		rp.MinLatencyMs = h.min
		rp.MaxLatencyMs = h.max
		rp.AvgLatencyMs = h.sum / float64(h.valid)
		rp.P50LatencyMs = h.quantile(0.50)
		rp.P95LatencyMs = h.quantile(0.95)
		rp.P99LatencyMs = h.quantile(0.99)
	}
	if h.jitterN > 0 {
		j := h.jitterSum / float64(h.jitterN)
		rp.JitterMs = &j
	}
	return rp
}

// hostAgg is the rollup state of one host: the jitter carry and the hour in progress.
type hostAgg struct {
	mu     sync.Mutex
	jitter jitterState
	hour   int64 // UnixNano start of the hour in stats; 0 if none
	stats  hourStats
}

// rollupMinute rolls up the samples of rb timestamped in [start, end), adds them to the hour
// in progress, and returns the minute point (false if the bucket has no samples). b is a
// scratch buffer.
func (a *hostAgg) rollupMinute(rb *HostRingBuffer, start, end time.Time, b *bucketStats) (RollupPoint, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	b.reset()
	startNs, endNs := start.UnixNano(), end.UnixNano()
	rb.mu.RLock()
	older, newer := rb.partsUnsafe()
	for _, part := range [2][]packedSample{older, newer} {
		for _, s := range part {
			if s.ts >= startNs && s.ts < endNs {
				b.add(round3(s.lat), s.ok, &a.jitter)
			}
		}
	}
	rb.mu.RUnlock()
	if b.total == 0 {
		return RollupPoint{}, false
	}

	if hour := start.Truncate(time.Hour).UnixNano(); a.hour != hour {
		a.stats.reset()
		a.hour = hour
	}
	a.stats.addBucket(b)
	return b.point(start, time.Minute), true
}

// rollupHour returns the point of the hour starting at start if it is the hour in progress,
// and ends that hour. An older unfinished hour (after a pause) is dropped.
func (a *hostAgg) rollupHour(start time.Time) (RollupPoint, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	startNs := start.UnixNano()
	var p RollupPoint
	ok := a.hour == startNs && a.stats.total > 0
	if ok {
		p = a.stats.point(start, false)
	}
	if a.hour != 0 && a.hour <= startNs {
		a.stats.reset()
		a.hour = 0
	}
	return p, ok
}

// currentHour returns the hour in progress as a partial point.
func (a *hostAgg) currentHour() (RollupPoint, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.hour == 0 || a.stats.total == 0 {
		return RollupPoint{}, false
	}
	return a.stats.point(time.Unix(0, a.hour), true), true
}

// packedRollup is the compact in-memory form of a RollupPoint. It is pointer-free
// (not scanned by the GC) and 44 bytes: a series holds hundreds of points for each of
// thousands of hosts, and a full hour series (720 points) then fits a 32 KB size class.
type packedRollup struct {
	ts                           uint32 // Unix seconds (bucket starts are whole seconds); 0 for a zero timestamp
	min, max, avg, p50, p95, p99 float32
	loss, upRatio, jitter        float32 // jitter is NaN when the point has none
	count                        uint32
}

func packRollup(p RollupPoint) packedRollup {
	var ts uint32
	if !p.Timestamp.IsZero() {
		ts = uint32(p.Timestamp.Unix())
	}
	jitter := float32(math.NaN())
	if p.JitterMs != nil {
		jitter = float32(*p.JitterMs)
	}
	return packedRollup{
		ts:      ts,
		min:     float32(p.MinLatencyMs),
		max:     float32(p.MaxLatencyMs),
		avg:     float32(p.AvgLatencyMs),
		p50:     float32(p.P50LatencyMs),
		p95:     float32(p.P95LatencyMs),
		p99:     float32(p.P99LatencyMs),
		loss:    float32(p.PacketLossPct),
		upRatio: float32(p.UpRatio),
		jitter:  jitter,
		count:   uint32(p.SampleCount),
	}
}

// round3 converts a stored float32 back to float64, dropping float32 representation noise.
func round3(v float32) float64 {
	return math.Round(float64(v)*1000) / 1000
}

func (pr packedRollup) unpack(bucket time.Duration) RollupPoint {
	var ts time.Time
	if pr.ts != 0 {
		ts = time.Unix(int64(pr.ts), 0)
	}
	var jitter *float64
	if !math.IsNaN(float64(pr.jitter)) {
		j := round3(pr.jitter)
		jitter = &j
	}
	return RollupPoint{
		Timestamp:         ts,
		BucketDuration:    bucket,
		BucketDurationSec: bucket.Seconds(),
		BucketDurationStr: bucket.String(),
		MinLatencyMs:      round3(pr.min),
		MaxLatencyMs:      round3(pr.max),
		AvgLatencyMs:      round3(pr.avg),
		P50LatencyMs:      round3(pr.p50),
		P95LatencyMs:      round3(pr.p95),
		P99LatencyMs:      round3(pr.p99),
		PacketLossPct:     round3(pr.loss),
		SampleCount:       int(pr.count),
		UpRatio:           round3(pr.upRatio),
		JitterMs:          jitter,
	}
}

// RollupSeries holds a circular buffer of RollupPoints for a single host.
type RollupSeries struct {
	mu       sync.RWMutex
	points   []packedRollup
	bucket   time.Duration
	capacity int
	head     int // position of the oldest point once the series is full
}

// NewRollupSeries creates a series buffer with a fixed capacity for rollups of the given bucket duration.
func NewRollupSeries(capacity int, bucket time.Duration) *RollupSeries {
	if capacity <= 0 {
		capacity = MinuteRollupRetention
	}
	return &RollupSeries{
		points:   make([]packedRollup, 0, min(8, capacity)),
		bucket:   bucket,
		capacity: capacity,
	}
}

// Append adds a new RollupPoint to the series.
func (rs *RollupSeries) Append(point RollupPoint) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	packed := packRollup(point)
	if len(rs.points) < rs.capacity {
		if len(rs.points) == cap(rs.points) {
			grown := make([]packedRollup, len(rs.points), grownCap(cap(rs.points), rs.capacity))
			copy(grown, rs.points)
			rs.points = grown
		}
		rs.points = append(rs.points, packed)
		rs.head = len(rs.points) % rs.capacity
		return
	}

	rs.points[rs.head] = packed
	rs.head = (rs.head + 1) % rs.capacity
}

// GetAll returns all recorded rollup points in chronological order.
func (rs *RollupSeries) GetAll() []RollupPoint {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	n := len(rs.points)
	if n == 0 {
		return nil
	}

	// Oldest point is at head once the buffer has wrapped, at index 0 before that
	start := 0
	if n == rs.capacity {
		start = rs.head
	}
	result := make([]RollupPoint, n)
	for i := 0; i < n; i++ {
		result[i] = rs.points[(start+i)%n].unpack(rs.bucket)
	}
	return result
}

// GetRange returns the rollups timestamped in [start, end), in chronological order.
func (rs *RollupSeries) GetRange(start, end time.Time) []RollupPoint {
	all := rs.GetAll()
	var result []RollupPoint
	for _, p := range all {
		if !p.Timestamp.Before(start) && p.Timestamp.Before(end) {
			result = append(result, p)
		}
	}
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
