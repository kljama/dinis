package timeseries

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func TestHostRingBuffer(t *testing.T) {
	rb := NewHostRingBuffer(5)
	now := time.Now()

	// Push 3 samples
	rb.Push(now.Add(-3*time.Second), 10.0, true)
	rb.Push(now.Add(-2*time.Second), 20.0, true)
	rb.Push(now.Add(-1*time.Second), 0.0, false)

	samples := rb.GetAll()
	if len(samples) != 3 {
		t.Fatalf("expected 3 samples, got %d", len(samples))
	}

	avg, min, max, p95, loss, jitter, count := rb.ComputeSummary()
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
	if min != 10.0 || max != 20.0 {
		t.Errorf("expected min 10 max 20, got min %f max %f", min, max)
	}
	if avg != 15.0 {
		t.Errorf("expected avg 15.0, got %f", avg)
	}
	if loss < 0.33 || loss > 0.34 {
		t.Errorf("expected loss ~0.333, got %f", loss)
	}
	if jitter != 10.0 {
		t.Errorf("expected jitter 10.0, got %f", jitter)
	}
	if p95 <= 0 {
		t.Errorf("expected valid p95, got %f", p95)
	}

	// Push 4 more samples to trigger circular wrap-around
	rb.Push(now.Add(1*time.Second), 30.0, true)
	rb.Push(now.Add(2*time.Second), 40.0, true)
	rb.Push(now.Add(3*time.Second), 50.0, true)
	rb.Push(now.Add(4*time.Second), 60.0, true)

	samplesWrapped := rb.GetAll()
	if len(samplesWrapped) != 5 {
		t.Fatalf("expected capacity 5 samples after wrap, got %d", len(samplesWrapped))
	}
	// Oldest should be 0.0 (fail), newest should be 60.0
	if samplesWrapped[len(samplesWrapped)-1].LatencyMs != 60.0 {
		t.Errorf("expected newest sample 60.0, got %f", samplesWrapped[len(samplesWrapped)-1].LatencyMs)
	}
}

func TestRollupComputation(t *testing.T) {
	now := time.Now()
	samples := []RawSample{
		{Timestamp: now.Add(-5 * time.Second), LatencyMs: 12.0, Success: true},
		{Timestamp: now.Add(-4 * time.Second), LatencyMs: 14.0, Success: true},
		{Timestamp: now.Add(-3 * time.Second), LatencyMs: 16.0, Success: true},
		{Timestamp: now.Add(-2 * time.Second), LatencyMs: 18.0, Success: true},
		{Timestamp: now.Add(-1 * time.Second), LatencyMs: 0.0, Success: false},
	}

	rp := ComputeRollup(now, 1*time.Minute, samples)
	if rp.SampleCount != 5 {
		t.Errorf("expected 5 samples, got %d", rp.SampleCount)
	}
	if rp.PacketLossPct != 20.0 {
		t.Errorf("expected 20%% loss, got %f", rp.PacketLossPct)
	}
	if rp.AvgLatencyMs != 15.0 {
		t.Errorf("expected avg 15.0, got %f", rp.AvgLatencyMs)
	}
	if rp.MinLatencyMs != 12.0 || rp.MaxLatencyMs != 18.0 {
		t.Errorf("expected min 12 max 18, got min %f max %f", rp.MinLatencyMs, rp.MaxLatencyMs)
	}
	if rp.JitterMs == nil || *rp.JitterMs != 2.0 {
		t.Errorf("expected jitter 2.0, got %v", rp.JitterMs)
	}
}

func TestRollupJitterOrder(t *testing.T) {
	now := time.Now()
	// Chronological samples with large temporal jumps: 10 -> 50 -> 10 -> 50
	// Sorted: 10, 10, 50, 50
	// Sorted consecutive diffs: |10-10| + |50-10| + |50-50| = 0 + 40 + 0 = 40 / 3 = 13.33 (INCORRECT)
	// True temporal consecutive diffs: |50-10| + |10-50| + |50-10| = 40 + 40 + 40 = 120 / 3 = 40.0 (CORRECT)
	samples := []RawSample{
		{Timestamp: now.Add(-4 * time.Second), LatencyMs: 10.0, Success: true},
		{Timestamp: now.Add(-3 * time.Second), LatencyMs: 50.0, Success: true},
		{Timestamp: now.Add(-2 * time.Second), LatencyMs: 10.0, Success: true},
		{Timestamp: now.Add(-1 * time.Second), LatencyMs: 50.0, Success: true},
	}

	rp := ComputeRollup(now, 1*time.Minute, samples)
	if rp.JitterMs == nil || *rp.JitterMs != 40.0 {
		t.Errorf("expected true temporal jitter 40.0 ms, got %v ms", rp.JitterMs)
	}
	if rp.P50LatencyMs != 10.0 && rp.P50LatencyMs != 50.0 {
		t.Errorf("unexpected P50 percentile: %f", rp.P50LatencyMs)
	}
}

func TestStoreIngestAndOutliers(t *testing.T) {
	st := NewStore()
	st.Start()
	defer st.Stop()

	now := time.Now()
	// Good host
	st.Record("10.0.0.1", now, 2.5, true)
	st.Record("10.0.0.1", now.Add(time.Second), 2.6, true)

	// Outlier host with loss and high jitter
	st.Record("10.0.0.2", now, 0, false)
	st.Record("10.0.0.2", now.Add(time.Second), 150.0, true)
	st.Record("10.0.0.2", now.Add(2*time.Second), 10.0, true)

	outliers := st.GetTopOutliers(10, func(ip string) (bool, string, string) { return true, "10.0.0.0/24", "Server 2" })
	if len(outliers) != 1 {
		t.Fatalf("expected 1 outlier, got %d", len(outliers))
	}
	if outliers[0].IP != "10.0.0.2" {
		t.Errorf("expected outlier 10.0.0.2, got %s", outliers[0].IP)
	}
	if outliers[0].Alias != "Server 2" {
		t.Errorf("expected outlier alias 'Server 2', got %s", outliers[0].Alias)
	}
	if outliers[0].JitterMs != 140.0 {
		t.Errorf("expected jitter 140.0, got %f", outliers[0].JitterMs)
	}

	// Test Pruning removes hosts from Outliers
	st.PruneHosts(map[string]bool{"10.0.0.1": true})
	outliersAfterPrune := st.GetTopOutliers(10, func(ip string) (bool, string, string) { return true, "10.0.0.0/24", "Server 2" })
	if len(outliersAfterPrune) != 0 {
		t.Fatalf("expected 0 outliers after pruning, got %d", len(outliersAfterPrune))
	}
}

func TestTimeseriesStoreLifecycleAndRestart(t *testing.T) {
	st := NewStore()

	// 1. Stop before Start should be safe and idempotent
	st.Stop()
	st.Stop()

	// 2. Start multiple times should not create competing loops
	st.Start()
	st.Start()

	now := time.Now()
	st.Record("10.0.0.1", now, 5.0, true)

	// 3. Stop
	st.Stop()
	st.Stop() // Idempotent Stop

	// 4. Restart store
	st.Start()
	st.Record("10.0.0.1", now.Add(time.Second), 6.0, true)

	samples := st.GetRecentRawSamples("10.0.0.1", 10)
	if len(samples) != 2 {
		t.Fatalf("expected 2 recorded samples across restart, got %d", len(samples))
	}

	st.Stop()
}

func TestGetSinceClockSkewNonMonotonic(t *testing.T) {
	now := time.Now()
	rb := NewHostRingBuffer(10)

	// Simulate clock jump backwards (e.g. NTP correction):
	// t0 = now - 50s
	// t1 = now - 20s
	// t2 = now - 40s (clock step backwards)
	// t3 = now - 10s
	rb.Push(now.Add(-50*time.Second), 10.0, true)
	rb.Push(now.Add(-20*time.Second), 20.0, true)
	rb.Push(now.Add(-40*time.Second), 30.0, true)
	rb.Push(now.Add(-10*time.Second), 40.0, true)

	// Query since now - 30s: should match t1 (now-20s) and t3 (now-10s)
	since := rb.GetSince(now.Add(-30 * time.Second))
	if len(since) != 2 {
		t.Fatalf("expected 2 samples despite clock skew, got %d", len(since))
	}
	if since[0].LatencyMs != 20.0 || since[1].LatencyMs != 40.0 {
		t.Errorf("unexpected samples returned: %+v", since)
	}

	// Test RollupSeries GetSince with non-monotonic points
	rs := NewRollupSeries(10, time.Minute)
	rs.Append(RollupPoint{Timestamp: now.Add(-50 * time.Second), AvgLatencyMs: 1.0})
	rs.Append(RollupPoint{Timestamp: now.Add(-20 * time.Second), AvgLatencyMs: 2.0})
	rs.Append(RollupPoint{Timestamp: now.Add(-40 * time.Second), AvgLatencyMs: 3.0})
	rs.Append(RollupPoint{Timestamp: now.Add(-10 * time.Second), AvgLatencyMs: 4.0})

	pts := rs.GetSince(now.Add(-30 * time.Second))
	if len(pts) != 2 {
		t.Fatalf("expected 2 rollup points despite clock skew, got %d", len(pts))
	}
	if pts[0].AvgLatencyMs != 2.0 || pts[1].AvgLatencyMs != 4.0 {
		t.Errorf("unexpected rollup points returned: %+v", pts)
	}
}

func TestGenerateSubnetMatrixNumericCIDRSorting(t *testing.T) {
	input := map[string][]SubnetMatrixCell{
		"192.168.1.0/24": {
			{IP: "192.168.1.10", Status: "UP", LatencyMs: 5.0},
		},
		"10.0.0.0/24": {
			{IP: "10.0.0.1", Status: "UP", LatencyMs: 2.0},
		},
		"9.0.0.0/24": {
			{IP: "9.0.0.5", Status: "UP", LatencyMs: 8.0},
		},
		"172.16.0.0/24": {
			{IP: "172.16.0.1", Status: "UP", LatencyMs: 3.0},
		},
		"2.0.0.0/24": {
			{IP: "2.0.0.1", Status: "UP", LatencyMs: 1.0},
		},
	}

	blocks := GenerateSubnetMatrix(input)
	if len(blocks) != 5 {
		t.Fatalf("expected 5 blocks, got %d", len(blocks))
	}

	expectedOrder := []string{
		"2.0.0.0/24",
		"9.0.0.0/24",
		"10.0.0.0/24",
		"172.16.0.0/24",
		"192.168.1.0/24",
	}

	for i, exp := range expectedOrder {
		if blocks[i].CIDR != exp {
			t.Errorf("block[%d] expected CIDR %s, got %s", i, exp, blocks[i].CIDR)
		}
	}
}

func BenchmarkStoreRecord(b *testing.B) {
	st := NewStore()
	now := time.Now()

	for b.Loop() {
		st.Record("192.168.1.50", now, 5.2, true)
	}
}

func BenchmarkGenerateSubnetMatrix(b *testing.B) {
	// Create sample subnet matrix input with 10 subnets, each having 256 cells
	input := make(map[string][]SubnetMatrixCell)
	for s := 0; s < 10; s++ {
		cidr := fmt.Sprintf("192.168.%d.0/24", s)
		cells := make([]SubnetMatrixCell, 256)
		for i := 0; i < 256; i++ {
			// Reverse/unordered IP order to test sorting performance
			hostIdx := (255 - i)
			cells[i] = SubnetMatrixCell{
				IP:        fmt.Sprintf("192.168.%d.%d", s, hostIdx),
				HostIndex: hostIdx,
				Status:    "UP",
				LatencyMs: float64(i) * 0.1,
			}
		}
		input[cidr] = cells
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		// Deep copy input map slices for each iteration to ensure sort works on same state
		iterInput := make(map[string][]SubnetMatrixCell, len(input))
		for cidr, cells := range input {
			cellsCopy := make([]SubnetMatrixCell, len(cells))
			copy(cellsCopy, cells)
			iterInput[cidr] = cellsCopy
		}

		_ = GenerateSubnetMatrix(iterInput)
	}
}

func TestHostRingBufferDynamicGrowthAndWrap(t *testing.T) {
	rb := NewHostRingBuffer(5)
	if rb.GetAll() != nil {
		t.Fatalf("expected nil for empty buffer")
	}

	now := time.Now()
	// Push 1
	rb.Push(now, 10.0, true)
	if len(rb.GetAll()) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(rb.GetAll()))
	}
	if rb.GetAll()[0].LatencyMs != 10.0 {
		t.Errorf("expected 10.0, got %f", rb.GetAll()[0].LatencyMs)
	}

	// Push up to capacity 5
	for i := 2; i <= 5; i++ {
		rb.Push(now.Add(time.Duration(i)*time.Second), float64(i*10), true)
	}
	samples := rb.GetAll()
	if len(samples) != 5 {
		t.Fatalf("expected 5 samples, got %d", len(samples))
	}
	for i, s := range samples {
		expected := float64((i + 1) * 10)
		if s.LatencyMs != expected {
			t.Errorf("samples[%d] expected %f, got %f", i, expected, s.LatencyMs)
		}
	}

	// Push 6th sample (overwrites oldest 10.0 with 60.0)
	rb.Push(now.Add(6*time.Second), 60.0, true)
	samples = rb.GetAll()
	if len(samples) != 5 {
		t.Fatalf("expected 5 samples after wrap, got %d", len(samples))
	}
	if samples[0].LatencyMs != 20.0 {
		t.Errorf("expected oldest sample 20.0, got %f", samples[0].LatencyMs)
	}
	if samples[4].LatencyMs != 60.0 {
		t.Errorf("expected newest sample 60.0, got %f", samples[4].LatencyMs)
	}
}

func TestRollupSeriesDynamicGrowthAndWrap(t *testing.T) {
	rs := NewRollupSeries(3, time.Minute)
	if rs.GetAll() != nil {
		t.Fatalf("expected nil for empty series")
	}

	now := time.Now()
	rs.Append(RollupPoint{Timestamp: now, AvgLatencyMs: 1.0})
	if len(rs.GetAll()) != 1 {
		t.Fatalf("expected 1 point, got %d", len(rs.GetAll()))
	}

	rs.Append(RollupPoint{Timestamp: now.Add(time.Minute), AvgLatencyMs: 2.0})
	rs.Append(RollupPoint{Timestamp: now.Add(2 * time.Minute), AvgLatencyMs: 3.0})

	pts := rs.GetAll()
	if len(pts) != 3 {
		t.Fatalf("expected 3 points, got %d", len(pts))
	}
	if pts[0].AvgLatencyMs != 1.0 || pts[2].AvgLatencyMs != 3.0 {
		t.Errorf("unexpected points: %+v", pts)
	}

	// Append 4th point (circular wrap)
	rs.Append(RollupPoint{Timestamp: now.Add(3 * time.Minute), AvgLatencyMs: 4.0})
	pts = rs.GetAll()
	if len(pts) != 3 {
		t.Fatalf("expected 3 points after wrap, got %d", len(pts))
	}
	if pts[0].AvgLatencyMs != 2.0 || pts[1].AvgLatencyMs != 3.0 || pts[2].AvgLatencyMs != 4.0 {
		t.Errorf("unexpected points after wrap: %+v", pts)
	}
}

func TestStoreMaxHostsLimitDoesNotEvictActiveHosts(t *testing.T) {
	st := NewStoreWithLimit(3)
	now := time.Now()

	st.Record("10.0.0.1", now, 1.0, true)
	st.Record("10.0.0.2", now, 2.0, true)
	st.Record("10.0.0.3", now, 3.0, true)

	// At capacity: a 4th host is not tracked, existing hosts keep their history
	st.Record("10.0.0.4", now, 4.0, true)
	if len(st.GetRecentRawSamples("10.0.0.4", 10)) != 0 {
		t.Errorf("expected 10.0.0.4 not to be stored while at capacity")
	}

	// Round-robin probing must not thrash existing hosts
	for i := 1; i <= 5; i++ {
		for _, ip := range []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"} {
			st.Record(ip, now.Add(time.Duration(i)*time.Second), 1.0, true)
		}
	}
	for _, ip := range []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"} {
		if n := len(st.GetRecentRawSamples(ip, 10)); n != 6 {
			t.Errorf("expected %s to keep 6 samples, got %d", ip, n)
		}
	}
	if len(st.GetRecentRawSamples("10.0.0.4", 10)) != 0 {
		t.Errorf("expected 10.0.0.4 to stay untracked")
	}

	// RemoveHost frees a slot for a new host
	st.RemoveHost("10.0.0.3")
	if len(st.GetRecentRawSamples("10.0.0.3", 10)) != 0 {
		t.Errorf("expected 10.0.0.3 to be removed")
	}
	st.Record("10.0.0.4", now, 4.0, true)
	if len(st.GetRecentRawSamples("10.0.0.4", 10)) != 1 {
		t.Errorf("expected 10.0.0.4 to be stored after a slot was freed")
	}

	// PruneHosts frees slots too
	st.PruneHosts(map[string]bool{"10.0.0.4": true})
	if len(st.GetRecentRawSamples("10.0.0.1", 10)) != 0 {
		t.Errorf("expected 10.0.0.1 to be pruned")
	}
	if len(st.GetRecentRawSamples("10.0.0.4", 10)) != 1 {
		t.Errorf("expected 10.0.0.4 to remain after pruning")
	}
	st.Record("10.0.0.5", now, 5.0, true)
	if len(st.GetRecentRawSamples("10.0.0.5", 10)) != 1 {
		t.Errorf("expected 10.0.0.5 to be stored after pruning")
	}
}

func TestStoreSetCapacityShrinkEvictsLeastRecent(t *testing.T) {
	st := NewStoreWithLimit(4)
	now := time.Now()

	for _, ip := range []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"} {
		st.Record(ip, now, 1.0, true)
	}
	// Touch host 1 so hosts 2 and 3 are the least recently updated
	st.Record("10.0.0.1", now.Add(time.Second), 1.0, true)

	st.SetCapacity(2)

	st.mu.RLock()
	count := len(st.rawBuffers)
	st.mu.RUnlock()
	if count != 2 {
		t.Fatalf("expected 2 hosts after shrinking capacity, got %d", count)
	}
	for _, ip := range []string{"10.0.0.2", "10.0.0.3"} {
		if len(st.GetRecentRawSamples(ip, 10)) != 0 {
			t.Errorf("expected %s to be evicted on shrink", ip)
		}
	}
	for _, ip := range []string{"10.0.0.1", "10.0.0.4"} {
		if len(st.GetRecentRawSamples(ip, 10)) == 0 {
			t.Errorf("expected %s to be retained on shrink", ip)
		}
	}
}

func TestStoreMassiveHostIngestionBoundedMemory(t *testing.T) {
	st := NewStoreWithLimit(100)
	now := time.Now()

	// Ingest 5000 distinct hosts
	for i := 1; i <= 5000; i++ {
		ip := fmt.Sprintf("172.16.%d.%d", (i/256)%256, i%256)
		st.Record(ip, now, 5.0, true)
	}

	st.mu.RLock()
	rawLen := len(st.rawBuffers)
	lruLen := st.lruList.Len()
	indexLen := len(st.lruIndex)
	st.mu.RUnlock()

	if rawLen != 100 {
		t.Errorf("expected rawBuffers capped at 100, got %d", rawLen)
	}
	if lruLen != 100 {
		t.Errorf("expected lruList length 100, got %d", lruLen)
	}
	if indexLen != 100 {
		t.Errorf("expected lruIndex length 100, got %d", indexLen)
	}
}

func TestStoreSetCapacity(t *testing.T) {
	st := NewStoreWithLimit(2)
	now := time.Now()

	st.Record("10.0.0.1", now, 1.0, true)
	st.Record("10.0.0.2", now, 2.0, true)

	// Expand capacity to 5
	st.SetCapacity(5)

	st.Record("10.0.0.3", now, 3.0, true)
	st.Record("10.0.0.4", now, 4.0, true)

	st.mu.RLock()
	count := len(st.rawBuffers)
	st.mu.RUnlock()

	if count != 4 {
		t.Errorf("expected 4 hosts retained after expanding capacity, got %d", count)
	}
}

func TestParseIPv4ToUint32Bounds(t *testing.T) {
	cases := []struct {
		input string
		want  uint32
	}{
		{"192.168.1.1", 0xC0A80101},
		{"10.0.0.1", 0x0A000001},
		{"0.0.0.0", 0},
		{"255.255.255.255", 0xFFFFFFFF},
		{"999.999.999.999", 0},
		{"256.0.0.1", 0},
		{"192.168.1", 0},
		{"192.168.1.1.1", 0},
		{"", 0},
		{"192.168..1", 0},
		{"abc.def.ghi.jkl", 0},
		{"10.0.0.-1", 0},
	}
	for _, tc := range cases {
		got := parseIPv4ToUint32(tc.input)
		if got != tc.want {
			t.Errorf("parseIPv4ToUint32(%q) = %x, want %x", tc.input, got, tc.want)
		}
	}
}

func TestRollupPointJSONDuration(t *testing.T) {
	now := time.Now()
	rp := ComputeRollup(now, time.Minute, nil)
	data, err := json.Marshal(rp)
	if err != nil {
		t.Fatalf("failed to marshal RollupPoint: %v", err)
	}
	var res map[string]interface{}
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}
	if sec, ok := res["bucketDurationSec"].(float64); !ok || sec != 60 {
		t.Errorf("expected bucketDurationSec=60, got %v", res["bucketDurationSec"])
	}
	if str, ok := res["bucketDurationStr"].(string); !ok || str != "1m0s" {
		t.Errorf("expected bucketDurationStr='1m0s', got %v", res["bucketDurationStr"])
	}
}

func TestRollupSeriesPackRoundTrip(t *testing.T) {
	if size := unsafe.Sizeof(packedRollup{}); size != 44 {
		t.Errorf("expected packedRollup to be 44 bytes, got %d", size)
	}

	ts := time.Date(2026, 10, 5, 12, 34, 0, 0, time.UTC) // bucket starts are whole seconds
	in := RollupPoint{
		Timestamp:     ts,
		MinLatencyMs:  0.137,
		MaxLatencyMs:  987.654,
		AvgLatencyMs:  12.345,
		P50LatencyMs:  11.111,
		P95LatencyMs:  45.678,
		P99LatencyMs:  123.456,
		PacketLossPct: 33.333,
		SampleCount:   60,
		UpRatio:       0.667,
		JitterMs:      ptr(2.5),
	}

	rs := NewRollupSeries(5, time.Minute)
	rs.Append(in)
	pts := rs.GetAll()
	if len(pts) != 1 {
		t.Fatalf("expected 1 point, got %d", len(pts))
	}
	out := pts[0]

	if !out.Timestamp.Equal(ts) {
		t.Errorf("timestamp mismatch: got %v want %v", out.Timestamp, ts)
	}
	if out.SampleCount != 60 {
		t.Errorf("sample count mismatch: got %d", out.SampleCount)
	}
	if out.BucketDuration != time.Minute || out.BucketDurationSec != 60 || out.BucketDurationStr != "1m0s" {
		t.Errorf("bucket duration mismatch: %v %v %q", out.BucketDuration, out.BucketDurationSec, out.BucketDurationStr)
	}
	pairs := map[string][2]float64{
		"min":    {in.MinLatencyMs, out.MinLatencyMs},
		"max":    {in.MaxLatencyMs, out.MaxLatencyMs},
		"avg":    {in.AvgLatencyMs, out.AvgLatencyMs},
		"p50":    {in.P50LatencyMs, out.P50LatencyMs},
		"p95":    {in.P95LatencyMs, out.P95LatencyMs},
		"p99":    {in.P99LatencyMs, out.P99LatencyMs},
		"loss":   {in.PacketLossPct, out.PacketLossPct},
		"up":     {in.UpRatio, out.UpRatio},
		"jitter": {*in.JitterMs, *out.JitterMs},
	}
	for name, p := range pairs {
		if math.Abs(p[0]-p[1]) > 1e-3 {
			t.Errorf("%s mismatch: in=%v out=%v", name, p[0], p[1])
		}
	}
}

func TestRollupDueBuckets(t *testing.T) {
	base := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	var got []time.Time
	record := func(start, end time.Time) {
		if end.Sub(start) != time.Minute {
			t.Errorf("bucket %v-%v is not one minute", start, end)
		}
		got = append(got, start)
	}

	// Not due yet: the bucket ending at 12:01 waits for the grace period
	next := rollupDueBuckets(base.Add(time.Minute+500*time.Millisecond), base.Add(time.Minute), time.Minute, record)
	if len(got) != 0 || !next.Equal(base.Add(time.Minute)) {
		t.Fatalf("expected nothing rolled up before the grace period, got %v next=%v", got, next)
	}

	// Due: exactly one bucket, then the next one is pending
	next = rollupDueBuckets(base.Add(time.Minute+rollupGrace), next, time.Minute, record)
	if len(got) != 1 || !got[0].Equal(base) || !next.Equal(base.Add(2*time.Minute)) {
		t.Fatalf("expected bucket 12:00 rolled up once, got %v next=%v", got, next)
	}

	// A late tick rolls up every missed bucket in order
	got = nil
	next = rollupDueBuckets(base.Add(3*time.Minute+5*time.Second), next, time.Minute, record)
	if len(got) != 2 || !got[0].Equal(base.Add(time.Minute)) || !got[1].Equal(base.Add(2*time.Minute)) {
		t.Fatalf("expected buckets 12:01 and 12:02, got %v", got)
	}

	// After a long pause only the most recent completed bucket is rolled up
	got = nil
	rollupDueBuckets(base.Add(3*time.Hour+30*time.Second), next, time.Minute, record)
	if len(got) != 1 || !got[0].Equal(base.Add(3*time.Hour-time.Minute)) {
		t.Fatalf("expected only bucket 14:59 after a long pause, got %v", got)
	}
}

func TestMinuteAndHourRollupsCoverEverySample(t *testing.T) {
	st := NewStoreWithLimit(10)
	hour := time.Now().Truncate(time.Hour).Add(-2 * time.Hour)

	// 3 samples per minute for 3 minutes, plus one sample exactly on each boundary
	var want int
	for m := 0; m < 3; m++ {
		bucket := hour.Add(time.Duration(m) * time.Minute)
		for _, off := range []time.Duration{0, 20 * time.Second, 59*time.Second + 999*time.Millisecond} {
			st.Record("10.0.0.1", bucket.Add(off), 2.0, true)
			want++
		}
	}

	total := 0
	for m := 0; m < 3; m++ {
		start := hour.Add(time.Duration(m) * time.Minute)
		st.computeMinuteRollups(start, start.Add(time.Minute))
	}
	pts := st.getOrCreateMinuteSeries("10.0.0.1").GetAll()
	if len(pts) != 3 {
		t.Fatalf("expected 3 minute points, got %d", len(pts))
	}
	for i, p := range pts {
		if !p.Timestamp.Equal(hour.Add(time.Duration(i) * time.Minute)) {
			t.Errorf("minute point %d timestamped %v, want bucket start", i, p.Timestamp)
		}
		if p.SampleCount != 3 {
			t.Errorf("minute point %d has %d samples, want 3", i, p.SampleCount)
		}
		total += p.SampleCount
	}
	if total != want {
		t.Errorf("minute rollups hold %d samples, recorded %d", total, want)
	}

	st.computeHourRollups(hour, hour.Add(time.Hour))
	hp := st.getOrCreateHourSeries("10.0.0.1").GetAll()
	if len(hp) != 1 || hp[0].SampleCount != want || !hp[0].Timestamp.Equal(hour) {
		t.Fatalf("expected one hour point with %d samples at %v, got %+v", want, hour, hp)
	}
}

func ptr(v float64) *float64 { return &v }

// One sample per minute (the default 60 s interval): the hour point must report the real
// percentiles of its 60 samples, and jitter across the minute boundaries.
func TestHourPercentilesAndJitterAtOneSamplePerMinute(t *testing.T) {
	st := NewStoreWithLimit(10)
	ip := "10.0.0.1"
	hour := time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC)

	var lat []float64
	for i := 0; i < 60; i++ {
		v := 10.0 + float64(i%2)*10.0 // 10, 20, 10, 20, ...
		if i%12 == 5 {                // 5 spikes of 500 ms (8.3 % of the samples)
			v = 500
		}
		lat = append(lat, v)
		st.Record(ip, hour.Add(time.Duration(i)*time.Minute+30*time.Second), v, true)
	}
	for i := 0; i < 60; i++ {
		start := hour.Add(time.Duration(i) * time.Minute)
		st.computeMinuteRollups(start, start.Add(time.Minute))
	}
	st.computeHourRollups(hour, hour.Add(time.Hour))

	hp := st.getOrCreateHourSeries(ip).GetAll()
	if len(hp) != 1 {
		t.Fatalf("expected 1 hour point, got %d", len(hp))
	}
	p := hp[0]
	var jit float64
	for i := 1; i < len(lat); i++ {
		jit += math.Abs(lat[i] - lat[i-1])
	}
	jit /= float64(len(lat) - 1)

	within := func(got, want, rel float64) bool { return math.Abs(got-want) <= want*rel }
	// 30 samples of 10 ms, 25 of 20 ms and 5 of 500 ms: the 30th of 60 sorted values is 10 ms
	if !within(p.P50LatencyMs, 10, 0.03) {
		t.Errorf("p50: got %.3f, want about 10", p.P50LatencyMs)
	}
	if !within(p.P95LatencyMs, 500, 0.03) || !within(p.P99LatencyMs, 500, 0.03) {
		t.Errorf("p95/p99: got %.3f/%.3f, want about 500 (the spikes)", p.P95LatencyMs, p.P99LatencyMs)
	}
	if p.MaxLatencyMs != 500 || p.MinLatencyMs != 10 {
		t.Errorf("min/max: got %.3f/%.3f, want 10/500", p.MinLatencyMs, p.MaxLatencyMs)
	}
	if p.JitterMs == nil || !within(*p.JitterMs, jit, 0.001) {
		t.Errorf("jitter: got %v, want %.3f", p.JitterMs, jit)
	}

	// Each minute has one sample: its jitter is the step from the previous minute's sample
	m := st.getOrCreateMinuteSeries(ip).GetAll()
	if m[0].JitterMs != nil {
		t.Errorf("the first sample has no predecessor: want null jitter, got %v", *m[0].JitterMs)
	}
	if m[1].JitterMs == nil || *m[1].JitterMs != 10 {
		t.Errorf("minute 1: want jitter 10 (step from 10 to 20 ms), got %v", m[1].JitterMs)
	}
}

func TestJitterIsNullWithoutSamplePairs(t *testing.T) {
	rp := ComputeRollup(time.Now(), time.Minute, []RawSample{{Timestamp: time.Now(), LatencyMs: 5, Success: true}})
	if rp.JitterMs != nil {
		t.Fatalf("expected null jitter for a single sample, got %v", *rp.JitterMs)
	}
	data, _ := json.Marshal(rp)
	if !strings.Contains(string(data), `"jitterMs":null`) {
		t.Fatalf("expected jitterMs null in JSON, got %s", data)
	}
	rs := NewRollupSeries(2, time.Minute)
	rs.Append(rp)
	if got := rs.GetAll()[0].JitterMs; got != nil {
		t.Fatalf("null jitter must survive packing, got %v", *got)
	}
}

// Windows longer than 2 hours read hour points; they must include the hour in progress.
func TestHostHistoryIncludesHourInProgress(t *testing.T) {
	st := NewStoreWithLimit(10)
	ip := "10.0.0.1"
	now := time.Now()
	thisHour := now.Truncate(time.Hour)
	lastHour := thisHour.Add(-time.Hour)

	st.Record(ip, lastHour.Add(10*time.Minute), 5, true)
	st.computeMinuteRollups(lastHour.Add(10*time.Minute), lastHour.Add(11*time.Minute))
	st.computeHourRollups(lastHour, thisHour)

	if now.Sub(thisHour) < time.Minute {
		t.Skip("too close to the start of the hour")
	}
	st.Record(ip, thisHour.Add(5*time.Second), 7, true)
	st.computeMinuteRollups(thisHour, thisHour.Add(time.Minute))

	pts := st.GetHostHistory(ip, 6*time.Hour)
	if len(pts) != 2 {
		t.Fatalf("expected the last full hour and the hour in progress, got %d points", len(pts))
	}
	if pts[0].Partial || !pts[1].Partial || !pts[1].Timestamp.Equal(thisHour) || pts[1].AvgLatencyMs != 7 {
		t.Fatalf("unexpected points %+v", pts)
	}

	// When the hour ends, the hour in progress becomes a normal hour point
	st.computeHourRollups(thisHour, thisHour.Add(time.Hour))
	pts = st.GetHostHistory(ip, 6*time.Hour)
	if len(pts) != 2 || pts[1].Partial {
		t.Fatalf("expected two complete hour points, got %+v", pts)
	}
}

func TestBuffersStopGrowingAtCapacity(t *testing.T) {
	rb := NewHostRingBuffer(RawSampleRetention)
	rs := NewRollupSeries(HourRollupRetention, time.Hour)
	now := time.Now()
	for i := 0; i < 1000; i++ {
		rb.Push(now, 1, true)
		rs.Append(RollupPoint{Timestamp: now, SampleCount: 1})
	}
	if cap(rb.samples) != RawSampleRetention || cap(rs.points) != HourRollupRetention {
		t.Fatalf("expected no slack: ring cap %d (want %d), series cap %d (want %d)",
			cap(rb.samples), RawSampleRetention, cap(rs.points), HourRollupRetention)
	}
	if size := unsafe.Sizeof(packedSample{}); size != 16 {
		t.Fatalf("expected packedSample to be 16 bytes, got %d", size)
	}
}

func TestComputeSummaryReusesScratch(t *testing.T) {
	rb := NewHostRingBuffer(8)
	now := time.Now()
	for i, v := range []float64{10, 50, 10, 50} {
		rb.Push(now.Add(time.Duration(i)*time.Second), v, true)
	}
	rb.Push(now.Add(5*time.Second), 0, false)
	scratch := make([]float64, 0, 16)
	avg, minL, maxL, p95, loss, jitter, count, out := rb.computeSummary(scratch)
	if avg != 30 || minL != 10 || maxL != 50 || p95 != 50 || loss != 0.2 || jitter != 40 || count != 5 {
		t.Fatalf("unexpected summary avg=%v min=%v max=%v p95=%v loss=%v jitter=%v count=%v", avg, minL, maxL, p95, loss, jitter, count)
	}
	if &out[:1][0] != &scratch[:1][0] {
		t.Fatal("expected the scratch buffer to be reused")
	}
}
