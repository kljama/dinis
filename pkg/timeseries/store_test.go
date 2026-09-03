package timeseries

import (
	"fmt"
	"testing"
	"time"
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
	if rp.JitterMs != 2.0 {
		t.Errorf("expected jitter 2.0, got %f", rp.JitterMs)
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
	if rp.JitterMs != 40.0 {
		t.Errorf("expected true temporal jitter 40.0 ms, got %f ms", rp.JitterMs)
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

	outliers := st.GetTopOutliers(10, func(ip string) (bool, string) { return true, "10.0.0.0/24" })
	if len(outliers) != 1 {
		t.Fatalf("expected 1 outlier, got %d", len(outliers))
	}
	if outliers[0].IP != "10.0.0.2" {
		t.Errorf("expected outlier 10.0.0.2, got %s", outliers[0].IP)
	}
	if outliers[0].JitterMs != 140.0 {
		t.Errorf("expected jitter 140.0, got %f", outliers[0].JitterMs)
	}

	// Test Pruning removes hosts from Outliers
	st.PruneHosts(map[string]bool{"10.0.0.1": true})
	outliersAfterPrune := st.GetTopOutliers(10, func(ip string) (bool, string) { return true, "10.0.0.0/24" })
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
	rs := NewRollupSeries(10)
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

func TestAggregateRollupsWeightedMath(t *testing.T) {
	now := time.Now()

	// Minute 1: 10 samples, 10ms avg, P50=10, P95=10, P99=10, jitter=1.0, 0% loss, UpRatio=1.0
	m1 := RollupPoint{
		Timestamp:      now.Add(-2 * time.Minute),
		BucketDuration: 1 * time.Minute,
		SampleCount:    10,
		UpRatio:        1.0,
		PacketLossPct:  0.0,
		MinLatencyMs:   8.0,
		MaxLatencyMs:   12.0,
		AvgLatencyMs:   10.0,
		P50LatencyMs:   10.0,
		P95LatencyMs:   12.0,
		P99LatencyMs:   12.0,
		JitterMs:       1.0,
	}

	// Minute 2: 90 samples, 20ms avg, P50=20, P95=25, P99=25, jitter=2.0, 0% loss, UpRatio=1.0
	m2 := RollupPoint{
		Timestamp:      now.Add(-1 * time.Minute),
		BucketDuration: 1 * time.Minute,
		SampleCount:    90,
		UpRatio:        1.0,
		PacketLossPct:  0.0,
		MinLatencyMs:   15.0,
		MaxLatencyMs:   30.0,
		AvgLatencyMs:   20.0,
		P50LatencyMs:   20.0,
		P95LatencyMs:   25.0,
		P99LatencyMs:   25.0,
		JitterMs:       2.0,
	}

	hourRollup := AggregateRollups(now, 1*time.Hour, []RollupPoint{m1, m2})

	// Total samples: 100
	if hourRollup.SampleCount != 100 {
		t.Errorf("expected SampleCount 100, got %d", hourRollup.SampleCount)
	}

	// Weighted average latency: (10*10 + 90*20)/100 = 19.0 (NOT (10+20)/2 = 15.0)
	if hourRollup.AvgLatencyMs != 19.0 {
		t.Errorf("expected weighted AvgLatencyMs 19.0, got %f", hourRollup.AvgLatencyMs)
	}

	// Weighted jitter: (10*1.0 + 90*2.0)/100 = 1.9
	if hourRollup.JitterMs != 1.9 {
		t.Errorf("expected weighted JitterMs 1.9, got %f", hourRollup.JitterMs)
	}

	// Weighted P50: (10*10 + 90*20)/100 = 19.0
	if hourRollup.P50LatencyMs != 19.0 {
		t.Errorf("expected weighted P50 19.0, got %f", hourRollup.P50LatencyMs)
	}

	// Weighted P95: (10*12 + 90*25)/100 = (120 + 2250)/100 = 23.7
	if hourRollup.P95LatencyMs != 23.7 {
		t.Errorf("expected weighted P95 23.7, got %f", hourRollup.P95LatencyMs)
	}

	// Min and Max
	if hourRollup.MinLatencyMs != 8.0 || hourRollup.MaxLatencyMs != 30.0 {
		t.Errorf("expected min 8.0 max 30.0, got min %f max %f", hourRollup.MinLatencyMs, hourRollup.MaxLatencyMs)
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
	rs := NewRollupSeries(3)
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

func TestStoreLRUEvictionAndMaxHostsLimit(t *testing.T) {
	st := NewStoreWithLimit(3)
	now := time.Now()

	// Insert host 1, 2, 3
	st.Record("10.0.0.1", now, 1.0, true)
	st.Record("10.0.0.2", now, 2.0, true)
	st.Record("10.0.0.3", now, 3.0, true)

	// Access host 1 to make it most recently used (LRU order becomes: 2, 3, 1)
	st.Record("10.0.0.1", now.Add(time.Second), 1.5, true)

	// Insert host 4 -> host 2 should be evicted
	st.Record("10.0.0.4", now, 4.0, true)

	if len(st.GetRecentRawSamples("10.0.0.2", 10)) != 0 {
		t.Errorf("expected 10.0.0.2 to be evicted by LRU")
	}
	if len(st.GetRecentRawSamples("10.0.0.1", 10)) != 2 {
		t.Errorf("expected 10.0.0.1 to remain in store with 2 samples")
	}
	if len(st.GetRecentRawSamples("10.0.0.3", 10)) != 1 {
		t.Errorf("expected 10.0.0.3 to remain in store")
	}
	if len(st.GetRecentRawSamples("10.0.0.4", 10)) != 1 {
		t.Errorf("expected 10.0.0.4 to be present in store")
	}

	// RemoveHost
	st.RemoveHost("10.0.0.3")
	if len(st.GetRecentRawSamples("10.0.0.3", 10)) != 0 {
		t.Errorf("expected 10.0.0.3 to be removed")
	}

	// PruneHosts
	st.PruneHosts(map[string]bool{"10.0.0.4": true})
	if len(st.GetRecentRawSamples("10.0.0.1", 10)) != 0 {
		t.Errorf("expected 10.0.0.1 to be pruned")
	}
	if len(st.GetRecentRawSamples("10.0.0.4", 10)) != 1 {
		t.Errorf("expected 10.0.0.4 to remain after pruning")
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




