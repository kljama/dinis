package timeseries

import (
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

	avg, min, max, p95, loss, count := rb.ComputeSummary()
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
		{Timestamp: now.Add(-50 * time.Second), LatencyMs: 12.0, Success: true},
		{Timestamp: now.Add(-40 * time.Second), LatencyMs: 14.0, Success: true},
		{Timestamp: now.Add(-30 * time.Second), LatencyMs: 16.0, Success: true},
		{Timestamp: now.Add(-20 * time.Second), LatencyMs: 18.0, Success: true},
		{Timestamp: now.Add(-10 * time.Second), LatencyMs: 0.0, Success: false},
	}

	rp := ComputeRollup(now, 1*time.Minute, samples)
	if rp.SampleCount != 5 {
		t.Errorf("expected 5 samples, got %d", rp.SampleCount)
	}
	if rp.PacketLossPct != 20.0 {
		t.Errorf("expected 20%% loss, got %f", rp.PacketLossPct)
	}
	if rp.MinLatencyMs != 12.0 || rp.MaxLatencyMs != 18.0 {
		t.Errorf("expected min 12 max 18, got min %f max %f", rp.MinLatencyMs, rp.MaxLatencyMs)
	}
	if rp.AvgLatencyMs != 15.0 {
		t.Errorf("expected avg 15.0, got %f", rp.AvgLatencyMs)
	}
	if rp.JitterMs <= 0 {
		t.Errorf("expected positive jitter, got %f", rp.JitterMs)
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

	// Outlier host with loss
	st.Record("10.0.0.2", now, 0, false)
	st.Record("10.0.0.2", now.Add(time.Second), 150.0, true)

	outliers := st.GetTopOutliers(10, func(ip string) (bool, string) { return true, "10.0.0.0/24" })
	if len(outliers) != 1 {
		t.Fatalf("expected 1 outlier, got %d", len(outliers))
	}
	if outliers[0].IP != "10.0.0.2" {
		t.Errorf("expected outlier 10.0.0.2, got %s", outliers[0].IP)
	}

	// Test Pruning removes hosts from Outliers
	st.PruneHosts(map[string]bool{"10.0.0.1": true})
	outliersAfterPrune := st.GetTopOutliers(10, func(ip string) (bool, string) { return true, "10.0.0.0/24" })
	if len(outliersAfterPrune) != 0 {
		t.Fatalf("expected 0 outliers after pruning, got %d", len(outliersAfterPrune))
	}
}

func BenchmarkStoreRecord(b *testing.B) {
	st := NewStore()
	now := time.Now()

	for b.Loop() {
		st.Record("192.168.1.50", now, 5.2, true)
	}
}
