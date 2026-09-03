package pinger

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestEngineLifecycle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 200 * time.Millisecond
	cfg.Timeout = 200 * time.Millisecond
	cfg.FailThreshold = 1

	engine := NewEngine(cfg)

	hosts := map[string]*HostState{
		"127.0.0.1": {
			IP:     "127.0.0.1",
			Alias:  "Localhost Loopback",
			CIDR:   "127.0.0.1/32",
			Status: StatusPending,
		},
		"192.0.2.1": {
			IP:         "192.0.2.1",
			Alias:      "Excluded Test Host",
			CIDR:       "192.0.2.0/24",
			Status:     StatusExcluded,
			IsExcluded: true,
		},
	}

	engine.SetHosts(hosts)

	// Test PingSingle
	res := engine.PingSingle(context.Background(), "127.0.0.1")
	if !res.Success {
		t.Fatalf("PingSingle failed: %v", res.Error)
	}

	h, ok := engine.GetHost("127.0.0.1")
	if !ok || h.Status != StatusUp {
		t.Fatalf("expected host 127.0.0.1 to be UP, got status %s", h.Status)
	}
	if h.SentPackets != 1 || h.RecvPackets != 1 {
		t.Errorf("expected 1 sent, 1 recv, got sent=%d, recv=%d", h.SentPackets, h.RecvPackets)
	}

	summary := engine.GetSummary()
	if summary.TotalTargets != 2 {
		t.Errorf("expected 2 total targets, got %d", summary.TotalTargets)
	}
	if summary.UpCount != 1 {
		t.Errorf("expected 1 UP host, got %d", summary.UpCount)
	}
	if summary.ExcludedCount != 1 {
		t.Errorf("expected 1 Excluded host, got %d", summary.ExcludedCount)
	}
}

func TestEnginePacing(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 1000 * time.Millisecond
	cfg.Timeout = 200 * time.Millisecond

	engine := NewEngine(cfg)

	hosts := make(map[string]*HostState)
	for i := 1; i <= 10; i++ {
		ip := fmt.Sprintf("127.0.0.%d", i)
		hosts[ip] = &HostState{
			IP:     ip,
			CIDR:   "127.0.0.0/24",
			Status: StatusPending,
		}
	}
	engine.SetHosts(hosts)

	summary := engine.GetSummary()
	if summary.PacketsPerSec <= 0 {
		t.Errorf("expected positive PacketsPerSec, got %f", summary.PacketsPerSec)
	}
	if summary.PacedDelayMs <= 0 {
		t.Errorf("expected positive PacedDelayMs, got %f", summary.PacedDelayMs)
	}

	// Verify that 10 hosts over 1000ms (with 200ms timeout reserve -> 800ms window) gives ~80ms pace delay
	expectedPace := 80.0
	if summary.PacedDelayMs < 75.0 || summary.PacedDelayMs > 85.0 {
		t.Errorf("expected ~%f ms pace delay, got %f ms", expectedPace, summary.PacedDelayMs)
	}
}

func TestEnginePacingWithWake(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 500 * time.Millisecond
	cfg.Timeout = 100 * time.Millisecond

	engine := NewEngine(cfg)

	hosts := make(map[string]*HostState)
	for i := 1; i <= 5; i++ {
		ip := fmt.Sprintf("127.0.0.%d", i)
		hosts[ip] = &HostState{
			IP:     ip,
			CIDR:   "127.0.0.0/24",
			Status: StatusPending,
		}
	}
	engine.SetHosts(hosts)

	// Simulate pre-startup Wake() call like RebuildTargetList does
	engine.Wake()

	start := time.Now()
	engine.runCycle()
	duration := time.Since(start)

	// With 5 hosts and 50ms max pace delay, the cycle should take at least (5-1)*40ms = 160ms,
	// verifying that pacing was NOT zeroed out by the pre-existing Wake signal.
	if duration < 100*time.Millisecond {
		t.Fatalf("expected runCycle to maintain pacing (>=100ms), but finished in %v (pacing was bypassed)", duration)
	}
}

func TestEngineMinMaxAvgLatencyProgression(t *testing.T) {
	engine := NewEngine(DefaultConfig())
	h := &HostState{
		IP:     "10.0.0.1",
		Status: StatusPending,
	}

	// 1st probe: 20ms
	engine.applyResult(h, PingResult{Success: true, LatencyMs: 20.0})
	if h.MinLatencyMs != 20.0 || h.MaxLatencyMs != 20.0 || h.AvgLatencyMs != 20.0 {
		t.Fatalf("expected min=20, max=20, avg=20 after first probe, got min=%f, max=%f, avg=%f", h.MinLatencyMs, h.MaxLatencyMs, h.AvgLatencyMs)
	}

	// 2nd probe: 50ms (higher latency -> max should update, min should remain 20ms)
	engine.applyResult(h, PingResult{Success: true, LatencyMs: 50.0})
	if h.MinLatencyMs != 20.0 || h.MaxLatencyMs != 50.0 {
		t.Fatalf("expected min=20, max=50 after higher probe, got min=%f, max=%f", h.MinLatencyMs, h.MaxLatencyMs)
	}

	// 3rd probe: 5ms (lower latency -> min should update to 5ms, max should remain 50ms)
	engine.applyResult(h, PingResult{Success: true, LatencyMs: 5.0})
	if h.MinLatencyMs != 5.0 || h.MaxLatencyMs != 50.0 {
		t.Fatalf("expected min=5, max=50 after lower probe, got min=%f, max=%f", h.MinLatencyMs, h.MaxLatencyMs)
	}
}

func TestEngineBeforeStateChangeHook(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FailThreshold = 1
	engine := NewEngine(cfg)

	hosts := map[string]*HostState{
		"127.0.0.1": {
			IP:     "127.0.0.1",
			CIDR:   "127.0.0.1/32",
			Status: StatusPending,
		},
	}
	engine.SetHosts(hosts)

	var beforeCalled, afterCalled bool
	var beforeOldStatus, beforeNewStatus HostStatus
	var observedAlertInAfter bool

	engine.BeforeStateChange = func(host *HostState, oldStatus, newStatus HostStatus) {
		beforeCalled = true
		beforeOldStatus = oldStatus
		beforeNewStatus = newStatus
		if newStatus == StatusUp {
			host.AlertActive = false
			host.AlertID = "custom-id"
		}
	}

	engine.OnStateChange = func(host *HostState, oldStatus, newStatus HostStatus) {
		afterCalled = true
		if host.AlertID == "custom-id" {
			observedAlertInAfter = true
		}
	}

	// 1st probe: Pending -> Up
	res := engine.PingSingle(context.Background(), "127.0.0.1")
	if !res.Success {
		t.Fatalf("expected probe success")
	}

	if !beforeCalled {
		t.Errorf("expected BeforeStateChange to be called")
	}
	if beforeOldStatus != StatusPending || beforeNewStatus != StatusUp {
		t.Errorf("expected Pending->Up, got %v->%v", beforeOldStatus, beforeNewStatus)
	}
	if !afterCalled {
		t.Errorf("expected OnStateChange to be called")
	}
	if !observedAlertInAfter {
		t.Errorf("expected mutations from BeforeStateChange to be visible in OnStateChange")
	}

	h, ok := engine.GetHost("127.0.0.1")
	if !ok || h.AlertID != "custom-id" {
		t.Errorf("expected GetHost to return canonical state updated by BeforeStateChange, got %+v", h)
	}
}

func TestPacketLossZeroLatency(t *testing.T) {
	cfg := EngineConfig{
		Interval:      100 * time.Millisecond,
		Timeout:       50 * time.Millisecond,
		FailThreshold: 2,
		HistorySize:   5,
	}
	engine := NewEngine(cfg)

	host := &HostState{
		IP: "127.0.0.1",
	}

	// Simulate host with ultra-fast 0.0ms latency probe
	engine.applyResult(host, PingResult{
		IP:        "127.0.0.1",
		Success:   true,
		LatencyMs: 0.0,
	})

	if host.PacketLoss != 0.0 {
		t.Errorf("expected 0%% packet loss for 0.0ms probe, got %f%%", host.PacketLoss)
	}

	// Now record a failed probe (-1)
	engine.applyResult(host, PingResult{
		IP:      "127.0.0.1",
		Success: false,
		Error:   "request timeout",
	})

	// 1 success (0.0ms), 1 failure (-1) => 50% packet loss
	if host.PacketLoss != 50.0 {
		t.Errorf("expected 50%% packet loss, got %f%%", host.PacketLoss)
	}
}

