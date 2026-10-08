package pinger

import (
	"context"
	"fmt"
	"sync"
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

	// 10 hosts at a 1000ms interval: one probe every 100ms on average
	expectedPace := 100.0
	if summary.PacedDelayMs < 95.0 || summary.PacedDelayMs > 105.0 {
		t.Errorf("expected ~%f ms pace delay, got %f ms", expectedPace, summary.PacedDelayMs)
	}
}

func TestEnginePacingWithWake(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 500 * time.Millisecond
	cfg.Timeout = 100 * time.Millisecond
	cfg.Concurrency = 10

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

	engine.Start()
	defer engine.Stop()

	// Simulate the Wake() call RebuildTargetList makes right after startup
	engine.Wake()

	// Every host is probed within one interval of Start (first probes are spread over it)
	deadline := time.Now().Add(3 * cfg.Interval)
	for {
		unprobed := 0
		for ip := range hosts {
			if h, ok := engine.GetHost(ip); !ok || h.SentPackets == 0 {
				unprobed++
			}
		}
		if unprobed == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected all hosts to be probed within %v of Start, %d still unprobed", 3*cfg.Interval, unprobed)
		}
		time.Sleep(20 * time.Millisecond)
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

func TestCumulativePacketLoss(t *testing.T) {
	cfg := EngineConfig{
		Interval:      100 * time.Millisecond,
		Timeout:       50 * time.Millisecond,
		FailThreshold: 2,
		HistorySize:   5,
	}
	engine := NewEngine(cfg)

	host := &HostState{
		IP: "192.168.1.1",
	}

	// 1 failure
	engine.applyResult(host, PingResult{
		IP:      "192.168.1.1",
		Success: false,
		Error:   "timeout",
	})
	if host.PacketLoss != 100.0 {
		t.Fatalf("expected 100%% loss after 1 failed probe, got %f%%", host.PacketLoss)
	}

	// 9 successes
	for i := 0; i < 9; i++ {
		engine.applyResult(host, PingResult{
			IP:        "192.168.1.1",
			Success:   true,
			LatencyMs: 10.0,
		})
	}

	// Total: 10 sent, 9 received, 1 lost => 10% packet loss
	// Even though HistorySize is 5 and the failure rolled out of LatencyHistory
	if host.SentPackets != 10 || host.RecvPackets != 9 {
		t.Fatalf("expected sent=10 recv=9, got sent=%d recv=%d", host.SentPackets, host.RecvPackets)
	}
	if host.PacketLoss != 10.0 {
		t.Errorf("expected cumulative packet loss of 10.0%%, got %f%%", host.PacketLoss)
	}
}

func TestPerSubnetPacingAndExecution(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 1000 * time.Millisecond
	cfg.Timeout = 100 * time.Millisecond
	cfg.Concurrency = 10

	engine := NewEngine(cfg)

	hosts := map[string]*HostState{
		"127.0.0.1": {
			IP:     "127.0.0.1",
			CIDR:   "127.0.0.0/24",
			Status: StatusPending,
		},
		"127.0.0.2": {
			IP:     "127.0.0.2",
			CIDR:   "127.0.0.0/24",
			Status: StatusPending,
		},
		"127.0.1.1": {
			IP:     "127.0.1.1",
			CIDR:   "127.0.1.0/24",
			Status: StatusPending,
		},
	}

	subnetIntervals := map[string]time.Duration{
		"127.0.0.0/24": 500 * time.Millisecond,
		"127.0.1.0/24": 1500 * time.Millisecond,
	}

	engine.SetTargetsAndIntervals(hosts, subnetIntervals)

	summary := engine.GetSummary()
	// 2 hosts @ 500ms = 4 pkts/sec; 1 host @ 1500ms = 0.67 pkt/sec => total ~4.67 pkts/sec
	if summary.PacketsPerSec < 4.5 || summary.PacketsPerSec > 4.8 {
		t.Errorf("expected ~4.67 pkts/sec across subnets, got %f", summary.PacketsPerSec)
	}
	if summary.PacedDelayMs <= 0 {
		t.Errorf("expected positive weighted PacedDelayMs, got %f", summary.PacedDelayMs)
	}

	engine.Start()
	time.Sleep(1600 * time.Millisecond)
	engine.Stop()

	// Verify that targets received probes according to their independent intervals
	h1, ok1 := engine.GetHost("127.0.0.1")
	if !ok1 || h1.SentPackets < 3 {
		t.Errorf("expected 127.0.0.1 (500ms interval) to have sent at least 3 packets, got ok=%v, sent=%d", ok1, h1.SentPackets)
	}
	h2, ok2 := engine.GetHost("127.0.1.1")
	if !ok2 || h2.SentPackets == 0 {
		t.Errorf("expected 127.0.1.1 (1500ms interval) to have sent packets, got ok=%v, sent=%d", ok2, h2.SentPackets)
	}
	if h1.SentPackets <= h2.SentPackets {
		t.Errorf("expected 500ms subnet host to send more packets than 1500ms subnet host, got h1=%d, h2=%d", h1.SentPackets, h2.SentPackets)
	}
}

func TestEngineUpdateConfigResizesWorkers(t *testing.T) {
	waitWorkers := func(e *Engine, want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if e.ActiveWorkers() == want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("expected %d active workers, got %d", want, e.ActiveWorkers())
	}

	cfg := DefaultConfig()
	cfg.Concurrency = 2
	engine := NewEngine(cfg)
	engine.Start()
	waitWorkers(engine, 2)

	cfg.Concurrency = 6
	engine.UpdateConfig(cfg)
	waitWorkers(engine, 6)

	cfg.Concurrency = 1
	engine.UpdateConfig(cfg)
	waitWorkers(engine, 1)

	engine.Stop()
	waitWorkers(engine, 0)
}

func TestGetAllHostsLiteOmitsHistory(t *testing.T) {
	engine := NewEngine(DefaultConfig())
	engine.SetHosts(map[string]*HostState{
		"127.0.0.1": {IP: "127.0.0.1", CIDR: "127.0.0.1/32", Status: StatusPending},
	})
	engine.PingSingle(context.Background(), "127.0.0.1")

	full := engine.GetAllHosts()
	if len(full) != 1 || len(full[0].LatencyHistory) == 0 {
		t.Fatalf("expected full snapshot to include latency history, got %+v", full)
	}

	lite := engine.GetAllHostsLite()
	if len(lite) != 1 {
		t.Fatalf("expected 1 host, got %d", len(lite))
	}
	if lite[0].LatencyHistory != nil {
		t.Errorf("expected lite snapshot without latency history")
	}
	if lite[0].IP != "127.0.0.1" || lite[0].SentPackets != 1 {
		t.Errorf("expected lite snapshot to carry host fields, got %+v", lite[0])
	}

	// Snapshot is a copy: mutating it must not affect engine state
	lite[0].Alias = "mutated"
	if h, _ := engine.GetHost("127.0.0.1"); h.Alias == "mutated" {
		t.Errorf("expected lite snapshot to be detached from engine state")
	}
}

// probeRecorder collects probe completion times per IP via OnProbeRecorded.
type probeRecorder struct {
	mu    sync.Mutex
	times map[string][]time.Time
}

func newProbeRecorder(e *Engine) *probeRecorder {
	r := &probeRecorder{times: map[string][]time.Time{}}
	e.OnProbeRecorded = func(ip, alias, subnet string, latencyMs float64, success bool, ts time.Time) {
		r.mu.Lock()
		r.times[ip] = append(r.times[ip], ts)
		r.mu.Unlock()
	}
	return r
}

func (r *probeRecorder) get(ip string) []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.times[ip]...)
}

func loopbackHosts(n int, cidr string) map[string]*HostState {
	hosts := make(map[string]*HostState, n)
	for i := 1; i <= n; i++ {
		ip := fmt.Sprintf("127.0.0.%d", i)
		hosts[ip] = &HostState{IP: ip, CIDR: cidr, Status: StatusPending}
	}
	return hosts
}

func TestNextSlot(t *testing.T) {
	s := time.Second
	cases := []struct {
		phase       float64
		iv, after   time.Duration
		want        time.Duration
		description string
	}{
		{0.25, 4 * s, 0, 1 * s, "first slot after start"},
		{0.25, 4 * s, 1 * s, 5 * s, "strictly after a slot"},
		{0.25, 4 * s, 4900 * time.Millisecond, 5 * s, "next slot in the same cycle"},
		{0.25, 4 * s, 5001 * time.Millisecond, 9 * s, "skips to the following cycle"},
		{0.5, 4 * s, -3 * s, -2 * s, "works before the epoch"},
		{0, 4 * s, 8 * s, 12 * s, "phase zero"},
	}
	for _, c := range cases {
		if got := nextSlot(c.phase, c.iv, c.after); got != c.want {
			t.Errorf("%s: nextSlot(%v, %v, %v) = %v, want %v", c.description, c.phase, c.iv, c.after, got, c.want)
		}
	}
}

func TestProbeIntervalBackoffForLongDownHosts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 60 * time.Second
	cfg.DownProbeInterval = 5 * time.Minute
	e := NewEngine(cfg)

	longAgo := time.Now().Add(-10 * time.Minute)
	recently := time.Now().Add(-1 * time.Minute)
	cases := []struct {
		host *HostState
		want time.Duration
		desc string
	}{
		{&HostState{IP: "10.0.0.1", Status: StatusDown, LastStateChange: &longAgo}, 5 * time.Minute, "DOWN for 10 minutes backs off"},
		{&HostState{IP: "10.0.0.2", Status: StatusDown, LastStateChange: &recently}, 60 * time.Second, "DOWN for 1 minute keeps the normal interval"},
		{&HostState{IP: "10.0.0.3", Status: StatusUp, LastStateChange: &longAgo}, 60 * time.Second, "UP host keeps the normal interval"},
	}
	for _, c := range cases {
		if got := e.probeIntervalUnsafe(c.host); got != c.want {
			t.Errorf("%s: got %v, want %v", c.desc, got, c.want)
		}
	}

	e.config.DownProbeInterval = 0
	if got := e.probeIntervalUnsafe(cases[0].host); got != 60*time.Second {
		t.Errorf("back-off disabled: got %v, want 60s", got)
	}
	e.config.DownProbeInterval = 30 * time.Second
	if got := e.probeIntervalUnsafe(cases[0].host); got != 60*time.Second {
		t.Errorf("down-probe interval shorter than the normal interval must not speed probing up: got %v", got)
	}
}

func TestScheduleProbesNewHostPromptly(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 30 * time.Second
	cfg.Timeout = 200 * time.Millisecond
	cfg.Concurrency = 4
	e := NewEngine(cfg)
	rec := newProbeRecorder(e)

	e.SetHosts(loopbackHosts(3, ""))
	e.Start()
	defer e.Stop()

	time.Sleep(500 * time.Millisecond)
	added := time.Now()
	hosts := loopbackHosts(3, "")
	hosts["127.0.0.50"] = &HostState{IP: "127.0.0.50", Status: StatusPending}
	e.SetHosts(hosts)

	deadline := added.Add(newHostSpread + 2*time.Second)
	for len(rec.get("127.0.0.50")) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("host added while running was not probed within %v (interval %v)", newHostSpread+2*time.Second, cfg.Interval)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestScheduleIntervalChangeTakesEffectImmediately(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 30 * time.Second
	cfg.Timeout = 200 * time.Millisecond
	cfg.Concurrency = 4
	e := NewEngine(cfg)
	rec := newProbeRecorder(e)

	hosts := loopbackHosts(3, "")
	e.SetHosts(hosts)
	e.Start()
	defer e.Stop()

	time.Sleep(500 * time.Millisecond)
	changed := time.Now()
	cfg.Interval = 1 * time.Second
	e.UpdateConfig(cfg)
	time.Sleep(4 * time.Second)

	for ip := range hosts {
		n := 0
		for _, ts := range rec.get(ip) {
			if ts.After(changed) {
				n++
			}
		}
		if n < 3 {
			t.Errorf("%s: expected at least 3 probes in the 4s after switching to a 1s interval, got %d", ip, n)
		}
	}
}

func TestScheduleKeepsPerHostSpacingEven(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 1 * time.Second
	cfg.Timeout = 200 * time.Millisecond
	cfg.Concurrency = 4
	e := NewEngine(cfg)
	rec := newProbeRecorder(e)

	hosts := loopbackHosts(5, "")
	e.SetHosts(hosts)
	e.Start()
	time.Sleep(5500 * time.Millisecond)
	e.Stop()

	for ip := range hosts {
		ts := rec.get(ip)
		if len(ts) < 4 {
			t.Errorf("%s: expected at least 4 probes in 5.5s at a 1s interval, got %d", ip, len(ts))
			continue
		}
		for i := 1; i < len(ts); i++ {
			if gap := ts[i].Sub(ts[i-1]); gap < 800*time.Millisecond || gap > 1200*time.Millisecond {
				t.Errorf("%s: probe gap %v outside 0.8-1.2s for a 1s interval", ip, gap)
			}
		}
	}
}

func TestScheduleStopsProbingRemovedAndExcludedHosts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 500 * time.Millisecond
	cfg.Timeout = 200 * time.Millisecond
	cfg.Concurrency = 4
	e := NewEngine(cfg)
	rec := newProbeRecorder(e)

	e.SetHosts(loopbackHosts(3, ""))
	e.Start()
	defer e.Stop()
	time.Sleep(1200 * time.Millisecond)

	// Remove 127.0.0.2 and exclude 127.0.0.3
	hosts := loopbackHosts(1, "")
	hosts["127.0.0.3"] = &HostState{IP: "127.0.0.3", Status: StatusExcluded, IsExcluded: true}
	e.SetHosts(hosts)
	time.Sleep(300 * time.Millisecond) // let probes already in flight finish
	before2, before3 := len(rec.get("127.0.0.2")), len(rec.get("127.0.0.3"))
	time.Sleep(1500 * time.Millisecond)

	if after := len(rec.get("127.0.0.2")); after != before2 {
		t.Errorf("removed host was probed %d more times", after-before2)
	}
	if after := len(rec.get("127.0.0.3")); after != before3 {
		t.Errorf("excluded host was probed %d more times", after-before3)
	}
	if len(rec.get("127.0.0.1")) < 4 {
		t.Errorf("remaining host should keep being probed, got %d probes", len(rec.get("127.0.0.1")))
	}
}

// Unreachable hosts hold a worker for the whole timeout. Their probes may use only part of
// the pool, so a host that answers keeps its interval during a large outage.
func TestScheduleFailingHostsCannotStarveHealthyHosts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 1 * time.Second
	cfg.Timeout = 1 * time.Second
	cfg.Concurrency = 4
	e := NewEngine(cfg)
	rec := newProbeRecorder(e)

	hosts := map[string]*HostState{
		"127.0.0.1": {IP: "127.0.0.1", CIDR: "127.0.0.0/24", Status: StatusPending},
	}
	for i := 1; i <= 20; i++ {
		ip := fmt.Sprintf("192.0.2.%d", i) // TEST-NET-1: unreachable
		hosts[ip] = &HostState{IP: ip, CIDR: "192.0.2.0/24", Status: StatusPending}
	}
	e.SetHosts(hosts)
	e.Start()
	start := time.Now()
	time.Sleep(10 * time.Second)
	e.Stop()

	// After the first round every unreachable host is a failing host, capped below the pool size.
	var maxGap time.Duration
	ts := rec.get("127.0.0.1")
	for i := 1; i < len(ts); i++ {
		if ts[i-1].Sub(start) < 6*time.Second {
			continue
		}
		if gap := ts[i].Sub(ts[i-1]); gap > maxGap {
			maxGap = gap
		}
	}
	if maxGap == 0 {
		t.Fatalf("healthy host was not probed after warm-up (%d probes total)", len(ts))
	}
	t.Logf("healthy host: max probe gap after warm-up %v (interval %v)", maxGap, cfg.Interval)
	if maxGap > 1500*time.Millisecond {
		t.Errorf("healthy host probe gap reached %v with a 1s interval while unreachable hosts saturated the pool", maxGap)
	}
}
