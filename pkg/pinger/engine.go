package pinger

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"dinis/pkg/timeseries"
)

// Status types for a monitored host.
type HostStatus string

const (
	StatusPending  HostStatus = "PENDING"
	StatusUp       HostStatus = "UP"
	StatusDown     HostStatus = "DOWN"
	StatusExcluded HostStatus = "EXCLUDED"
)

// HostState represents the live monitoring and metric state of an IP target.
type HostState struct {
	IP                string     `json:"ip"`
	Alias             string     `json:"alias"`
	CIDR              string     `json:"cidr"`
	Status            HostStatus `json:"status"`
	LatencyMs         float64    `json:"latencyMs"`
	MinLatencyMs      float64    `json:"minLatencyMs"`
	MaxLatencyMs      float64    `json:"maxLatencyMs"`
	AvgLatencyMs      float64    `json:"avgLatencyMs"`
	PacketLoss        float64    `json:"packetLoss"`
	SentPackets       uint64     `json:"sentPackets"`
	RecvPackets       uint64     `json:"recvPackets"`
	ConsecutiveFails  int        `json:"consecutiveFails"`
	LastSeen          *time.Time `json:"lastSeen"`
	LastChecked       *time.Time `json:"lastChecked"`
	LastStateChange   *time.Time `json:"lastStateChange"`
	LatencyHistory    []float64  `json:"latencyHistory"`
	IsExcluded        bool       `json:"isExcluded"`
	ExclusionReason   string     `json:"exclusionReason"`
	DiscoveredAt      *time.Time `json:"discoveredAt,omitempty"`
	LastDiscovered    *time.Time `json:"lastDiscovered,omitempty"`
	IsStatic          bool       `json:"isStatic"`
	AlertActive       bool       `json:"alertActive"`
	AlertID           string     `json:"alertId"`
	AlertAcknowledged bool       `json:"alertAcknowledged"`
	AlertAckBy        string     `json:"alertAckBy"`
	AlertAckNote      string     `json:"alertAckNote"`
	AlertAckAt        *time.Time `json:"alertAckAt"`
	AlertStartedAt    *time.Time `json:"alertStartedAt"`
	LastError         string     `json:"lastError"`
}

// EngineConfig holds configuration parameters for the async ICMP engine.
type EngineConfig struct {
	Interval       time.Duration
	Timeout        time.Duration
	Concurrency    int
	FailThreshold  int
	HistorySize    int
	MaxMetricHosts int
}

// DefaultConfig returns default engine settings.
func DefaultConfig() EngineConfig {
	return EngineConfig{
		Interval:       60 * time.Second,
		Timeout:        1000 * time.Millisecond,
		Concurrency:    100,
		FailThreshold:  2,
		HistorySize:    20,
		MaxMetricHosts: 10000,
	}
}

// CycleSummary provides aggregated statistics across all monitored targets.
type CycleSummary struct {
	TotalTargets   int       `json:"totalTargets"`
	SubnetCapacity int       `json:"subnetCapacity"`
	UpCount        int       `json:"upCount"`
	DownCount      int       `json:"downCount"`
	AckCount       int       `json:"ackCount"`
	PendingCount   int       `json:"pendingCount"`
	ExcludedCount  int       `json:"excludedCount"`
	AlertsActive   int       `json:"alertsActive"`
	AlertsUnack    int       `json:"alertsUnack"`
	AvgLatencyMs   float64   `json:"avgLatencyMs"`
	PacketsPerSec  float64   `json:"packetsPerSec"`
	PacedDelayMs   float64   `json:"pacedDelayMs"`
	Timestamp      time.Time `json:"timestamp"`
}

type subnetRunner struct {
	cidr     string
	interval time.Duration
	inFlight int32
	wakeChan chan struct{}
	stopChan chan struct{}
}

// Engine runs the periodic, high-concurrency ICMP probing loops across targets.
type Engine struct {
	mu      sync.RWMutex
	cycleMu sync.Mutex
	config  EngineConfig
	prober  *SingleProber
	tsStore *timeseries.Store

	hosts           map[string]*HostState
	subnetIntervals map[string]time.Duration
	subnetRunners   map[string]*subnetRunner
	runnerWg        sync.WaitGroup

	workChan chan string
	workerWg sync.WaitGroup

	// Callbacks
	BeforeStateChange func(host *HostState, oldStatus, newStatus HostStatus)
	OnHostUpdated     func(host *HostState)
	OnStateChange     func(host *HostState, oldStatus, newStatus HostStatus)
	OnProbeRecorded   func(ip, alias, subnet string, latencyMs float64, success bool, ts time.Time)

	wakeChan chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewEngine creates a new ICMP probing engine.
func NewEngine(cfg EngineConfig) *Engine {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 100
	}
	if cfg.Interval < 500*time.Millisecond {
		cfg.Interval = 500 * time.Millisecond
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 1000 * time.Millisecond
	}
	if cfg.FailThreshold <= 0 {
		cfg.FailThreshold = 2
	}
	if cfg.HistorySize <= 0 {
		cfg.HistorySize = 20
	}

	maxMetricHosts := cfg.MaxMetricHosts
	if maxMetricHosts <= 0 {
		maxMetricHosts = timeseries.DefaultMaxHosts
	}

	return &Engine{
		config:          cfg,
		prober:          NewSingleProber(),
		tsStore:         timeseries.NewStoreWithLimit(maxMetricHosts),
		hosts:           make(map[string]*HostState),
		subnetIntervals: make(map[string]time.Duration),
		subnetRunners:   make(map[string]*subnetRunner),
		wakeChan:        make(chan struct{}, 1),
	}
}

// GetTimeseriesStore returns the underlying time-series metric store.
func (e *Engine) GetTimeseriesStore() *timeseries.Store {
	return e.tsStore
}

// Wake signals all subnet runners to immediately start a cycle.
func (e *Engine) Wake() {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, sr := range e.subnetRunners {
		select {
		case sr.wakeChan <- struct{}{}:
		default:
		}
	}
}

// UpdateConfig dynamically updates the engine configuration and wakes the polling loop.
func (e *Engine) UpdateConfig(cfg EngineConfig) {
	e.mu.Lock()
	e.config = cfg
	if cfg.MaxMetricHosts > 0 && e.tsStore != nil {
		e.tsStore.SetCapacity(cfg.MaxMetricHosts)
	}
	e.reconcileRunnersUnsafe()
	e.mu.Unlock()
	e.Wake()
}

// SetTargetsAndIntervals updates both the target host map and per-subnet intervals atomically.
// If subnetIntervals is nil, existing subnet intervals are preserved.
func (e *Engine) SetTargetsAndIntervals(hosts map[string]*HostState, subnetIntervals map[string]time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if subnetIntervals != nil {
		e.subnetIntervals = make(map[string]time.Duration, len(subnetIntervals))
		for cidr, interval := range subnetIntervals {
			if interval > 0 {
				e.subnetIntervals[cidr] = interval
			}
		}
	}

	// Merge existing stats if present
	newMap := make(map[string]*HostState, len(hosts))
	for ip, newH := range hosts {
		if oldH, exists := e.hosts[ip]; exists {
			// Keep existing metrics and history, but update metadata
			oldH.Alias = newH.Alias
			oldH.CIDR = newH.CIDR
			oldH.IsExcluded = newH.IsExcluded
			oldH.ExclusionReason = newH.ExclusionReason
			oldH.DiscoveredAt = newH.DiscoveredAt
			oldH.LastDiscovered = newH.LastDiscovered
			oldH.IsStatic = newH.IsStatic
			if newH.IsExcluded {
				oldH.Status = StatusExcluded
				oldH.AlertActive = false
				oldH.AlertAcknowledged = false
				oldH.AlertID = ""
				oldH.AlertAckBy = ""
				oldH.AlertAckNote = ""
				oldH.AlertAckAt = nil
				oldH.AlertStartedAt = nil
			} else if oldH.Status == StatusExcluded {
				oldH.Status = StatusPending
				oldH.ConsecutiveFails = 0
			} else {
				// Keep alert state synchronized with coordinator alert manager
				oldH.AlertActive = newH.AlertActive
				oldH.AlertID = newH.AlertID
				oldH.AlertAcknowledged = newH.AlertAcknowledged
				oldH.AlertAckBy = newH.AlertAckBy
				oldH.AlertAckNote = newH.AlertAckNote
				oldH.AlertAckAt = newH.AlertAckAt
				oldH.AlertStartedAt = newH.AlertStartedAt
			}

			newMap[ip] = oldH
		} else {
			if newH.LatencyHistory == nil {
				newH.LatencyHistory = make([]float64, 0, e.config.HistorySize)
			}
			newMap[ip] = newH
		}
	}
	e.hosts = newMap
	if e.tsStore != nil {
		activeIPs := make(map[string]bool, len(newMap))
		for ip := range newMap {
			activeIPs[ip] = true
		}
		e.tsStore.PruneHosts(activeIPs)
	}

	e.reconcileRunnersUnsafe()
}

// SetHosts updates the target host map, retaining existing subnet intervals atomically.
func (e *Engine) SetHosts(hosts map[string]*HostState) {
	e.SetTargetsAndIntervals(hosts, nil)
}

func (e *Engine) effectiveIntervalUnsafe(cidr string) time.Duration {
	if interval, ok := e.subnetIntervals[cidr]; ok && interval > 0 {
		if interval < 500*time.Millisecond {
			return 500 * time.Millisecond
		}
		return interval
	}
	if e.config.Interval < 500*time.Millisecond {
		return 500 * time.Millisecond
	}
	return e.config.Interval
}

// GetHost returns a copy of the host state for an IP.
func (e *Engine) GetHost(ip string) (*HostState, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	h, ok := e.hosts[ip]
	if !ok {
		return nil, false
	}
	cpy := *h
	cpy.LatencyHistory = append([]float64(nil), h.LatencyHistory...)
	return &cpy, true
}

// GetAllHosts returns a snapshot slice of all monitored hosts.
func (e *Engine) GetAllHosts() []*HostState {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*HostState, 0, len(e.hosts))
	for _, h := range e.hosts {
		cpy := *h
		cpy.LatencyHistory = append([]float64(nil), h.LatencyHistory...)
		result = append(result, &cpy)
	}
	return result
}

// GetSummary calculates an aggregated summary across all hosts.
func (e *Engine) GetSummary() CycleSummary {
	e.mu.RLock()
	defer e.mu.RUnlock()

	summary := CycleSummary{
		TotalTargets: len(e.hosts),
		Timestamp:    time.Now(),
	}

	var sumLatency float64
	var upWithLatency int
	subnetCounts := make(map[string]int)

	for _, h := range e.hosts {
		switch h.Status {
		case StatusUp:
			summary.UpCount++
			if h.LatencyMs > 0 {
				sumLatency += h.LatencyMs
				upWithLatency++
			}
		case StatusDown:
			summary.DownCount++
		case StatusExcluded:
			summary.ExcludedCount++
		case StatusPending:
			summary.PendingCount++
		}

		if !h.IsExcluded {
			subnetCounts[h.CIDR]++
			if h.Status == StatusDown {
				if h.AlertAcknowledged {
					summary.AckCount++
				}
				if h.AlertActive {
					summary.AlertsActive++
					if !h.AlertAcknowledged {
						summary.AlertsUnack++
					}
				}
			}
		}
	}

	if upWithLatency > 0 {
		summary.AvgLatencyMs = math.Round((sumLatency/float64(upWithLatency))*100) / 100
	}

	// Calculate pacing rates across subnets
	activeTargets := summary.TotalTargets - summary.ExcludedCount
	if activeTargets > 0 {
		var totalPacketsPerSec float64
		var sumWeightedPace float64
		var totalPacedHosts int

		for cidr, count := range subnetCounts {
			interval := e.effectiveIntervalUnsafe(cidr)
			if interval > 0 && count > 0 {
				totalPacketsPerSec += float64(count) / interval.Seconds()

				reserveTail := e.config.Timeout
				if reserveTail > interval/2 {
					reserveTail = interval / 2
				}
				dispatchWindow := interval - reserveTail
				if dispatchWindow < 100*time.Millisecond {
					dispatchWindow = interval
				}
				paceDelay := dispatchWindow / time.Duration(count)
				sumWeightedPace += (float64(paceDelay.Microseconds()) / 1000.0) * float64(count)
				totalPacedHosts += count
			}
		}

		if totalPacketsPerSec > 0 {
			summary.PacketsPerSec = math.Round(totalPacketsPerSec*10) / 10
		}
		if totalPacedHosts > 0 {
			summary.PacedDelayMs = math.Round((sumWeightedPace/float64(totalPacedHosts))*100) / 100
		}
	}

	return summary
}

// SetHostAlertState updates the alert properties for a host.
func (e *Engine) SetHostAlertState(ip string, active bool, id string, ack bool, ackBy, ackNote string, ackAt, startedAt *time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if h, ok := e.hosts[ip]; ok {
		h.AlertActive = active
		h.AlertID = id
		h.AlertAcknowledged = ack
		h.AlertAckBy = ackBy
		h.AlertAckNote = ackNote
		h.AlertAckAt = ackAt
		h.AlertStartedAt = startedAt
	}
}

// TriggerSweep asynchronously initiates an immediate probing round.
func (e *Engine) TriggerSweep() {
	e.Wake()
}

func (e *Engine) reconcileRunnersUnsafe() {
	if e.ctx == nil {
		return
	}

	neededRunners := make(map[string]time.Duration)
	for cidr, interval := range e.subnetIntervals {
		if interval > 0 {
			neededRunners[cidr] = interval
		} else {
			neededRunners[cidr] = e.config.Interval
		}
	}

	// Check if we need default runner for unassigned/static targets
	hasUnassigned := false
	for _, h := range e.hosts {
		if h.CIDR == "" {
			hasUnassigned = true
			break
		}
		if _, exists := neededRunners[h.CIDR]; !exists {
			hasUnassigned = true
			break
		}
	}
	if hasUnassigned || len(neededRunners) == 0 {
		neededRunners[""] = e.config.Interval
	}

	// Stop runners no longer needed, and update interval in-place if changed
	for cidr, r := range e.subnetRunners {
		expectedInterval, exists := neededRunners[cidr]
		if !exists {
			close(r.stopChan)
			delete(e.subnetRunners, cidr)
		} else if expectedInterval != r.interval {
			r.interval = expectedInterval
		}
	}

	// Start new runners
	for cidr, interval := range neededRunners {
		if _, running := e.subnetRunners[cidr]; !running {
			sr := &subnetRunner{
				cidr:     cidr,
				interval: interval,
				wakeChan: make(chan struct{}, 1),
				stopChan: make(chan struct{}),
			}
			e.subnetRunners[cidr] = sr
			e.runnerWg.Add(1)
			go e.runSubnetLoop(sr)
		}
	}
}

// Start starts the background polling loops and shared worker pool.
func (e *Engine) Start() {
	e.mu.Lock()
	if e.ctx != nil {
		e.mu.Unlock()
		return
	}
	e.ctx, e.cancel = context.WithCancel(context.Background())
	if e.tsStore != nil {
		e.tsStore.Start()
	}

	numWorkers := e.config.Concurrency
	if numWorkers <= 0 {
		numWorkers = 100
	}
	workChan := make(chan string, numWorkers*2)
	e.workChan = workChan
	e.workerWg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go e.workerLoop(workChan)
	}

	e.reconcileRunnersUnsafe()
	e.mu.Unlock()
}

// Stop stops all background polling loops and closes prober socket resources.
func (e *Engine) Stop() {
	e.mu.Lock()
	if e.cancel != nil {
		e.cancel()
	}
	for cidr, r := range e.subnetRunners {
		close(r.stopChan)
		delete(e.subnetRunners, cidr)
	}
	if e.tsStore != nil {
		e.tsStore.Stop()
	}
	e.mu.Unlock()

	e.runnerWg.Wait()

	e.mu.Lock()
	if e.workChan != nil {
		close(e.workChan)
	}
	e.mu.Unlock()

	e.workerWg.Wait()

	e.mu.Lock()
	e.workChan = nil
	e.ctx = nil
	e.cancel = nil
	e.mu.Unlock()

	if e.prober != nil {
		e.prober.Close()
	}
}

func (e *Engine) workerLoop(workChan <-chan string) {
	defer e.workerWg.Done()
	for {
		select {
		case <-e.ctx.Done():
			return
		case ip, ok := <-workChan:
			if !ok {
				return
			}
			e.probeAndApply(ip)
		}
	}
}

func (e *Engine) probeAndApply(ip string) {
	e.mu.RLock()
	timeout := e.config.Timeout
	ctx := e.ctx
	e.mu.RUnlock()

	probeCtx := context.Background()
	if ctx != nil {
		probeCtx = ctx
	}

	res := e.prober.Probe(probeCtx, ip, timeout)

	e.mu.Lock()
	h, exists := e.hosts[ip]
	if !exists || h.IsExcluded {
		e.mu.Unlock()
		return
	}

	oldStatus := h.Status
	e.applyResult(h, res)
	newStatus := h.Status
	statusChanged := (oldStatus != newStatus)

	if statusChanged && e.BeforeStateChange != nil {
		e.BeforeStateChange(h, oldStatus, newStatus)
	}

	cpy := *h
	cpy.LatencyHistory = append([]float64(nil), h.LatencyHistory...)
	e.mu.Unlock()

	if statusChanged && e.OnStateChange != nil {
		e.OnStateChange(&cpy, oldStatus, newStatus)
	}
	if e.OnHostUpdated != nil {
		e.OnHostUpdated(&cpy)
	}
}

// PingSingle immediately probes a single host and updates its state.
func (e *Engine) PingSingle(ctx context.Context, ip string) PingResult {
	e.mu.RLock()
	timeout := e.config.Timeout
	e.mu.RUnlock()

	res := e.prober.Probe(ctx, ip, timeout)

	e.mu.Lock()
	h, exists := e.hosts[ip]
	var oldStatus HostStatus
	var statusChanged bool
	var updatedHost *HostState

	if exists {
		oldStatus = h.Status
		e.applyResult(h, res)
		if h.Status != oldStatus {
			statusChanged = true
			if e.BeforeStateChange != nil {
				e.BeforeStateChange(h, oldStatus, h.Status)
			}
		}
		cpy := *h
		cpy.LatencyHistory = append([]float64(nil), h.LatencyHistory...)
		updatedHost = &cpy
	}
	e.mu.Unlock()

	if exists {
		if statusChanged && e.OnStateChange != nil {
			e.OnStateChange(updatedHost, oldStatus, updatedHost.Status)
		}
		if e.OnHostUpdated != nil {
			e.OnHostUpdated(updatedHost)
		}
	}

	return res
}

func (e *Engine) runSubnetLoop(sr *subnetRunner) {
	defer e.runnerWg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-sr.stopChan:
			return
		default:
		}

		cycleStart := time.Now()
		e.runSubnetCycle(sr)

		e.mu.RLock()
		interval := sr.interval
		if interval <= 0 {
			interval = e.config.Interval
		}
		e.mu.RUnlock()

		elapsed := time.Since(cycleStart)
		remaining := interval - elapsed
		if remaining > 10*time.Millisecond {
			select {
			case <-e.ctx.Done():
				return
			case <-sr.stopChan:
				return
			case <-sr.wakeChan:
			case <-time.After(remaining):
			}
		}
	}
}

func (e *Engine) runSubnetCycle(sr *subnetRunner) {
	if !atomic.CompareAndSwapInt32(&sr.inFlight, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&sr.inFlight, 0)

	e.mu.RLock()
	targets := make([]string, 0)
	for ip, h := range e.hosts {
		if !h.IsExcluded {
			if sr.cidr == "" {
				if _, hasExplicit := e.subnetIntervals[h.CIDR]; !hasExplicit {
					targets = append(targets, ip)
				}
			} else if h.CIDR == sr.cidr {
				targets = append(targets, ip)
			}
		}
	}
	timeout := e.config.Timeout
	interval := sr.interval
	if interval <= 0 {
		interval = e.config.Interval
	}
	e.mu.RUnlock()

	if len(targets) == 0 {
		return
	}

	reserveTail := timeout
	if reserveTail > interval/2 {
		reserveTail = interval / 2
	}
	dispatchWindow := interval - reserveTail
	if dispatchWindow < 100*time.Millisecond {
		dispatchWindow = interval
	}

	var paceDelay time.Duration
	if len(targets) > 1 && dispatchWindow > 0 {
		paceDelay = dispatchWindow / time.Duration(len(targets))
	}

	var paceTimer *time.Timer
	if paceDelay > 0 {
		paceTimer = time.NewTimer(paceDelay)
		defer paceTimer.Stop()
	}

	for _, ip := range targets {
		select {
		case <-e.ctx.Done():
			return
		case <-sr.stopChan:
			return
		case e.workChan <- ip:
		}

		if paceTimer != nil {
			if !paceTimer.Stop() {
				select {
				case <-paceTimer.C:
				default:
				}
			}
			paceTimer.Reset(paceDelay)
			select {
			case <-e.ctx.Done():
				return
			case <-sr.stopChan:
				return
			case <-sr.wakeChan:
			case <-paceTimer.C:
			}
		}
	}
}

func (e *Engine) applyResult(h *HostState, res PingResult) {
	now := time.Now()
	h.LastChecked = &now
	h.SentPackets++

	if e.tsStore != nil {
		e.tsStore.Record(h.IP, now, res.LatencyMs, res.Success)
	}

	if e.OnProbeRecorded != nil {
		e.OnProbeRecorded(h.IP, h.Alias, h.CIDR, res.LatencyMs, res.Success, now)
	}

	if res.Success {
		h.RecvPackets++
		h.ConsecutiveFails = 0
		h.LatencyMs = res.LatencyMs
		h.LastError = ""
		h.LastSeen = &now

		if h.RecvPackets == 1 {
			h.MinLatencyMs = res.LatencyMs
			h.MaxLatencyMs = res.LatencyMs
			h.AvgLatencyMs = res.LatencyMs
		} else {
			if h.MinLatencyMs <= 0 || res.LatencyMs < h.MinLatencyMs {
				h.MinLatencyMs = res.LatencyMs
			}
			if res.LatencyMs > h.MaxLatencyMs {
				h.MaxLatencyMs = res.LatencyMs
			}
			if h.AvgLatencyMs <= 0 {
				h.AvgLatencyMs = res.LatencyMs
			} else {
				h.AvgLatencyMs = math.Round(((h.AvgLatencyMs*0.8)+(res.LatencyMs*0.2))*100) / 100
			}
		}

		// Append to history, copying into a fresh slice when trimming
		// to release the old backing array and prevent a slow memory leak.
		h.LatencyHistory = append(h.LatencyHistory, res.LatencyMs)
		if len(h.LatencyHistory) > e.config.HistorySize {
			trimmed := make([]float64, e.config.HistorySize)
			copy(trimmed, h.LatencyHistory[len(h.LatencyHistory)-e.config.HistorySize:])
			h.LatencyHistory = trimmed
		}

		if h.Status != StatusUp {
			h.Status = StatusUp
			h.LastStateChange = &now
		}
	} else {
		h.ConsecutiveFails++
		h.LastError = res.Error

		// Record -1 in latency history to denote packet loss in graphs.
		// Same fresh-slice trim as above to prevent backing array leak.
		h.LatencyHistory = append(h.LatencyHistory, -1)
		if len(h.LatencyHistory) > e.config.HistorySize {
			trimmed := make([]float64, e.config.HistorySize)
			copy(trimmed, h.LatencyHistory[len(h.LatencyHistory)-e.config.HistorySize:])
			h.LatencyHistory = trimmed
		}

		if h.ConsecutiveFails >= e.config.FailThreshold {
			if h.Status != StatusDown {
				h.Status = StatusDown
				h.LastStateChange = &now
			}
		}
	}

	// Compute cumulative packet loss percentage
	if h.SentPackets > 0 && h.SentPackets >= h.RecvPackets {
		lost := float64(h.SentPackets - h.RecvPackets)
		h.PacketLoss = math.Round((lost/float64(h.SentPackets))*1000) / 10
	} else {
		h.PacketLoss = 0
	}
}
