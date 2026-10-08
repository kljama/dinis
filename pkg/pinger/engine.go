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
	Notes             string     `json:"notes"`
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
	// DownProbeInterval is how often a host that has been DOWN for at least this long is
	// probed, when that is longer than its normal interval. 0 disables the back-off.
	DownProbeInterval time.Duration
}

// DefaultConfig returns default engine settings.
func DefaultConfig() EngineConfig {
	return EngineConfig{
		Interval:          60 * time.Second,
		Timeout:           1000 * time.Millisecond,
		Concurrency:       100,
		FailThreshold:     2,
		HistorySize:       20,
		MaxMetricHosts:    10000,
		DownProbeInterval: 5 * time.Minute,
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

// Engine probes every monitored host on its own schedule using a shared worker pool.
type Engine struct {
	mu      sync.RWMutex
	config  EngineConfig
	prober  *SingleProber
	tsStore *timeseries.Store

	hosts           map[string]*HostState
	subnetIntervals map[string]time.Duration

	// Probe schedule, guarded by mu (see schedule.go). Each probed host has an entry, queued
	// by due time in healthyQ or, if its last probe failed, in suspectQ.
	epoch           time.Time
	sched           map[string]*schedEntry
	healthyQ        schedHeap
	suspectQ        schedHeap
	suspectInFlight int
	schedWake       chan struct{}
	dispatchWg      sync.WaitGroup

	workChan      chan probeJob
	workerWg      sync.WaitGroup
	workerStops   []chan struct{} // one stop channel per running worker
	activeWorkers int32

	// Callbacks
	BeforeStateChange func(host *HostState, oldStatus, newStatus HostStatus)
	OnHostUpdated     func(host *HostState)
	OnStateChange     func(host *HostState, oldStatus, newStatus HostStatus)
	OnProbeRecorded   func(ip, alias, subnet string, latencyMs float64, success bool, ts time.Time)

	ctx    context.Context
	cancel context.CancelFunc
}

// NewEngine creates a new ICMP probing engine.
func NewEngine(cfg EngineConfig) *Engine {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 100
	}
	if cfg.Interval < minProbeInterval {
		cfg.Interval = minProbeInterval
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
		epoch:           time.Now(),
		sched:           make(map[string]*schedEntry),
		schedWake:       make(chan struct{}, 1),
	}
}

// GetTimeseriesStore returns the underlying time-series metric store.
func (e *Engine) GetTimeseriesStore() *timeseries.Store {
	return e.tsStore
}

// Wake makes the dispatcher re-evaluate the probe schedule.
func (e *Engine) Wake() {
	e.signalSchedule()
}

// UpdateConfig applies a new configuration. Interval changes take effect immediately: hosts
// move to their slot in the new interval instead of finishing the old one.
func (e *Engine) UpdateConfig(cfg EngineConfig) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 100
	}
	e.mu.Lock()
	e.config = cfg
	if cfg.MaxMetricHosts > 0 && e.tsStore != nil {
		e.tsStore.SetCapacity(cfg.MaxMetricHosts)
	}
	if e.ctx != nil {
		e.resizeWorkersUnsafe(cfg.Concurrency)
	}
	e.schedSyncUnsafe()
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
			oldH.Notes = newH.Notes
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

	e.schedSyncUnsafe()
}

// SetHosts updates the target host map, retaining existing subnet intervals atomically.
func (e *Engine) SetHosts(hosts map[string]*HostState) {
	e.SetTargetsAndIntervals(hosts, nil)
}

// effectiveIntervalUnsafe returns the configured probe interval for hosts in a subnet.
func (e *Engine) effectiveIntervalUnsafe(cidr string) time.Duration {
	if interval, ok := e.subnetIntervals[cidr]; ok && interval > 0 {
		if interval < minProbeInterval {
			return minProbeInterval
		}
		return interval
	}
	if e.config.Interval < minProbeInterval {
		return minProbeInterval
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

// GetAllHostsLite returns a snapshot of all monitored hosts without their latency
// history. It is cheaper than GetAllHosts and holds the engine lock for less time.
func (e *Engine) GetAllHostsLite() []*HostState {
	e.mu.RLock()
	defer e.mu.RUnlock()

	slab := make([]HostState, len(e.hosts))
	result := make([]*HostState, 0, len(e.hosts))
	i := 0
	for _, h := range e.hosts {
		slab[i] = *h
		slab[i].LatencyHistory = nil
		result = append(result, &slab[i])
		i++
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
	var probesPerSec float64

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
			probesPerSec += 1 / e.probeIntervalUnsafe(h).Seconds()
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

	// Probe rate across all scheduled hosts (including backed-off DOWN hosts) and the
	// average gap between two consecutive probes.
	if probesPerSec > 0 {
		summary.PacketsPerSec = math.Round(probesPerSec*10) / 10
		summary.PacedDelayMs = math.Round((1000/probesPerSec)*100) / 100
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

// TriggerSweep makes the dispatcher pick up schedule changes immediately. New hosts are
// probed within newHostSpread regardless.
func (e *Engine) TriggerSweep() {
	e.Wake()
}

// Start starts the probe dispatcher and the shared worker pool.
func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ctx != nil {
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
	// Unbuffered: the dispatcher decides which probe goes next when a worker is free.
	e.workChan = make(chan probeJob)
	e.resizeWorkersUnsafe(numWorkers)

	// First probes fall on each host's slot, spread over one interval.
	e.sched = make(map[string]*schedEntry, len(e.hosts))
	e.healthyQ, e.suspectQ = nil, nil
	e.suspectInFlight = 0
	for _, h := range e.hosts {
		e.schedAddUnsafe(h, true)
	}

	e.dispatchWg.Add(1)
	go e.dispatchLoop(e.ctx, e.workChan)
}

// Stop stops the dispatcher and workers and closes prober socket resources.
func (e *Engine) Stop() {
	e.mu.Lock()
	if e.cancel != nil {
		e.cancel()
	}
	if e.tsStore != nil {
		e.tsStore.Stop()
	}
	e.mu.Unlock()

	e.dispatchWg.Wait()

	e.mu.Lock()
	if e.workChan != nil {
		close(e.workChan)
	}
	e.mu.Unlock()

	e.workerWg.Wait()

	e.mu.Lock()
	e.workChan = nil
	e.workerStops = nil
	e.sched = make(map[string]*schedEntry)
	e.healthyQ, e.suspectQ = nil, nil
	e.suspectInFlight = 0
	e.ctx = nil
	e.cancel = nil
	e.mu.Unlock()

	if e.prober != nil {
		e.prober.Close()
	}
}

// resizeWorkersUnsafe grows or shrinks the probe worker pool to n workers.
// Removed workers exit after finishing their current probe. Caller must hold e.mu.
func (e *Engine) resizeWorkersUnsafe(n int) {
	if e.workChan == nil {
		return
	}
	for len(e.workerStops) < n {
		stop := make(chan struct{})
		e.workerStops = append(e.workerStops, stop)
		e.workerWg.Add(1)
		go e.workerLoop(e.ctx, e.workChan, stop)
	}
	for len(e.workerStops) > n {
		last := len(e.workerStops) - 1
		close(e.workerStops[last])
		e.workerStops = e.workerStops[:last]
	}
	e.signalSchedule() // the suspect cap depends on the pool size
}

// ActiveWorkers returns the number of running probe workers.
func (e *Engine) ActiveWorkers() int {
	return int(atomic.LoadInt32(&e.activeWorkers))
}

func (e *Engine) workerLoop(ctx context.Context, workChan <-chan probeJob, stop <-chan struct{}) {
	atomic.AddInt32(&e.activeWorkers, 1)
	defer atomic.AddInt32(&e.activeWorkers, -1)
	defer e.workerWg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case job, ok := <-workChan:
			if !ok {
				return
			}
			e.probeAndApply(job)
		}
	}
}

func (e *Engine) probeAndApply(job probeJob) {
	e.mu.RLock()
	timeout := e.config.Timeout
	ctx := e.ctx
	e.mu.RUnlock()

	probeCtx := context.Background()
	if ctx != nil {
		probeCtx = ctx
	}

	res := e.prober.Probe(probeCtx, job.ip, timeout)

	e.mu.Lock()
	if job.suspect {
		e.suspectInFlight--
		e.signalSchedule() // a suspect slot is free again
	}
	h, exists := e.hosts[job.ip]
	if !exists || h.IsExcluded {
		e.schedDoneUnsafe(job.ip, nil)
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
	e.schedDoneUnsafe(job.ip, h)

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
