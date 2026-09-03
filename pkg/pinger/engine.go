package pinger

import (
	"context"
	"math"
	"sync"
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

// Engine runs the periodic, high-concurrency ICMP probing loops across targets.
type Engine struct {
	mu      sync.RWMutex
	cycleMu sync.Mutex
	config  EngineConfig
	prober  *SingleProber
	tsStore *timeseries.Store

	hosts map[string]*HostState // IP -> HostState

	// Callbacks
	BeforeStateChange func(host *HostState, oldStatus, newStatus HostStatus)
	OnHostUpdated     func(host *HostState)
	OnStateChange     func(host *HostState, oldStatus, newStatus HostStatus)
	OnCycleComplete   func(summary *CycleSummary)
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
		config:   cfg,
		prober:   NewSingleProber(),
		tsStore:  timeseries.NewStoreWithLimit(maxMetricHosts),
		hosts:    make(map[string]*HostState),
		wakeChan: make(chan struct{}, 1),
	}
}

// GetTimeseriesStore returns the underlying time-series metric store.
func (e *Engine) GetTimeseriesStore() *timeseries.Store {
	return e.tsStore
}

// Wake signals the background loop to immediately start a cycle.
func (e *Engine) Wake() {
	select {
	case e.wakeChan <- struct{}{}:
	default:
	}
}

// UpdateConfig dynamically updates the engine configuration and wakes the polling loop.
func (e *Engine) UpdateConfig(cfg EngineConfig) {
	e.mu.Lock()
	e.config = cfg
	if cfg.MaxMetricHosts > 0 && e.tsStore != nil {
		e.tsStore.SetCapacity(cfg.MaxMetricHosts)
	}
	e.mu.Unlock()
	e.Wake()
}

// SetHosts updates the target host map.
func (e *Engine) SetHosts(hosts map[string]*HostState) {
	e.mu.Lock()
	defer e.mu.Unlock()

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

		if !h.IsExcluded && h.Status == StatusDown {
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

	if upWithLatency > 0 {
		summary.AvgLatencyMs = math.Round((sumLatency/float64(upWithLatency))*100) / 100
	}

	// Calculate pacing rates
	activeTargets := summary.TotalTargets - summary.ExcludedCount
	if activeTargets > 0 && e.config.Interval > 0 {
		summary.PacketsPerSec = math.Round((float64(activeTargets)/e.config.Interval.Seconds())*10) / 10
		reserveTail := e.config.Timeout
		if reserveTail > e.config.Interval/2 {
			reserveTail = e.config.Interval / 2
		}
		dispatchWindow := e.config.Interval - reserveTail
		if dispatchWindow < 100*time.Millisecond {
			dispatchWindow = e.config.Interval
		}
		if activeTargets > 1 {
			paceDelay := dispatchWindow / time.Duration(activeTargets)
			summary.PacedDelayMs = math.Round((float64(paceDelay.Microseconds())/1000.0)*100) / 100
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

// Start starts the background polling loop.
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
	e.mu.Unlock()

	e.wg.Add(1)
	go e.runLoop()
}

// Stop stops the background polling loop and closes prober socket resources.
func (e *Engine) Stop() {
	e.mu.Lock()
	if e.cancel != nil {
		e.cancel()
	}
	if e.tsStore != nil {
		e.tsStore.Stop()
	}
	e.mu.Unlock()

	e.wg.Wait()

	if e.prober != nil {
		e.prober.Close()
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

func (e *Engine) runLoop() {
	defer e.wg.Done()

	// Drain any pre-startup wake signal so the initial cycle starts cleanly
	select {
	case <-e.wakeChan:
	default:
	}

	for {
		select {
		case <-e.ctx.Done():
			return
		default:
		}

		cycleStart := time.Now()
		e.runCycle()

		e.mu.RLock()
		interval := e.config.Interval
		e.mu.RUnlock()

		elapsed := time.Since(cycleStart)
		remaining := interval - elapsed
		if remaining > 10*time.Millisecond {
			select {
			case <-e.ctx.Done():
				return
			case <-e.wakeChan:
			case <-time.After(remaining):
			}
		}
	}
}

func (e *Engine) runCycle() {
	e.cycleMu.Lock()
	defer e.cycleMu.Unlock()

	e.mu.RLock()
	targets := make([]string, 0, len(e.hosts))
	for ip, h := range e.hosts {
		if !h.IsExcluded {
			targets = append(targets, ip)
		}
	}
	concurrency := e.config.Concurrency
	timeout := e.config.Timeout
	interval := e.config.Interval
	e.mu.RUnlock()

	if len(targets) == 0 {
		summary := e.GetSummary()
		if e.OnCycleComplete != nil {
			e.OnCycleComplete(&summary)
		}
		return
	}

	// Calculate pacing window and delay between feeding target probes.
	// We reserve a tail buffer for timeout so the last dispatched probe completes before the cycle window ends.
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
		if paceDelay > 50*time.Millisecond {
			paceDelay = 50 * time.Millisecond
		}
	}

	numWorkers := concurrency
	if numWorkers > len(targets) {
		numWorkers = len(targets)
	}
	if numWorkers <= 0 {
		numWorkers = 1
	}

	workChan := make(chan string, numWorkers*2)
	var cycleWg sync.WaitGroup
	cycleWg.Add(numWorkers)

	e.mu.RLock()
	ctx := e.ctx
	e.mu.RUnlock()

	var ctxDone <-chan struct{}
	probeCtx := context.Background()
	if ctx != nil {
		ctxDone = ctx.Done()
		probeCtx = ctx
	}

	for w := 0; w < numWorkers; w++ {
		go func() {
			defer cycleWg.Done()
			for ip := range workChan {
				select {
				case <-ctxDone:
					return
				default:
				}

				res := e.prober.Probe(probeCtx, ip, timeout)

				e.mu.Lock()
				h, exists := e.hosts[ip]
				if !exists || h.IsExcluded {
					e.mu.Unlock()
					continue
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
		}()
	}

	// Paced feeder: dispatch target IPs smoothly across the interval window
	var paceTimer *time.Timer
	if paceDelay > 0 {
		paceTimer = time.NewTimer(paceDelay)
		defer paceTimer.Stop()
	}
	for _, ip := range targets {
		select {
		case <-ctxDone:
			close(workChan)
			cycleWg.Wait()
			return
		case workChan <- ip:
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
			case <-ctxDone:
				close(workChan)
				cycleWg.Wait()
				return
			case <-paceTimer.C:
			}
		}
	}
	close(workChan)

	cycleWg.Wait()

	summary := e.GetSummary()
	if e.OnCycleComplete != nil {
		e.OnCycleComplete(&summary)
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

	// Compute packet loss percentage over recent rolling history window
	if len(h.LatencyHistory) > 0 {
		lostCount := 0
		for _, lat := range h.LatencyHistory {
			if lat < 0 {
				lostCount++
			}
		}
		h.PacketLoss = math.Round((float64(lostCount)/float64(len(h.LatencyHistory)))*1000) / 10
	} else if h.SentPackets > 0 && h.SentPackets >= h.RecvPackets {
		lost := float64(h.SentPackets - h.RecvPackets)
		h.PacketLoss = math.Round((lost/float64(h.SentPackets))*1000) / 10
	} else {
		h.PacketLoss = 0
	}
}
