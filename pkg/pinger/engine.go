package pinger

import (
	"context"
	"log"
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
	IP               string     `json:"ip"`
	Alias            string     `json:"alias"`
	Notes            string     `json:"notes"`
	CIDR             string     `json:"cidr"`
	Status           HostStatus `json:"status"`
	LatencyMs        float64    `json:"latencyMs"`
	MinLatencyMs     float64    `json:"minLatencyMs"`
	MaxLatencyMs     float64    `json:"maxLatencyMs"`
	AvgLatencyMs     float64    `json:"avgLatencyMs"` // mean of the replies in LatencyHistory
	PacketLoss       float64    `json:"packetLoss"`
	SentPackets      uint64     `json:"sentPackets"`
	RecvPackets      uint64     `json:"recvPackets"`
	ConsecutiveFails int        `json:"consecutiveFails"`
	// ConsecutiveSuccesses counts replies in sequence; a DOWN host needs RecoveryThreshold.
	ConsecutiveSuccesses int        `json:"consecutiveSuccesses"`
	LastSeen             *time.Time `json:"lastSeen"`
	LastChecked          *time.Time `json:"lastChecked"`
	LastStateChange      *time.Time `json:"lastStateChange"`
	LatencyHistory       []float64  `json:"latencyHistory"`
	IsExcluded           bool       `json:"isExcluded"`
	ExclusionReason      string     `json:"exclusionReason"`
	ExclusionRule        string     `json:"exclusionRule,omitempty"` // the rule that excludes the host
	DiscoveredAt         *time.Time `json:"discoveredAt,omitempty"`
	LastDiscovered       *time.Time `json:"lastDiscovered,omitempty"`
	IsStatic             bool       `json:"isStatic"`
	AlertActive          bool       `json:"alertActive"`
	AlertID              string     `json:"alertId"`
	AlertAcknowledged    bool       `json:"alertAcknowledged"`
	AlertAckBy           string     `json:"alertAckBy"`
	AlertAckNote         string     `json:"alertAckNote"`
	AlertAckAt           *time.Time `json:"alertAckAt"`
	AlertStartedAt       *time.Time `json:"alertStartedAt"`
	LastError            string     `json:"lastError"`
}

// EngineConfig holds configuration parameters for the async ICMP engine.
type EngineConfig struct {
	Interval      time.Duration
	Timeout       time.Duration
	Concurrency   int
	FailThreshold int
	// RecoveryThreshold is the number of successful probes in sequence after which a DOWN
	// host is UP again. 1 recovers on the first reply.
	RecoveryThreshold int
	HistorySize       int
	MaxMetricHosts    int
	// DownProbeInterval is how often a host that has been DOWN for at least this long is
	// probed, when that is longer than its normal interval. 0 disables the back-off.
	DownProbeInterval time.Duration
	// ConfirmDelay: after a failed probe of a host that is not DOWN, the next probe follows
	// after this delay instead of at the next slot, until the host replies or is DOWN. It
	// applies when it is shorter than the host's interval. 0 disables it.
	ConfirmDelay time.Duration
}

// DefaultConcurrency is the default number of probe workers. A worker waiting for a reply is
// a parked goroutine, so a pool this large is cheap; it lets many hosts fail at the same time
// without delaying other probes (each failing probe waits for the whole timeout).
const DefaultConcurrency = 1000

// DefaultConfig returns default engine settings.
func DefaultConfig() EngineConfig {
	return EngineConfig{
		Interval:          60 * time.Second,
		Timeout:           1000 * time.Millisecond,
		Concurrency:       DefaultConcurrency,
		FailThreshold:     2,
		RecoveryThreshold: 2,
		HistorySize:       20,
		MaxMetricHosts:    10000,
		DownProbeInterval: 5 * time.Minute,
		ConfirmDelay:      10 * time.Second,
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

// prober sends the probes: a *SingleProber, or a fake in tests.
type prober interface {
	Probe(ctx context.Context, ip string, timeout time.Duration) PingResult
	Mode() string
	Stats() ProberStats
	Close()
}

// Engine probes every monitored host on its own schedule using a shared worker pool.
type Engine struct {
	mu      sync.RWMutex
	config  EngineConfig
	prober  prober
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
	inFlight      int // dispatched probes not yet applied, guarded by mu

	// Counters for Stats
	probesTotal   atomic.Uint64
	failuresTotal atomic.Uint64
	lastProbe     atomic.Int64 // UnixNano of the last completed scheduled probe
	// Probes that failed because of this machine (PingResult.LocalError), and when one was
	// last logged
	localErrorsTotal atomic.Uint64
	localErrorLogged atomic.Int64

	// Callbacks
	BeforeStateChange func(host *HostState, oldStatus, newStatus HostStatus)
	OnHostUpdated     func(host *HostState)
	OnStateChange     func(host *HostState, oldStatus, newStatus HostStatus)
	OnProbeRecorded   func(ip, alias, subnet string, latencyMs float64, success bool, ts time.Time)

	ctx    context.Context
	cancel context.CancelFunc
}

// normalizeConfig replaces invalid configuration values with safe defaults.
func normalizeConfig(cfg EngineConfig) EngineConfig {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultConcurrency
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
	if cfg.RecoveryThreshold <= 0 {
		cfg.RecoveryThreshold = 1
	}
	if cfg.HistorySize <= 0 {
		cfg.HistorySize = 20
	}
	if cfg.ConfirmDelay < 0 {
		cfg.ConfirmDelay = 0
	}
	return cfg
}

// NewEngine creates a new ICMP probing engine.
func NewEngine(cfg EngineConfig) *Engine {
	cfg = normalizeConfig(cfg)

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
	cfg = normalizeConfig(cfg)
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
			oldH.ExclusionRule = newH.ExclusionRule
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
				oldH.ConsecutiveSuccesses = 0
			}
			// The alert fields of a monitored host are not taken from the new state: the probes
			// keep them current (BeforeStateChange, SetHostAlertState), and the new state can
			// be older than a status change that happened while it was built.

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

// GetStatusMap returns the current status of every monitored host.
func (e *Engine) GetStatusMap() map[string]HostStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()
	res := make(map[string]HostStatus, len(e.hosts))
	for ip, h := range e.hosts {
		res[ip] = h.Status
	}
	return res
}

// FillLatencyHistory copies the current latency history into the given host snapshots, for
// example the hosts of one page from GetAllHostsLite.
func (e *Engine) FillLatencyHistory(hosts []*HostState) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, h := range hosts {
		if live, ok := e.hosts[h.IP]; ok {
			h.LatencyHistory = append([]float64(nil), live.LatencyHistory...)
		}
	}
}

// SetHostMeta updates the alias and notes of a monitored host. It reports false if the host
// is not monitored.
func (e *Engine) SetHostMeta(ip, alias, notes string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.hosts[ip]
	if !ok {
		return false
	}
	h.Alias = alias
	h.Notes = notes
	return true
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
		numWorkers = DefaultConcurrency
	}
	// Unbuffered: the dispatcher decides which probe goes next when a worker is free.
	e.workChan = make(chan probeJob)
	e.resizeWorkersUnsafe(numWorkers)

	// First probes fall on each host's slot, spread over one interval.
	e.sched = make(map[string]*schedEntry, len(e.hosts))
	e.healthyQ, e.suspectQ = nil, nil
	e.suspectInFlight = 0
	e.inFlight = 0
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
	e.inFlight = 0
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
	e.inFlight--
	if job.suspect {
		e.suspectInFlight--
		e.signalSchedule() // a suspect slot is free again
	}
	if probeCtx.Err() != nil && !res.Success {
		// The engine stops and cut the probe short: this is not a result of the host.
		e.mu.Unlock()
		return
	}
	h, exists := e.hosts[job.ip]
	if !exists || h.IsExcluded {
		e.schedDoneUnsafe(job.ip, nil)
		e.mu.Unlock()
		return
	}
	if res.LocalError {
		// A problem of this machine, not of the host: no packet loss and no status change
		e.schedDoneUnsafe(job.ip, h)
		e.mu.Unlock()
		e.noteLocalError(res)
		return
	}

	oldStatus := h.Status
	e.applyResult(h, res)
	newStatus := h.Status
	statusChanged := (oldStatus != newStatus)
	e.probesTotal.Add(1)
	if !res.Success {
		e.failuresTotal.Add(1)
	}
	e.lastProbe.Store(time.Now().UnixNano())

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

// PingSingle immediately probes a single host and updates its status. It does not change the
// statistics and the history, which hold only the scheduled probes. The result of an excluded
// host, of a probe that ctx cut short and of a local error is returned but not applied.
func (e *Engine) PingSingle(ctx context.Context, ip string) PingResult {
	e.mu.RLock()
	timeout := e.config.Timeout
	e.mu.RUnlock()

	res := e.prober.Probe(ctx, ip, timeout)
	if ctx != nil && ctx.Err() != nil && !res.Success {
		return res
	}
	if res.LocalError {
		e.noteLocalError(res)
		return res
	}

	e.mu.Lock()
	h, exists := e.hosts[ip]
	if exists && h.IsExcluded {
		exists = false // an excluded host keeps its state; only the probe result is returned
	}
	var oldStatus HostStatus
	var statusChanged bool
	var updatedHost *HostState

	if exists {
		oldStatus = h.Status
		e.applyStatus(h, res, time.Now())
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

// noteLocalError counts a probe that failed because of this machine and logs it, at most
// once a minute.
func (e *Engine) noteLocalError(res PingResult) {
	n := e.localErrorsTotal.Add(1)
	now := time.Now().UnixNano()
	last := e.localErrorLogged.Load()
	if now-last >= int64(time.Minute) && e.localErrorLogged.CompareAndSwap(last, now) {
		log.Printf("[DINIS] Probe of %s failed on this machine: %s (%d such probes since the start; they do not count as packet loss)", res.IP, res.Error, n)
	}
}

// applyResult applies the result of a scheduled probe: the statistics, the history and the
// status of the host.
func (e *Engine) applyResult(h *HostState, res PingResult) {
	now := time.Now()
	h.SentPackets++

	if e.tsStore != nil {
		e.tsStore.Record(h.IP, now, res.LatencyMs, res.Success)
	}

	if e.OnProbeRecorded != nil {
		e.OnProbeRecorded(h.IP, h.Alias, h.CIDR, res.LatencyMs, res.Success, now)
	}

	if res.Success {
		h.RecvPackets++
		if h.RecvPackets == 1 || h.MinLatencyMs <= 0 || res.LatencyMs < h.MinLatencyMs {
			h.MinLatencyMs = res.LatencyMs
		}
		if h.RecvPackets == 1 || res.LatencyMs > h.MaxLatencyMs {
			h.MaxLatencyMs = res.LatencyMs
		}
		e.appendHistory(h, res.LatencyMs)
	} else {
		e.appendHistory(h, -1) // -1 denotes packet loss in graphs
	}
	h.AvgLatencyMs = historyAvg(h.LatencyHistory)

	// Compute cumulative packet loss percentage
	if h.SentPackets > 0 && h.SentPackets >= h.RecvPackets {
		lost := float64(h.SentPackets - h.RecvPackets)
		h.PacketLoss = math.Round((lost/float64(h.SentPackets))*1000) / 10
	} else {
		h.PacketLoss = 0
	}

	e.applyStatus(h, res, now)
}

// applyStatus applies a probe result to the state of a host: its status, its current latency
// and its last error. A manual probe changes only this, not the statistics and the history.
func (e *Engine) applyStatus(h *HostState, res PingResult, now time.Time) {
	h.LastChecked = &now
	if res.Success {
		h.ConsecutiveFails = 0
		h.ConsecutiveSuccesses++
		h.LatencyMs = res.LatencyMs
		h.LastError = ""
		h.LastSeen = &now

		// A DOWN host is UP again only after RecoveryThreshold replies in sequence, so a host
		// with intermittent loss does not start a new alert for each lost probe.
		recovered := h.Status != StatusDown || h.ConsecutiveSuccesses >= e.config.RecoveryThreshold
		if h.Status != StatusUp && recovered {
			h.Status = StatusUp
			h.LastStateChange = &now
		}
		return
	}
	h.ConsecutiveFails++
	h.ConsecutiveSuccesses = 0
	h.LastError = res.Error
	if h.ConsecutiveFails >= e.config.FailThreshold && h.Status != StatusDown {
		h.Status = StatusDown
		h.LastStateChange = &now
	}
}

// appendHistory adds a value to the latency history. When trimming, it copies into a fresh
// slice to release the old backing array.
func (e *Engine) appendHistory(h *HostState, v float64) {
	h.LatencyHistory = append(h.LatencyHistory, v)
	if len(h.LatencyHistory) > e.config.HistorySize {
		trimmed := make([]float64, e.config.HistorySize)
		copy(trimmed, h.LatencyHistory[len(h.LatencyHistory)-e.config.HistorySize:])
		h.LatencyHistory = trimmed
	}
}

// historyAvg returns the mean latency of the replies in the history (lost probes are -1), or
// 0 if it has no reply.
func historyAvg(hist []float64) float64 {
	var sum float64
	n := 0
	for _, v := range hist {
		if v >= 0 {
			sum += v
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return math.Round(sum/float64(n)*100) / 100
}
