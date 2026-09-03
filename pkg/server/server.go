package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"dinis/pkg/alerts"
	"dinis/pkg/network"
	"dinis/pkg/pinger"
	"dinis/pkg/store"
	"dinis/pkg/timeseries"
)

//go:embed web_dist/*
var embeddedAssets embed.FS

// DiscoveryStatus tracks the progress and results of discovery sweeps.
type DiscoveryStatus struct {
	IsScanning          bool       `json:"isScanning"`
	LastRun             *time.Time `json:"lastRun"`
	NextRun             *time.Time `json:"nextRun"`
	LastScannedCount    int        `json:"lastScannedCount"`
	LastDiscoveredCount int        `json:"lastDiscoveredCount"`
	SubnetCapacity      int        `json:"subnetCapacity"`
	IntervalMin         int        `json:"intervalMin"`
}

const (
	defaultMaxSSEClients       = 256
	discoveryRateLimitInterval = 30 * time.Second
)

// Coordinator orchestrates between Storage, CIDR Engine, ICMP Engine, and Alert Manager.
type Coordinator struct {
	rebuildMu   sync.Mutex
	store       *store.Store
	pinger      *pinger.Engine
	alerts      *alerts.Manager
	broadcastMu sync.Mutex
	clientsMu   sync.RWMutex
	sseClients  map[chan []byte]bool
	stopChan    chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup

	discMu          sync.RWMutex
	discoveryStatus DiscoveryStatus

	discoveryLastTriggered time.Time // rate-limit manual discovery
	maxSSEClients          int
}

// NewCoordinator creates and wires the entire monitoring subsystem.
func NewCoordinator(st *store.Store) *Coordinator {
	settings := st.GetSettings()
	maxMetricHosts := settings.MaxMetricHosts
	if maxMetricHosts <= 0 {
		maxMetricHosts = 10000
	}
	cfg := pinger.EngineConfig{
		Interval:       time.Duration(settings.IntervalSec * float64(time.Second)),
		Timeout:        time.Duration(settings.TimeoutMs) * time.Millisecond,
		Concurrency:    settings.Concurrency,
		FailThreshold:  settings.FailThreshold,
		HistorySize:    25,
		MaxMetricHosts: maxMetricHosts,
	}

	p := pinger.NewEngine(cfg)
	altMgr := alerts.NewManager(500)

	c := &Coordinator{
		store:         st,
		pinger:        p,
		alerts:        altMgr,
		sseClients:    make(map[chan []byte]bool),
		stopChan:      make(chan struct{}),
		maxSSEClients: defaultMaxSSEClients,
		discoveryStatus: DiscoveryStatus{
			IntervalMin: settings.DiscoveryIntervalMin,
		},
	}

	// Wire engine callbacks
	p.BeforeStateChange = c.handleBeforeStateChange
	p.OnStateChange = c.handleStateChange
	p.OnHostUpdated = c.handleHostUpdated
	p.OnCycleComplete = c.handleCycleComplete

	// Wire alert manager callbacks
	altMgr.OnAlertTriggered = func(a *alerts.Alert) {
		c.broadcastEvent("alert_fired", a)
	}
	altMgr.OnAlertAcknowledged = func(a *alerts.Alert) {
		c.broadcastEvent("alert_acknowledged", a)
	}
	altMgr.OnAlertResolved = func(a *alerts.Alert) {
		c.broadcastEvent("alert_resolved", a)
	}

	// Initial sync of hosts from storage
	c.RebuildTargetList()

	return c
}

// Start begins background monitoring and heartbeat routines.
func (c *Coordinator) Start() {
	c.pinger.Start()
	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop()
	}()
	go func() {
		defer c.wg.Done()
		c.discoveryLoop()
	}()
}

// Stop gracefully terminates all subsystems and persists all state.
func (c *Coordinator) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopChan)
		c.wg.Wait()
		c.pinger.Stop()

		c.clientsMu.Lock()
		for ch := range c.sseClients {
			delete(c.sseClients, ch)
			close(ch)
		}
		c.clientsMu.Unlock()
	})
}

// SetProbeExporter registers a callback that is invoked for every ICMP probe result,
// enabling export to external stores such as InfluxDB.
func (c *Coordinator) SetProbeExporter(fn func(ip, alias, subnet string, latencyMs float64, success bool, ts time.Time)) {
	c.pinger.OnProbeRecorded = fn
}

// RebuildTargetList recalculates the list of active target hosts based on discovered hosts, configured CIDRs, and exclusions.
func (c *Coordinator) RebuildTargetList() {
	c.rebuildMu.Lock()
	defer c.rebuildMu.Unlock()

	cidrs := c.store.GetCIDRs()
	exclusions := c.store.GetExclusions()
	discovered := c.store.GetDiscoveredHosts()
	allMeta := c.store.GetAllHostMeta()

	matcher := network.NewExclusionMatcher()
	for _, excl := range exclusions {
		if excl.Enabled {
			if err := matcher.AddExclusion(excl.Rule, excl.Reason); err != nil {
				log.Printf("[DINIS] Warning: invalid exclusion rule %q in store: %v", excl.Rule, err)
			}
		}
	}

	allConfiguredCIDRs := make(map[string]bool)
	validCIDRs := make(map[string]bool)
	cidrMap := make(map[string]*network.CIDRInfo)
	totalCapacity := 0
	var newStaticHosts []store.DiscoveredHost

	for _, cfg := range cidrs {
		allConfiguredCIDRs[cfg.CIDR] = true
		if !cfg.Enabled {
			continue
		}
		info, err := network.ParseCIDR(cfg.CIDR, cfg.IncludeNetAndBcast)
		if err != nil {
			continue
		}
		validCIDRs[cfg.CIDR] = true
		cidrMap[cfg.CIDR] = info
		totalCapacity += info.TotalHosts

		// Single IP targets (/32 or 1 host) are automatically treated as static targets
		if info.TotalHosts == 1 {
			ip := info.IPs[0]
			if _, exists := discovered[ip]; !exists {
				now := time.Now()
				discHost := store.DiscoveredHost{
					IP:             ip,
					CIDR:           cfg.CIDR,
					DiscoveredAt:   now,
					LastDiscovered: now,
					IsStatic:       true,
				}
				newStaticHosts = append(newStaticHosts, discHost)
				discovered[ip] = discHost
			}
		}
	}

	// Batch persist any newly created single-host targets
	if len(newStaticHosts) > 0 {
		if err := c.store.AddOrUpdateDiscoveredHostsBatch(newStaticHosts); err != nil {
			log.Printf("[DINIS] Error persisting static targets to disk: %v", err)
		}
	}

	// Prune discovered hosts belonging to deleted CIDRs (preserves IsStatic hosts and disabled CIDRs)
	if err := c.store.PruneDiscoveredHosts(allConfiguredCIDRs); err != nil {
		log.Printf("[DINIS] Error pruning unmanaged discovered hosts from disk: %v", err)
	}

	hostMap := make(map[string]*pinger.HostState)

	for ip, disc := range discovered {
		if !disc.IsStatic && !validCIDRs[disc.CIDR] {
			continue
		}

		hostCIDR := disc.CIDR
		if hostCIDR == "" || hostCIDR == "Static" {
			parsedIP := net.ParseIP(ip)
			found := false
			if parsedIP != nil {
				for _, cfg := range cidrs {
					if !cfg.Enabled {
						continue
					}
					_, ipNet, err := net.ParseCIDR(cfg.CIDR)
					if err == nil && ipNet.Contains(parsedIP) {
						hostCIDR = cfg.CIDR
						found = true
						break
					}
				}
			}
			if !found {
				hostCIDR = ip + "/32"
			}
		}

		matched, _, reason := matcher.Matches(ip)
		meta := allMeta[ip]
		alias := meta.Alias
		if alias == "" {
			if info, ok := cidrMap[hostCIDR]; ok && info.TotalHosts == 1 {
				for _, cfg := range cidrs {
					if cfg.CIDR == hostCIDR && cfg.Description != "" {
						alias = cfg.Description
						break
					}
				}
			}
		}

		status := pinger.StatusPending
		if matched {
			status = pinger.StatusExcluded
		} else if existing, ok := c.pinger.GetHost(ip); ok {
			// Retain existing live status (e.g. UP, DOWN) across rebuilds
			status = existing.Status
		}

		activeAlert, hasAlert := c.alerts.GetAlertForIP(ip)
		alertActive := false
		alertID := ""
		alertAck := false
		alertAckBy := ""
		alertAckNote := ""
		var alertAckAt *time.Time
		var alertStartedAt *time.Time

		if hasAlert && !matched {
			alertActive = true
			alertID = activeAlert.ID
			alertAck = activeAlert.Acknowledged
			alertAckBy = activeAlert.AcknowledgedBy
			alertAckNote = activeAlert.AckNote
			alertAckAt = activeAlert.AcknowledgedAt
			alertStartedAt = &activeAlert.StartedAt
		}

		discAt := disc.DiscoveredAt
		lastDisc := disc.LastDiscovered

		hostMap[ip] = &pinger.HostState{
			IP:                ip,
			Alias:             alias,
			CIDR:              hostCIDR,
			Status:            status,
			IsExcluded:        matched,
			ExclusionReason:   reason,
			DiscoveredAt:      &discAt,
			LastDiscovered:    &lastDisc,
			IsStatic:          disc.IsStatic,
			AlertActive:       alertActive,
			AlertID:           alertID,
			AlertAcknowledged: alertAck,
			AlertAckBy:        alertAckBy,
			AlertAckNote:      alertAckNote,
			AlertAckAt:        alertAckAt,
			AlertStartedAt:    alertStartedAt,
			LatencyHistory:    make([]float64, 0, 25),
		}
	}

	// Clean up any active alerts for hosts that are no longer monitored or are now excluded
	c.alerts.ResolveIf(func(a *alerts.Alert) bool {
		h, exists := hostMap[a.IP]
		return !exists || h.IsExcluded
	})

	c.pinger.SetHosts(hostMap)
	c.pinger.Wake()

	c.discMu.Lock()
	c.discoveryStatus.SubnetCapacity = totalCapacity
	c.discoveryStatus.LastDiscoveredCount = len(discovered)
	c.discMu.Unlock()
}

// RunDiscovery performs a concurrent ICMP discovery sweep across candidate IPs in the CIDR ranges.
func (c *Coordinator) RunDiscovery(specificCIDR string) (int, int, error) {
	c.discMu.Lock()
	if c.discoveryStatus.IsScanning {
		c.discMu.Unlock()
		return 0, 0, fmt.Errorf("discovery scan is already running")
	}
	c.discoveryStatus.IsScanning = true
	c.discMu.Unlock()

	c.broadcastEvent("discovery_started", map[string]interface{}{
		"specificCIDR": specificCIDR,
		"timestamp":    time.Now(),
	})

	cidrs := c.store.GetCIDRs()
	exclusions := c.store.GetExclusions()

	matcher := network.NewExclusionMatcher()
	for _, excl := range exclusions {
		if excl.Enabled {
			_ = matcher.AddExclusion(excl.Rule, excl.Reason)
		}
	}

	type candidate struct {
		ip   string
		cidr string
	}
	var targets []candidate

	for _, cfg := range cidrs {
		if !cfg.Enabled {
			continue
		}
		if specificCIDR != "" && cfg.CIDR != specificCIDR {
			continue
		}

		info, err := network.ParseCIDR(cfg.CIDR, cfg.IncludeNetAndBcast)
		if err != nil {
			continue
		}

		for _, ip := range info.IPs {
			if matched, _, _ := matcher.Matches(ip); !matched {
				targets = append(targets, candidate{ip: ip, cidr: cfg.CIDR})
			}
		}
	}

	settings := c.store.GetSettings()
	if len(targets) == 0 {
		now := time.Now()
		c.discMu.Lock()
		c.discoveryStatus.IsScanning = false
		c.discoveryStatus.LastRun = &now
		if settings.DiscoveryIntervalMin > 0 {
			next := now.Add(time.Duration(settings.DiscoveryIntervalMin) * time.Minute)
			c.discoveryStatus.NextRun = &next
		} else {
			c.discoveryStatus.NextRun = nil
		}
		c.discoveryStatus.LastScannedCount = 0
		c.discoveryStatus.LastDiscoveredCount = len(c.store.GetDiscoveredHosts())
		statusCpy := c.discoveryStatus
		c.discMu.Unlock()

		c.broadcastEvent("discovery_completed", map[string]interface{}{
			"status":           statusCpy,
			"discoveredOnline": 0,
			"newDiscovered":    0,
			"scannedCount":     0,
		})

		return 0, 0, nil
	}

	prober := pinger.NewSingleProber()
	defer prober.Close()
	concurrency := settings.Concurrency
	if concurrency <= 0 {
		concurrency = 100
	}
	if concurrency > len(targets) {
		concurrency = len(targets)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-c.stopChan:
			cancel()
		case <-ctx.Done():
		}
	}()

	workChan := make(chan candidate, concurrency*2)
	var discoveredMu sync.Mutex
	discoveredOnline := 0
	newDiscovered := 0
	existingDiscovered := c.store.GetDiscoveredHosts()
	var discoveredBatch []store.DiscoveredHost

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case cand, ok := <-workChan:
					if !ok {
						return
					}
					res := prober.Probe(ctx, cand.ip, 750*time.Millisecond)
					if res.Success {
						now := time.Now()
						discoveredMu.Lock()
						discoveredOnline++
						discAt := now
						isStatic := false
						if existing, exists := existingDiscovered[cand.ip]; exists {
							discAt = existing.DiscoveredAt
							isStatic = existing.IsStatic
						} else {
							newDiscovered++
						}
						discoveredBatch = append(discoveredBatch, store.DiscoveredHost{
							IP:             cand.ip,
							CIDR:           cand.cidr,
							DiscoveredAt:   discAt,
							LastDiscovered: now,
							IsStatic:       isStatic,
						})
						discoveredMu.Unlock()
					}
				}
			}
		}()
	}

	// Paced discovery feeder: slight spacing to prevent ARP / NIC bursts on large ranges
	var discPace time.Duration
	if len(targets) > 50 {
		// Space discovery across ~2 to 5 seconds depending on range size
		discPace = (3 * time.Second) / time.Duration(len(targets))
		if discPace > 20*time.Millisecond {
			discPace = 20 * time.Millisecond
		}
	}

	var paceTimer *time.Timer
	if discPace > 0 {
		paceTimer = time.NewTimer(discPace)
		defer paceTimer.Stop()
	}

	aborted := false
feeder:
	for _, t := range targets {
		select {
		case <-ctx.Done():
			aborted = true
			break feeder
		case workChan <- t:
		}

		if paceTimer != nil {
			if !paceTimer.Stop() {
				select {
				case <-paceTimer.C:
				default:
				}
			}
			paceTimer.Reset(discPace)
			select {
			case <-ctx.Done():
				aborted = true
				break feeder
			case <-paceTimer.C:
			}
		}
	}
	close(workChan)

	wg.Wait()

	// Batch-write all discovered hosts to disk in a single atomic operation,
	// ensuring any partially-discovered hosts are persisted even during shutdown.
	if len(discoveredBatch) > 0 {
		if err := c.store.AddOrUpdateDiscoveredHostsBatch(discoveredBatch); err != nil {
			log.Printf("[DINIS] Error persisting discovered hosts batch to disk: %v", err)
		}
	}

	if aborted {
		c.discMu.Lock()
		c.discoveryStatus.IsScanning = false
		c.discMu.Unlock()
		return discoveredOnline, newDiscovered, nil
	}

	// Rebuild target list so newly discovered hosts are immediately monitored
	c.RebuildTargetList()
	c.pinger.TriggerSweep()

	now := time.Now()
	c.discMu.Lock()
	c.discoveryStatus.IsScanning = false
	c.discoveryStatus.LastRun = &now
	if settings.DiscoveryIntervalMin > 0 {
		next := now.Add(time.Duration(settings.DiscoveryIntervalMin) * time.Minute)
		c.discoveryStatus.NextRun = &next
	} else {
		c.discoveryStatus.NextRun = nil
	}
	c.discoveryStatus.LastScannedCount = len(targets)
	c.discoveryStatus.LastDiscoveredCount = len(c.store.GetDiscoveredHosts())
	statusCpy := c.discoveryStatus
	c.discMu.Unlock()

	c.broadcastEvent("discovery_completed", map[string]interface{}{
		"status":           statusCpy,
		"discoveredOnline": discoveredOnline,
		"newDiscovered":    newDiscovered,
		"scannedCount":     len(targets),
	})

	return discoveredOnline, newDiscovered, nil
}

// TriggerDiscovery initiates a discovery sweep asynchronously. Returns false if server is shutting down.
func (c *Coordinator) TriggerDiscovery(specificCIDR string) bool {
	c.discMu.Lock()
	select {
	case <-c.stopChan:
		c.discMu.Unlock()
		return false
	default:
	}
	c.wg.Add(1)
	c.discMu.Unlock()

	go func() {
		defer c.wg.Done()
		if _, _, err := c.RunDiscovery(specificCIDR); err != nil {
			log.Printf("[DINIS] Triggered discovery sweep error: %v", err)
		}
	}()
	return true
}

// GetDiscoveryStatus returns a copy of current discovery status.
func (c *Coordinator) GetDiscoveryStatus() DiscoveryStatus {
	c.discMu.RLock()
	defer c.discMu.RUnlock()
	return c.discoveryStatus
}

func (c *Coordinator) discoveryLoop() {
	// Run initial discovery sweep on startup after a 5s warmup to avoid colliding with initial engine cycle
	select {
	case <-c.stopChan:
		return
	case <-time.After(5 * time.Second):
	}
	settings := c.store.GetSettings()
	if settings.AutoDiscovery {
		if _, _, err := c.RunDiscovery(""); err != nil {
			log.Printf("[DINIS] Initial discovery sweep error: %v", err)
		}
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return
		case <-ticker.C:
			s := c.store.GetSettings()
			if s.DiscoveryIntervalMin <= 0 {
				continue
			}

			c.discMu.RLock()
			lastRun := c.discoveryStatus.LastRun
			isScanning := c.discoveryStatus.IsScanning
			c.discMu.RUnlock()

			if isScanning {
				continue
			}

			if lastRun == nil || time.Since(*lastRun) >= time.Duration(s.DiscoveryIntervalMin)*time.Minute {
				if _, _, err := c.RunDiscovery(""); err != nil {
					log.Printf("[DINIS] Periodic discovery sweep error: %v", err)
				}
			}
		}
	}
}

func (c *Coordinator) handleBeforeStateChange(h *pinger.HostState, oldStatus, newStatus pinger.HostStatus) {
	switch newStatus {
	case pinger.StatusDown:
		alt := c.alerts.Trigger(h.IP, h.Alias, h.CIDR, h.LastError)
		h.AlertActive = true
		h.AlertID = alt.ID
		h.AlertAcknowledged = alt.Acknowledged
		h.AlertAckBy = alt.AcknowledgedBy
		h.AlertAckNote = alt.AckNote
		h.AlertAckAt = alt.AcknowledgedAt
		h.AlertStartedAt = &alt.StartedAt
	case pinger.StatusUp, pinger.StatusExcluded:
		c.alerts.Resolve(h.IP)
		h.AlertActive = false
		h.AlertAcknowledged = false
		h.AlertID = ""
		h.AlertAckBy = ""
		h.AlertAckNote = ""
		h.AlertAckAt = nil
		h.AlertStartedAt = nil
	}
}

func (c *Coordinator) hasActiveAlert(ip string) bool {
	_, exists := c.alerts.GetAlertForIP(ip)
	return exists
}

func (c *Coordinator) handleStateChange(h *pinger.HostState, oldStatus, newStatus pinger.HostStatus) {
	// If called standalone outside the engine probe cycle (e.g. in manual calls/tests),
	// ensure alert manager and engine host state are synchronized.
	if (newStatus == pinger.StatusDown && !h.AlertActive) || ((newStatus == pinger.StatusUp || newStatus == pinger.StatusExcluded) && (h.AlertActive || c.hasActiveAlert(h.IP))) {
		c.handleBeforeStateChange(h, oldStatus, newStatus)
		c.pinger.SetHostAlertState(h.IP, h.AlertActive, h.AlertID, h.AlertAcknowledged, h.AlertAckBy, h.AlertAckNote, h.AlertAckAt, h.AlertStartedAt)
	}

	c.broadcastEvent("host_state_change", map[string]interface{}{
		"host":      h,
		"oldStatus": oldStatus,
		"newStatus": newStatus,
	})
}

func (c *Coordinator) handleHostUpdated(h *pinger.HostState) {
	// Per-packet SSE broadcasting is decoupled to enable 20,000+ host scalability.
	// State transitions and associated alert lifecycle are handled authoritatively via handleStateChange.
	// Aggregated metrics are broadcasted at cycle completion via handleCycleComplete.
}

func (c *Coordinator) handleCycleComplete(summary *pinger.CycleSummary) {
	c.discMu.RLock()
	summary.SubnetCapacity = c.discoveryStatus.SubnetCapacity
	c.discMu.RUnlock()
	c.broadcastEvent("summary_update", summary)
}

func (c *Coordinator) broadcastEvent(eventType string, data interface{}) {
	payload, err := json.Marshal(data)
	if err != nil {
		return
	}

	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", eventType, string(payload))
	msgBytes := []byte(msg)

	c.broadcastMu.Lock()
	defer c.broadcastMu.Unlock()

	c.clientsMu.RLock()
	defer c.clientsMu.RUnlock()

	desyncMsg := []byte("event: desync\ndata: {\"reason\":\"buffer_overflow\",\"message\":\"Client fell behind; refresh required\"}\n\n")

	for ch := range c.sseClients {
		select {
		case ch <- msgBytes:
		default:
			// Client channel is full/slow. Drain stale queued messages until channel is empty.
		drainLoop:
			for {
				select {
				case <-ch:
				default:
					break drainLoop
				}
			}
			// Queue a single desync event so the client knows it fell behind and needs a full state refresh.
			select {
			case ch <- desyncMsg:
			default:
			}
		}
	}
}

func (c *Coordinator) heartbeatLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return
		case <-ticker.C:
			pingMsg := []byte(": keepalive\n\n")
			c.broadcastMu.Lock()
			c.clientsMu.RLock()
			for ch := range c.sseClients {
				select {
				case ch <- pingMsg:
				default:
				}
			}
			c.clientsMu.RUnlock()
			c.broadcastMu.Unlock()
		}
	}
}

// Server holds the HTTP handler routing.
type Server struct {
	coord             *Coordinator
	mux               *http.ServeMux
	distFS            http.FileSystem
	staticPath        string // If provided, serves live directory instead of embedded
	allowedOrigins    []string
	allowedHosts      []string
	allowedClientIPs  []net.IP
	allowedClientNets []*net.IPNet
	trustedProxyIPs   []net.IP
	trustedProxyNets  []*net.IPNet
	apiToken          string
	manualPingMu      sync.Mutex
	manualPingLast    map[string]time.Time
	etagMu            sync.RWMutex
	assetETags        map[string]string
}

// NewServer creates a new HTTP server routing all REST APIs and web dashboard assets.
func NewServer(coord *Coordinator, staticDir string) *Server {
	s := &Server{
		coord:          coord,
		mux:            http.NewServeMux(),
		staticPath:     staticDir,
		manualPingLast: make(map[string]time.Time),
		assetETags:     make(map[string]string),
	}

	if staticDir != "" {
		s.distFS = http.Dir(staticDir)
	} else {
		sub, err := fs.Sub(embeddedAssets, "web_dist")
		if err == nil {
			s.distFS = http.FS(sub)
		}
	}

	s.routes()
	return s
}

// SetAllowedOrigins configures explicit CORS allowed origins.
func (s *Server) SetAllowedOrigins(origins []string) {
	s.allowedOrigins = origins
}

// SetAllowedHosts configures explicit allowed Host header values for DNS rebinding protection.
func (s *Server) SetAllowedHosts(hosts []string) {
	s.allowedHosts = hosts
}

// SetAllowedClientIPs configures an IP filter whitelist (individual IPs or CIDR subnets).
func (s *Server) SetAllowedClientIPs(rules []string) {
	var ips []net.IP
	var nets []*net.IPNet

	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}
		if strings.Contains(rule, "/") {
			_, ipNet, err := net.ParseCIDR(rule)
			if err == nil && ipNet != nil {
				nets = append(nets, ipNet)
			}
		} else {
			ip := net.ParseIP(rule)
			if ip != nil {
				ips = append(ips, ip)
			}
		}
	}

	s.allowedClientIPs = ips
	s.allowedClientNets = nets
}

// SetTrustedProxies configures reverse proxy IPs/CIDRs that are permitted to provide X-Forwarded-For, X-Real-IP, and X-Forwarded-Host.
// It accepts individual IPs (e.g. "127.0.0.1"), CIDR ranges (e.g. "172.16.0.0/12"), or the preset "docker" / "private".
func (s *Server) SetTrustedProxies(rules []string) {
	var ips []net.IP
	var nets []*net.IPNet

	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}
		if strings.EqualFold(rule, "docker") || strings.EqualFold(rule, "private") {
			presets := []string{
				"127.0.0.0/8",    // IPv4 Loopback
				"::1/128",        // IPv6 Loopback
				"10.0.0.0/8",     // RFC 1918 Class A
				"172.16.0.0/12",  // RFC 1918 Class B (includes Docker default bridges 172.17.0.0/16 - 172.31.0.0/16)
				"192.168.0.0/16", // RFC 1918 Class C
				"fc00::/7",       // IPv6 Unique Local Addresses
			}
			for _, p := range presets {
				_, ipNet, err := net.ParseCIDR(p)
				if err == nil && ipNet != nil {
					nets = append(nets, ipNet)
				}
			}
			continue
		}

		if strings.Contains(rule, "/") {
			_, ipNet, err := net.ParseCIDR(rule)
			if err == nil && ipNet != nil {
				nets = append(nets, ipNet)
			}
		} else {
			ip := net.ParseIP(rule)
			if ip != nil {
				ips = append(ips, ip)
			}
		}
	}

	s.trustedProxyIPs = ips
	s.trustedProxyNets = nets
}

func (s *Server) isTrustedProxy(remoteIP net.IP) bool {
	if remoteIP == nil {
		return false
	}
	for _, ip := range s.trustedProxyIPs {
		if remoteIP.Equal(ip) {
			return true
		}
	}
	for _, ipNet := range s.trustedProxyNets {
		if ipNet.Contains(remoteIP) {
			return true
		}
	}
	return false
}

// getClientIP extracts the effective client IP, checking X-Forwarded-For or X-Real-IP
// only if the direct socket connection originates from a configured trusted proxy.
func (s *Server) getClientIP(r *http.Request) net.IP {
	host := r.RemoteAddr
	if h, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		host = h
	}

	remoteIP := net.ParseIP(host)
	if remoteIP == nil {
		return nil
	}

	if s.isTrustedProxy(remoteIP) {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			// Walk backwards from right to left to find the first untrusted client IP
			for i := len(parts) - 1; i >= 0; i-- {
				p := strings.TrimSpace(parts[i])
				if parsed := net.ParseIP(p); parsed != nil {
					if !s.isTrustedProxy(parsed) {
						return parsed
					}
				}
			}
			// If all IPs in chain are trusted proxies, use the leftmost
			if len(parts) > 0 {
				if first := net.ParseIP(strings.TrimSpace(parts[0])); first != nil {
					return first
				}
			}
		}

		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			if parsed := net.ParseIP(strings.TrimSpace(xri)); parsed != nil {
				return parsed
			}
		}
	}

	return remoteIP
}

func (s *Server) isAllowedClientIP(r *http.Request) bool {
	if len(s.allowedClientIPs) == 0 && len(s.allowedClientNets) == 0 {
		return true
	}

	clientIP := s.getClientIP(r)
	if clientIP == nil {
		return false
	}

	// Always allow loopback access
	if clientIP.IsLoopback() {
		return true
	}

	for _, ip := range s.allowedClientIPs {
		if clientIP.Equal(ip) {
			return true
		}
	}

	for _, ipNet := range s.allowedClientNets {
		if ipNet.Contains(clientIP) {
			return true
		}
	}

	return false
}

// SetAPIToken configures an optional authentication token required on API endpoints.
func (s *Server) SetAPIToken(token string) {
	s.apiToken = token
}

func (s *Server) isAllowedHostName(reqHost string) bool {
	if len(s.allowedHosts) == 0 {
		return true
	}
	if reqHost == "" {
		return false
	}
	hostName := reqHost
	if h, _, err := net.SplitHostPort(reqHost); err == nil {
		hostName = h
	}

	// Always allow loopback
	if hostName == "localhost" || hostName == "127.0.0.1" || hostName == "::1" {
		return true
	}

	// Check against explicitly configured allowed hosts
	for _, ah := range s.allowedHosts {
		if ah == "*" || strings.EqualFold(ah, reqHost) || strings.EqualFold(ah, hostName) {
			return true
		}
	}

	return false
}

func (s *Server) isAllowedHost(r *http.Request) bool {
	if len(s.allowedHosts) == 0 {
		return true
	}

	reqHost := r.Host
	host := r.RemoteAddr
	if h, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		host = h
	}
	if remoteIP := net.ParseIP(host); remoteIP != nil && s.isTrustedProxy(remoteIP) {
		if xfh := r.Header.Get("X-Forwarded-Host"); xfh != "" {
			reqHost = strings.TrimSpace(strings.Split(xfh, ",")[0])
		}
	}

	return s.isAllowedHostName(reqHost)
}

func (s *Server) isAllowedOrigin(origin, reqHost string) bool {
	if origin == "" {
		return false
	}

	for _, ao := range s.allowedOrigins {
		if ao == "*" || strings.EqualFold(ao, origin) {
			return true
		}
	}

	u, err := url.Parse(origin)
	if err != nil {
		return false
	}

	// Match request Host header (same host and port).
	// If allowed hosts are configured, enforce that reqHost is in the allowed list.
	if strings.EqualFold(u.Host, reqHost) {
		if len(s.allowedHosts) > 0 && !s.isAllowedHostName(reqHost) {
			return false
		}
		return true
	}

	// Allow localhost / 127.0.0.1 / [::1] on any port for local browser administration
	hostname := u.Hostname()
	if hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1" {
		return true
	}

	return false
}

// maxRequestBodyBytes limits incoming request JSON bodies to 1 MB to prevent memory exhaustion DoS attacks.
const maxRequestBodyBytes = 1 << 20 // 1 MB (1,048,576 bytes)

// maxPaginationLimit defines the maximum allowable limit parameter for paged endpoints.
const maxPaginationLimit = 500

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Standard Security Headers
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "SAMEORIGIN")
	w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

	// Public Health check endpoint for Docker / Kubernetes liveness & readiness probes
	if r.URL.Path == "/health" || r.URL.Path == "/api/health" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}

	// Validate client IP filter if configured
	if !s.isAllowedClientIP(r) {
		http.Error(w, "Forbidden: Client IP not allowed", http.StatusForbidden)
		return
	}

	// Validate Host header if allowed hosts are configured (protects against DNS rebinding attacks)
	if len(s.allowedHosts) > 0 && !s.isAllowedHost(r) {
		http.Error(w, "Forbidden: Invalid Host header", http.StatusForbidden)
		return
	}

	// Validate API token if configured
	if s.apiToken != "" && strings.HasPrefix(r.URL.Path, "/api/") {
		token := ""
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token = strings.TrimPrefix(authHeader, "Bearer ")
		} else if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
			token = apiKey
		} else {
			token = r.URL.Query().Get("token")
		}

		if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(s.apiToken)) != 1 {
			writeError(w, http.StatusUnauthorized, "Unauthorized: invalid or missing API token")
			return
		}
	}

	origin := r.Header.Get("Origin")
	if origin != "" {
		if s.isAllowedOrigin(origin, r.Host) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
			w.Header().Set("Vary", "Origin")
		} else if r.Method == http.MethodOptions {
			http.Error(w, "Forbidden: CORS origin not allowed", http.StatusForbidden)
			return
		}
	}

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Limit request body size across all incoming HTTP requests to prevent memory exhaustion attacks
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch {
			rc := http.NewResponseController(w)
			_ = rc.SetReadDeadline(time.Now().Add(15 * time.Second))
		}
	}

	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	// SSE Real-time Stream
	s.mux.HandleFunc("/api/stream", s.handleSSE)

	// Discovery
	s.mux.HandleFunc("/api/discovery/status", s.handleDiscoveryStatus)
	s.mux.HandleFunc("/api/discovery/run", s.handleDiscoveryRun)

	// Summary & Stats
	s.mux.HandleFunc("/api/summary", s.handleGetSummary)

	// Hosts
	s.mux.HandleFunc("/api/hosts", s.handleHosts)
	s.mux.HandleFunc("/api/hosts/", s.handleHostDetailOrAction)

	// Subnet Matrix & Heatmap
	s.mux.HandleFunc("/api/subnets/matrix", s.handleSubnetsMatrix)

	// Outlier & degraded host queue
	s.mux.HandleFunc("/api/outliers", s.handleOutliers)

	// CIDRs
	s.mux.HandleFunc("/api/cidrs", s.handleCIDRs)

	// Exclusions
	s.mux.HandleFunc("/api/exclusions", s.handleExclusions)

	// Alerts
	s.mux.HandleFunc("/api/alerts", s.handleAlerts)
	s.mux.HandleFunc("/api/alerts/history", s.handleAlertHistory)
	s.mux.HandleFunc("/api/alerts/acknowledge", s.handleAlertAcknowledge)
	s.mux.HandleFunc("/api/alerts/acknowledge-all", s.handleAlertAcknowledgeAll)

	// Settings
	s.mux.HandleFunc("/api/settings", s.handleSettings)

	// Static UI assets
	if s.distFS != nil {
		fileServer := http.FileServer(s.distFS)
		s.mux.Handle("/", s.cacheControlMiddleware(fileServer))
	} else {
		s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<h1>DINIS ICMP Monitor Running</h1>"))
		})
	}
}

func (s *Server) allowManualPing(ip string) bool {
	s.manualPingMu.Lock()
	defer s.manualPingMu.Unlock()

	now := time.Now()
	if last, exists := s.manualPingLast[ip]; exists {
		if now.Sub(last) < 1*time.Second {
			return false
		}
	}

	s.manualPingLast[ip] = now

	// Periodic cleanup to avoid unbounded memory growth
	if len(s.manualPingLast) > 1000 {
		cutoff := now.Add(-5 * time.Minute)
		for k, t := range s.manualPingLast {
			if t.Before(cutoff) {
				delete(s.manualPingLast, k)
			}
		}
	}

	return true
}

func (s *Server) getAssetETag(path string) string {
	s.etagMu.RLock()
	etag, ok := s.assetETags[path]
	s.etagMu.RUnlock()
	if ok {
		return etag
	}

	s.etagMu.Lock()
	defer s.etagMu.Unlock()
	if etag, ok := s.assetETags[path]; ok {
		return etag
	}

	if s.distFS != nil {
		cleanPath := strings.TrimPrefix(path, "/")
		f, err := s.distFS.Open(cleanPath)
		if err == nil {
			defer f.Close()
			hasher := sha256.New()
			if _, err := io.Copy(hasher, f); err == nil {
				etag = fmt.Sprintf(`W/"%x"`, hasher.Sum(nil)[:16])
				s.assetETags[path] = etag
				return etag
			}
		}
	}

	etag = fmt.Sprintf(`W/"dinis-%s"`, strings.ReplaceAll(path, "/", "-"))
	s.assetETags[path] = etag
	return etag
}

func (s *Server) cacheControlMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/" || path == "/index.html" {
			w.Header().Set("Cache-Control", "no-cache, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
			next.ServeHTTP(w, r)
			return
		}

		ext := filepath.Ext(path)
		switch ext {
		case ".js", ".css", ".svg", ".png", ".ico", ".woff2":
			w.Header().Set("Cache-Control", "public, max-age=86400, must-revalidate")
			etag := s.getAssetETag(path)
			w.Header().Set("ETag", etag)
			if match := r.Header.Get("If-None-Match"); match != "" && strings.Contains(match, etag) {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// JSON Helpers
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// decodeJSON decodes a JSON request body.
func decodeJSON(r *http.Request, v interface{}) error {
	if r.Body == nil {
		return fmt.Errorf("empty request body")
	}
	return json.NewDecoder(r.Body).Decode(v)
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("Request body exceeds %d byte limit", maxRequestBodyBytes))
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON payload: %v", err))
		return
	}
	writeError(w, http.StatusBadRequest, "Invalid JSON payload")
}

// SSE Handler
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})  // Clear read deadline for indefinite SSE streaming
	_ = rc.SetWriteDeadline(time.Time{}) // Disable HTTP write deadline for this long-lived SSE streaming connection

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	clientChan := make(chan []byte, 100)

	s.coord.clientsMu.Lock()
	if len(s.coord.sseClients) >= s.coord.maxSSEClients {
		s.coord.clientsMu.Unlock()
		http.Error(w, "Too many SSE clients", http.StatusServiceUnavailable)
		return
	}
	s.coord.sseClients[clientChan] = true
	s.coord.clientsMu.Unlock()

	defer func() {
		s.coord.clientsMu.Lock()
		if _, ok := s.coord.sseClients[clientChan]; ok {
			delete(s.coord.sseClients, clientChan)
			close(clientChan)
		}
		s.coord.clientsMu.Unlock()
	}()

	// Send initial greeting / sync state
	summary := s.coord.pinger.GetSummary()
	sumBytes, _ := json.Marshal(summary)
	_ = rc.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, _ = fmt.Fprintf(w, "event: summary_update\ndata: %s\n\n", string(sumBytes))
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, open := <-clientChan:
			if !open {
				return
			}
			_ = rc.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err := w.Write(msg)
			if err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) handleDiscoveryStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	status := s.coord.GetDiscoveryStatus()
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleDiscoveryRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Rate-limit: allow at most one manual discovery trigger per interval
	s.coord.discMu.Lock()
	if time.Since(s.coord.discoveryLastTriggered) < discoveryRateLimitInterval {
		s.coord.discMu.Unlock()
		writeError(w, http.StatusTooManyRequests, "Discovery rate limited. Try again later.")
		return
	}
	s.coord.discoveryLastTriggered = time.Now()
	s.coord.discMu.Unlock()

	var req struct {
		CIDR string `json:"cidr"`
	}
	if r.Body != nil {
		_ = decodeJSON(r, &req)
	}

	if !s.coord.TriggerDiscovery(req.CIDR) {
		writeError(w, http.StatusServiceUnavailable, "Server is shutting down")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"message": "Discovery sweep initiated",
	})
}

func (s *Server) handleGetSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	summary := s.coord.pinger.GetSummary()
	discStatus := s.coord.GetDiscoveryStatus()
	summary.SubnetCapacity = discStatus.SubnetCapacity
	writeJSON(w, http.StatusOK, summary)
}

func ipToUint32(ipStr string) uint32 {
	ip := net.ParseIP(ipStr).To4()
	if ip == nil {
		return 0
	}
	return binary.BigEndian.Uint32(ip)
}

func getStatusWeight(h *pinger.HostState) int {
	if h.IsExcluded || h.Status == pinger.StatusExcluded {
		return 5
	}
	if h.Status == pinger.StatusDown {
		if h.AlertAcknowledged {
			return 2
		}
		return 1
	}
	if h.Status == pinger.StatusUp {
		return 3
	}
	if h.Status == pinger.StatusPending {
		return 4
	}
	return 6
}

func (s *Server) handleHosts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	pageStr := r.URL.Query().Get("page")
	limitStr := r.URL.Query().Get("limit")
	search := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search")))
	status := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status")))
	sortField := r.URL.Query().Get("sort")
	lightweight := r.URL.Query().Get("lightweight") == "true"

	allHosts := s.coord.pinger.GetAllHosts()

	// If no query parameters, maintain legacy response format (raw slice) for backward compatibility
	if pageStr == "" && limitStr == "" && search == "" && status == "" && sortField == "" {
		if lightweight {
			for _, h := range allHosts {
				h.LatencyHistory = nil
			}
		}
		writeJSON(w, http.StatusOK, allHosts)
		return
	}

	var filtered []*pinger.HostState
	for _, h := range allHosts {
		// Search filter
		if search != "" {
			matchIP := strings.Contains(strings.ToLower(h.IP), search)
			matchAlias := strings.Contains(strings.ToLower(h.Alias), search)
			matchCIDR := strings.Contains(strings.ToLower(h.CIDR), search)
			matchExcl := strings.Contains(strings.ToLower(h.ExclusionReason), search)
			if !matchIP && !matchAlias && !matchCIDR && !matchExcl {
				continue
			}
		}

		// Status filter
		if status != "" && status != "all" {
			switch status {
			case "down":
				if h.Status != pinger.StatusDown || h.AlertAcknowledged || h.IsExcluded {
					continue
				}
			case "ack":
				if h.Status != pinger.StatusDown || !h.AlertAcknowledged || h.IsExcluded {
					continue
				}
			case "up":
				if h.Status != pinger.StatusUp || h.IsExcluded {
					continue
				}
			case "pending":
				if h.Status != pinger.StatusPending || h.IsExcluded {
					continue
				}
			case "excluded":
				if !h.IsExcluded && h.Status != pinger.StatusExcluded {
					continue
				}
			default:
				if string(h.Status) != strings.ToUpper(status) {
					continue
				}
			}
		}

		if lightweight {
			cpy := *h
			cpy.LatencyHistory = nil
			filtered = append(filtered, &cpy)
		} else {
			filtered = append(filtered, h)
		}
	}

	// Sort
	if sortField != "" {
		sort.Slice(filtered, func(i, j int) bool {
			a, b := filtered[i], filtered[j]
			switch sortField {
			case "ip-asc":
				return ipToUint32(a.IP) < ipToUint32(b.IP)
			case "ip-desc":
				return ipToUint32(a.IP) > ipToUint32(b.IP)
			case "status":
				wa, wb := getStatusWeight(a), getStatusWeight(b)
				if wa != wb {
					return wa < wb
				}
				return ipToUint32(a.IP) < ipToUint32(b.IP)
			case "latency-desc":
				return a.LatencyMs > b.LatencyMs
			case "latency-asc":
				latA, latB := a.LatencyMs, b.LatencyMs
				if latA == 0 {
					latA = 99999
				}
				if latB == 0 {
					latB = 99999
				}
				return latA < latB
			case "loss":
				return a.PacketLoss > b.PacketLoss
			default:
				return ipToUint32(a.IP) < ipToUint32(b.IP)
			}
		})
	}

	total := len(filtered)
	page := 1
	limit := 50

	if pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
			if limit > maxPaginationLimit {
				limit = maxPaginationLimit
			}
		}
	}

	start := (page - 1) * limit
	if start > total {
		start = total
	}
	end := start + limit
	if end > total {
		end = total
	}

	pagedHosts := filtered[start:end]
	totalPages := 0
	if limit > 0 {
		totalPages = (total + limit - 1) / limit
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total":      total,
		"page":       page,
		"limit":      limit,
		"totalPages": totalPages,
		"hosts":      pagedHosts,
	})
}

func getSubnetGroupKey(h *pinger.HostState) string {
	cidr := strings.TrimSpace(h.CIDR)
	if cidr == "" || cidr == "Static" {
		return h.IP + "/32"
	}

	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil || ipNet == nil {
		return cidr
	}

	ones, _ := ipNet.Mask.Size()
	// If prefix length is 24 or more (e.g. /24, /28, /29, /30, /32), preserve exact subnet
	if ones >= 24 {
		return cidr
	}

	// For large CIDRs (/16, /20, /22), group by /24 sub-blocks
	ip4 := net.ParseIP(h.IP).To4()
	if ip4 != nil {
		return fmt.Sprintf("%d.%d.%d.0/24", ip4[0], ip4[1], ip4[2])
	}
	return cidr
}

func (s *Server) handleSubnetsMatrix(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	hosts := s.coord.pinger.GetAllHosts()
	hostsBySubnet := make(map[string][]timeseries.SubnetMatrixCell)

	for _, h := range hosts {
		subnetKey := getSubnetGroupKey(h)

		parsedIP := net.ParseIP(h.IP).To4()
		hostIdx := 0
		if parsedIP != nil {
			hostIdx = int(parsedIP[3])
		}

		cell := timeseries.SubnetMatrixCell{
			IP:            h.IP,
			HostIndex:     hostIdx,
			Status:        string(h.Status),
			LatencyMs:     h.LatencyMs,
			PacketLossPct: h.PacketLoss,
			AlertActive:   h.AlertActive,
			AlertAck:      h.AlertAcknowledged,
			Alias:         h.Alias,
		}
		hostsBySubnet[subnetKey] = append(hostsBySubnet[subnetKey], cell)
	}

	matrix := timeseries.GenerateSubnetMatrix(hostsBySubnet)
	writeJSON(w, http.StatusOK, matrix)
}

func (s *Server) handleOutliers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	tsStore := s.coord.pinger.GetTimeseriesStore()
	limit := 50
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
			limit = l
			if limit > maxPaginationLimit {
				limit = maxPaginationLimit
			}
		}
	}

	outliers := tsStore.GetTopOutliers(limit, func(ip string) (bool, string) {
		if h, ok := s.coord.pinger.GetHost(ip); ok && !h.IsExcluded {
			return true, h.CIDR
		}
		return false, ""
	})

	writeJSON(w, http.StatusOK, outliers)
}

func (s *Server) handleHostDetailOrAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/hosts/")
	parts := strings.Split(path, "/")
	ip := parts[0]

	if ip == "" {
		writeError(w, http.StatusBadRequest, "Missing IP address")
		return
	}

	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		writeError(w, http.StatusBadRequest, "Invalid IP address")
		return
	}
	ip = parsedIP.String()

	if len(parts) == 1 {
		// GET /api/hosts/{ip}
		if r.Method == http.MethodGet {
			h, ok := s.coord.pinger.GetHost(ip)
			if !ok {
				writeError(w, http.StatusNotFound, "Host not found")
				return
			}
			writeJSON(w, http.StatusOK, h)
			return
		}
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	action := parts[1]
	switch action {
	case "history":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		windowStr := r.URL.Query().Get("window")
		windowDur := 1 * time.Hour
		if windowStr != "" {
			if d, err := time.ParseDuration(windowStr); err == nil && d > 0 {
				windowDur = d
			}
		}
		history := s.coord.pinger.GetTimeseriesStore().GetHostHistory(ip, windowDur)
		writeJSON(w, http.StatusOK, history)

	case "ping":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		if !s.allowManualPing(ip) {
			writeError(w, http.StatusTooManyRequests, "Rate limit exceeded: manual ping allows at most 1 probe per second per host")
			return
		}
		res := s.coord.pinger.PingSingle(r.Context(), ip)
		writeJSON(w, http.StatusOK, res)

	case "enrollment":
		if r.Method != http.MethodDelete {
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		if err := s.coord.store.RemoveDiscoveredHost(ip); err != nil {
			log.Printf("[DINIS] Error removing host %s: %v", ip, err)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to un-enroll host: %v", err))
			return
		}
		s.coord.alerts.Resolve(ip)
		s.coord.RebuildTargetList()
		writeJSON(w, http.StatusOK, map[string]string{"message": "Host un-enrolled from active monitoring"})

	case "promote":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}

		now := time.Now()
		cidr := ip + "/32"
		discovered := s.coord.store.GetDiscoveredHosts()
		discAt := now
		if existing, ok := discovered[ip]; ok {
			if existing.CIDR != "" && existing.CIDR != "Static" {
				cidr = existing.CIDR
			}
			if !existing.DiscoveredAt.IsZero() {
				discAt = existing.DiscoveredAt
			}
		} else {
			cidrs := s.coord.store.GetCIDRs()
			for _, c := range cidrs {
				if !c.Enabled {
					continue
				}
				_, ipNet, err := net.ParseCIDR(c.CIDR)
				if err == nil && ipNet.Contains(parsedIP) {
					cidr = c.CIDR
					break
				}
			}
		}

		if err := s.coord.store.AddOrUpdateDiscoveredHost(store.DiscoveredHost{
			IP:             ip,
			CIDR:           cidr,
			DiscoveredAt:   discAt,
			LastDiscovered: now,
			IsStatic:       true,
		}); err != nil {
			log.Printf("[DINIS] Error promoting host %s: %v", ip, err)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to promote host: %v", err))
			return
		}
		s.coord.RebuildTargetList()
		writeJSON(w, http.StatusOK, map[string]string{"message": "Host promoted to static monitored target"})

	case "meta":
		if r.Method != http.MethodPut && r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		var req struct {
			Alias string `json:"alias"`
			Notes string `json:"notes"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeDecodeError(w, err)
			return
		}

		err := s.coord.store.SetHostMeta(store.HostMeta{
			IP:    ip,
			Alias: req.Alias,
			Notes: req.Notes,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		s.coord.RebuildTargetList()
		h, _ := s.coord.pinger.GetHost(ip)
		writeJSON(w, http.StatusOK, h)

	default:
		writeError(w, http.StatusNotFound, "Unknown host subaction")
	}
}

func (s *Server) handleCIDRs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		cidrs := s.coord.store.GetCIDRs()
		writeJSON(w, http.StatusOK, cidrs)

	case http.MethodPost:
		var req struct {
			CIDR               string `json:"cidr"`
			Description        string `json:"description"`
			Enabled            *bool  `json:"enabled"`
			IncludeNetAndBcast bool   `json:"includeNetAndBcast"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeDecodeError(w, err)
			return
		}

		// Validate CIDR
		info, err := network.ParseCIDR(req.CIDR, req.IncludeNetAndBcast)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid CIDR: %v", err))
			return
		}

		enabled := true
		if req.Enabled != nil {
			enabled = *req.Enabled
		}

		cidrCfg := store.CIDRConfig{
			CIDR:               info.CIDR,
			Description:        req.Description,
			Enabled:            enabled,
			IncludeNetAndBcast: req.IncludeNetAndBcast,
			CreatedAt:          time.Now(),
		}

		if err := s.coord.store.AddOrUpdateCIDR(cidrCfg); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		s.coord.RebuildTargetList()

		// Trigger background discovery for this new CIDR to find active online hosts
		settings := s.coord.store.GetSettings()
		if settings.AutoDiscovery {
			s.coord.TriggerDiscovery(cidrCfg.CIDR)
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"cidr":       cidrCfg,
			"totalHosts": info.TotalHosts,
		})

	case http.MethodDelete:
		cidr := r.URL.Query().Get("cidr")
		if cidr == "" && r.Body != nil {
			var body struct {
				CIDR string `json:"cidr"`
			}
			_ = decodeJSON(r, &body)
			cidr = body.CIDR
		}
		if cidr == "" {
			writeError(w, http.StatusBadRequest, "Missing cidr parameter")
			return
		}

		if err := s.coord.store.DeleteCIDR(cidr); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		s.coord.RebuildTargetList()
		writeJSON(w, http.StatusOK, map[string]string{"message": "CIDR deleted successfully"})

	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (s *Server) handleExclusions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		excls := s.coord.store.GetExclusions()
		writeJSON(w, http.StatusOK, excls)

	case http.MethodPost:
		var req struct {
			Rule    string `json:"rule"`
			Reason  string `json:"reason"`
			Enabled *bool  `json:"enabled"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeDecodeError(w, err)
			return
		}

		req.Rule = strings.TrimSpace(req.Rule)
		if req.Rule == "" {
			writeError(w, http.StatusBadRequest, "Rule is required")
			return
		}

		// Validate rule
		matcher := network.NewExclusionMatcher()
		if err := matcher.AddExclusion(req.Rule, req.Reason); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid exclusion rule: %v", err))
			return
		}

		enabled := true
		if req.Enabled != nil {
			enabled = *req.Enabled
		}

		exclCfg := store.ExclusionConfig{
			Rule:      req.Rule,
			Reason:    req.Reason,
			Enabled:   enabled,
			CreatedAt: time.Now(),
		}

		if err := s.coord.store.AddOrUpdateExclusion(exclCfg); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		s.coord.RebuildTargetList()
		writeJSON(w, http.StatusOK, exclCfg)

	case http.MethodDelete:
		rule := r.URL.Query().Get("rule")
		if rule == "" && r.Body != nil {
			var body struct {
				Rule string `json:"rule"`
			}
			_ = decodeJSON(r, &body)
			rule = body.Rule
		}
		if rule == "" {
			writeError(w, http.StatusBadRequest, "Missing rule parameter")
			return
		}

		if err := s.coord.store.DeleteExclusion(rule); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		s.coord.RebuildTargetList()
		writeJSON(w, http.StatusOK, map[string]string{"message": "Exclusion removed successfully"})

	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	active := s.coord.alerts.GetActiveAlerts()
	writeJSON(w, http.StatusOK, active)
}

func (s *Server) handleAlertHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	history := s.coord.alerts.GetAlertHistory(100)
	writeJSON(w, http.StatusOK, history)
}

func (s *Server) handleAlertAcknowledge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req struct {
		IP    string `json:"ip"`
		ID    string `json:"id"`
		AckBy string `json:"ackBy"`
		Note  string `json:"note"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeDecodeError(w, err)
		return
	}

	target := req.IP
	if target == "" {
		target = req.ID
	}
	if target == "" {
		writeError(w, http.StatusBadRequest, "Target IP or alert ID required")
		return
	}

	alert, err := s.coord.alerts.Acknowledge(target, req.AckBy, req.Note)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Update host state in pinger engine
	s.coord.pinger.SetHostAlertState(alert.IP, true, alert.ID, true, alert.AcknowledgedBy, alert.AckNote, alert.AcknowledgedAt, &alert.StartedAt)
	if h, ok := s.coord.pinger.GetHost(alert.IP); ok {
		s.coord.broadcastEvent("host_update", h)
	}

	writeJSON(w, http.StatusOK, alert)
}

func (s *Server) handleAlertAcknowledgeAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req struct {
		AckBy string `json:"ackBy"`
		Note  string `json:"note"`
	}
	if r.Body != nil {
		_ = decodeJSON(r, &req)
	}

	ackAlerts := s.coord.alerts.AcknowledgeAll(req.AckBy, req.Note)
	for _, a := range ackAlerts {
		s.coord.pinger.SetHostAlertState(a.IP, true, a.ID, true, a.AcknowledgedBy, a.AckNote, a.AcknowledgedAt, &a.StartedAt)
		if h, ok := s.coord.pinger.GetHost(a.IP); ok {
			s.coord.broadcastEvent("host_update", h)
		}
	}

	writeJSON(w, http.StatusOK, ackAlerts)
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		settings := s.coord.store.GetSettings()
		writeJSON(w, http.StatusOK, settings)

	case http.MethodPut, http.MethodPost:
		var req store.AppSettings
		if err := decodeJSON(r, &req); err != nil {
			writeDecodeError(w, err)
			return
		}

		if req.IntervalSec < 0.5 {
			req.IntervalSec = 0.5
		}
		if req.IntervalSec > 3600 {
			req.IntervalSec = 3600
		}
		if req.TimeoutMs <= 0 {
			req.TimeoutMs = 1000
		}
		if req.TimeoutMs > 30000 {
			req.TimeoutMs = 30000
		}
		if req.FailThreshold <= 0 {
			req.FailThreshold = 2
		}
		if req.FailThreshold > 100 {
			req.FailThreshold = 100
		}
		if req.Concurrency <= 0 {
			req.Concurrency = 100
		}
		if req.Concurrency > 1024 {
			req.Concurrency = 1024
		}
		if req.DiscoveryIntervalMin < 0 {
			req.DiscoveryIntervalMin = 0
		}
		if req.MaxMetricHosts <= 0 {
			req.MaxMetricHosts = 10000
		} else if req.MaxMetricHosts < 500 {
			req.MaxMetricHosts = 500
		} else if req.MaxMetricHosts > 500000 {
			req.MaxMetricHosts = 500000
		}

		if err := s.coord.store.UpdateSettings(req); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Update engine config live
		s.coord.pinger.UpdateConfig(pinger.EngineConfig{
			Interval:       time.Duration(req.IntervalSec * float64(time.Second)),
			Timeout:        time.Duration(req.TimeoutMs) * time.Millisecond,
			Concurrency:    req.Concurrency,
			FailThreshold:  req.FailThreshold,
			HistorySize:    25,
			MaxMetricHosts: req.MaxMetricHosts,
		})

		s.coord.discMu.Lock()
		s.coord.discoveryStatus.IntervalMin = req.DiscoveryIntervalMin
		if req.DiscoveryIntervalMin > 0 {
			if s.coord.discoveryStatus.LastRun != nil {
				next := s.coord.discoveryStatus.LastRun.Add(time.Duration(req.DiscoveryIntervalMin) * time.Minute)
				s.coord.discoveryStatus.NextRun = &next
			}
		} else {
			s.coord.discoveryStatus.NextRun = nil
		}
		s.coord.discMu.Unlock()

		writeJSON(w, http.StatusOK, req)

	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}
