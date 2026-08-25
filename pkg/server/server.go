package server

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"time"

	"dinis/pkg/alerts"
	"dinis/pkg/network"
	"dinis/pkg/pinger"
	"dinis/pkg/store"
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

// Coordinator orchestrates between Storage, CIDR Engine, ICMP Engine, and Alert Manager.
type Coordinator struct {
	mu         sync.RWMutex
	store      *store.Store
	pinger     *pinger.Engine
	alerts     *alerts.Manager
	clientsMu  sync.RWMutex
	sseClients map[chan []byte]bool
	stopChan   chan struct{}

	discMu          sync.RWMutex
	discoveryStatus DiscoveryStatus
}

// NewCoordinator creates and wires the entire monitoring subsystem.
func NewCoordinator(st *store.Store) *Coordinator {
	settings := st.GetSettings()
	cfg := pinger.EngineConfig{
		Interval:      time.Duration(settings.IntervalSec * float64(time.Second)),
		Timeout:       time.Duration(settings.TimeoutMs) * time.Millisecond,
		Concurrency:   settings.Concurrency,
		FailThreshold: settings.FailThreshold,
		HistorySize:   25,
	}

	p := pinger.NewEngine(cfg)
	altMgr := alerts.NewManager(500)

	c := &Coordinator{
		store:      st,
		pinger:     p,
		alerts:     altMgr,
		sseClients: make(map[chan []byte]bool),
		stopChan:   make(chan struct{}),
		discoveryStatus: DiscoveryStatus{
			IntervalMin: settings.DiscoveryIntervalMin,
		},
	}

	// Wire engine callbacks
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
	go c.heartbeatLoop()
	go c.discoveryLoop()
}

// Stop gracefully terminates all subsystems.
func (c *Coordinator) Stop() {
	close(c.stopChan)
	c.pinger.Stop()

	c.clientsMu.Lock()
	for ch := range c.sseClients {
		close(ch)
		delete(c.sseClients, ch)
	}
	c.clientsMu.Unlock()
}

// RebuildTargetList recalculates the list of active target hosts based on discovered hosts, configured CIDRs, and exclusions.
func (c *Coordinator) RebuildTargetList() {
	c.mu.Lock()
	defer c.mu.Unlock()

	cidrs := c.store.GetCIDRs()
	exclusions := c.store.GetExclusions()
	discovered := c.store.GetDiscoveredHosts()

	matcher := network.NewExclusionMatcher()
	for _, excl := range exclusions {
		if excl.Enabled {
			_ = matcher.AddExclusion(excl.Rule, excl.Reason)
		}
	}

	validCIDRs := make(map[string]bool)
	cidrMap := make(map[string]*network.CIDRInfo)
	totalCapacity := 0

	for _, cfg := range cidrs {
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
				_ = c.store.AddOrUpdateDiscoveredHost(discHost)
				discovered[ip] = discHost
			}
		}
	}

	// Prune discovered hosts belonging to deleted CIDRs
	_ = c.store.PruneDiscoveredHosts(validCIDRs)

	hostMap := make(map[string]*pinger.HostState)

	for ip, disc := range discovered {
		if !validCIDRs[disc.CIDR] {
			continue
		}

		matched, _, reason := matcher.Matches(ip)
		meta, _ := c.store.GetHostMeta(ip)
		alias := meta.Alias
		if alias == "" {
			if info, ok := cidrMap[disc.CIDR]; ok && info.TotalHosts == 1 {
				for _, cfg := range cidrs {
					if cfg.CIDR == disc.CIDR && cfg.Description != "" {
						alias = cfg.Description
						break
					}
				}
			}
		}

		status := pinger.StatusPending
		if matched {
			status = pinger.StatusExcluded
			// If host was previously alerting but is now excluded, resolve its alert
			c.alerts.Resolve(ip)
		}

		// Check if existing alert is active
		var isAlertActive, isAck bool
		var alertID, ackBy, ackNote string
		var ackAt, startAt *time.Time

		if !matched {
			if alt, hasAlt := c.alerts.GetAlertForIP(ip); hasAlt {
				isAlertActive = true
				alertID = alt.ID
				isAck = alt.Acknowledged
				ackBy = alt.AcknowledgedBy
				ackNote = alt.AckNote
				ackAt = alt.AcknowledgedAt
				startAt = &alt.StartedAt
			}
		}

		discAt := disc.DiscoveredAt
		lastDisc := disc.LastDiscovered

		hostMap[ip] = &pinger.HostState{
			IP:                ip,
			Alias:             alias,
			CIDR:              disc.CIDR,
			Status:            status,
			IsExcluded:        matched,
			ExclusionReason:   reason,
			DiscoveredAt:      &discAt,
			LastDiscovered:    &lastDisc,
			IsStatic:          disc.IsStatic,
			AlertActive:       isAlertActive,
			AlertID:           alertID,
			AlertAcknowledged: isAck,
			AlertAckBy:        ackBy,
			AlertAckNote:      ackNote,
			AlertAckAt:        ackAt,
			AlertStartedAt:    startAt,
			LatencyHistory:    make([]float64, 0, 25),
		}
	}

	// Clean up any active alerts for hosts that are no longer monitored or are now excluded
	for _, a := range c.alerts.GetActiveAlerts() {
		if h, exists := hostMap[a.IP]; !exists || h.IsExcluded {
			c.alerts.Resolve(a.IP)
		}
	}

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

	prober := pinger.NewSingleProber()
	settings := c.store.GetSettings()
	concurrency := settings.Concurrency
	if concurrency <= 0 {
		concurrency = 100
	}
	if concurrency > len(targets) {
		concurrency = len(targets)
	}
	if concurrency <= 0 {
		concurrency = 1
	}

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
			for cand := range workChan {
				res := prober.Probe(context.TODO(), cand.ip, 750*time.Millisecond)
				if res.Success {
					now := time.Now()
					discoveredMu.Lock()
					discoveredOnline++
					if _, exists := existingDiscovered[cand.ip]; !exists {
						newDiscovered++
					}
					discoveredBatch = append(discoveredBatch, store.DiscoveredHost{
						IP:             cand.ip,
						CIDR:           cand.cidr,
						DiscoveredAt:   now,
						LastDiscovered: now,
					})
					discoveredMu.Unlock()
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

	for _, t := range targets {
		select {
		case <-c.stopChan:
			close(workChan)
			wg.Wait()
			return 0, 0, nil
		case workChan <- t:
		}

		if discPace > 0 {
			time.Sleep(discPace)
		}
	}
	close(workChan)

	wg.Wait()

	// Batch-write all discovered hosts to disk in a single atomic operation,
	// instead of N individual writes during the sweep.
	if len(discoveredBatch) > 0 {
		_ = c.store.AddOrUpdateDiscoveredHostsBatch(discoveredBatch)
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

// TriggerDiscovery initiates a discovery sweep asynchronously.
func (c *Coordinator) TriggerDiscovery(specificCIDR string) {
	go func() {
		_, _, _ = c.RunDiscovery(specificCIDR)
	}()
}

// GetDiscoveryStatus returns a copy of current discovery status.
func (c *Coordinator) GetDiscoveryStatus() DiscoveryStatus {
	c.discMu.RLock()
	defer c.discMu.RUnlock()
	return c.discoveryStatus
}

func (c *Coordinator) discoveryLoop() {
	// Run initial discovery sweep on startup after a 500ms warmup
	time.Sleep(500 * time.Millisecond)
	settings := c.store.GetSettings()
	if settings.AutoDiscovery {
		_, _, _ = c.RunDiscovery("")
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
				_, _, _ = c.RunDiscovery("")
			}
		}
	}
}

func (c *Coordinator) handleStateChange(h *pinger.HostState, oldStatus, newStatus pinger.HostStatus) {
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
		c.pinger.SetHostAlertState(h.IP, true, alt.ID, alt.Acknowledged, alt.AcknowledgedBy, alt.AckNote, alt.AcknowledgedAt, &alt.StartedAt)
	case pinger.StatusUp, pinger.StatusExcluded:
		c.alerts.Resolve(h.IP)
		h.AlertActive = false
		h.AlertAcknowledged = false
		h.AlertID = ""
		h.AlertAckBy = ""
		h.AlertAckNote = ""
		h.AlertAckAt = nil
		h.AlertStartedAt = nil
		c.pinger.SetHostAlertState(h.IP, false, "", false, "", "", nil, nil)
	}

	c.broadcastEvent("host_state_change", map[string]interface{}{
		"host":      h,
		"oldStatus": oldStatus,
		"newStatus": newStatus,
	})
}

func (c *Coordinator) handleHostUpdated(h *pinger.HostState) {
	if alt, hasAlt := c.alerts.GetAlertForIP(h.IP); hasAlt {
		h.AlertActive = true
		h.AlertID = alt.ID
		h.AlertAcknowledged = alt.Acknowledged
		h.AlertAckBy = alt.AcknowledgedBy
		h.AlertAckNote = alt.AckNote
		h.AlertAckAt = alt.AcknowledgedAt
		h.AlertStartedAt = &alt.StartedAt
	} else {
		h.AlertActive = false
		h.AlertID = ""
		h.AlertAcknowledged = false
		h.AlertAckBy = ""
		h.AlertAckNote = ""
		h.AlertAckAt = nil
		h.AlertStartedAt = nil
		c.pinger.SetHostAlertState(h.IP, false, "", false, "", "", nil, nil)
	}
	c.broadcastEvent("host_update", h)
}

func (c *Coordinator) handleCycleComplete(summary *pinger.CycleSummary) {
	c.broadcastEvent("summary_update", summary)
}

func (c *Coordinator) broadcastEvent(eventType string, data interface{}) {
	payload, err := json.Marshal(data)
	if err != nil {
		return
	}

	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", eventType, string(payload))
	msgBytes := []byte(msg)

	c.clientsMu.RLock()
	defer c.clientsMu.RUnlock()

	for ch := range c.sseClients {
		select {
		case ch <- msgBytes:
		default:
			// Client channel full or slow, skip
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
			c.clientsMu.RLock()
			for ch := range c.sseClients {
				select {
				case ch <- pingMsg:
				default:
				}
			}
			c.clientsMu.RUnlock()
		}
	}
}

// Server holds the HTTP handler routing.
type Server struct {
	coord      *Coordinator
	mux        *http.ServeMux
	distFS     http.FileSystem
	staticPath string // If provided, serves live directory instead of embedded
}

// NewServer creates a new HTTP server routing all REST APIs and web dashboard assets.
func NewServer(coord *Coordinator, staticDir string) *Server {
	s := &Server{
		coord:      coord,
		mux:        http.NewServeMux(),
		staticPath: staticDir,
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

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Global CORS and security headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
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
		s.mux.Handle("/", fileServer)
	} else {
		s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<h1>DINIS ICMP Monitor Running</h1>"))
		})
	}
}

// JSON Helper
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// SSE Handler
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
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
	s.coord.sseClients[clientChan] = true
	s.coord.clientsMu.Unlock()

	defer func() {
		s.coord.clientsMu.Lock()
		delete(s.coord.sseClients, clientChan)
		close(clientChan)
		s.coord.clientsMu.Unlock()
	}()

	// Send initial greeting / sync state
	summary := s.coord.pinger.GetSummary()
	sumBytes, _ := json.Marshal(summary)
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

	var req struct {
		CIDR string `json:"cidr"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	s.coord.TriggerDiscovery(req.CIDR)
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

func (s *Server) handleHosts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	hosts := s.coord.pinger.GetAllHosts()
	writeJSON(w, http.StatusOK, hosts)
}

func (s *Server) handleHostDetailOrAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/hosts/")
	parts := strings.Split(path, "/")
	ip := parts[0]

	if ip == "" {
		writeError(w, http.StatusBadRequest, "Missing IP address")
		return
	}

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
	case "ping":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		res := s.coord.pinger.PingSingle(r.Context(), ip)
		writeJSON(w, http.StatusOK, res)

	case "enrollment":
		if r.Method != http.MethodDelete {
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		_ = s.coord.store.RemoveDiscoveredHost(ip)
		s.coord.alerts.Resolve(ip)
		s.coord.RebuildTargetList()
		writeJSON(w, http.StatusOK, map[string]string{"message": "Host un-enrolled from active monitoring"})

	case "promote":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}
		now := time.Now()
		_ = s.coord.store.AddOrUpdateDiscoveredHost(store.DiscoveredHost{
			IP:             ip,
			CIDR:           "Static",
			DiscoveredAt:   now,
			LastDiscovered: now,
			IsStatic:       true,
		})
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid JSON body")
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid JSON payload")
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
		if cidr == "" {
			var body struct {
				CIDR string `json:"cidr"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid JSON payload")
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
		if rule == "" {
			var body struct {
				Rule string `json:"rule"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON payload")
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
	_ = json.NewDecoder(r.Body).Decode(&req)

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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid JSON payload")
			return
		}

		if req.IntervalSec < 0.5 {
			req.IntervalSec = 0.5
		}
		if req.TimeoutMs <= 0 {
			req.TimeoutMs = 1000
		}
		if req.FailThreshold <= 0 {
			req.FailThreshold = 2
		}
		if req.Concurrency <= 0 {
			req.Concurrency = 100
		}
		if req.DiscoveryIntervalMin < 0 {
			req.DiscoveryIntervalMin = 0
		}

		if err := s.coord.store.UpdateSettings(req); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Update engine config live
		s.coord.pinger.UpdateConfig(pinger.EngineConfig{
			Interval:      time.Duration(req.IntervalSec * float64(time.Second)),
			Timeout:       time.Duration(req.TimeoutMs) * time.Millisecond,
			Concurrency:   req.Concurrency,
			FailThreshold: req.FailThreshold,
			HistorySize:   25,
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
