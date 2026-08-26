package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dinis/pkg/pinger"
	"dinis/pkg/store"
	"dinis/pkg/timeseries"
)

func setupTestServer(t *testing.T) (*Server, *Coordinator, func()) {
	tmpDir, err := os.MkdirTemp("", "dinis_server_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	dataPath := filepath.Join(tmpDir, "test_dinis.json")

	st, err := store.NewStore(dataPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	coord := NewCoordinator(st)
	srv := NewServer(coord, "")

	cleanup := func() {
		coord.Stop()
		_ = os.RemoveAll(tmpDir)
	}

	return srv, coord, cleanup
}

func TestServerEndpoints(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()

	// Add test discovered hosts
	now := time.Now()
	_ = coord.store.AddOrUpdateDiscoveredHost(store.DiscoveredHost{
		IP:             "10.0.0.1",
		CIDR:           "10.0.0.0/24",
		DiscoveredAt:   now,
		LastDiscovered: now,
		IsStatic:       true,
	})
	_ = coord.store.AddOrUpdateDiscoveredHost(store.DiscoveredHost{
		IP:             "10.0.0.2",
		CIDR:           "10.0.0.0/24",
		DiscoveredAt:   now,
		LastDiscovered: now,
		IsStatic:       true,
	})
	coord.RebuildTargetList()

	// Ingest metrics in timeseries store
	tsStore := coord.pinger.GetTimeseriesStore()
	tsStore.Record("10.0.0.1", now, 2.4, true)
	tsStore.Record("10.0.0.2", now, 180.0, false)

	// 1. Test GET /api/hosts without params (legacy array)
	req := httptest.NewRequest(http.MethodGet, "/api/hosts", nil)
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var legacyHosts []interface{}
	if err := json.NewDecoder(rec.Body).Decode(&legacyHosts); err != nil {
		t.Fatalf("failed to decode legacy hosts array: %v", err)
	}
	if len(legacyHosts) < 2 {
		t.Errorf("expected at least 2 hosts in legacy array, got %d", len(legacyHosts))
	}

	// 2. Test GET /api/hosts with pagination
	req = httptest.NewRequest(http.MethodGet, "/api/hosts?page=1&limit=1", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var pagedResp struct {
		Total int           `json:"total"`
		Page  int           `json:"page"`
		Limit int           `json:"limit"`
		Hosts []interface{} `json:"hosts"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&pagedResp); err != nil {
		t.Fatalf("failed to decode paged response: %v", err)
	}
	if pagedResp.Total < 2 || len(pagedResp.Hosts) != 1 {
		t.Errorf("expected total >= 2 and limit 1 host, got total=%d len=%d", pagedResp.Total, len(pagedResp.Hosts))
	}

	// 2b. Test GET /api/hosts with excessive limit clamped to max (500)
	req = httptest.NewRequest(http.MethodGet, "/api/hosts?page=1&limit=999999", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var clampedResp struct {
		Limit int `json:"limit"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&clampedResp); err != nil {
		t.Fatalf("failed to decode clamped response: %v", err)
	}
	if clampedResp.Limit != 500 {
		t.Errorf("expected limit clamped to 500, got %d", clampedResp.Limit)
	}

	// 3. Test GET /api/subnets/matrix
	req = httptest.NewRequest(http.MethodGet, "/api/subnets/matrix", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var matrix []struct {
		CIDR       string `json:"cidr"`
		TotalHosts int    `json:"totalHosts"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&matrix); err != nil {
		t.Fatalf("failed to decode matrix response: %v", err)
	}
	if len(matrix) == 0 {
		t.Errorf("expected at least 1 subnet matrix block")
	}

	// 4. Test GET /api/outliers
	req = httptest.NewRequest(http.MethodGet, "/api/outliers", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// 5. Test GET /api/hosts/10.0.0.1/history
	req = httptest.NewRequest(http.MethodGet, "/api/hosts/10.0.0.1/history?window=1h", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for host history, got %d", rec.Code)
	}

	// 6. Test CIDR deletion & outlier removal
	req = httptest.NewRequest(http.MethodDelete, "/api/cidrs?cidr=10.0.0.0/24", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for CIDR deletion, got %d", rec.Code)
	}

	// Verify outliers after deletion is empty
	req = httptest.NewRequest(http.MethodGet, "/api/outliers", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for outliers, got %d", rec.Code)
	}
	var postDelOutliers []interface{}
	if err := json.NewDecoder(rec.Body).Decode(&postDelOutliers); err != nil {
		t.Fatalf("failed to decode outliers response: %v", err)
	}
	if len(postDelOutliers) != 0 {
		t.Errorf("expected 0 outliers after CIDR deletion, got %d", len(postDelOutliers))
	}
}

func TestConcurrentRebuildTargetList(t *testing.T) {
	_, coord, cleanup := setupTestServer(t)
	defer cleanup()

	// Seed some initial CIDRs
	_ = coord.store.AddOrUpdateCIDR(store.CIDRConfig{
		CIDR:        "10.10.0.0/24",
		Description: "Office LAN",
		Enabled:     true,
	})

	const numWorkers = 20
	const iterations = 10
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				ip := fmt.Sprintf("10.10.%d.%d", workerID, i%250+1)

				// Interleave store modifications and rebuilds
				switch (workerID + i) % 4 {
				case 0:
					_ = coord.store.AddOrUpdateDiscoveredHost(store.DiscoveredHost{
						IP:             ip,
						CIDR:           "10.10.0.0/24",
						DiscoveredAt:   time.Now(),
						LastDiscovered: time.Now(),
					})
				case 1:
					_ = coord.store.SetHostMeta(store.HostMeta{
						IP:    ip,
						Alias: fmt.Sprintf("Host-%d-%d", workerID, i),
						Notes: "Concurrent test note",
					})
				case 2:
					_ = coord.store.AddOrUpdateExclusion(store.ExclusionConfig{
						Rule:    ip,
						Reason:  "Maintenance",
						Enabled: i%2 == 0,
					})
				case 3:
					coord.alerts.Trigger(ip, "Test Host", "10.10.0.0/24", "Simulated packet loss")
				}

				coord.RebuildTargetList()
			}
		}()
	}

	wg.Wait()

	// Verify the final coordinator state is intact and accessible
	hosts := coord.pinger.GetAllHosts()
	if len(hosts) == 0 {
		t.Errorf("expected monitored hosts to exist after concurrent rebuilds")
	}
}

func TestConcurrentRebuildAndAPI(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()

	const numWorkers = 15
	const iterations = 8
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				switch (workerID + i) % 5 {
				case 0:
					// GET /api/hosts
					req := httptest.NewRequest(http.MethodGet, "/api/hosts", nil)
					rec := httptest.NewRecorder()
					srv.mux.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						t.Errorf("GET /api/hosts returned %d", rec.Code)
					}
				case 1:
					// GET /api/summary
					req := httptest.NewRequest(http.MethodGet, "/api/summary", nil)
					rec := httptest.NewRecorder()
					srv.mux.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						t.Errorf("GET /api/summary returned %d", rec.Code)
					}
				case 2:
					// POST /api/cidrs
					body := fmt.Sprintf(`{"cidr":"172.16.%d.0/24","description":"VLAN %d"}`, workerID, workerID)
					req := httptest.NewRequest(http.MethodPost, "/api/cidrs", bytes.NewBufferString(body))
					rec := httptest.NewRecorder()
					srv.mux.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						t.Errorf("POST /api/cidrs returned %d", rec.Code)
					}
				case 3:
					// POST /api/exclusions
					body := fmt.Sprintf(`{"rule":"172.16.%d.50","reason":"Gateway %d"}`, workerID, workerID)
					req := httptest.NewRequest(http.MethodPost, "/api/exclusions", bytes.NewBufferString(body))
					rec := httptest.NewRecorder()
					srv.mux.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						t.Errorf("POST /api/exclusions returned %d", rec.Code)
					}
				case 4:
					// Explicit RebuildTargetList call
					coord.RebuildTargetList()
				}
			}
		}()
	}

	wg.Wait()
}

func TestPromoteHostStaticTarget(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()

	// Configure a CIDR
	_ = coord.store.AddOrUpdateCIDR(store.CIDRConfig{
		CIDR:        "192.168.10.0/24",
		Description: "Office Subnet",
		Enabled:     true,
	})
	coord.RebuildTargetList()

	// 1. Promote an IP within the configured CIDR
	req := httptest.NewRequest(http.MethodPost, "/api/hosts/192.168.10.25/promote", nil)
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 promoting host within CIDR, got %d", rec.Code)
	}

	// 2. Promote a standalone IP outside any configured CIDR
	req = httptest.NewRequest(http.MethodPost, "/api/hosts/8.8.4.4/promote", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 promoting standalone host, got %d", rec.Code)
	}

	// Trigger a rebuild to ensure pruning does not drop static hosts
	coord.RebuildTargetList()

	h1, ok1 := coord.pinger.GetHost("192.168.10.25")
	if !ok1 {
		t.Fatalf("expected 192.168.10.25 to still be monitored after rebuild")
	}
	if !h1.IsStatic {
		t.Errorf("expected 192.168.10.25 to be static")
	}
	if h1.CIDR != "192.168.10.0/24" {
		t.Errorf("expected 192.168.10.25 to have CIDR 192.168.10.0/24, got %s", h1.CIDR)
	}

	h2, ok2 := coord.pinger.GetHost("8.8.4.4")
	if !ok2 {
		t.Fatalf("expected 8.8.4.4 to still be monitored after rebuild")
	}
	if !h2.IsStatic {
		t.Errorf("expected 8.8.4.4 to be static")
	}
	if h2.CIDR != "8.8.4.4/32" {
		t.Errorf("expected 8.8.4.4 to have CIDR 8.8.4.4/32, got %s", h2.CIDR)
	}

	// Verify subnet matrix grouping for the promoted subnet host
	req = httptest.NewRequest(http.MethodGet, "/api/subnets/matrix", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for subnets matrix, got %d", rec.Code)
	}
	var matrix []struct {
		CIDR  string `json:"cidr"`
		Cells []struct {
			IP string `json:"ip"`
		} `json:"cells"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&matrix); err != nil {
		t.Fatalf("failed to decode matrix: %v", err)
	}

	foundInSubnet := false
	for _, block := range matrix {
		if block.CIDR == "192.168.10.0/24" {
			for _, cell := range block.Cells {
				if cell.IP == "192.168.10.25" {
					foundInSubnet = true
					break
				}
			}
		}
	}
	if !foundInSubnet {
		t.Errorf("expected 192.168.10.25 to be grouped in 192.168.10.0/24 matrix block")
	}
}

func TestCORSAndSecurityHeaders(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// 1. Same-host Origin
	req := httptest.NewRequest(http.MethodGet, "/api/summary", nil)
	req.Host = "monitor.corp.net:8080"
	req.Header.Set("Origin", "http://monitor.corp.net:8080")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 for matching host origin, got %d", rec.Code)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "http://monitor.corp.net:8080" {
		t.Errorf("expected matching allow origin, got %q", rec.Header().Get("Access-Control-Allow-Origin"))
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("expected X-Content-Type-Options: nosniff")
	}
	if rec.Header().Get("X-Frame-Options") != "SAMEORIGIN" {
		t.Errorf("expected X-Frame-Options: SAMEORIGIN")
	}

	// 2. Localhost Origin
	req = httptest.NewRequest(http.MethodGet, "/api/summary", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Errorf("expected allow for localhost origin, got %q", rec.Header().Get("Access-Control-Allow-Origin"))
	}

	// 3. Untrusted cross-origin preflight should be rejected with 403
	req = httptest.NewRequest(http.MethodOptions, "/api/settings", nil)
	req.Header.Set("Origin", "https://malicious-site.com")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for untrusted cross-origin preflight, got %d", rec.Code)
	}

	// 4. Untrusted cross-origin GET should not receive Access-Control-Allow-Origin
	req = httptest.NewRequest(http.MethodGet, "/api/hosts", nil)
	req.Header.Set("Origin", "https://malicious-site.com")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected empty Allow-Origin for untrusted origin, got %q", rec.Header().Get("Access-Control-Allow-Origin"))
	}

	// 5. Configured explicit allowed origins
	srv.SetAllowedOrigins([]string{"https://dashboard.internal.net"})
	req = httptest.NewRequest(http.MethodGet, "/api/summary", nil)
	req.Header.Set("Origin", "https://dashboard.internal.net")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://dashboard.internal.net" {
		t.Errorf("expected explicit allowed origin to be reflected, got %q", rec.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestSSEStreamShutdownConcurrentWithStop(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()

	coord.Start()

	// Launch multiple SSE client connections
	var wg sync.WaitGroup
	const numClients = 10
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/api/stream", nil)
			rec := httptest.NewRecorder()
			// Serve SSE stream
			srv.handleSSE(rec, req)
		}()
	}

	// Wait briefly for clients to register in sseClients
	time.Sleep(20 * time.Millisecond)

	// Broadcast an event to registered SSE clients
	coord.broadcastEvent("test_event", map[string]string{"foo": "bar"})

	// Stop the coordinator while SSE clients are active
	coord.Stop()

	// Wait for all client goroutines to exit without panic
	wg.Wait()
}

func TestSSEClientBufferOverflowDesync(t *testing.T) {
	_, coord, cleanup := setupTestServer(t)
	defer cleanup()

	clientChan := make(chan []byte, 5)
	coord.clientsMu.Lock()
	coord.sseClients[clientChan] = true
	coord.clientsMu.Unlock()

	// Fill the client's channel to capacity
	for i := 0; i < 5; i++ {
		coord.broadcastEvent("fill_event", map[string]int{"seq": i})
	}
	if len(clientChan) != 5 {
		t.Fatalf("expected channel to have 5 messages, got %d", len(clientChan))
	}

	// Next broadcast will trigger buffer overflow -> drain stale messages -> push desync event
	coord.broadcastEvent("overflow_event", map[string]string{"foo": "bar"})

	// Verify the channel now contains the desync event
	if len(clientChan) != 1 {
		t.Fatalf("expected 1 desync message in channel, got %d", len(clientChan))
	}

	msg := string(<-clientChan)
	if !strings.Contains(msg, "event: desync") || !strings.Contains(msg, "buffer_overflow") {
		t.Errorf("expected desync event payload, got %q", msg)
	}
}

func TestConcurrentBroadcastEventBufferOverflow(t *testing.T) {
	_, coord, cleanup := setupTestServer(t)
	defer cleanup()

	// Register 5 clients with tiny buffers
	const numClients = 5
	clients := make([]chan []byte, numClients)
	coord.clientsMu.Lock()
	for i := 0; i < numClients; i++ {
		clients[i] = make(chan []byte, 3)
		coord.sseClients[clients[i]] = true
	}
	coord.clientsMu.Unlock()

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Spawn 10 concurrent broadcasting goroutines hammering events
	for w := 0; w < 10; w++ {
		workerID := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			seq := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				seq++
				coord.broadcastEvent("concurrent_event", map[string]interface{}{
					"worker": workerID,
					"seq":    seq,
				})
			}
		}()
	}

	// Concurrently read from some clients slowly
	for i := 0; i < numClients; i++ {
		ch := clients[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-ch:
					if !ok {
						return
					}
					if len(msg) == 0 {
						t.Errorf("received empty message from SSE client channel")
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	wg.Wait()
}


func TestHostDetailOrActionInvalidIPValidation(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	invalidPaths := []string{
		"/api/hosts/not-an-ip",
		"/api/hosts/999.999.999.999",
		"/api/hosts/256.0.0.1",
		"/api/hosts/192.168.1.500/history",
		"/api/hosts/invalid-ip/ping",
		"/api/hosts/invalid-ip/enrollment",
		"/api/hosts/invalid-ip/promote",
		"/api/hosts/invalid-ip/meta",
	}

	for _, p := range invalidPaths {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		if strings.HasSuffix(p, "/ping") || strings.HasSuffix(p, "/promote") || strings.HasSuffix(p, "/meta") {
			req = httptest.NewRequest(http.MethodPost, p, bytes.NewBufferString(`{"alias":"bad"}`))
		} else if strings.HasSuffix(p, "/enrollment") {
			req = httptest.NewRequest(http.MethodDelete, p, nil)
		}
		rec := httptest.NewRecorder()
		srv.mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("path %s expected 400 Bad Request, got %d", p, rec.Code)
		}
	}
}

func TestRunDiscoveryEmptyTargets(t *testing.T) {
	_, coord, cleanup := setupTestServer(t)
	defer cleanup()

	// Disable all CIDRs so targets is completely empty
	cidrs := coord.store.GetCIDRs()
	for _, c := range cidrs {
		c.Enabled = false
		_ = coord.store.AddOrUpdateCIDR(c)
	}

	online, newDisc, err := coord.RunDiscovery("")
	if err != nil {
		t.Fatalf("unexpected error running discovery with empty targets: %v", err)
	}
	if online != 0 || newDisc != 0 {
		t.Errorf("expected 0 online, 0 newDisc, got %d, %d", online, newDisc)
	}

	status := coord.GetDiscoveryStatus()
	if status.IsScanning {
		t.Errorf("expected IsScanning to be false after empty discovery")
	}
	if status.LastScannedCount != 0 {
		t.Errorf("expected LastScannedCount 0, got %d", status.LastScannedCount)
	}
}

func TestRequestBodySizeLimit(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// 1. Valid payload under 1MB should succeed
	validPadding := strings.Repeat("a", 500*1024)
	validBody := fmt.Sprintf(`{"alias":"valid-host","notes":"%s"}`, validPadding)

	reqValid := httptest.NewRequest(http.MethodPost, "/api/hosts/127.0.0.1/meta", bytes.NewBufferString(validBody))
	recValid := httptest.NewRecorder()
	srv.ServeHTTP(recValid, reqValid)

	if recValid.Code != http.StatusOK {
		t.Errorf("expected 200 OK for valid 500KB payload, got %d", recValid.Code)
	}

	// 2. Create a payload larger than 1MB (1.5MB) which should be rejected
	largePadding := strings.Repeat("x", 1500*1024)
	largeBody := fmt.Sprintf(`{"alias":"bad","notes":"%s"}`, largePadding)

	req := httptest.NewRequest(http.MethodPost, "/api/hosts/127.0.0.1/meta", bytes.NewBufferString(largeBody))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413 Request Entity Too Large, got %d", rec.Code)
	}
}

func TestJSONDecodeErrorDetails(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Malformed JSON (syntax error)
	req := httptest.NewRequest(http.MethodPost, "/api/hosts/127.0.0.1/meta", bytes.NewBufferString(`{"alias": "broken",`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", rec.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !strings.HasPrefix(resp["error"], "Invalid JSON payload: ") {
		t.Errorf("expected error to contain detailed decode message, got %q", resp["error"])
	}
}

func TestHostsFilterPendingAndMatrixPendingCount(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()

	// Initially, configured hosts start with StatusPending
	req := httptest.NewRequest(http.MethodGet, "/api/hosts?status=pending", nil)
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var resp struct {
		Total int                  `json:"total"`
		Hosts []*pinger.HostState `json:"hosts"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Total == 0 {
		t.Errorf("expected pending hosts to be returned, got 0")
	}
	for _, h := range resp.Hosts {
		if h.Status != pinger.StatusPending {
			t.Errorf("expected all returned hosts to be StatusPending, got %s for %s", h.Status, h.IP)
		}
	}

	// Verify subnet matrix has pendingCount populated
	req = httptest.NewRequest(http.MethodGet, "/api/subnets/matrix", nil)
	rec = httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for subnets matrix, got %d", rec.Code)
	}

	var matrix []timeseries.SubnetMatrixBlock
	if err := json.NewDecoder(rec.Body).Decode(&matrix); err != nil {
		t.Fatalf("failed to decode matrix: %v", err)
	}

	totalPending := 0
	for _, block := range matrix {
		totalPending += block.PendingCount
	}
	if totalPending != len(coord.pinger.GetAllHosts()) {
		t.Errorf("expected total PendingCount %d in matrix, got %d", len(coord.pinger.GetAllHosts()), totalPending)
	}
}

func TestConcurrentRebuildAndStateChange(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()

	coord.Start()

	// Seed some initial hosts
	for i := 1; i <= 20; i++ {
		ip := fmt.Sprintf("192.168.1.%d", i)
		_ = coord.store.AddOrUpdateDiscoveredHost(store.DiscoveredHost{
			IP:             ip,
			CIDR:           "192.168.1.0/24",
			DiscoveredAt:   time.Now(),
			LastDiscovered: time.Now(),
		})
	}
	coord.RebuildTargetList()

	const numWorkers = 12
	const iterations = 30
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				ip := fmt.Sprintf("192.168.1.%d", (workerID+i)%20+1)
				switch (workerID + i) % 6 {
				case 0:
					// Simulate host transitioning to Down
					h, ok := coord.pinger.GetHost(ip)
					if ok {
						coord.handleStateChange(h, pinger.StatusUp, pinger.StatusDown)
					}
				case 1:
					// Simulate host transitioning to Up
					h, ok := coord.pinger.GetHost(ip)
					if ok {
						coord.handleStateChange(h, pinger.StatusDown, pinger.StatusUp)
					}
				case 2:
					// Trigger RebuildTargetList
					coord.RebuildTargetList()
				case 3:
					// Trigger Alert Acknowledge
					_ = coord.alerts.AcknowledgeAll("Operator", "Batch test ack")
				case 4:
					// Add/update exclusion and rebuild
					_ = coord.store.AddOrUpdateExclusion(store.ExclusionConfig{
						Rule:    ip,
						Reason:  "Flapping test",
						Enabled: i%2 == 0,
					})
					coord.RebuildTargetList()
				case 5:
					// Wake / TriggerSweep
					coord.pinger.TriggerSweep()
				}
			}
		}()
	}

	wg.Wait()
	_ = srv
}

func TestHostStateChangeAlertConsistency(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()
	_ = srv

	_ = coord.store.AddOrUpdateCIDR(store.CIDRConfig{
		CIDR:    "10.10.0.0/24",
		Enabled: true,
	})

	var hosts []store.DiscoveredHost
	now := time.Now()
	for i := 1; i <= 20; i++ {
		hosts = append(hosts, store.DiscoveredHost{
			IP:             fmt.Sprintf("10.10.0.%d", i),
			CIDR:           "10.10.0.0/24",
			DiscoveredAt:   now,
			LastDiscovered: now,
		})
	}
	_ = coord.store.AddOrUpdateDiscoveredHostsBatch(hosts)
	coord.RebuildTargetList()

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var inconsistencyCount int64

	// Concurrent writers simulating state transitions
	for w := 0; w < 4; w++ {
		workerID := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				i++
				ip := fmt.Sprintf("10.10.0.%d", (workerID+i)%20+1)
				if i%2 == 0 {
					// Simulate transition to Down
					h, ok := coord.pinger.GetHost(ip)
					if ok {
						coord.handleStateChange(h, pinger.StatusUp, pinger.StatusDown)
					}
				} else {
					// Simulate transition to Up
					h, ok := coord.pinger.GetHost(ip)
					if ok {
						coord.handleStateChange(h, pinger.StatusDown, pinger.StatusUp)
					}
				}
			}
		}()
	}

	// Concurrent readers inspecting state consistency
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				allHosts := coord.pinger.GetAllHosts()
				for _, h := range allHosts {
					if h.Status == pinger.StatusDown && !h.AlertActive {
						atomic.AddInt64(&inconsistencyCount, 1)
					}
					if h.Status == pinger.StatusUp && h.AlertActive {
						atomic.AddInt64(&inconsistencyCount, 1)
					}
				}

				summary := coord.pinger.GetSummary()
				if summary.DownCount > 0 && summary.AlertsActive == 0 {
					atomic.AddInt64(&inconsistencyCount, 1)
				}
			}
		}()
	}

	wg.Wait()

	if inconsistencyCount > 0 {
		t.Fatalf("Detected %d inconsistent host/alert state reads during concurrent state changes", inconsistencyCount)
	}
}

func TestCoordinatorStopPersistsPartialDiscovery(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()
	_ = srv

	_ = coord.store.AddOrUpdateCIDR(store.CIDRConfig{
		CIDR:    "127.0.0.0/24",
		Enabled: true,
	})

	coord.Start()

	// Trigger async discovery sweep
	coord.TriggerDiscovery("127.0.0.0/24")

	// Wait briefly for discovery to start and probe some hosts
	time.Sleep(50 * time.Millisecond)

	// Stop coordinator in the middle of discovery sweep
	coord.Stop()

	// Verify discovery scanning status is reset to false
	status := coord.GetDiscoveryStatus()
	if status.IsScanning {
		t.Errorf("expected IsScanning to be false after Stop(), got true")
	}

	// Verify store can be read and any discovered hosts were persisted
	discovered := coord.store.GetDiscoveredHosts()
	// Loopback 127.0.0.1 responds to ICMP and should be discovered
	if host, exists := discovered["127.0.0.1"]; exists {
		if host.IP != "127.0.0.1" {
			t.Errorf("unexpected host IP: %v", host.IP)
		}
	}
}

func TestAlertHistoryEndpointAndLifecycle(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()

	// Initial history should be empty
	req := httptest.NewRequest(http.MethodGet, "/api/alerts/history", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	var hist []map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&hist); err != nil {
		t.Fatalf("failed to decode alert history JSON: %v", err)
	}
	if len(hist) != 0 {
		t.Fatalf("expected 0 initial alert history, got %d", len(hist))
	}

	// Trigger alert by setting host 10.0.0.5 to DOWN
	h := &pinger.HostState{
		IP:     "10.0.0.5",
		CIDR:   "10.0.0.0/24",
		Status: pinger.StatusDown,
	}
	coord.handleStateChange(h, pinger.StatusUp, pinger.StatusDown)

	// Verify active alerts
	wActive := httptest.NewRecorder()
	srv.mux.ServeHTTP(wActive, httptest.NewRequest(http.MethodGet, "/api/alerts", nil))
	var active []map[string]interface{}
	_ = json.NewDecoder(wActive.Body).Decode(&active)
	if len(active) != 1 || active[0]["ip"] != "10.0.0.5" {
		t.Fatalf("expected 1 active alert for 10.0.0.5, got %+v", active)
	}

	// Resolve alert by setting host 10.0.0.5 back to UP
	hUp := &pinger.HostState{
		IP:     "10.0.0.5",
		CIDR:   "10.0.0.0/24",
		Status: pinger.StatusUp,
	}
	coord.handleStateChange(hUp, pinger.StatusDown, pinger.StatusUp)

	// Verify history now contains the resolved incident
	wHist := httptest.NewRecorder()
	srv.mux.ServeHTTP(wHist, httptest.NewRequest(http.MethodGet, "/api/alerts/history", nil))
	if wHist.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", wHist.Code)
	}
	if err := json.NewDecoder(wHist.Body).Decode(&hist); err != nil {
		t.Fatalf("failed to decode alert history JSON: %v", err)
	}
	if len(hist) != 1 {
		t.Fatalf("expected 1 resolved alert in history, got %d", len(hist))
	}
	if hist[0]["ip"] != "10.0.0.5" {
		t.Errorf("expected IP 10.0.0.5, got %v", hist[0]["ip"])
	}
	if hist[0]["state"] != "RESOLVED" {
		t.Errorf("expected state RESOLVED, got %v", hist[0]["state"])
	}
	if hist[0]["resolvedAt"] == nil {
		t.Errorf("expected non-nil resolvedAt")
	}

	// Method Not Allowed test
	wPost := httptest.NewRecorder()
	srv.mux.ServeHTTP(wPost, httptest.NewRequest(http.MethodPost, "/api/alerts/history", nil))
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed, got %d", wPost.Code)
	}
}










