package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dinis/pkg/store"
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
