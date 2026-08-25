package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dinis-store-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "data.json")
	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// 1. Verify default CIDRs
	cidrs := s.GetCIDRs()
	if len(cidrs) == 0 {
		t.Fatalf("expected default CIDRs, got none")
	}

	// 2. Add custom CIDR
	err = s.AddOrUpdateCIDR(CIDRConfig{
		CIDR:        "10.0.0.0/24",
		Description: "Office LAN",
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("failed to add CIDR: %v", err)
	}

	// 3. Add exclusion
	err = s.AddOrUpdateExclusion(ExclusionConfig{
		Rule:    "10.0.0.1",
		Reason:  "Default Gateway",
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("failed to add exclusion: %v", err)
	}

	// 4. Reload from disk to verify persistence
	s2, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to reload store: %v", err)
	}

	var foundCIDR, foundExcl bool
	for _, c := range s2.GetCIDRs() {
		if c.CIDR == "10.0.0.0/24" {
			foundCIDR = true
		}
	}
	for _, e := range s2.GetExclusions() {
		if e.Rule == "10.0.0.1" {
			foundExcl = true
		}
	}

	if !foundCIDR || !foundExcl {
		t.Fatalf("reloaded store missing data: cidr=%v, excl=%v", foundCIDR, foundExcl)
	}
}
