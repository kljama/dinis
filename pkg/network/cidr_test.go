package network

import (
	"fmt"
	"sync"
	"testing"
)

func TestParseCIDR(t *testing.T) {
	// Test single IP
	info, err := ParseCIDR("192.168.1.1", false)
	if err != nil {
		t.Fatalf("unexpected error for single IP: %v", err)
	}
	if info.TotalHosts != 1 || info.IPs[0] != "192.168.1.1" {
		t.Errorf("expected 1 host 192.168.1.1, got %v", info)
	}

	// Test /30 without net/broadcast
	info, err = ParseCIDR("192.168.1.0/30", false)
	if err != nil {
		t.Fatalf("unexpected error for /30: %v", err)
	}
	if info.TotalHosts != 2 {
		t.Errorf("expected 2 hosts for /30 without net/bcast, got %d", info.TotalHosts)
	}
	if info.IPs[0] != "192.168.1.1" || info.IPs[1] != "192.168.1.2" {
		t.Errorf("unexpected IPs: %v", info.IPs)
	}

	// Test /30 with net/broadcast
	info, err = ParseCIDR("192.168.1.0/30", true)
	if err != nil {
		t.Fatalf("unexpected error for /30 with net/bcast: %v", err)
	}
	if info.TotalHosts != 4 {
		t.Errorf("expected 4 hosts for /30 with net/bcast, got %d", info.TotalHosts)
	}

	// Test /31
	info, err = ParseCIDR("10.0.0.0/31", false)
	if err != nil {
		t.Fatalf("unexpected error for /31: %v", err)
	}
	if info.TotalHosts != 2 {
		t.Errorf("expected 2 hosts for /31, got %d", info.TotalHosts)
	}

	// Test /32
	info, err = ParseCIDR("10.0.0.5/32", false)
	if err != nil {
		t.Fatalf("unexpected error for /32: %v", err)
	}
	if info.TotalHosts != 1 || info.IPs[0] != "10.0.0.5" {
		t.Errorf("expected 1 host for /32, got %v", info)
	}

	// Test boundary condition: subnet ending at 255.255.255.255 (0xFFFFFFFF)
	info, err = ParseCIDR("255.255.255.240/28", true)
	if err != nil {
		t.Fatalf("unexpected error for 255.255.255.240/28: %v", err)
	}
	if info.TotalHosts != 16 {
		t.Errorf("expected 16 hosts for boundary /28, got %d", info.TotalHosts)
	}
	if len(info.IPs) != 16 || info.IPs[15] != "255.255.255.255" {
		t.Errorf("expected last IP to be 255.255.255.255, got %v", info.IPs)
	}
}

func TestExclusionMatcher(t *testing.T) {
	matcher := NewExclusionMatcher()
	if err := matcher.AddExclusion("192.168.1.50", "Gateway server"); err != nil {
		t.Fatalf("error adding exact IP: %v", err)
	}
	if err := matcher.AddExclusion("10.0.0.0/24", "Maintenance range"); err != nil {
		t.Fatalf("error adding CIDR: %v", err)
	}

	// Test exact IP match
	matched, rule, reason := matcher.Matches("192.168.1.50")
	if !matched || reason != "Gateway server" {
		t.Errorf("expected match for 192.168.1.50, got %v, %s, %s", matched, rule, reason)
	}

	// Test non-match in same /24
	matched, _, _ = matcher.Matches("192.168.1.51")
	if matched {
		t.Errorf("expected no match for 192.168.1.51")
	}

	// Test subnet match
	matched, rule, reason = matcher.Matches("10.0.0.42")
	if !matched || rule != "10.0.0.0/24" || reason != "Maintenance range" {
		t.Errorf("expected subnet match for 10.0.0.42, got %v, %s, %s", matched, rule, reason)
	}

	// Test non-match outside subnet
	matched, _, _ = matcher.Matches("10.0.1.42")
	if matched {
		t.Errorf("expected no match for 10.0.1.42")
	}

	// Test normalized IPv4-mapped IPv6 matching exact IPv4 rule
	matched, rule, reason = matcher.Matches("::ffff:192.168.1.50")
	if !matched || reason != "Gateway server" || rule != "192.168.1.50" {
		t.Errorf("expected normalized match for ::ffff:192.168.1.50, got %v, %s, %s", matched, rule, reason)
	}

	// Test IPv4-mapped IPv6 exclusion rule matching plain IPv4 query
	if err := matcher.AddExclusion("::ffff:172.16.0.1", "Mapped rule"); err != nil {
		t.Fatalf("error adding mapped rule: %v", err)
	}
	matched, rule, reason = matcher.Matches("172.16.0.1")
	if !matched || reason != "Mapped rule" || rule != "172.16.0.1" {
		t.Errorf("expected match for 172.16.0.1 against mapped rule, got %v, %s, %s", matched, rule, reason)
	}
}

func TestExclusionMatcherConcurrent(t *testing.T) {
	matcher := NewExclusionMatcher()

	const numWorkers = 20
	const iterations = 50
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				ip := fmt.Sprintf("10.%d.%d.1", workerID, i)
				cidr := fmt.Sprintf("172.%d.%d.0/24", workerID, i)

				if i%2 == 0 {
					_ = matcher.AddExclusion(ip, "Exact rule")
				} else {
					_ = matcher.AddExclusion(cidr, "Subnet rule")
				}

				// Concurrent query while mutations are happening
				matcher.Matches(ip)
				matcher.Matches(fmt.Sprintf("172.%d.%d.5", workerID, i))
				matcher.Matches("192.168.1.1")
			}
		}()
	}

	wg.Wait()
}
