package network

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
)

// CIDRInfo contains parsed details about a CIDR block.
type CIDRInfo struct {
	CIDR       string   `json:"cidr"`
	Network    string   `json:"network"`
	Mask       string   `json:"mask"`
	PrefixLen  int      `json:"prefixLen"`
	TotalHosts int      `json:"totalHosts"`
	IPs        []string `json:"ips"`
}

// ParseCIDR parses an IP or CIDR string (e.g., "192.168.1.0/24" or "8.8.8.8")
// and returns the list of host IP strings and metadata.
func ParseCIDR(input string, includeNetAndBcast bool) (*CIDRInfo, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("empty CIDR or IP input")
	}

	// If no slash, treat as /32 (single IP)
	if !strings.Contains(input, "/") {
		ip := net.ParseIP(input)
		if ip == nil {
			return nil, fmt.Errorf("invalid IP address: %s", input)
		}
		if ip.To4() != nil {
			input += "/32"
		} else {
			input += "/128"
		}
	}

	ip, ipNet, err := net.ParseCIDR(input)
	if err != nil {
		return nil, fmt.Errorf("invalid CIDR %q: %w", input, err)
	}

	ones, bits := ipNet.Mask.Size()
	if bits != 32 {
		// For IPv6, support /128 single host or small ranges
		if ones == 128 {
			return &CIDRInfo{
				CIDR:       input,
				Network:    ipNet.IP.String(),
				Mask:       net.IP(ipNet.Mask).String(),
				PrefixLen:  ones,
				TotalHosts: 1,
				IPs:        []string{ip.String()},
			}, nil
		}
		return nil, fmt.Errorf("IPv6 ranges larger than /128 are currently not expanded to avoid memory exhaustion")
	}

	// IPv4 expansion
	ip4 := ipNet.IP.To4()
	if ip4 == nil {
		return nil, fmt.Errorf("invalid IPv4 address")
	}

	start := binary.BigEndian.Uint32(ip4)
	mask := binary.BigEndian.Uint32(net.IP(ipNet.Mask).To4())
	broadcast := start | ^mask
	total := uint64(broadcast) - uint64(start) + 1

	// Safety check to prevent allocating millions of IPs at once
	if total > 65536 {
		return nil, fmt.Errorf("CIDR range contains %d hosts; maximum allowed in a single block is 65,536 (/16)", total)
	}

	var ips []string
	switch ones {
	case 32:
		ips = append(ips, ip.String())
	case 31:
		ips = append(ips, intToIP(start).String(), intToIP(broadcast).String())
	default:
		// For /30 or larger
		first := start
		last := broadcast
		if !includeNetAndBcast {
			first = start + 1
			last = broadcast - 1
		}
		if first <= last {
			count := int(last - first + 1)
			ips = make([]string, 0, count)
			for i := 0; i < count; i++ {
				ips = append(ips, intToIP(first+uint32(i)).String())
			}
		}
	}

	return &CIDRInfo{
		CIDR:       ipNet.String(),
		Network:    ipNet.IP.String(),
		Mask:       net.IP(ipNet.Mask).String(),
		PrefixLen:  ones,
		TotalHosts: len(ips),
		IPs:        ips,
	}, nil
}

func intToIP(n uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, n)
	return ip
}

// ExclusionMatcher checks whether a given IP matches any exclusion rules in a thread-safe manner.
type ExclusionMatcher struct {
	mu       sync.RWMutex
	exactIPs map[string]string // ip -> reason
	subnets  []subnetExclusion
}

type subnetExclusion struct {
	net    *net.IPNet
	reason string
	raw    string
}

// NewExclusionMatcher creates a new matcher from a list of IP/CIDR exclusion rules.
func NewExclusionMatcher() *ExclusionMatcher {
	return &ExclusionMatcher{
		exactIPs: make(map[string]string),
		subnets:  make([]subnetExclusion, 0),
	}
}

// AddExclusion adds an IP or CIDR rule with an optional note/reason.
func (m *ExclusionMatcher) AddExclusion(rule string, reason string) error {
	rule = strings.TrimSpace(rule)
	if rule == "" {
		return nil
	}

	if !strings.Contains(rule, "/") {
		ip := net.ParseIP(rule)
		if ip == nil {
			return fmt.Errorf("invalid IP address: %s", rule)
		}
		if v4 := ip.To4(); v4 != nil {
			ip = v4
		}
		m.mu.Lock()
		m.exactIPs[ip.String()] = reason
		m.mu.Unlock()
		return nil
	}

	_, ipNet, err := net.ParseCIDR(rule)
	if err != nil {
		return fmt.Errorf("invalid exclusion CIDR: %w", err)
	}

	m.mu.Lock()
	m.subnets = append(m.subnets, subnetExclusion{
		net:    ipNet,
		reason: reason,
		raw:    rule,
	})
	m.mu.Unlock()
	return nil
}

// Matches checks if the given IP string matches any exclusion.
// Returns matched bool, matchedRule, and reason.
func (m *ExclusionMatcher) Matches(ipStr string) (bool, string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Fast path: exact raw string match
	if reason, ok := m.exactIPs[ipStr]; ok {
		return true, ipStr, reason
	}

	parsed := net.ParseIP(ipStr)
	if parsed == nil {
		return false, "", ""
	}

	// Normalized exact match (handles non-canonical formatting & IPv4-mapped IPv6)
	normIP := parsed.String()
	if v4 := parsed.To4(); v4 != nil {
		normIP = v4.String()
	}

	if reason, ok := m.exactIPs[normIP]; ok {
		return true, normIP, reason
	}

	for _, sub := range m.subnets {
		if sub.net.Contains(parsed) {
			return true, sub.raw, sub.reason
		}
	}

	return false, "", ""
}
