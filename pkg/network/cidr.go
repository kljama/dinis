package network

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
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
	total := int(broadcast - start + 1)

	// Safety check to prevent allocating millions of IPs at once
	if total > 65536 {
		return nil, fmt.Errorf("CIDR range contains %d hosts; maximum allowed in a single block is 65,536 (/16)", total)
	}

	var ips []string
	if ones == 32 {
		ips = append(ips, ip.String())
	} else if ones == 31 {
		ips = append(ips, intToIP(start).String(), intToIP(broadcast).String())
	} else {
		// For /30 or larger
		first := start
		last := broadcast
		if !includeNetAndBcast {
			first = start + 1
			last = broadcast - 1
		}
		if first <= last {
			for cur := first; cur <= last; cur++ {
				ips = append(ips, intToIP(cur).String())
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

// ExclusionMatcher checks whether a given IP matches any exclusion rules.
type ExclusionMatcher struct {
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
		m.exactIPs[ip.String()] = reason
		return nil
	}

	_, ipNet, err := net.ParseCIDR(rule)
	if err != nil {
		return fmt.Errorf("invalid exclusion CIDR: %w", err)
	}

	m.subnets = append(m.subnets, subnetExclusion{
		net:    ipNet,
		reason: reason,
		raw:    rule,
	})
	return nil
}

// Matches checks if the given IP string matches any exclusion.
// Returns matched bool, matchedRule, and reason.
func (m *ExclusionMatcher) Matches(ipStr string) (bool, string, string) {
	if reason, ok := m.exactIPs[ipStr]; ok {
		return true, ipStr, reason
	}

	parsed := net.ParseIP(ipStr)
	if parsed == nil {
		return false, "", ""
	}

	for _, sub := range m.subnets {
		if sub.net.Contains(parsed) {
			return true, sub.raw, sub.reason
		}
	}

	return false, "", ""
}
