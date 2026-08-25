package pinger

import (
	"context"
	"encoding/binary"
	"math/rand"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func checksum(b []byte) uint16 {
	var sum uint32
	for i := 0; i < len(b)-1; i += 2 {
		sum += uint32(binary.BigEndian.Uint16(b[i : i+2]))
	}
	if len(b)%2 == 1 {
		sum += uint32(b[len(b)-1]) << 8
	}
	for sum > 0xffff {
		sum = (sum >> 16) + (sum & 0xffff)
	}
	return ^uint16(sum)
}

// PingResult represents the outcome of a single ICMP echo probe.
type PingResult struct {
	IP        string
	Success   bool
	Latency   time.Duration
	LatencyMs float64
	Error     string
	Timestamp time.Time
}

// SingleProber performs individual ICMP echo requests using native Linux ICMP datagram/raw sockets,
// with graceful fallback to ping executable if needed.
type SingleProber struct {
	seq uint32
	id  uint16
	mu  sync.Mutex
}

// NewSingleProber creates a new prober.
func NewSingleProber() *SingleProber {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &SingleProber{
		id: uint16(r.Intn(65535)),
	}
}

// Probe probes an IP address once with the specified timeout.
func (p *SingleProber) Probe(ctx context.Context, ipStr string, timeout time.Duration) PingResult {
	parsedIP := net.ParseIP(ipStr).To4()
	if parsedIP == nil {
		return PingResult{
			IP:        ipStr,
			Success:   false,
			Error:     "invalid IPv4 address",
			Timestamp: time.Now(),
		}
	}

	result, err := p.nativeProbe(parsedIP, timeout)
	if err == nil && result.Success {
		return result
	}

	// If native probe timed out, retry once after a short 25ms backoff to eliminate single-packet jitter
	if ctx.Err() == nil {
		time.Sleep(25 * time.Millisecond)
		retryRes, retryErr := p.nativeProbe(parsedIP, timeout)
		if retryErr == nil && retryRes.Success {
			return retryRes
		}

		// Fallback to system ping as authoritative verification
		execRes := p.execProbe(ctx, ipStr, timeout)
		if execRes.Success {
			return execRes
		}
	}

	if err == nil {
		return result
	}

	// Fallback to system ping if native socket fails (e.g. restricted environment)
	return p.execProbe(ctx, ipStr, timeout)
}

func (p *SingleProber) nativeProbe(ip net.IP, timeout time.Duration) (PingResult, error) {
	// Try SOCK_DGRAM (unprivileged) first, then SOCK_RAW.
	// We must track which type succeeded because SOCK_RAW includes the IP
	// header in received packets while SOCK_DGRAM strips it.
	isRaw := false
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_ICMP)
	if err != nil {
		fd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_RAW, syscall.IPPROTO_ICMP)
		if err != nil {
			return PingResult{}, err
		}
		isRaw = true
	}
	defer syscall.Close(fd)

	tv := syscall.NsecToTimeval(timeout.Nanoseconds())
	_ = syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &tv)
	_ = syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_SNDTIMEO, &tv)
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, 65536)

	var dest [4]byte
	copy(dest[:], ip)
	sa := &syscall.SockaddrInet4{
		Port: 0,
		Addr: dest,
	}

	seq := atomic.AddUint32(&p.seq, 1)
	expectedSeq := uint16(seq)

	// Standard 64-byte ICMP packet (8 bytes header + 56 bytes payload) matching standard Linux ping.
	// Prevents firewalls / NAT middleboxes from dropping undersized packets.
	pkt := make([]byte, 64)
	pkt[0] = 8 // Echo Request
	pkt[1] = 0 // Code
	pkt[2] = 0 // Checksum placeholder
	pkt[3] = 0
	binary.BigEndian.PutUint16(pkt[4:6], p.id)
	binary.BigEndian.PutUint16(pkt[6:8], expectedSeq)
	sendTime := time.Now()
	binary.BigEndian.PutUint64(pkt[8:16], uint64(sendTime.UnixNano()))
	for i := 16; i < 64; i++ {
		pkt[i] = byte(i & 0xff)
	}

	cs := checksum(pkt)
	binary.BigEndian.PutUint16(pkt[2:4], cs)

	err = syscall.Sendto(fd, pkt, 0, sa)
	if err != nil {
		return PingResult{
			IP:        ip.String(),
			Success:   false,
			Error:     err.Error(),
			Timestamp: time.Now(),
		}, nil
	}

	// Read replies in a loop, skipping packets that don't match our probe.
	// The socket receive timeout bounds this loop so it cannot spin forever.
	buf := make([]byte, 512)
	for {
		n, from, err := syscall.Recvfrom(fd, buf, 0)
		recvTime := time.Now()
		if err != nil {
			return PingResult{
				IP:        ip.String(),
				Success:   false,
				Error:     "Request timeout",
				Timestamp: recvTime,
			}, nil
		}

		// Determine where the ICMP header starts in the receive buffer.
		// SOCK_RAW: buf = [IP header][ICMP data...], IHL field gives IP header length.
		// SOCK_DGRAM: buf = [ICMP data...], kernel strips the IP header.
		icmpOffset := 0
		if isRaw {
			if n < 20 {
				continue // Too short for an IP header
			}
			ihl := int(buf[0]&0x0f) * 4 // IP Header Length in bytes
			if ihl < 20 || n < ihl+8 {
				continue // Malformed or too short for ICMP header after IP
			}
			icmpOffset = ihl
		} else {
			if n < 8 {
				continue // Too short for ICMP header
			}
		}

		icmpData := buf[icmpOffset:n]

		// Validate ICMP type: must be Echo Reply (type 0, code 0).
		// Immediately return failure for Destination Unreachable (type 3) or Time Exceeded (type 11)
		if icmpData[0] != 0 {
			if icmpData[0] == 3 {
				return PingResult{
					IP:        ip.String(),
					Success:   false,
					Error:     "Destination Unreachable",
					Timestamp: recvTime,
				}, nil
			}
			if icmpData[0] == 11 {
				return PingResult{
					IP:        ip.String(),
					Success:   false,
					Error:     "Time Exceeded",
					Timestamp: recvTime,
				}, nil
			}
			continue
		}

		// Validate ICMP identifier matches our prober ID.
		// Only checked for SOCK_RAW: with SOCK_DGRAM, the kernel rewrites
		// the identifier with its own socket-bound value and filters replies
		// by that value, so we cannot (and don't need to) match on p.id.
		if isRaw {
			replyID := binary.BigEndian.Uint16(icmpData[4:6])
			if replyID != p.id {
				continue
			}
		}

		// Validate sequence number matches what we sent.
		replySeq := binary.BigEndian.Uint16(icmpData[6:8])
		if replySeq != expectedSeq {
			continue
		}

		// Validate source IP matches the target we pinged.
		if from != nil {
			if sa4, ok := from.(*syscall.SockaddrInet4); ok {
				if !net.IP(sa4.Addr[:]).Equal(ip) {
					continue
				}
			}
		}

		rtt := recvTime.Sub(sendTime)
		rttMs := float64(rtt.Microseconds()) / 1000.0
		if rttMs < 0.01 {
			rttMs = 0.01
		}

		return PingResult{
			IP:        ip.String(),
			Success:   true,
			Latency:   rtt,
			LatencyMs: rttMs,
			Timestamp: recvTime,
		}, nil
	}
}

func (p *SingleProber) execProbe(ctx context.Context, ipStr string, timeout time.Duration) PingResult {
	timeoutSec := int(timeout.Seconds())
	if timeoutSec < 1 {
		timeoutSec = 1
	}

	cmdCtx, cancel := context.WithTimeout(ctx, timeout+500*time.Millisecond)
	defer cancel()

	start := time.Now()
	out, err := exec.CommandContext(cmdCtx, "ping", "-c", "1", "-W", strconv.Itoa(timeoutSec), ipStr).CombinedOutput()
	rtt := time.Since(start)

	if err != nil {
		return PingResult{
			IP:        ipStr,
			Success:   false,
			Error:     "Request timeout / Unreachable",
			Timestamp: time.Now(),
		}
	}

	// Parse RTT from output if available: e.g. "time=1.23 ms"
	outStr := string(out)
	rttMs := float64(rtt.Microseconds()) / 1000.0
	if idx := strings.Index(outStr, "time="); idx != -1 {
		part := outStr[idx+5:]
		if end := strings.Index(part, " "); end != -1 {
			if parsed, pErr := strconv.ParseFloat(part[:end], 64); pErr == nil {
				rttMs = parsed
			}
		}
	}

	return PingResult{
		IP:        ipStr,
		Success:   true,
		Latency:   time.Duration(rttMs * float64(time.Millisecond)),
		LatencyMs: rttMs,
		Timestamp: time.Now(),
	}
}
