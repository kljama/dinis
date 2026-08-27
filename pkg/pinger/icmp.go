package pinger

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
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

type probeSocket struct {
	fd    int
	isRaw bool
}

// SingleProber performs individual ICMP echo requests using native Linux ICMP datagram/raw sockets,
// with graceful fallback to ping executable if needed. Sockets are pooled across probe requests
// to eliminate kernel sock_alloc / inode / FD churn under high probe concurrency.
type SingleProber struct {
	seq    uint64
	id     uint16
	pool   chan *probeSocket
	closed int32
}

// NewSingleProber creates a new prober with cryptographically random ID, sequence counter, and socket pool.
func NewSingleProber() *SingleProber {
	var buf [10]byte
	if _, err := crand.Read(buf[:]); err != nil {
		now := time.Now().UnixNano()
		binary.BigEndian.PutUint16(buf[0:2], uint16(now))
		binary.BigEndian.PutUint64(buf[2:10], uint64(now))
	}

	id := binary.BigEndian.Uint16(buf[0:2])
	if id == 0 {
		id = 1
	}
	seq := binary.BigEndian.Uint64(buf[2:10])

	return &SingleProber{
		id:   id,
		seq:  seq,
		pool: make(chan *probeSocket, 512),
	}
}

func (p *SingleProber) newSocket() (*probeSocket, error) {
	isRaw := false
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_ICMP)
	if err != nil {
		fd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_RAW, syscall.IPPROTO_ICMP)
		if err != nil {
			return nil, err
		}
		isRaw = true
	}
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, 65536)
	return &probeSocket{
		fd:    fd,
		isRaw: isRaw,
	}, nil
}

func (p *SingleProber) getSocket() (*probeSocket, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, fmt.Errorf("prober is closed")
	}

	select {
	case sock := <-p.pool:
		return sock, nil
	default:
		return p.newSocket()
	}
}

func (p *SingleProber) putSocket(sock *probeSocket, isFatal bool) {
	if sock == nil || sock.fd < 0 {
		return
	}
	if isFatal || atomic.LoadInt32(&p.closed) == 1 {
		_ = syscall.Close(sock.fd)
		return
	}

	select {
	case p.pool <- sock:
	default:
		// Pool is full, close excess socket
		_ = syscall.Close(sock.fd)
	}
}

// Close releases all pooled ICMP sockets.
func (p *SingleProber) Close() {
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		for {
			select {
			case sock := <-p.pool:
				if sock != nil && sock.fd >= 0 {
					_ = syscall.Close(sock.fd)
				}
			default:
				return
			}
		}
	}
}

func isFatalSocketError(err error) bool {
	if err == nil {
		return false
	}
	if errno, ok := err.(syscall.Errno); ok {
		return errno == syscall.EBADF || errno == syscall.ENOTSOCK || errno == syscall.EPIPE
	}
	return false
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
	if err == nil {
		return result
	}

	// Fallback to system ping only if native socket creation fails (e.g. restricted environment)
	return p.execProbe(ctx, ipStr, timeout)
}

func (p *SingleProber) nativeProbe(ip net.IP, timeout time.Duration) (PingResult, error) {
	sock, err := p.getSocket()
	if err != nil {
		return PingResult{}, err
	}
	isFatal := false
	defer func() {
		p.putSocket(sock, isFatal)
	}()

	tv := syscall.NsecToTimeval(timeout.Nanoseconds())
	_ = syscall.SetsockoptTimeval(sock.fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &tv)
	_ = syscall.SetsockoptTimeval(sock.fd, syscall.SOL_SOCKET, syscall.SO_SNDTIMEO, &tv)

	var dest [4]byte
	copy(dest[:], ip)
	sa := &syscall.SockaddrInet4{
		Port: 0,
		Addr: dest,
	}

	seq := atomic.AddUint64(&p.seq, 1)
	expectedID := uint16(p.id + uint16(seq>>16))
	expectedSeq := uint16(seq & 0xffff)
	probeToken := (uint64(p.id) << 48) | (seq & 0x0000FFFFFFFFFFFF)

	// Standard 64-byte ICMP packet (8 bytes header + 56 bytes payload) matching standard Linux ping.
	// Prevents firewalls / NAT middleboxes from dropping undersized packets.
	pkt := make([]byte, 64)
	pkt[0] = 8 // Echo Request
	pkt[1] = 0 // Code
	pkt[2] = 0 // Checksum placeholder
	pkt[3] = 0
	binary.BigEndian.PutUint16(pkt[4:6], expectedID)
	binary.BigEndian.PutUint16(pkt[6:8], expectedSeq)
	sendTime := time.Now()
	sendNano := uint64(sendTime.UnixNano())
	binary.BigEndian.PutUint64(pkt[8:16], sendNano)
	binary.BigEndian.PutUint64(pkt[16:24], probeToken)
	for i := 24; i < 64; i++ {
		pkt[i] = byte(i & 0xff)
	}

	cs := checksum(pkt)
	binary.BigEndian.PutUint16(pkt[2:4], cs)

	err = syscall.Sendto(sock.fd, pkt, 0, sa)
	if err != nil {
		if isFatalSocketError(err) {
			isFatal = true
		}
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
		n, from, err := syscall.Recvfrom(sock.fd, buf, 0)
		recvTime := time.Now()
		if err != nil {
			if isFatalSocketError(err) {
				isFatal = true
			}
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
		if sock.isRaw {
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

		// Validate source IP strictly matches the target we pinged.
		// Reject if from is nil, wrong type, or IP address does not match target.
		sa4, ok := from.(*syscall.SockaddrInet4)
		if !ok || !net.IP(sa4.Addr[:]).Equal(ip) {
			continue
		}

		// Validate ICMP identifier matches our probe ID.
		// Only checked for SOCK_RAW: with SOCK_DGRAM, the kernel rewrites
		// the identifier with its own socket-bound value and filters replies
		// by that value, so we cannot (and don't need to) match on expectedID.
		if sock.isRaw {
			replyID := binary.BigEndian.Uint16(icmpData[4:6])
			if replyID != expectedID {
				continue
			}
		}

		// Validate sequence number matches what we sent.
		replySeq := binary.BigEndian.Uint16(icmpData[6:8])
		if replySeq != expectedSeq {
			continue
		}

		// Validate payload timestamp and probe token.
		// On SOCK_DGRAM, the kernel rewrites the ICMP header ID field, but the
		// ICMP payload (timestamp and probe token) is preserved end-to-end.
		// Requiring matching timestamp and probe token prevents any cross-host
		// or cross-probe collision regardless of socket pooling or ID recycling.
		if len(icmpData) < 24 {
			continue
		}
		replyNano := binary.BigEndian.Uint64(icmpData[8:16])
		if replyNano != sendNano {
			continue
		}
		replyToken := binary.BigEndian.Uint64(icmpData[16:24])
		if replyToken != probeToken {
			continue
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
	// Defense-in-depth: validate ipStr is a valid IP to prevent command injection
	if net.ParseIP(ipStr) == nil {
		return PingResult{
			IP:        ipStr,
			Success:   false,
			Error:     "Invalid IP address",
			Timestamp: time.Now(),
		}
	}

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
