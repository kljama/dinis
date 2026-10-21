package pinger

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
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

// Socket modes of a prober (see SingleProber.Mode).
const (
	ModePingSocket  = "ping_socket"  // unprivileged ICMP datagram socket
	ModeRawSocket   = "raw_socket"   // raw ICMP socket (CAP_NET_RAW)
	ModePingCommand = "ping_command" // no ICMP socket permitted: the ping command for each probe
)

// readSlice bounds each blocking read of the reader goroutine, so that it notices Close.
const readSlice = 100 * time.Millisecond

// rcvBufBytes is the receive buffer of the shared socket. Without CAP_NET_ADMIN the kernel
// caps it at twice net.core.rmem_max (416 KiB by default), still hundreds of replies.
const rcvBufBytes = 1 << 20

// Linux constants that package syscall does not define.
const (
	solRaw     = 255 // SOL_RAW
	icmpFilter = 1   // ICMP_FILTER (linux/icmp.h): a set bit in the mask blocks that ICMP type

	icmpFilterMask = ^int32(1<<0 | 1<<3 | 1<<11)

	soEEOriginICMP = 2 // SO_EE_ORIGIN_ICMP (linux/errqueue.h)
)

// socketTypes are the ICMP socket types a prober tries, in this order. Tests change it to
// force raw sockets.
var socketTypes = []int{syscall.SOCK_DGRAM, syscall.SOCK_RAW}

// quotedEcho parses an ICMP error message (Destination Unreachable / Time Exceeded) that
// quotes an echo request. icmpData starts at the ICMP header of the error message. It returns
// the destination, identifier and sequence number of the quoted request.
func quotedEcho(icmpData []byte) (dst net.IP, id, seq uint16, ok bool) {
	const errHdrLen = 8
	if len(icmpData) < errHdrLen+20 {
		return nil, 0, 0, false
	}
	quotedIP := icmpData[errHdrLen:]
	if quotedIP[0]>>4 != 4 {
		return nil, 0, 0, false
	}
	ihl := int(quotedIP[0]&0x0f) * 4
	if ihl < 20 || len(quotedIP) < ihl+8 {
		return nil, 0, 0, false
	}
	if quotedIP[9] != syscall.IPPROTO_ICMP {
		return nil, 0, 0, false
	}
	quotedICMP := quotedIP[ihl:]
	if quotedICMP[0] != 8 {
		return nil, 0, 0, false
	}
	return net.IP(quotedIP[16:20]), binary.BigEndian.Uint16(quotedICMP[4:6]), binary.BigEndian.Uint16(quotedICMP[6:8]), true
}

// icmpErrorText describes an ICMP error message for the probe result.
func icmpErrorText(icmpType, code uint8) string {
	if icmpType == 11 {
		return "Time Exceeded"
	}
	if icmpType != 3 {
		return "ICMP error type " + strconv.Itoa(int(icmpType))
	}
	switch code {
	case 0:
		return "Destination Unreachable (network unreachable)"
	case 1:
		return "Destination Unreachable (host unreachable)"
	case 9, 10, 13:
		return "Destination Unreachable (administratively prohibited)"
	}
	return "Destination Unreachable (code " + strconv.Itoa(int(code)) + ")"
}

// PingResult represents the outcome of a single ICMP echo probe.
type PingResult struct {
	IP        string
	Success   bool
	Latency   time.Duration
	LatencyMs float64
	Error     string
	Timestamp time.Time
	// LocalError marks a probe that failed because of this machine, for example no buffer
	// space or a local firewall rule. It says nothing about the target.
	LocalError bool `json:"-"`
}

// proberCounters survive a reopened socket.
type proberCounters struct {
	lateReplies atomic.Uint64
}

// ProberStats is a snapshot of a prober.
type ProberStats struct {
	Mode        string // empty before the first probe
	LateReplies uint64 // replies that arrived after their probe timed out
	RxDropped   uint64 // packets the kernel dropped because the socket buffer was full
}

// errNoICMPSocket: neither a ping socket nor a raw socket is permitted.
var errNoICMPSocket = errors.New("no ICMP socket permitted")

// SingleProber sends ICMP echo requests. All probes share one ICMP socket: an unprivileged
// ping socket if net.ipv4.ping_group_range allows it, otherwise a raw socket (CAP_NET_RAW).
// One reader goroutine receives the replies, takes the kernel receive time of each and hands
// it to the waiting probe, so a waiting probe holds no OS thread. If no ICMP socket is
// permitted, each probe runs the system ping command.
type SingleProber struct {
	counters proberCounters

	mu          sync.Mutex
	conn        *icmpConn // nil before the first probe and after Close
	command     bool      // no ICMP socket permitted: use the ping command
	closedDrops uint64    // kernel drops of sockets closed before conn
}

// NewSingleProber creates a prober. Its socket is opened by the first probe.
func NewSingleProber() *SingleProber {
	return &SingleProber{}
}

// connection returns the open socket, opening one if needed. It returns errNoICMPSocket if
// no ICMP socket is permitted, or another error if opening failed for now (for example no
// free file descriptor).
func (p *SingleProber) connection() (*icmpConn, error) {
	p.mu.Lock()
	if p.conn != nil && !p.conn.broken.Load() {
		c := p.conn
		p.mu.Unlock()
		return c, nil
	}
	if p.command {
		p.mu.Unlock()
		return nil, errNoICMPSocket
	}
	old := p.conn
	p.conn = nil
	if old != nil {
		p.closedDrops += old.drops.Load()
	}
	c, err := openICMPConn(&p.counters)
	if err != nil && isPermissionError(err) {
		p.command = true
		err = errNoICMPSocket
	}
	p.conn = c
	p.mu.Unlock()
	if old != nil {
		go old.close() // waits for its reader, so not under p.mu
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

func isPermissionError(err error) bool {
	switch err {
	case syscall.EACCES, syscall.EPERM, syscall.EPROTONOSUPPORT, syscall.EAFNOSUPPORT, syscall.ESOCKTNOSUPPORT:
		return true
	}
	return false
}

// Mode returns how the prober sends probes (ModePingSocket, ModeRawSocket or
// ModePingCommand), opening its socket if needed. It is empty if no socket could be opened
// for now.
func (p *SingleProber) Mode() string {
	c, err := p.connection()
	switch {
	case err == nil:
		return c.mode()
	case errors.Is(err, errNoICMPSocket):
		return ModePingCommand
	}
	return ""
}

// Stats returns a snapshot of the prober counters.
func (p *SingleProber) Stats() ProberStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := ProberStats{LateReplies: p.counters.lateReplies.Load(), RxDropped: p.closedDrops}
	switch {
	case p.conn != nil:
		s.Mode = p.conn.mode()
		s.RxDropped += p.conn.drops.Load()
	case p.command:
		s.Mode = ModePingCommand
	}
	return s
}

// Close closes the socket. Probes still waiting end with a local error. A later probe opens
// a new socket.
func (p *SingleProber) Close() {
	p.mu.Lock()
	c := p.conn
	p.conn = nil
	if c != nil {
		p.closedDrops += c.drops.Load()
	}
	p.mu.Unlock()
	if c != nil {
		c.close()
	}
}

// Probe probes an IP address once with the specified timeout.
func (p *SingleProber) Probe(ctx context.Context, ipStr string, timeout time.Duration) PingResult {
	if ctx == nil {
		ctx = context.Background()
	}
	ip := net.ParseIP(ipStr).To4()
	if ip == nil {
		return PingResult{IP: ipStr, Error: "invalid IPv4 address", Timestamp: time.Now()}
	}
	if err := ctx.Err(); err != nil {
		return PingResult{IP: ip.String(), Error: err.Error(), Timestamp: time.Now()}
	}
	c, err := p.connection()
	if errors.Is(err, errNoICMPSocket) {
		return p.execProbe(ctx, ipStr, timeout)
	}
	if err != nil {
		return PingResult{IP: ip.String(), Error: "cannot open ICMP socket: " + err.Error(), Timestamp: time.Now(), LocalError: true}
	}
	return c.probe(ctx, ip, timeout)
}

// icmpConn is an ICMP socket shared by all probes of a prober.
type icmpConn struct {
	fd       int
	raw      bool
	id       uint16 // identifier of our echo requests on a raw socket (a ping socket sets its own)
	token    uint64 // random marker in the payload of our echo requests
	counters *proberCounters

	mu      sync.Mutex
	pending map[uint16]*probeWaiter // by sequence number
	nextSeq uint16
	sent    uint64 // probes sent, numbers each probe

	fdMu    sync.RWMutex // held for reading while sending; Close takes it before closing fd
	broken  atomic.Bool  // the socket failed: the prober opens a new one
	closing atomic.Bool
	done    chan struct{} // closed when the reader exits
	drops   atomic.Uint64 // kernel drop count of this socket (SO_RXQ_OVFL)
}

// probeWaiter is a probe waiting for its reply.
type probeWaiter struct {
	target [4]byte
	num    uint64          // probe number, also in the payload
	result chan probeReply // buffered: the reader never blocks
}

// probeReply is what the reader found for a probe.
type probeReply struct {
	err      string    // empty for an echo reply
	local    bool      // err is a problem of this machine
	kernelRx int64     // kernel receive time (Unix ns), 0 if unknown
	userRx   time.Time // when the reader received the packet
}

func newICMPConn(fd int, raw bool, counters *proberCounters) *icmpConn {
	var b [10]byte
	if _, err := crand.Read(b[:]); err != nil {
		now := uint64(time.Now().UnixNano())
		binary.BigEndian.PutUint16(b[0:2], uint16(now))
		binary.BigEndian.PutUint64(b[2:10], now)
	}
	c := &icmpConn{
		fd:       fd,
		raw:      raw,
		id:       binary.BigEndian.Uint16(b[0:2]),
		token:    binary.BigEndian.Uint64(b[2:10]),
		counters: counters,
		pending:  make(map[uint16]*probeWaiter),
		done:     make(chan struct{}),
	}
	if c.id == 0 {
		c.id = 1
	}
	return c
}

// openICMPConn opens the shared socket and starts its reader.
func openICMPConn(counters *proberCounters) (*icmpConn, error) {
	fd, err := -1, error(nil)
	raw := false
	for _, typ := range socketTypes {
		fd, err = syscall.Socket(syscall.AF_INET, typ|syscall.SOCK_CLOEXEC, syscall.IPPROTO_ICMP)
		if err == nil {
			raw = typ == syscall.SOCK_RAW
			break
		}
	}
	if err != nil {
		return nil, err
	}
	// The options are best effort: without one, probes still work, only that feature is missing.
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_TIMESTAMPNS, 1)
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RXQ_OVFL, 1)
	if syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUFFORCE, rcvBufBytes) != nil {
		_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, rcvBufBytes)
	}
	tv := syscall.NsecToTimeval(readSlice.Nanoseconds())
	_ = syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &tv)
	if raw {
		// A raw socket receives a copy of every inbound ICMP packet; let the kernel drop
		// everything except Echo Reply, Destination Unreachable and Time Exceeded.
		_ = syscall.SetsockoptInt(fd, solRaw, icmpFilter, int(icmpFilterMask))
	} else {
		// A ping socket reports ICMP errors for its echo requests only through its error queue
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_RECVERR, 1)
	}
	c := newICMPConn(fd, raw, counters)
	go c.readLoop()
	return c, nil
}

func (c *icmpConn) mode() string {
	if c.raw {
		return ModeRawSocket
	}
	return ModePingSocket
}

// register gives w a free sequence number and a probe number.
func (c *icmpConn) register(w *probeWaiter) (uint16, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() || len(c.pending) >= 1<<16 {
		return 0, false
	}
	for {
		c.nextSeq++
		if _, busy := c.pending[c.nextSeq]; !busy {
			break
		}
	}
	c.sent++
	w.num = c.sent
	c.pending[c.nextSeq] = w
	return c.nextSeq, true
}

func (c *icmpConn) unregister(seq uint16, w *probeWaiter) {
	c.mu.Lock()
	if c.pending[seq] == w {
		delete(c.pending, seq)
	}
	c.mu.Unlock()
}

// deliver hands r to the probe waiting with sequence number seq if match accepts it.
func (c *icmpConn) deliver(seq uint16, match func(*probeWaiter) bool, r probeReply) bool {
	c.mu.Lock()
	w := c.pending[seq]
	if w == nil || !match(w) {
		c.mu.Unlock()
		return false
	}
	delete(c.pending, seq)
	c.mu.Unlock()
	w.result <- r
	return true
}

// failPending ends every waiting probe with a local error.
func (c *icmpConn) failPending(msg string) {
	c.mu.Lock()
	pending := c.pending
	c.pending = make(map[uint16]*probeWaiter)
	c.mu.Unlock()
	now := time.Now()
	for _, w := range pending {
		w.result <- probeReply{err: msg, local: true, userRx: now}
	}
}

// close stops the reader, closes the socket and ends the waiting probes.
func (c *icmpConn) close() {
	if !c.closing.CompareAndSwap(false, true) {
		return
	}
	<-c.done // the reader notices closing within readSlice
	c.fdMu.Lock()
	_ = syscall.Close(c.fd)
	c.fdMu.Unlock()
	c.failPending("prober closed")
}

// echoRequest builds a 64-byte echo request (8-byte header + 56-byte payload, like Linux ping).
// The payload carries the probe number and the token, so a reply matches exactly one probe.
func (c *icmpConn) echoRequest(seq uint16, num uint64) []byte {
	pkt := make([]byte, 64)
	pkt[0] = 8                                 // Echo Request
	binary.BigEndian.PutUint16(pkt[4:6], c.id) // a ping socket replaces the identifier
	binary.BigEndian.PutUint16(pkt[6:8], seq)
	binary.BigEndian.PutUint64(pkt[8:16], num)
	binary.BigEndian.PutUint64(pkt[16:24], c.token)
	for i := 24; i < len(pkt); i++ {
		pkt[i] = byte(i)
	}
	binary.BigEndian.PutUint16(pkt[2:4], checksum(pkt))
	return pkt
}

func (c *icmpConn) probe(ctx context.Context, ip net.IP, timeout time.Duration) PingResult {
	ipStr := ip.String()
	w := &probeWaiter{result: make(chan probeReply, 1)}
	copy(w.target[:], ip)
	seq, ok := c.register(w)
	if !ok {
		return PingResult{IP: ipStr, Error: "prober closed", Timestamp: time.Now(), LocalError: true}
	}
	defer c.unregister(seq, w)

	pkt := c.echoRequest(seq, w.num)
	sa := &syscall.SockaddrInet4{Addr: w.target}
	c.fdMu.RLock()
	if c.closing.Load() {
		c.fdMu.RUnlock()
		return PingResult{IP: ipStr, Error: "prober closed", Timestamp: time.Now(), LocalError: true}
	}
	sent := time.Now()
	err := syscall.Sendto(c.fd, pkt, 0, sa)
	c.fdMu.RUnlock()
	if err != nil {
		return c.sendFailure(ipStr, err)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-w.result:
		if r.err != "" {
			return PingResult{IP: ipStr, Error: r.err, Timestamp: r.userRx, LocalError: r.local}
		}
		rtt := roundTrip(sent, r)
		rttMs := float64(rtt.Microseconds()) / 1000.0
		if rttMs < 0.01 {
			rttMs = 0.01
		}
		return PingResult{IP: ipStr, Success: true, Latency: rtt, LatencyMs: rttMs, Timestamp: r.userRx}
	case <-timer.C:
		return PingResult{IP: ipStr, Error: "Request timeout", Timestamp: time.Now()}
	case <-ctx.Done():
		return PingResult{IP: ipStr, Error: ctx.Err().Error(), Timestamp: time.Now()}
	}
}

// sendFailure turns an error of sendto into a probe result. Only "no route" errors concern
// the target; all others are problems of this machine.
func (c *icmpConn) sendFailure(ip string, err error) PingResult {
	res := PingResult{IP: ip, Error: err.Error(), Timestamp: time.Now()}
	switch err {
	case syscall.ENETUNREACH, syscall.EHOSTUNREACH, syscall.EHOSTDOWN:
	case syscall.EBADF, syscall.ENOTSOCK:
		c.broken.Store(true)
		res.LocalError = true
	default:
		res.LocalError = true
	}
	return res
}

// roundTrip returns the round-trip time of a reply from the kernel receive time, so the time
// until the Go scheduler runs the reader is not part of it. If the kernel time is missing or
// not plausible (the wall clock was set between send and reply), the time measured in Go is
// used.
func roundTrip(sent time.Time, r probeReply) time.Duration {
	measured := r.userRx.Sub(sent)
	if r.kernelRx > 0 {
		if k := time.Duration(r.kernelRx - sent.UnixNano()); k >= 0 && k <= measured {
			return k
		}
	}
	return measured
}

// readLoop receives every packet of the socket until close.
func (c *icmpConn) readLoop() {
	defer close(c.done)
	buf := make([]byte, 1500)
	oob := make([]byte, 512)
	for !c.closing.Load() {
		n, oobn, _, from, err := syscall.Recvmsg(c.fd, buf, oob, 0)
		rx := time.Now()
		if err == nil {
			c.handlePacket(buf[:n], oob[:oobn], from, rx)
			continue
		}
		switch err {
		case syscall.EAGAIN, syscall.EINTR: // read slice ended
		case syscall.EBADF, syscall.ENOTSOCK:
			c.broken.Store(true)
			c.failPending("ICMP socket failed: " + err.Error())
			return
		default:
			// A pending socket error, reported once. A raw socket also receives the ICMP
			// message itself as a packet; a ping socket has it in its error queue.
			if !c.raw && !c.readErrors(buf, oob) {
				time.Sleep(10 * time.Millisecond) // an unexpected error: do not spin
			}
		}
	}
}

// readErrors reads the error queue of a ping socket. It reports whether it read anything.
func (c *icmpConn) readErrors(buf, oob []byte) bool {
	read := false
	for {
		n, oobn, _, from, err := syscall.Recvmsg(c.fd, buf, oob, syscall.MSG_ERRQUEUE|syscall.MSG_DONTWAIT)
		if err != nil {
			return read
		}
		read = true
		c.handleQueuedError(buf[:n], oob[:oobn], from, time.Now())
	}
}

// rxControl holds the control messages of a received packet.
type rxControl struct {
	kernelRx int64 // SO_TIMESTAMPNS, Unix ns
	drops    uint32
	hasDrops bool
	icmpErr  *extendedErr
}

// extendedErr is the part of struct sock_extended_err that DINIS uses.
type extendedErr struct {
	errno  syscall.Errno
	origin uint8
	typ    uint8
	code   uint8
}

func parseRxControl(oob []byte) rxControl {
	var rc rxControl
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return rc
	}
	for _, m := range msgs {
		switch {
		case m.Header.Level == syscall.SOL_SOCKET && m.Header.Type == syscall.SCM_TIMESTAMPNS &&
			len(m.Data) >= int(unsafe.Sizeof(syscall.Timespec{})):
			ts := (*syscall.Timespec)(unsafe.Pointer(&m.Data[0]))
			rc.kernelRx = ts.Nano()
		case m.Header.Level == syscall.SOL_SOCKET && m.Header.Type == syscall.SO_RXQ_OVFL && len(m.Data) >= 4:
			rc.drops, rc.hasDrops = binary.NativeEndian.Uint32(m.Data), true
		case m.Header.Level == syscall.IPPROTO_IP && m.Header.Type == syscall.IP_RECVERR && len(m.Data) >= 8:
			rc.icmpErr = &extendedErr{
				errno:  syscall.Errno(binary.NativeEndian.Uint32(m.Data[0:4])),
				origin: m.Data[4],
				typ:    m.Data[5],
				code:   m.Data[6],
			}
		}
	}
	return rc
}

// handlePacket matches a received packet to a waiting probe. A ping socket receives only the
// echo replies to its own requests, without IP header; a raw socket receives every echo
// reply, Destination Unreachable and Time Exceeded of the host, with IP header.
func (c *icmpConn) handlePacket(pkt, oob []byte, from syscall.Sockaddr, rx time.Time) {
	icmp := pkt
	if c.raw {
		if len(pkt) < 20 || pkt[0]>>4 != 4 {
			return
		}
		ihl := int(pkt[0]&0x0f) * 4
		if ihl < 20 || len(pkt) < ihl+8 {
			return
		}
		icmp = pkt[ihl:]
	}
	if len(icmp) < 8 {
		return
	}
	rc := parseRxControl(oob)
	if rc.hasDrops {
		c.drops.Store(uint64(rc.drops))
	}
	switch icmp[0] {
	case 0: // Echo Reply
		if c.raw && binary.BigEndian.Uint16(icmp[4:6]) != c.id {
			return // the reply to another program's ping
		}
		if len(icmp) < 24 || binary.BigEndian.Uint64(icmp[16:24]) != c.token {
			return
		}
		src, _ := from.(*syscall.SockaddrInet4)
		seq, num := binary.BigEndian.Uint16(icmp[6:8]), binary.BigEndian.Uint64(icmp[8:16])
		matched := c.deliver(seq, func(w *probeWaiter) bool {
			return src != nil && src.Addr == w.target && w.num == num
		}, probeReply{kernelRx: rc.kernelRx, userRx: rx})
		if !matched {
			c.counters.lateReplies.Add(1)
		}
	case 3, 11: // Destination Unreachable, Time Exceeded: as packets only on a raw socket
		if !c.raw {
			return
		}
		dst, id, seq, ok := quotedEcho(icmp)
		if !ok || id != c.id {
			return
		}
		var target [4]byte
		copy(target[:], dst)
		c.deliver(seq, func(w *probeWaiter) bool { return w.target == target },
			probeReply{err: icmpErrorText(icmp[0], icmp[1]), userRx: rx})
	}
}

// handleQueuedError matches an entry of the error queue to a waiting probe. payload is the
// echo request as far as the error message quoted it, from is its destination.
func (c *icmpConn) handleQueuedError(payload, oob []byte, from syscall.Sockaddr, rx time.Time) {
	ee := parseRxControl(oob).icmpErr
	if ee == nil || len(payload) < 8 || payload[0] != 8 {
		return
	}
	seq := binary.BigEndian.Uint16(payload[6:8])
	// Routers quote at least 8 bytes of our request; most quote all of it.
	hasNum := len(payload) >= 24
	if hasNum && binary.BigEndian.Uint64(payload[16:24]) != c.token {
		return
	}
	var num uint64
	if hasNum {
		num = binary.BigEndian.Uint64(payload[8:16])
	}
	dst, _ := from.(*syscall.SockaddrInet4)
	r := probeReply{userRx: rx}
	if ee.origin == soEEOriginICMP {
		r.err = icmpErrorText(ee.typ, ee.code)
	} else {
		r.err, r.local = ee.errno.Error(), true
	}
	c.deliver(seq, func(w *probeWaiter) bool {
		return (dst == nil || dst.Addr == w.target) && (!hasNum || w.num == num)
	}, r)
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
