package pinger

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func TestSingleProberLocalhost(t *testing.T) {
	prober := NewSingleProber()
	defer prober.Close()
	res := prober.Probe(context.Background(), "127.0.0.1", 1*time.Second)
	if !res.Success {
		t.Fatalf("expected ping to 127.0.0.1 to succeed, got error: %s", res.Error)
	}
	if res.LatencyMs <= 0 {
		t.Errorf("expected positive latency, got %f ms", res.LatencyMs)
	}
	t.Logf("prober mode: %s", prober.Mode())
}

func TestSingleProberInvalidIP(t *testing.T) {
	prober := NewSingleProber()
	res := prober.Probe(context.Background(), "999.999.999.999", 500*time.Millisecond)
	if res.Success || res.LocalError {
		t.Errorf("expected a failure of the target on an invalid IP, got %+v", res)
	}
}

func TestChecksum(t *testing.T) {
	b := []byte{0x08, 0x00, 0x00, 0x00, 0x12, 0x34, 0x00, 0x01}
	cs := checksum(b)
	if cs == 0 {
		t.Errorf("expected non-zero checksum, got 0")
	}
}

func TestSingleProberSequentialProbes(t *testing.T) {
	prober := NewSingleProber()
	defer prober.Close()

	for i := 0; i < 5; i++ {
		res := prober.Probe(context.Background(), "127.0.0.1", 1*time.Second)
		if !res.Success {
			t.Fatalf("expected ping #%d to 127.0.0.1 to succeed, got: %s", i, res.Error)
		}
	}
}

func TestSingleProberConcurrentLocalhost(t *testing.T) {
	prober := NewSingleProber()
	defer prober.Close()

	const count = 100
	errChan := make(chan error, count)

	for i := 0; i < count; i++ {
		go func(idx int) {
			res := prober.Probe(context.Background(), "127.0.0.1", 1*time.Second)
			if !res.Success {
				errChan <- fmt.Errorf("probe #%d failed: %s", idx, res.Error)
				return
			}
			errChan <- nil
		}(i)
	}

	var failures []error
	for i := 0; i < count; i++ {
		if err := <-errChan; err != nil {
			failures = append(failures, err)
		}
	}

	if len(failures) > 0 {
		t.Fatalf("%d out of %d concurrent probes failed. First error: %v", len(failures), count, failures[0])
	}
}

func TestSingleProberContextCancellation(t *testing.T) {
	prober := NewSingleProber()
	defer prober.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context

	start := time.Now()
	res := prober.Probe(ctx, "127.0.0.1", 5*time.Second)
	elapsed := time.Since(start)

	if res.Success {
		t.Errorf("expected cancelled probe to fail, but got success")
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("expected immediate cancellation, took %v", elapsed)
	}
}

func TestSingleProberReopensAfterClose(t *testing.T) {
	prober := NewSingleProber()
	if res := prober.Probe(context.Background(), "127.0.0.1", time.Second); !res.Success {
		t.Fatalf("first probe failed: %s", res.Error)
	}
	prober.Close()
	defer prober.Close()
	if res := prober.Probe(context.Background(), "127.0.0.1", time.Second); !res.Success {
		t.Fatalf("a probe after Close must open a new socket, got: %s", res.Error)
	}
}

func buildICMPError(icmpType byte, dst net.IP, ipOptions int, quotedType byte, id, seq uint16) []byte {
	ihl := 20 + ipOptions
	pkt := make([]byte, 8+ihl+8)
	pkt[0] = icmpType
	q := pkt[8:]
	q[0] = 0x40 | byte(ihl/4)
	q[9] = 1 // ICMP
	copy(q[12:16], net.IPv4(10, 0, 0, 1).To4())
	copy(q[16:20], dst.To4())
	qi := q[ihl:]
	qi[0] = quotedType
	binary.BigEndian.PutUint16(qi[4:6], id)
	binary.BigEndian.PutUint16(qi[6:8], seq)
	return pkt
}

func TestQuotedEcho(t *testing.T) {
	target := net.ParseIP("192.0.2.10").To4()
	const id, seq = 0x1234, 0x0042

	full := buildICMPError(3, target, 0, 8, id, seq)

	tests := []struct {
		name   string
		data   []byte
		wantOK bool
	}{
		{"dest unreachable", full, true},
		{"time exceeded", buildICMPError(11, target, 0, 8, id, seq), true},
		{"quoted header with IP options", buildICMPError(3, target, 8, 8, id, seq), true},
		{"quoted packet not an echo request", buildICMPError(3, target, 0, 0, id, seq), false},
		{"truncated quoted icmp", full[:len(full)-4], false},
		{"truncated quoted ip header", full[:20], false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dst, gotID, gotSeq, ok := quotedEcho(tc.data)
			if ok != tc.wantOK {
				t.Fatalf("quotedEcho() ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && (!dst.Equal(target) || gotID != id || gotSeq != seq) {
				t.Errorf("quotedEcho() = %v %#x %#x, want %v %#x %#x", dst, gotID, gotSeq, target, id, seq)
			}
		})
	}
}

// testConn returns a connection without a socket, for feeding packets to its handlers.
func testConn(raw bool) *icmpConn {
	return newICMPConn(-1, raw, &proberCounters{})
}

// waitFor registers a probe of target and returns its sequence number and waiter.
func waitFor(t *testing.T, c *icmpConn, target string) (uint16, *probeWaiter) {
	t.Helper()
	w := &probeWaiter{result: make(chan probeReply, 1)}
	copy(w.target[:], net.ParseIP(target).To4())
	seq, ok := c.register(w)
	if !ok {
		t.Fatal("register failed")
	}
	return seq, w
}

func cmsg(level, typ int32, data []byte) []byte {
	b := make([]byte, syscall.CmsgSpace(len(data)))
	h := (*syscall.Cmsghdr)(unsafe.Pointer(&b[0]))
	h.Level, h.Type = level, typ
	h.SetLen(syscall.CmsgLen(len(data)))
	copy(b[syscall.CmsgLen(0):], data)
	return b
}

func timestampCmsg(t time.Time) []byte {
	ts := syscall.NsecToTimespec(t.UnixNano())
	return cmsg(syscall.SOL_SOCKET, syscall.SCM_TIMESTAMPNS, unsafe.Slice((*byte)(unsafe.Pointer(&ts)), unsafe.Sizeof(ts)))
}

// echoReply builds the reply to the request c.echoRequest(seq, num), with an IP header for a
// raw socket.
func echoReply(c *icmpConn, seq uint16, num uint64, src string) []byte {
	icmp := c.echoRequest(seq, num)
	icmp[0] = 0
	if !c.raw {
		return icmp
	}
	ip := make([]byte, 20, 20+len(icmp))
	ip[0] = 0x45
	ip[9] = syscall.IPPROTO_ICMP
	copy(ip[12:16], net.ParseIP(src).To4())
	return append(ip, icmp...)
}

func fromAddr(ip string) *syscall.SockaddrInet4 {
	sa := &syscall.SockaddrInet4{}
	copy(sa.Addr[:], net.ParseIP(ip).To4())
	return sa
}

func TestHandlePacketMatchesReplies(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("raw=%v", raw), func(t *testing.T) {
			c := testConn(raw)
			seq, w := waitFor(t, c, "192.0.2.1")
			kernel := time.Now()
			rx := kernel.Add(5 * time.Millisecond)

			// Replies that are not for this probe are ignored
			c.handlePacket(echoReply(c, seq, w.num, "192.0.2.99"), nil, fromAddr("192.0.2.99"), rx)
			c.handlePacket(echoReply(c, seq, w.num+1, "192.0.2.1"), nil, fromAddr("192.0.2.1"), rx)
			foreign := echoReply(c, seq, w.num, "192.0.2.1")
			binary.BigEndian.PutUint64(foreign[len(foreign)-64+16:], c.token+1)
			c.handlePacket(foreign, nil, fromAddr("192.0.2.1"), rx)
			if raw {
				otherID := echoReply(c, seq, w.num, "192.0.2.1")
				binary.BigEndian.PutUint16(otherID[20+4:], c.id+1)
				c.handlePacket(otherID, nil, fromAddr("192.0.2.1"), rx)
			}
			select {
			case r := <-w.result:
				t.Fatalf("a reply for another probe was delivered: %+v", r)
			default:
			}

			c.handlePacket(echoReply(c, seq, w.num, "192.0.2.1"), timestampCmsg(kernel), fromAddr("192.0.2.1"), rx)
			select {
			case r := <-w.result:
				if r.err != "" || r.kernelRx != kernel.UnixNano() || !r.userRx.Equal(rx) {
					t.Fatalf("unexpected reply %+v", r)
				}
			default:
				t.Fatal("the matching reply was not delivered")
			}

			// A duplicate or late reply has no waiter
			late := c.counters.lateReplies.Load()
			c.handlePacket(echoReply(c, seq, w.num, "192.0.2.1"), nil, fromAddr("192.0.2.1"), rx)
			if c.counters.lateReplies.Load() != late+1 {
				t.Errorf("expected the late reply to be counted")
			}
		})
	}
}

func TestHandlePacketRawDestinationUnreachable(t *testing.T) {
	c := testConn(true)
	seq, w := waitFor(t, c, "192.0.2.1")
	ipHdr := make([]byte, 20)
	ipHdr[0] = 0x45
	ipHdr[9] = syscall.IPPROTO_ICMP

	// An error for another target, and for another program's ping, is ignored
	c.handlePacket(append(ipHdr, buildICMPError(3, net.ParseIP("192.0.2.2"), 0, 8, c.id, seq)...), nil, fromAddr("10.0.0.1"), time.Now())
	c.handlePacket(append(ipHdr, buildICMPError(3, net.ParseIP("192.0.2.1"), 0, 8, c.id+1, seq)...), nil, fromAddr("10.0.0.1"), time.Now())
	select {
	case r := <-w.result:
		t.Fatalf("an error for another probe was delivered: %+v", r)
	default:
	}

	errPkt := buildICMPError(3, net.ParseIP("192.0.2.1"), 0, 8, c.id, seq)
	errPkt[1] = 1 // host unreachable
	c.handlePacket(append(ipHdr, errPkt...), nil, fromAddr("10.0.0.1"), time.Now())
	r := <-w.result
	if r.err != "Destination Unreachable (host unreachable)" || r.local {
		t.Fatalf("unexpected result %+v", r)
	}
}

func TestHandleQueuedError(t *testing.T) {
	c := testConn(false)
	seq, w := waitFor(t, c, "192.0.2.1")
	ee := func(origin, typ, code uint8, errno syscall.Errno) []byte {
		d := make([]byte, 16)
		binary.NativeEndian.PutUint32(d[0:4], uint32(errno))
		d[4], d[5], d[6] = origin, typ, code
		return cmsg(syscall.IPPROTO_IP, syscall.IP_RECVERR, d)
	}
	req := c.echoRequest(seq, w.num)

	// Another destination, or a request of an earlier probe with the same sequence number
	c.handleQueuedError(req, ee(soEEOriginICMP, 3, 1, syscall.EHOSTUNREACH), fromAddr("192.0.2.2"), time.Now())
	c.handleQueuedError(c.echoRequest(seq, w.num-1), ee(soEEOriginICMP, 3, 1, syscall.EHOSTUNREACH), fromAddr("192.0.2.1"), time.Now())
	select {
	case r := <-w.result:
		t.Fatalf("an error for another probe was delivered: %+v", r)
	default:
	}

	// A router that quotes only the first 8 bytes still matches by sequence number
	c.handleQueuedError(req[:8], ee(soEEOriginICMP, 11, 0, syscall.EHOSTUNREACH), fromAddr("192.0.2.1"), time.Now())
	if r := <-w.result; r.err != "Time Exceeded" || r.local {
		t.Fatalf("unexpected result %+v", r)
	}

	seq, w = waitFor(t, c, "192.0.2.1")
	c.handleQueuedError(c.echoRequest(seq, w.num), ee(1, 0, 0, syscall.EMSGSIZE), fromAddr("192.0.2.1"), time.Now())
	if r := <-w.result; !r.local || r.err == "" {
		t.Fatalf("a local error must be reported as local, got %+v", r)
	}
}

func TestRegisterSkipsPendingSequenceNumbers(t *testing.T) {
	c := testConn(false)
	c.nextSeq = 0xfffe
	s1, _ := waitFor(t, c, "192.0.2.1") // 0xffff
	c.nextSeq = 0xfffe
	s2, w2 := waitFor(t, c, "192.0.2.1")
	if s1 != 0xffff || s2 != 0 {
		t.Fatalf("expected 0xffff and then 0 (skipping the pending 0xffff), got %#x %#x", s1, s2)
	}
	if w2.num <= 1 {
		t.Errorf("expected increasing probe numbers, got %d", w2.num)
	}
}

func TestRoundTrip(t *testing.T) {
	sent := time.Now()
	rx := sent.Add(3 * time.Millisecond)
	cases := []struct {
		name     string
		kernelRx int64
		want     time.Duration
	}{
		{"kernel time", sent.Add(time.Millisecond).UnixNano(), time.Millisecond},
		{"no kernel time", 0, 3 * time.Millisecond},
		{"clock set back", sent.Add(-time.Second).UnixNano(), 3 * time.Millisecond},
		{"clock set forward", sent.Add(time.Second).UnixNano(), 3 * time.Millisecond},
	}
	for _, tc := range cases {
		if got := roundTrip(sent, probeReply{kernelRx: tc.kernelRx, userRx: rx}); got != tc.want {
			t.Errorf("%s: roundTrip() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestSendFailure(t *testing.T) {
	c := testConn(false)
	cases := []struct {
		err   syscall.Errno
		local bool
	}{
		{syscall.EHOSTUNREACH, false},
		{syscall.ENETUNREACH, false},
		{syscall.ENOBUFS, true},
		{syscall.EPERM, true},
		{syscall.EAGAIN, true},
	}
	for _, tc := range cases {
		if r := c.sendFailure("192.0.2.1", tc.err); r.Success || r.LocalError != tc.local || r.Error == "" {
			t.Errorf("%v: unexpected result %+v", tc.err, r)
		}
	}
	if c.broken.Load() {
		t.Fatal("the socket must not be marked broken by these errors")
	}
	c.sendFailure("192.0.2.1", syscall.EBADF)
	if !c.broken.Load() {
		t.Fatal("EBADF must mark the socket broken")
	}
}

func TestFailPendingEndsWaitingProbes(t *testing.T) {
	c := testConn(false)
	_, w := waitFor(t, c, "192.0.2.1")
	c.failPending("prober closed")
	if r := <-w.result; !r.local || r.err != "prober closed" {
		t.Fatalf("unexpected result %+v", r)
	}
}
