//go:build netns

// Tests that need real ICMP sockets and a test network. Run them with scripts/netns-test.sh,
// which creates a private network namespace (no root needed) with:
//   - ping sockets allowed (net.ipv4.ping_group_range), raw sockets through CAP_NET_RAW in the
//     namespace
//   - veth0 with 10.99.0.1/24, where no host answers ARP for 10.99.0.77

package pinger

import (
	"context"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const netnsUnreachable = "10.99.0.77"

func requireNetns(t *testing.T) {
	t.Helper()
	if _, err := net.InterfaceByName("veth0"); err != nil {
		t.Skip("no test network: run scripts/netns-test.sh")
	}
}

// forceRaw makes new probers use raw sockets until the test ends.
func forceRaw(t *testing.T) {
	old := socketTypes
	socketTypes = []int{syscall.SOCK_RAW}
	t.Cleanup(func() { socketTypes = old })
}

func p99(lat []float64) float64 {
	sort.Float64s(lat)
	return lat[len(lat)*99/100]
}

// The kernel reports Host Unreachable after its ARP attempts (about 3 s). A ping socket only
// sees it through its error queue.
func TestNetnsDestinationUnreachable(t *testing.T) {
	requireNetns(t)
	for _, mode := range []string{ModePingSocket, ModeRawSocket} {
		t.Run(mode, func(t *testing.T) {
			if mode == ModeRawSocket {
				forceRaw(t)
			}
			p := NewSingleProber()
			defer p.Close()
			if got := p.Mode(); got != mode {
				t.Fatalf("expected mode %s, got %s", mode, got)
			}
			start := time.Now()
			res := p.Probe(context.Background(), netnsUnreachable, 5*time.Second)
			elapsed := time.Since(start)
			if res.Success || res.LocalError || !strings.HasPrefix(res.Error, "Destination Unreachable") {
				t.Fatalf("expected Destination Unreachable, got %+v", res)
			}
			if elapsed > 4500*time.Millisecond {
				t.Fatalf("the error took %v; it must end the probe before its 5 s timeout", elapsed)
			}
			t.Logf("%s after %v", res.Error, elapsed.Round(time.Millisecond))
		})
	}
}

// Every raw ICMP socket receives a copy of every reply. With one socket per probe, idle
// sockets filled their buffers and dropped their own replies: 300 of 300 probes to the
// loopback address failed at 200 echo replies per second from other probes.
func TestNetnsRawSocketUnderBackgroundTraffic(t *testing.T) {
	requireNetns(t)
	forceRaw(t)
	p := NewSingleProber()
	defer p.Close()
	bg := NewSingleProber() // another program pinging
	defer bg.Close()

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			bg.Probe(context.Background(), "127.0.0.1", time.Second)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	defer func() { stop.Store(true); wg.Wait() }()
	time.Sleep(2 * time.Second)

	fails := 0
	for i := 0; i < 300; i++ {
		if res := p.Probe(context.Background(), "127.0.0.1", time.Second); !res.Success {
			fails++
		}
		time.Sleep(5 * time.Millisecond)
	}
	if fails > 0 {
		t.Fatalf("%d of 300 probes failed under background ICMP traffic", fails)
	}
	if s := p.Stats(); s.RxDropped > 0 {
		t.Errorf("the kernel dropped %d packets of the prober socket", s.RxDropped)
	}
}

// The round-trip time comes from the kernel receive time, so a busy Go process does not add
// to it. Before, the loopback p99 rose from 0.05 ms to 14-20 ms under this load.
func TestNetnsRoundTripUnderLoad(t *testing.T) {
	requireNetns(t)
	p := NewSingleProber()
	defer p.Close()

	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < runtime.GOMAXPROCS(0)*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var sink [][]byte
			for !stop.Load() {
				sink = append(sink, make([]byte, 4096)) // CPU and GC load
				if len(sink) > 2000 {
					sink = nil
				}
			}
		}()
	}
	var lat []float64
	for i := 0; i < 300; i++ {
		res := p.Probe(context.Background(), "127.0.0.1", time.Second)
		if !res.Success {
			t.Fatalf("probe failed: %s", res.Error)
		}
		lat = append(lat, res.LatencyMs)
		time.Sleep(5 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	got := p99(lat)
	t.Logf("loopback p99 under load: %.3f ms", got)
	if got > 2 {
		t.Fatalf("loopback p99 under load is %.3f ms; expected the kernel round-trip time (well below 1 ms)", got)
	}
}

func threadCount(t *testing.T) int {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if v, ok := strings.CutPrefix(line, "Threads:"); ok {
			n, _ := strconv.Atoi(strings.TrimSpace(v))
			return n
		}
	}
	t.Fatal("no thread count in /proc/self/status")
	return 0
}

// A waiting probe holds no OS thread: before, each probe in flight blocked one in recvfrom.
func TestNetnsWaitingProbesHoldNoThreads(t *testing.T) {
	requireNetns(t)
	p := NewSingleProber()
	defer p.Close()
	p.Mode() // open the socket

	const n = 1000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Probe(context.Background(), netnsUnreachable, 2*time.Second)
		}()
	}
	time.Sleep(time.Second) // all probes wait for a reply now
	threads := threadCount(t)
	wg.Wait()
	t.Logf("%d OS threads with %d probes in flight", threads, n)
	// The burst of sends can start some threads, and Go keeps idle threads of earlier tests.
	if threads > n/4 {
		t.Fatalf("%d OS threads with %d probes in flight", threads, n)
	}
}
