package mtglib_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/antireplay"
	"github.com/9seconds/mtg/v2/events"
	"github.com/9seconds/mtg/v2/internal/proxyprotocol"
	"github.com/9seconds/mtg/v2/internal/utils"
	"github.com/9seconds/mtg/v2/logger"
	"github.com/9seconds/mtg/v2/mtglib"
	"github.com/9seconds/mtg/v2/network"
	"github.com/pires/go-proxyproto"
)

const pendingHeaderBudget = 2 * time.Second

type signallingListener struct {
	net.Listener

	accepted chan struct{}
}

func (l *signallingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		select {
		case l.accepted <- struct{}{}:
		default:
		}
	}

	return conn, err //nolint: wrapcheck
}

func newSignallingProxyProtocolListener(t *testing.T) *signallingListener {
	t.Helper()

	base, err := utils.NewListener("127.0.0.1:0", 0, logger.NewNoopLogger())
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}

	listener := &signallingListener{
		Listener: &proxyprotocol.ListenerAdapter{
			Listener: proxyproto.Listener{Listener: base, ReadHeaderTimeout: 10 * time.Second},
		},
		accepted: make(chan struct{}, 64),
	}

	t.Cleanup(func() { listener.Close() }) //nolint: errcheck

	return listener
}

type allowAll struct {
	consulted atomic.Int64
}

func (a *allowAll) Contains(net.IP) bool {
	a.consulted.Add(1)

	return true
}

func (a *allowAll) Run(time.Duration) {}
func (a *allowAll) Shutdown()         {}

func dialSilent(t *testing.T, addr net.Addr) net.Conn {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr.String(), proxyProtocolDeadline)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}

	t.Cleanup(func() { conn.Close() }) //nolint: errcheck

	return conn
}

func waitAccepted(t *testing.T, listener *signallingListener) {
	t.Helper()

	select {
	case <-listener.accepted:
	case <-time.After(proxyProtocolDeadline):
		t.Fatalf("the listener did not accept a connection")
	}
}

func pendingHeaderReaders() int {
	var buf bytes.Buffer

	pprof.Lookup("goroutine").WriteTo(&buf, 2) //nolint: errcheck

	count := 0

	for _, stack := range strings.Split(buf.String(), "\n\n") {
		if strings.Contains(stack, "go-proxyproto.(*Conn).readHeader") {
			count++
		}
	}

	return count
}

func assertNoPendingHeaderReaders(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(pendingHeaderBudget)

	for pendingHeaderReaders() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines are still reading a PROXY header", pendingHeaderReaders())
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func assertServeEndedCleanly(t *testing.T, outcome serveOutcome) {
	t.Helper()

	if outcome.recovered != nil {
		t.Fatalf("Serve panicked: %v", outcome.recovered)
	}

	if outcome.err != nil && !errors.Is(outcome.err, net.ErrClosed) {
		t.Fatalf("Serve ended with an unexpected error: %v", outcome.err)
	}
}

func countStreams(evts []mtglib.Event) (int, int) {
	started, finished := 0, 0

	for _, evt := range evts {
		switch evt.(type) {
		case mtglib.EventStart:
			started++
		case mtglib.EventFinish:
			finished++
		}
	}

	return started, finished
}

func shutdownWithin(t *testing.T, proxy *mtglib.Proxy, budget time.Duration) time.Duration {
	t.Helper()

	started := time.Now()
	done := make(chan struct{})

	go func() {
		proxy.Shutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(budget):
		t.Fatalf("Shutdown did not return within %v", budget)
	}

	return time.Since(started)
}

func TestServeKeepsAcceptingWhileAProxyHeaderIsPending(t *testing.T) {
	allowlist := newRecordingAllowlist()
	proxy := newProxyProtocolProxy(t, allowlist, events.NewNoopStream())
	listener := newSignallingProxyProtocolListener(t)
	done := serveRecovering(proxy, listener)

	silent := dialSilent(t, listener.Addr())
	waitAccepted(t, listener)

	healthy := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 50), Port: 5000}
	dst := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443}
	healthyConn := sendRaw(t, listener.Addr(), proxyHeaderBytes(t, healthy, dst))

	started := time.Now()

	select {
	case ip := <-allowlist.hit:
		if ip != healthy.IP.String() {
			t.Fatalf("the allowlist saw %q first, want the healthy client %q", ip, healthy.IP.String())
		}
	case <-time.After(pendingHeaderBudget):
		t.Errorf("a silent client kept the healthy client out of admission for more than %v", pendingHeaderBudget)
	}

	t.Logf("healthy client admitted after %v", time.Since(started))

	assertClosedByProxy(t, healthyConn)

	silent.Close()   //nolint: errcheck
	listener.Close() //nolint: errcheck
	shutdownWithin(t, proxy, 15*time.Second)

	assertServeEndedCleanly(t, <-done)

	assertNoPendingHeaderReaders(t)
}

func TestShutdownDoesNotWaitForAPendingProxyHeader(t *testing.T) {
	stream := &recordingEvents{}
	allowlist := &allowAll{}
	proxy := newProxyProtocolProxy(t, allowlist, stream)
	listener := newSignallingProxyProtocolListener(t)
	done := serveRecovering(proxy, listener)

	silent := dialSilent(t, listener.Addr())
	waitAccepted(t, listener)
	time.Sleep(100 * time.Millisecond)

	listener.Close() //nolint: errcheck

	took := shutdownWithin(t, proxy, 2*pendingHeaderBudget)
	if took > pendingHeaderBudget {
		t.Errorf("Shutdown waited %v for a client that never sent its PROXY header", took)
	}

	select {
	case outcome := <-done:
		assertServeEndedCleanly(t, outcome)
	case <-time.After(proxyProtocolDeadline):
		t.Fatalf("Serve did not return after Shutdown")
	}

	assertClosedByProxy(t, silent)
	assertNoPendingHeaderReaders(t)

	started, finished := countStreams(stream.snapshot())
	if started != 1 || finished != 1 {
		t.Fatalf("the accepted connection was not served and awaited by Shutdown: %d started, %d finished", started, finished)
	}

	if got := allowlist.consulted.Load(); got != 1 {
		t.Fatalf("the allowlist judged %d connections, want the one Serve accepted", got)
	}
}

func TestShutdownRacingProxyHeadersLeavesNoStreamBehind(t *testing.T) {
	dst := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443}
	totalStarted := 0

	t.Cleanup(func() { t.Logf("streams started across all rounds: %d", totalStarted) })

	for round := range 20 {
		stream := &recordingEvents{}
		proxy := newProxyProtocolProxy(t, &allowAll{}, stream)
		listener := newSignallingProxyProtocolListener(t)
		done := serveRecovering(proxy, listener)

		clients := make([]net.Conn, 0, 16)
		silent := dialSilent(t, listener.Addr())
		clients = append(clients, silent)

		var wg sync.WaitGroup

		release := make(chan struct{})

		for i := range 15 {
			conn := dialSilent(t, listener.Addr())
			clients = append(clients, conn)
			src := &net.TCPAddr{IP: net.IPv4(192, 0, 2, byte(10+i)), Port: 5000 + i}
			raw := proxyHeaderBytes(t, src, dst)

			wg.Go(func() {
				<-release

				if i%3 == 0 {
					time.Sleep(time.Duration(i) * time.Millisecond)
				}

				conn.Write(raw) //nolint: errcheck
			})
		}

		close(release)
		time.Sleep(time.Duration(round%5) * time.Millisecond)

		listener.Close() //nolint: errcheck
		shutdownWithin(t, proxy, 2*pendingHeaderBudget)

		after := len(stream.snapshot())

		wg.Wait()

		select {
		case outcome := <-done:
			assertServeEndedCleanly(t, outcome)
		case <-time.After(proxyProtocolDeadline):
			t.Fatalf("round %d: Serve did not return after Shutdown", round)
		}

		for _, conn := range clients {
			assertClosedByProxy(t, conn)
		}

		time.Sleep(50 * time.Millisecond)

		if got := stream.snapshot(); len(got) != after {
			t.Fatalf("round %d: %d events arrived after Shutdown returned: %#v", round, len(got)-after, got[after:])
		}

		started, finished := countStreams(stream.snapshot())
		totalStarted += started

		if started != finished {
			t.Fatalf("round %d: %d streams started but %d finished before Shutdown returned", round, started, finished)
		}

		assertNoPendingHeaderReaders(t)
	}
}

type blockOne struct {
	ip net.IP
}

func (b blockOne) Contains(ip net.IP) bool { return ip.Equal(b.ip) }
func (blockOne) Run(time.Duration)         {}
func (blockOne) Shutdown()                 {}

func TestServeJudgesABlocklistedClientBeforeTheWorkerPool(t *testing.T) {
	dialer, err := network.NewDefaultDialer(0, 0)
	if err != nil {
		t.Fatalf("cannot build dialer: %v", err)
	}

	ntw, err := network.NewNetwork(dialer, "mtgtest", "1.1.1.1", 0)
	if err != nil {
		t.Fatalf("cannot build network: %v", err)
	}

	blocked := net.IPv4(192, 0, 2, 66)
	stream := &recordingEvents{}

	proxy, err := mtglib.NewProxy(mtglib.ProxyOpts{
		Secret:           mtglib.GenerateSecret("example.com"),
		PlainMode:        true,
		Network:          ntw,
		AntiReplayCache:  antireplay.NewNoop(),
		IPBlocklist:      blockOne{ip: blocked},
		IPAllowlist:      &allowAll{},
		EventStream:      stream,
		Logger:           logger.NewNoopLogger(),
		UseTestDCs:       true,
		HandshakeTimeout: 3 * time.Second,
		Concurrency:      1,
	})
	if err != nil {
		t.Fatalf("cannot build proxy: %v", err)
	}

	listener := newSignallingProxyProtocolListener(t)
	done := serveRecovering(proxy, listener)
	dst := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443}

	busy := sendRaw(t, listener.Addr(), proxyHeaderBytes(t, &net.TCPAddr{IP: net.IPv4(192, 0, 2, 65), Port: 5000}, dst))

	deadline := time.Now().Add(pendingHeaderBudget)

	for started, _ := countStreams(stream.snapshot()); started == 0; started, _ = countStreams(stream.snapshot()) {
		if time.Now().After(deadline) {
			t.Fatalf("the first client never took the only worker")
		}

		time.Sleep(5 * time.Millisecond)
	}

	blockedConn := sendRaw(t, listener.Addr(), proxyHeaderBytes(t, &net.TCPAddr{IP: blocked, Port: 5001}, dst))
	assertClosedByProxy(t, blockedConn)

	blocklisted, limited := 0, 0

	for _, evt := range stream.snapshot() {
		switch evt.(type) {
		case mtglib.EventIPBlocklisted:
			blocklisted++
		case mtglib.EventConcurrencyLimited:
			limited++
		}
	}

	if blocklisted != 1 || limited != 0 {
		t.Fatalf("a blocklisted client behind a full worker pool gave %d blocklisted and %d concurrency-limited events, want 1 and 0",
			blocklisted, limited)
	}

	busy.Close()     //nolint: errcheck
	listener.Close() //nolint: errcheck
	shutdownWithin(t, proxy, 2*pendingHeaderBudget)
	assertServeEndedCleanly(t, <-done)
}

type gatedEvents struct {
	recordingEvents

	entered chan struct{}
	release chan struct{}
}

func (g *gatedEvents) Send(ctx context.Context, evt mtglib.Event) {
	if _, ok := evt.(mtglib.EventIPBlocklisted); ok {
		close(g.entered)
		<-g.release
	}

	g.recordingEvents.Send(ctx, evt)
}

func TestShutdownWaitsForTheRejectionEventOfAnAdmission(t *testing.T) {
	stream := &gatedEvents{entered: make(chan struct{}), release: make(chan struct{})}
	proxy := newProxyProtocolProxy(t, newRecordingAllowlist(), stream)
	listener := newSignallingProxyProtocolListener(t)
	done := serveRecovering(proxy, listener)
	dst := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443}

	sendRaw(t, listener.Addr(), proxyHeaderBytes(t, &net.TCPAddr{IP: net.IPv4(192, 0, 2, 77), Port: 5000}, dst))

	select {
	case <-stream.entered:
	case <-time.After(proxyProtocolDeadline):
		t.Fatalf("the allowlist rejection event was never sent")
	}

	listener.Close() //nolint: errcheck
	assertServeEndedCleanly(t, <-done)

	shutdownDone := make(chan struct{})

	go func() {
		proxy.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		close(stream.release)
		t.Fatalf("Shutdown returned while an admission was still sending its rejection event")
	case <-time.After(200 * time.Millisecond):
	}

	close(stream.release)

	select {
	case <-shutdownDone:
	case <-time.After(proxyProtocolDeadline):
		t.Fatalf("Shutdown did not return after the rejection event was sent")
	}
}
