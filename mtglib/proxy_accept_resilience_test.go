package mtglib_test

import (
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/antireplay"
	"github.com/9seconds/mtg/v2/events"
	"github.com/9seconds/mtg/v2/ipblocklist"
	"github.com/9seconds/mtg/v2/logger"
	"github.com/9seconds/mtg/v2/mtglib"
	"github.com/9seconds/mtg/v2/network"
)

const acceptResilienceDeadline = 5 * time.Second

type acceptTimeoutError struct{}

func (acceptTimeoutError) Error() string   { return "accept timeout" }
func (acceptTimeoutError) Timeout() bool   { return true }
func (acceptTimeoutError) Temporary() bool { return true }

func acceptOpError(errno syscall.Errno) error {
	return &net.OpError{Op: "accept", Net: "tcp", Err: os.NewSyscallError("accept", errno)}
}

type flakyListener struct {
	net.Listener

	mu       sync.Mutex
	closed   bool
	failures []error
	endless  error
	accepted atomic.Int64
	served   chan net.Conn
}

func newFlakyListener(t *testing.T, failures []error, endless error) *flakyListener {
	t.Helper()

	inner, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}

	listener := &flakyListener{
		Listener: inner,
		failures: failures,
		endless:  endless,
		served:   make(chan net.Conn, 64),
	}

	t.Cleanup(func() { listener.Close() }) //nolint: errcheck

	return listener
}

func (l *flakyListener) nextFailure() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return net.ErrClosed
	}

	if len(l.failures) > 0 {
		err := l.failures[0]
		l.failures = l.failures[1:]

		return err
	}

	return l.endless
}

func (l *flakyListener) Accept() (net.Conn, error) {
	if err := l.nextFailure(); err != nil {
		return nil, err
	}

	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err //nolint: wrapcheck
	}

	l.accepted.Add(1)
	l.served <- conn

	return conn, nil
}

func (l *flakyListener) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()

	return l.Listener.Close() //nolint: wrapcheck
}

type acceptAllowAll struct{}

func (acceptAllowAll) Contains(net.IP) bool { return true }
func (acceptAllowAll) Run(time.Duration)    {}
func (acceptAllowAll) Shutdown()            {}

func newAcceptResilienceProxy(t *testing.T) *mtglib.Proxy {
	t.Helper()

	dialer, err := network.NewDefaultDialer(0, 0)
	if err != nil {
		t.Fatalf("cannot build dialer: %v", err)
	}

	ntw, err := network.NewNetwork(dialer, "mtgtest", "1.1.1.1", 0)
	if err != nil {
		t.Fatalf("cannot build network: %v", err)
	}

	proxy, err := mtglib.NewProxy(mtglib.ProxyOpts{
		Secret:           mtglib.GenerateSecret("example.com"),
		PlainMode:        true,
		Network:          ntw,
		AntiReplayCache:  antireplay.NewNoop(),
		IPBlocklist:      ipblocklist.NewNoop(),
		IPAllowlist:      acceptAllowAll{},
		EventStream:      events.NewNoopStream(),
		Logger:           logger.NewNoopLogger(),
		UseTestDCs:       true,
		HandshakeTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("cannot build proxy: %v", err)
	}

	return proxy
}

func serveInBackground(proxy *mtglib.Proxy, listener net.Listener) <-chan error {
	done := make(chan error, 1)

	go func() {
		done <- proxy.Serve(listener)
	}()

	return done
}

func stopProxy(t *testing.T, proxy *mtglib.Proxy, listener io.Closer, done <-chan error) {
	t.Helper()

	finished := make(chan struct{})

	go func() {
		proxy.Shutdown()
		close(finished)
	}()

	if listener != nil {
		listener.Close() //nolint: errcheck
	}

	select {
	case <-finished:
	case <-time.After(acceptResilienceDeadline):
		t.Fatalf("Shutdown did not return within %s", acceptResilienceDeadline)
	}

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Serve must stop quietly on Shutdown, got %v", err)
		}
	case <-time.After(acceptResilienceDeadline):
		t.Fatalf("Serve did not return within %s after Shutdown", acceptResilienceDeadline)
	}
}

func TestServeKeepsAcceptingAfterTransientAcceptError(t *testing.T) {
	t.Parallel()

	cases := map[string]error{
		"EMFILE":       acceptOpError(syscall.EMFILE),
		"ENFILE":       acceptOpError(syscall.ENFILE),
		"ENOBUFS":      acceptOpError(syscall.ENOBUFS),
		"ENOMEM":       acceptOpError(syscall.ENOMEM),
		"ECONNABORTED": acceptOpError(syscall.ECONNABORTED),
		"ECONNRESET":   acceptOpError(syscall.ECONNRESET),
		"timeout":      &net.OpError{Op: "accept", Net: "tcp", Err: acceptTimeoutError{}},
	}

	if runtime.GOOS == "linux" {
		cases["EPROTO"] = acceptOpError(syscall.EPROTO)
		cases["ENETUNREACH"] = acceptOpError(syscall.ENETUNREACH)
	}

	for name, transient := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			listener := newFlakyListener(t, []error{transient, transient}, nil)
			proxy := newAcceptResilienceProxy(t)
			done := serveInBackground(proxy, listener)

			client, err := net.DialTimeout("tcp", listener.Addr().String(), acceptResilienceDeadline)
			if err != nil {
				t.Fatalf("cannot dial: %v", err)
			}
			defer client.Close() //nolint: errcheck

			select {
			case err := <-done:
				t.Fatalf("Serve stopped after a transient accept error (%s): %v", name, err)
			case <-listener.served:
			case <-time.After(acceptResilienceDeadline):
				t.Fatalf("no connection was accepted within %s after a transient accept error", acceptResilienceDeadline)
			}

			client.SetReadDeadline(time.Now().Add(acceptResilienceDeadline)) //nolint: errcheck

			_, err = client.Read(make([]byte, 1))

			var netErr net.Error
			if err == nil || (errors.As(err, &netErr) && netErr.Timeout()) {
				t.Fatalf("the proxy did not serve the connection accepted after the transient error: %v", err)
			}

			stopProxy(t, proxy, listener, done)
		})
	}
}

func TestServeStopsOnPermanentAcceptError(t *testing.T) {
	t.Parallel()

	permanent := errors.New("listener is broken")
	listener := newFlakyListener(t, []error{permanent}, nil)
	proxy := newAcceptResilienceProxy(t)

	select {
	case err := <-serveInBackground(proxy, listener):
		if !errors.Is(err, permanent) {
			t.Fatalf("Serve must report the permanent accept error, got %v", err)
		}
	case <-time.After(acceptResilienceDeadline):
		t.Fatalf("Serve kept running after a permanent accept error")
	}

	finished := make(chan struct{})

	go func() {
		proxy.Shutdown()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(acceptResilienceDeadline):
		t.Fatalf("Shutdown did not return within %s", acceptResilienceDeadline)
	}
}

func TestShutdownInterruptsAcceptRetryPause(t *testing.T) {
	t.Parallel()

	listener := newFlakyListener(t, nil, acceptOpError(syscall.EMFILE))
	proxy := newAcceptResilienceProxy(t)
	done := serveInBackground(proxy, listener)

	time.Sleep(1500 * time.Millisecond)

	select {
	case err := <-done:
		t.Fatalf("Serve stopped while descriptors were exhausted: %v", err)
	default:
	}

	started := time.Now()

	stopProxy(t, proxy, nil, done)

	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("Shutdown waited %v for the accept retry pause", elapsed)
	}
}

func TestServeAcceptErrorsInterleavedWithClientsAndShutdown(t *testing.T) {
	t.Parallel()

	const clientsPerRound = 16

	for round := range 20 {
		failures := make([]error, 0, 4*clientsPerRound)
		for i := range 4 * clientsPerRound {
			switch i % 4 {
			case 0:
				failures = append(failures, acceptOpError(syscall.EMFILE))
			case 1:
				failures = append(failures, acceptOpError(syscall.ECONNABORTED))
			default:
				failures = append(failures, nil)
			}
		}

		listener := newFlakyListener(t, failures, nil)
		proxy := newAcceptResilienceProxy(t)
		done := serveInBackground(proxy, listener)

		var clients sync.WaitGroup

		clientConns := make(chan net.Conn, clientsPerRound)

		for range clientsPerRound {
			clients.Go(func() {
				conn, err := net.DialTimeout("tcp", listener.Addr().String(), acceptResilienceDeadline)
				if err == nil {
					clientConns <- conn
				}
			})
		}

		if round%2 == 0 {
			deadline := time.Now().Add(acceptResilienceDeadline)
			for listener.accepted.Load() < clientsPerRound && time.Now().Before(deadline) {
				select {
				case err := <-done:
					t.Fatalf("round %d: Serve stopped on a transient accept error: %v", round, err)
				case <-time.After(5 * time.Millisecond):
				}
			}

			if accepted := listener.accepted.Load(); accepted < clientsPerRound {
				t.Fatalf("round %d: only %d of %d clients were accepted", round, accepted, clientsPerRound)
			}
		} else {
			time.Sleep(time.Duration(round%5) * 7 * time.Millisecond)
		}

		stopProxy(t, proxy, listener, done)

		close(listener.served)

		for conn := range listener.served {
			if _, err := conn.Write([]byte{0}); err == nil {
				t.Errorf("round %d: Shutdown returned while an accepted connection was still open", round)
			}
		}

		clients.Wait()
		close(clientConns)

		for conn := range clientConns {
			conn.SetReadDeadline(time.Now().Add(acceptResilienceDeadline)) //nolint: errcheck

			if _, err := conn.Read(make([]byte, 1)); err == nil {
				t.Errorf("round %d: a client got data from a proxy that was shut down", round)
			}

			conn.Close() //nolint: errcheck
		}
	}
}

type serveProbeListener struct {
	calls atomic.Int64
}

func (l *serveProbeListener) Accept() (net.Conn, error) {
	l.calls.Add(1)

	return nil, net.ErrClosed
}

func (l *serveProbeListener) Close() error   { return nil }
func (l *serveProbeListener) Addr() net.Addr { return &net.TCPAddr{} }

func waitUntilProxyRefusesStreams(t *testing.T, proxy *mtglib.Proxy) {
	t.Helper()

	deadline := time.Now().Add(acceptResilienceDeadline)

	for time.Now().Before(deadline) {
		probe := &serveProbeListener{}
		proxy.Serve(probe) //nolint: errcheck

		if probe.calls.Load() == 0 {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Errorf("the proxy kept accepting new streams %s after Shutdown started", acceptResilienceDeadline)
}

type acceptDuringShutdownListener struct {
	net.Listener

	t        *testing.T
	proxy    *mtglib.Proxy
	served   chan net.Conn
	shutdown chan struct{}
}

func (l *acceptDuringShutdownListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err //nolint: wrapcheck
	}

	go func() {
		l.proxy.Shutdown()
		close(l.shutdown)
	}()

	waitUntilProxyRefusesStreams(l.t, l.proxy)

	l.served <- conn

	return conn, nil
}

func TestServeClosesConnectionAcceptedDuringShutdown(t *testing.T) {
	t.Parallel()

	inner, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}

	proxy := newAcceptResilienceProxy(t)
	listener := &acceptDuringShutdownListener{
		Listener: inner,
		t:        t,
		proxy:    proxy,
		served:   make(chan net.Conn, 1),
		shutdown: make(chan struct{}),
	}

	t.Cleanup(func() { listener.Close() }) //nolint: errcheck

	done := serveInBackground(proxy, listener)

	client, err := net.DialTimeout("tcp", listener.Addr().String(), acceptResilienceDeadline)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer client.Close() //nolint: errcheck

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve must stop quietly when Shutdown starts during Accept, got %v", err)
		}
	case <-time.After(acceptResilienceDeadline):
		t.Fatalf("Serve did not return within %s after Shutdown started", acceptResilienceDeadline)
	}

	select {
	case <-listener.shutdown:
	case <-time.After(acceptResilienceDeadline):
		t.Fatalf("Shutdown did not return within %s", acceptResilienceDeadline)
	}

	server := <-listener.served

	if _, err := server.Write([]byte{0}); err == nil {
		t.Fatal("a connection accepted while the proxy was shutting down was left open")
	}
}
