package doppel

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const panicTestDeadline = 5 * time.Second

type panicTestLogger struct {
	mu        sync.Mutex
	warnings  []string
	onWarning func()
}

func (l *panicTestLogger) Info(string) {}

func (l *panicTestLogger) WarningError(msg string, err error) {
	l.mu.Lock()
	l.warnings = append(l.warnings, msg+": "+err.Error())
	onWarning := l.onWarning
	l.mu.Unlock()

	if onWarning != nil {
		onWarning()
	}
}

func (l *panicTestLogger) logged(value string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	count := 0

	for _, warning := range l.warnings {
		if strings.Contains(warning, value) && strings.Contains(warning, "goroutine ") {
			count++
		}
	}

	return count
}

func (l *panicTestLogger) waitForPanic(t *testing.T, value string, times int) {
	t.Helper()

	deadline := time.Now().Add(panicTestDeadline)

	for l.logged(value) < times {
		if time.Now().After(deadline) {
			t.Fatalf("the panic %q with its stack was not logged %d times within %s", value, times, panicTestDeadline)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

type panicTestNetwork struct {
	SimpleNetwork
}

func (panicTestNetwork) NativeDialer() *net.Dialer {
	panic("injected scout panic")
}

type panicTestConn struct {
	closed       chan struct{}
	closeOnce    sync.Once
	panicOnWrite atomic.Bool
}

func newPanicTestConn() *panicTestConn {
	return &panicTestConn{closed: make(chan struct{})}
}

func (c *panicTestConn) Read([]byte) (int, error) {
	<-c.closed

	return 0, net.ErrClosed
}

func (c *panicTestConn) Write(p []byte) (int, error) {
	if c.panicOnWrite.Load() {
		panic("injected doppel conn write panic")
	}

	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
		return len(p), nil
	}
}

func (c *panicTestConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})

	return nil
}

func (c *panicTestConn) CloseRead() error                 { return nil }
func (c *panicTestConn) CloseWrite() error                { return nil }
func (c *panicTestConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *panicTestConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *panicTestConn) SetDeadline(time.Time) error      { return nil }
func (c *panicTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *panicTestConn) SetWriteDeadline(time.Time) error { return nil }

func panicTestWithin(t *testing.T, what string, fn func()) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		defer close(done)

		fn()
	}()

	select {
	case <-done:
	case <-time.After(panicTestDeadline):
		t.Fatalf("%s did not return within %s", what, panicTestDeadline)
	}
}

func panicTestNewConn(t *testing.T, g *Ganger, conn *panicTestConn) Conn {
	t.Helper()

	var (
		rv  Conn
		err error
	)

	panicTestWithin(t, "Ganger.NewConn", func() {
		rv, err = g.NewConn(conn)
	})

	if err != nil {
		t.Fatalf("Ganger.NewConn failed: %v", err)
	}

	return rv
}

func panicTestWritesFail(t *testing.T, conn Conn) {
	t.Helper()

	deadline := time.Now().Add(panicTestDeadline)

	for {
		var err error

		panicTestWithin(t, "Conn.Write", func() {
			_, err = conn.Write([]byte{1, 2, 3})
		})

		if err != nil {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("writes kept succeeding after the doppelganger writer panicked")
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func TestGangerSurvivesAPanicInAScoutRaid(t *testing.T) {
	logger := &panicTestLogger{}
	g := NewGanger(context.Background(), panicTestNetwork{}, logger, time.Hour, 1, []string{"https://127.0.0.1:9"}, false)
	g.Run()

	logger.waitForPanic(t, "injected scout panic", 1)

	conn := panicTestNewConn(t, g, newPanicTestConn())

	panicTestWithin(t, "Conn.Stop", conn.Stop)
	panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)
}

func TestGangerSurvivesAPanicInItsRunLoopAndStillShutsDown(t *testing.T) {
	logger := &panicTestLogger{}
	g := NewGanger(context.Background(), SimpleNetwork{}, logger, -time.Second, 1, nil, false)
	g.Run()

	logger.waitForPanic(t, "non-positive interval", 1)

	panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)
}

func TestGangerRestartsItsRunLoopAfterAPanic(t *testing.T) {
	logger := &panicTestLogger{}
	g := NewGanger(context.Background(), SimpleNetwork{}, logger, -time.Second, 1, nil, false)
	logger.onWarning = func() {
		g.scoutRaidEach = time.Hour
	}
	g.Run()

	logger.waitForPanic(t, "non-positive interval", 1)

	conn := panicTestNewConn(t, g, newPanicTestConn())

	panicTestWithin(t, "Conn.Stop", conn.Stop)
	panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)
}

type panicTestRestarts struct {
	mu    sync.Mutex
	times []time.Time
}

func (r *panicTestRestarts) record() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.times = append(r.times, time.Now())
}

func (r *panicTestRestarts) recorded() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.times)
}

func (r *panicTestRestarts) waitFor(t *testing.T, count int, deadline time.Duration) []time.Duration {
	t.Helper()

	until := time.Now().Add(deadline)

	for {
		r.mu.Lock()
		times := append([]time.Time(nil), r.times...)
		r.mu.Unlock()

		if len(times) >= count {
			gaps := make([]time.Duration, 0, count-1)
			for i := 1; i < count; i++ {
				gaps = append(gaps, times[i].Sub(times[i-1]))
			}

			return gaps
		}

		if time.Now().After(until) {
			t.Fatalf("the run loop panicked %d times within %s, want %d", len(times), deadline, count)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func TestGangerBacksOffRestartingARunLoopThatKeepsPanicking(t *testing.T) {
	restarts := &panicTestRestarts{}
	logger := &panicTestLogger{onWarning: restarts.record}
	g := NewGanger(context.Background(), SimpleNetwork{}, logger, -time.Second, 1, nil, false)
	g.Run()

	gaps := restarts.waitFor(t, 3, 5*gangerRestartDelay)

	panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)

	if gaps[0] < gangerRestartDelay || gaps[1] < 2*gangerRestartDelay {
		t.Fatalf("a run loop that keeps panicking was restarted after %v, want a delay that doubles from %s", gaps, gangerRestartDelay)
	}
}

func TestGangerCapsItsRestartBackoffAndResetsItAfterALongRun(t *testing.T) {
	const (
		base    = 25 * time.Millisecond
		ceiling = 200 * time.Millisecond
		longRun = 250 * time.Millisecond
	)

	restarts := &panicTestRestarts{}
	logger := &panicTestLogger{}
	logger.onWarning = func() {
		restarts.record()

		if restarts.recorded() == 6 {
			time.Sleep(longRun)
		}
	}

	g := NewGanger(context.Background(), SimpleNetwork{}, logger, -time.Second, 1, nil, false)
	g.restartDelay = base
	g.restartMaxDelay = ceiling
	g.Run()

	gaps := restarts.waitFor(t, 7, panicTestDeadline)

	panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)

	for i, want := range []time.Duration{base, 2 * base, 4 * base, ceiling, ceiling} {
		if gaps[i] < want {
			t.Fatalf("restart %d came after %s, want at least %s, gaps %v", i+1, gaps[i], want, gaps)
		}
	}

	if gaps[4] >= 2*ceiling {
		t.Fatalf("the restart delay grew past its ceiling %s: gaps %v", ceiling, gaps)
	}

	if afterLongRun := gaps[5] - longRun; afterLongRun >= ceiling {
		t.Fatalf("a run that lasted %s did not reset the restart delay: waited %s, gaps %v", longRun, afterLongRun, gaps)
	}
}

func panicTestChattyTLSServer(t *testing.T) *httptest.Server {
	t.Helper()

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher, _ := w.(http.Flusher)

		for range 2 * MinDurationsToCalculate {
			if _, err := w.Write([]byte{1}); err != nil {
				return
			}

			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	t.Cleanup(server.Close)

	return server
}

func TestGangerKeepsLearningAfterAPanicWhileComputingStats(t *testing.T) {
	server := panicTestChattyTLSServer(t)
	logger := &panicTestLogger{}
	g := NewGanger(context.Background(), SimpleNetwork{}, logger, 20*time.Millisecond, 1, []string{server.URL}, false)
	defaults := g.stats

	computed := make(chan struct{}, 1)
	release := make(chan struct{})
	calls := atomic.Int32{}

	g.newStats = func(durations []time.Duration, drs bool) Stats {
		switch calls.Add(1) {
		case 1:
			panic("injected doppel stats panic")
		case 2:
			computed <- struct{}{}
			<-release
		}

		return NewStats(durations, drs)
	}

	g.Run()

	defer func() {
		panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)
	}()
	defer close(release)

	logger.waitForPanic(t, "injected doppel stats panic", 1)

	select {
	case <-computed:
	case <-time.After(panicTestDeadline):
		t.Fatalf("the ganger stopped collecting scout results after a panic while computing stats")
	}

	conn := panicTestNewConn(t, g, newPanicTestConn())
	panicTestWithin(t, "Conn.Stop", conn.Stop)

	got := conn.p.stats
	if got.k != defaults.k || got.lambda != defaults.lambda || got.drs != defaults.drs {
		t.Fatalf("a panic while computing stats replaced the stats %+v with %+v", defaults, got)
	}
}

func TestDoppelWriterPanicWhileHoldingTheBufferLockDoesNotHangWritersOrShutdown(t *testing.T) {
	previous := beforeWrittenRead
	fired := atomic.Bool{}
	beforeWrittenRead = func() {
		if fired.CompareAndSwap(false, true) {
			panic("injected doppel writer panic")
		}
	}

	t.Cleanup(func() {
		beforeWrittenRead = previous
	})

	logger := &panicTestLogger{}
	g := NewGanger(context.Background(), SimpleNetwork{}, logger, time.Hour, 1, nil, false)
	g.Run()

	conn := panicTestNewConn(t, g, newPanicTestConn())

	panicTestWithin(t, "Conn.Write", func() {
		conn.Write([]byte{1, 2, 3}) //nolint: errcheck
	})

	logger.waitForPanic(t, "injected doppel writer panic", 1)
	panicTestWritesFail(t, conn)

	panicTestWithin(t, "Conn.Stop", conn.Stop)
	panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)
}

func TestDoppelWriterPanicInTheUnderlyingWriteDoesNotHangWritersOrShutdown(t *testing.T) {
	logger := &panicTestLogger{}
	g := NewGanger(context.Background(), SimpleNetwork{}, logger, time.Hour, 1, nil, false)
	g.Run()

	underlying := newPanicTestConn()
	underlying.panicOnWrite.Store(true)
	conn := panicTestNewConn(t, g, underlying)

	panicTestWithin(t, "Conn.Write", func() {
		conn.Write([]byte{1, 2, 3}) //nolint: errcheck
	})

	logger.waitForPanic(t, "injected doppel conn write panic", 1)
	panicTestWritesFail(t, conn)

	panicTestWithin(t, "Conn.Stop", conn.Stop)
	panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)
}
