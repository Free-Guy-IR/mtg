package mtglib_test

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/antireplay"
	"github.com/9seconds/mtg/v2/ipblocklist"
	"github.com/9seconds/mtg/v2/logger"
	"github.com/9seconds/mtg/v2/mtglib"
)

const proxyPanicDeadline = 5 * time.Second

type proxyPanicLogger struct {
	mtglib.Logger

	mu       *sync.Mutex
	warnings *[]string
}

func newProxyPanicLogger() proxyPanicLogger {
	return proxyPanicLogger{
		Logger:   logger.NewNoopLogger(),
		mu:       &sync.Mutex{},
		warnings: &[]string{},
	}
}

func (l proxyPanicLogger) Named(string) mtglib.Logger            { return l }
func (l proxyPanicLogger) BindInt(string, int) mtglib.Logger     { return l }
func (l proxyPanicLogger) BindStr(string, string) mtglib.Logger  { return l }
func (l proxyPanicLogger) BindJSON(string, string) mtglib.Logger { return l }

func (l proxyPanicLogger) WarningError(msg string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	*l.warnings = append(*l.warnings, msg+": "+err.Error())
}

func (l proxyPanicLogger) logged(value string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, warning := range *l.warnings {
		if strings.Contains(warning, value) && strings.Contains(warning, "goroutine ") {
			return true
		}
	}

	return false
}

func (l proxyPanicLogger) expectPanicLogged(t *testing.T, value string) {
	t.Helper()

	deadline := time.Now().Add(proxyPanicDeadline)

	for !l.logged(value) {
		if time.Now().After(deadline) {
			t.Fatalf("no warning with the panic %q and its stack was logged within %s", value, proxyPanicDeadline)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

type proxyPanicStarts struct {
	started atomic.Int64
}

func (s *proxyPanicStarts) Send(_ context.Context, evt mtglib.Event) {
	if _, ok := evt.(mtglib.EventStart); ok {
		s.started.Add(1)
	}
}

type proxyPanicConn struct {
	closed       chan struct{}
	closeOnce    sync.Once
	panicOnClose atomic.Bool
}

func (c *proxyPanicConn) Read([]byte) (int, error) {
	<-c.closed

	return 0, io.EOF
}

func (c *proxyPanicConn) Write(p []byte) (int, error) { return len(p), nil }

func (c *proxyPanicConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})

	if c.panicOnClose.CompareAndSwap(true, false) {
		panic("injected stream close panic")
	}

	return nil
}

func (c *proxyPanicConn) CloseRead() error  { return nil }
func (c *proxyPanicConn) CloseWrite() error { return nil }
func (c *proxyPanicConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443}
}

func (c *proxyPanicConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 50000}
}

func (c *proxyPanicConn) SetDeadline(time.Time) error      { return nil }
func (c *proxyPanicConn) SetReadDeadline(time.Time) error  { return nil }
func (c *proxyPanicConn) SetWriteDeadline(time.Time) error { return nil }

func TestShutdownSurvivesAPanicWhileClosingAStream(t *testing.T) {
	run := newShutdownRaceRun(t)
	panicLogger := newProxyPanicLogger()
	starts := &proxyPanicStarts{}

	proxy, err := mtglib.NewProxy(mtglib.ProxyOpts{
		Secret:          mtglib.GenerateSecret("example.com"),
		PlainMode:       true,
		Network:         run.ntw,
		AntiReplayCache: antireplay.NewNoop(),
		IPBlocklist:     ipblocklist.NewNoop(),
		IPAllowlist:     shutdownRaceAllowAll{},
		EventStream:     starts,
		Logger:          panicLogger,
		UseTestDCs:      true,
	})
	if err != nil {
		t.Fatalf("cannot build proxy: %v", err)
	}

	conn := &proxyPanicConn{closed: make(chan struct{})}
	conn.panicOnClose.Store(true)

	served := make(chan any, 1)

	go func() {
		defer func() {
			served <- recover()
		}()

		proxy.ServeConn(conn)
	}()

	deadline := time.Now().Add(proxyPanicDeadline)
	for starts.started.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the stream did not start within %s", proxyPanicDeadline)
		}

		time.Sleep(5 * time.Millisecond)
	}

	if value := shutdownRaceShutdown(t, proxy); value != nil {
		t.Fatalf("Shutdown panicked: %v", value)
	}

	select {
	case value := <-served:
		if value != nil {
			t.Fatalf("ServeConn panicked: %v", value)
		}
	case <-time.After(proxyPanicDeadline):
		t.Fatalf("ServeConn did not return within %s", proxyPanicDeadline)
	}

	panicLogger.expectPanicLogged(t, "injected stream close panic")
}
