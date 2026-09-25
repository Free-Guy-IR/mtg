package relay_test

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/mtglib/internal/relay"
)

const relayPanicDeadline = 5 * time.Second

type relayPanicConn struct {
	closed       chan struct{}
	closeOnce    sync.Once
	panicOnRead  bool
	panicOnClose atomic.Bool
}

func newRelayPanicConn() *relayPanicConn {
	return &relayPanicConn{closed: make(chan struct{})}
}

func (c *relayPanicConn) Read([]byte) (int, error) {
	if c.panicOnRead {
		panic("injected relay read panic")
	}

	<-c.closed

	return 0, io.EOF
}

func (c *relayPanicConn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
		return len(p), nil
	}
}

func (c *relayPanicConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})

	if c.panicOnClose.CompareAndSwap(true, false) {
		panic("injected relay close panic")
	}

	return nil
}

func (c *relayPanicConn) CloseRead() error                 { return nil }
func (c *relayPanicConn) CloseWrite() error                { return nil }
func (c *relayPanicConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *relayPanicConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *relayPanicConn) SetDeadline(time.Time) error      { return nil }
func (c *relayPanicConn) SetReadDeadline(time.Time) error  { return nil }
func (c *relayPanicConn) SetWriteDeadline(time.Time) error { return nil }

type relayPanicLogger struct {
	mu       sync.Mutex
	warnings []string
}

func (l *relayPanicLogger) Printf(string, ...any) {}

func (l *relayPanicLogger) WarningError(msg string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.warnings = append(l.warnings, msg+": "+err.Error())
}

func (l *relayPanicLogger) logged(value string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, warning := range l.warnings {
		if strings.Contains(warning, value) && strings.Contains(warning, "goroutine ") {
			return true
		}
	}

	return false
}

func (l *relayPanicLogger) expectPanicLogged(t *testing.T, value string) {
	t.Helper()

	deadline := time.Now().Add(relayPanicDeadline)

	for !l.logged(value) {
		if time.Now().After(deadline) {
			t.Fatalf("no warning with the panic %q and its stack was logged within %s", value, relayPanicDeadline)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func relayPanicRun(t *testing.T, ctx context.Context, logger relay.Logger, telegramConn, clientConn *relayPanicConn) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		defer close(done)

		relay.Relay(ctx, logger, telegramConn, clientConn)
	}()

	select {
	case <-done:
	case <-time.After(relayPanicDeadline):
		t.Fatalf("Relay did not return within %s", relayPanicDeadline)
	}
}

func TestRelaySurvivesAPanicInTheClientToTelegramPump(t *testing.T) {
	telegramConn := newRelayPanicConn()
	clientConn := newRelayPanicConn()
	clientConn.panicOnRead = true
	logger := &relayPanicLogger{}

	relayPanicRun(t, context.Background(), logger, telegramConn, clientConn)

	logger.expectPanicLogged(t, "injected relay read panic")
}

func TestRelaySurvivesAPanicWhileClosingOnCancellation(t *testing.T) {
	telegramConn := newRelayPanicConn()
	telegramConn.panicOnClose.Store(true)
	clientConn := newRelayPanicConn()
	logger := &relayPanicLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	relayPanicRun(t, ctx, logger, telegramConn, clientConn)

	logger.expectPanicLogged(t, "injected relay close panic")
}
