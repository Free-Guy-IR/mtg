package mtglib

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// scriptedListener returns the scripted results of Accept in order and then
// net.ErrClosed.
type scriptedListener struct {
	mu      sync.Mutex
	results []error
	calls   int
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.calls++

	if len(l.results) == 0 {
		return nil, net.ErrClosed
	}

	err := l.results[0]
	l.results = l.results[1:]

	if err != nil {
		return nil, err
	}

	client, server := net.Pipe()
	client.Close() //nolint: errcheck

	return server, nil
}

func (l *scriptedListener) Close() error   { return nil }
func (l *scriptedListener) Addr() net.Addr { return &net.TCPAddr{} }

type failingListener struct {
	err      error
	calls    atomic.Int64
	notifyAt int64
	notify   chan struct{}
}

func (l *failingListener) Accept() (net.Conn, error) {
	if l.calls.Add(1) == l.notifyAt {
		close(l.notify)
	}

	return nil, l.err
}

func (l *failingListener) Close() error   { return nil }
func (l *failingListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestIsTemporaryAcceptError(t *testing.T) {
	t.Parallel()

	temporary := []error{
		syscall.EMFILE,
		fmt.Errorf("accept tcp: %w", syscall.ENFILE),
		&net.OpError{Op: "accept", Err: syscall.ECONNABORTED},
		syscall.ENOBUFS,
	}
	for _, err := range temporary {
		if !isTemporaryAcceptError(err) {
			t.Errorf("%v must be temporary", err)
		}
	}

	permanent := []error{nil, net.ErrClosed, fmt.Errorf("wrapped: %w", net.ErrClosed), errors.New("boom")}
	for _, err := range permanent {
		if isTemporaryAcceptError(err) {
			t.Errorf("%v must not be temporary", err)
		}
	}
}

func TestAcceptRetryDelayGrowsUpToOneSecond(t *testing.T) {
	t.Parallel()

	delay := acceptRetryDelay(0)
	if delay != acceptRetryDelayMin {
		t.Fatalf("first delay: got %v", delay)
	}

	for range 20 {
		delay = acceptRetryDelay(delay)
	}

	if delay != acceptRetryDelayMax {
		t.Fatalf("delay must be capped at %v, got %v", acceptRetryDelayMax, delay)
	}
}

// Running out of file descriptors under a connection flood must not stop the
// accept loop: the next connection is accepted once descriptors are available.
func TestAcceptWithRetrySurvivesTemporaryErrors(t *testing.T) {
	t.Parallel()

	listener := &scriptedListener{results: []error{syscall.EMFILE, syscall.EMFILE, nil}}

	conn, err := acceptWithRetry(context.Background(), listener, NoopLogger{})
	if err != nil {
		t.Fatalf("expected a connection after temporary errors, got %v", err)
	}

	conn.Close() //nolint: errcheck

	if listener.calls != 3 {
		t.Fatalf("expected 3 Accept calls, got %d", listener.calls)
	}
}

func TestAcceptWithRetryStopsOnClosedListener(t *testing.T) {
	t.Parallel()

	_, err := acceptWithRetry(context.Background(), &scriptedListener{}, NoopLogger{})
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected net.ErrClosed, got %v", err)
	}
}

func TestAcceptWithRetryStopsOnShutdownDuringPause(t *testing.T) {
	t.Parallel()

	errs := make([]error, 100)
	for i := range errs {
		errs[i] = syscall.EMFILE
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	start := time.Now()

	_, err := acceptWithRetry(ctx, &scriptedListener{results: errs}, NoopLogger{})
	if err == nil {
		t.Fatal("expected an error after shutdown")
	}

	if time.Since(start) > time.Second {
		t.Fatalf("shutdown must interrupt the retry pause, took %v", time.Since(start))
	}
}

func TestAcceptWithRetryPausesBetweenTemporaryErrors(t *testing.T) {
	t.Parallel()

	listener := &failingListener{err: syscall.EMFILE}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	if _, err := acceptWithRetry(ctx, listener, NoopLogger{}); err == nil {
		t.Fatal("expected an error once the context is done")
	}

	calls := listener.calls.Load()
	if calls < 2 {
		t.Fatalf("EMFILE must be retried, got %d Accept calls in 300ms", calls)
	}

	if calls > 20 {
		t.Fatalf("EMFILE must be retried after a pause, got %d Accept calls in 300ms", calls)
	}
}

func TestAcceptWithRetryShutdownInterruptsTheLongestPause(t *testing.T) {
	t.Parallel()

	const firstCallBeforeOneSecondPause = 9

	listener := &failingListener{
		err:      syscall.EMFILE,
		notifyAt: firstCallBeforeOneSecondPause,
		notify:   make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := make(chan error, 1)

	go func() {
		_, err := acceptWithRetry(ctx, listener, NoopLogger{})
		result <- err
	}()

	select {
	case <-listener.notify:
	case <-time.After(10 * time.Second):
		t.Fatalf("Accept was not called %d times within 10s", firstCallBeforeOneSecondPause)
	}

	time.Sleep(50 * time.Millisecond)

	cancelled := time.Now()

	cancel()

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected an error after shutdown")
		}

		if elapsed := time.Since(cancelled); elapsed > 250*time.Millisecond {
			t.Fatalf("shutdown must interrupt the 1s retry pause, it returned after %v", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acceptWithRetry did not return within 5s after shutdown")
	}
}
