package doppel

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func gangerWriterStacks(frame string) (int, string) {
	buf := make([]byte, 1<<24)
	n := runtime.Stack(buf, true)

	count := 0
	first := ""

	for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
		if !strings.Contains(stack, frame) {
			continue
		}

		count++

		if first == "" {
			first = stack
		}
	}

	return count, first
}

func gangerWaitForWriters(t *testing.T, frame string, atLeast int) {
	t.Helper()

	deadline := time.Now().Add(panicTestDeadline)

	for {
		count, _ := gangerWriterStacks(frame)
		if count >= atLeast {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines in %s, want at least %d within %s", count, frame, atLeast, panicTestDeadline)
		}

		time.Sleep(time.Millisecond)
	}
}

func gangerAssertNoWriterOutlives(t *testing.T, baseline int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)

	for {
		count, stack := gangerWriterStacks("doppel.Conn.start")
		if count <= baseline {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%d doppelganger writer goroutines outlived Ganger.Shutdown and every requester, first:\n%s", count-baseline, stack)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func TestGangerNewConnRacingShutdownDoesNotPanic(t *testing.T) {
	const (
		rounds     = 3000
		requesters = 8
	)

	logger := &panicTestLogger{}

	for range rounds {
		ctx, cancel := context.WithCancel(context.Background())
		g := NewGanger(ctx, SimpleNetwork{}, logger, time.Hour, 1, nil, false)
		g.Run()

		var requested sync.WaitGroup

		for range requesters {
			requested.Go(func() {
				if conn, err := g.NewConn(newPanicTestConn()); err == nil {
					conn.Stop()
				}
			})
		}

		cancel()
		requested.Wait()

		panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)
	}

	if count := logger.logged("send on closed channel"); count > 0 {
		t.Fatalf("the run loop panicked with a send on a closed channel %d times in %d rounds", count, rounds)
	}
}

func TestGangerStopsTheConnItBuiltForARequesterThatLeftForShutdown(t *testing.T) {
	baseline, _ := gangerWriterStacks("doppel.Conn.start")
	parked, _ := gangerWriterStacks("doppel.Conn.takeWritten")

	g := NewGanger(context.Background(), SimpleNetwork{}, &panicTestLogger{}, time.Hour, 1, nil, false)
	g.Run()

	abandoned := gangerConnRequest{
		ret:     make(chan Conn),
		payload: newPanicTestConn(),
	}

	panicTestWithin(t, "handing a request to the run loop", func() {
		g.connRequests <- abandoned
	})

	gangerWaitForWriters(t, "doppel.Conn.takeWritten", parked+1)

	panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)

	gangerAssertNoWriterOutlives(t, baseline)
}

func TestNoDoppelWriterOutlivesGangerShutdown(t *testing.T) {
	const (
		rounds     = 3000
		requesters = 8
	)

	logger := &panicTestLogger{}
	baseline, _ := gangerWriterStacks("doppel.Conn.start")

	for range rounds {
		ctx, cancel := context.WithCancel(context.Background())
		g := NewGanger(ctx, SimpleNetwork{}, logger, time.Hour, 1, nil, false)
		g.Run()

		var requested sync.WaitGroup

		for range requesters {
			requested.Go(func() {
				if conn, err := g.NewConn(newPanicTestConn()); err == nil {
					conn.Stop()
				}
			})
		}

		cancel()
		requested.Wait()

		panicTestWithin(t, "Ganger.Shutdown", g.Shutdown)
	}

	gangerAssertNoWriterOutlives(t, baseline)
}
