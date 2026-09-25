package doppel

import (
	"context"
	"sync"
	"testing"
	"time"
)

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
