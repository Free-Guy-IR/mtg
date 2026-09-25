package doppel

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/9seconds/mtg/v2/essentials"
	"github.com/9seconds/mtg/v2/mtglib/internal/recovery"
	"github.com/9seconds/mtg/v2/mtglib/internal/tls"
)

var doppelBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, tls.MaxRecordSize)
		return &b
	},
}

var beforeWrittenRead = func() {}

var errWriterPanicked = errors.New("doppelganger writer panicked")

type Conn struct {
	essentials.Conn

	p *connPayload
}

type connPayload struct {
	ctx         context.Context
	ctxCancel   context.CancelCauseFunc
	stats       Stats
	wg          sync.WaitGroup
	writeStream bytes.Buffer
	writtenCond sync.Cond
	done        bool
}

func (c Conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, context.Cause(c.p.ctx)
	}

	c.p.writtenCond.L.Lock()
	c.p.writeStream.Write(p)
	c.p.writtenCond.L.Unlock()

	c.p.writtenCond.Signal()

	return len(p), context.Cause(c.p.ctx)
}

func (c Conn) start() {
	bp := doppelBufPool.Get().(*[]byte)
	buf := *bp
	defer doppelBufPool.Put(bp)

	timer := time.NewTimer(c.p.stats.Delay())
	defer timer.Stop()

	for {
		select {
		case <-c.p.ctx.Done():
			return
		case <-timer.C:
			timer.Reset(c.p.stats.Delay())
		}

		size := c.p.stats.Size()

		n := c.takeWritten(buf[tls.SizeHeader : tls.SizeHeader+size])
		if n == 0 {
			continue
		}

		if err := tls.WriteRecordInPlace(c.Conn, buf, n); err != nil {
			c.p.ctxCancel(err)
			return
		}
	}
}

func (c Conn) takeWritten(buf []byte) int {
	c.p.writtenCond.L.Lock()
	defer c.p.writtenCond.L.Unlock()

	for c.p.writeStream.Len() == 0 && !c.p.done {
		c.p.writtenCond.Wait()
	}

	beforeWrittenRead()

	n, _ := c.p.writeStream.Read(buf)

	return n
}

func (c Conn) abandon() {
	c.p.ctxCancel(errWriterPanicked)
	c.Conn.Close() //nolint: errcheck
}

func (c Conn) Stop() {
	c.p.ctxCancel(nil)

	c.p.writtenCond.L.Lock()
	c.p.done = true
	c.p.writtenCond.L.Unlock()
	c.p.writtenCond.Broadcast()

	c.p.wg.Wait()
}

func NewConn(ctx context.Context, logger Logger, conn essentials.Conn, stats Stats) Conn {
	ctx, cancel := context.WithCancelCause(ctx)
	rv := Conn{
		Conn: conn,
		p: &connPayload{
			ctx:       ctx,
			ctxCancel: cancel,
			stats:     stats,
			writtenCond: sync.Cond{
				L: &sync.Mutex{},
			},
		},
	}

	rv.p.writeStream.Grow(tls.DefaultBufferSize)

	rv.p.wg.Go(func() {
		defer recovery.Guard(logger, "doppelganger writer", rv.abandon)

		rv.start()
	})

	return rv
}
