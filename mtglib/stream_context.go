package mtglib

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net"
	"sync"
	"time"

	"github.com/9seconds/mtg/v2/essentials"
)

type streamContext struct {
	ctx          context.Context
	ctxCancel    context.CancelFunc
	acceptedConn essentials.Conn
	clientConn   essentials.Conn
	telegramMu   sync.Mutex
	telegramConn essentials.Conn
	streamID     string
	dc           int
	logger       Logger

	// secretID is the key of the entry in Proxy.secrets that matched this
	// stream's ClientHello, set by doFakeTLSHandshake. Empty until a match
	// happens (and stays empty for streams that fall back to domain
	// fronting). Free-Guy-IR/PasarGuard addition.
	secretID string
}

func (s *streamContext) Deadline() (time.Time, bool) {
	return s.ctx.Deadline()
}

func (s *streamContext) Done() <-chan struct{} {
	return s.ctx.Done()
}

func (s *streamContext) Err() error {
	return s.ctx.Err() //nolint: wrapcheck
}

func (s *streamContext) Value(key any) any {
	return s.ctx.Value(key)
}

func (s *streamContext) Close() {
	s.ctxCancel()

	s.acceptedConn.Close() //nolint: errcheck

	if telegramConn := s.telegramConnection(); telegramConn != nil {
		telegramConn.Close() //nolint: errcheck
	}
}

func (s *streamContext) setTelegramConn(conn essentials.Conn) {
	s.telegramMu.Lock()
	defer s.telegramMu.Unlock()

	s.telegramConn = conn
}

func (s *streamContext) telegramConnection() essentials.Conn {
	s.telegramMu.Lock()
	defer s.telegramMu.Unlock()

	return s.telegramConn
}

func (s *streamContext) ClientIP() net.IP {
	return s.acceptedConn.RemoteAddr().(*net.TCPAddr).IP //nolint: forcetypeassert
}

func newStreamContext(ctx context.Context, logger Logger, clientConn essentials.Conn) *streamContext {
	connIDBytes := make([]byte, ConnectionIDBytesLength)

	if _, err := rand.Read(connIDBytes); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(ctx)
	streamCtx := &streamContext{
		ctx:          ctx,
		ctxCancel:    cancel,
		acceptedConn: clientConn,
		clientConn:   clientConn,
		streamID:     base64.RawURLEncoding.EncodeToString(connIDBytes),
	}
	streamCtx.logger = logger.
		BindStr("stream-id", streamCtx.streamID).
		BindStr("client-ip", streamCtx.ClientIP().String())

	return streamCtx
}
