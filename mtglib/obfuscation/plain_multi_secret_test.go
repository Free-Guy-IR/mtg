package obfuscation

import (
	"bytes"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/essentials"
)

type frameConn struct {
	net.Conn
	r *bytes.Reader
}

func (f frameConn) Read(p []byte) (int, error)       { return f.r.Read(p) }
func (f frameConn) Write(p []byte) (int, error)      { return len(p), nil }
func (f frameConn) Close() error                     { return nil }
func (f frameConn) CloseRead() error                 { return nil }
func (f frameConn) CloseWrite() error                { return nil }
func (f frameConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (f frameConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (f frameConn) SetDeadline(time.Time) error      { return nil }
func (f frameConn) SetReadDeadline(time.Time) error  { return nil }
func (f frameConn) SetWriteDeadline(time.Time) error { return nil }

var _ essentials.Conn = frameConn{}

func TestHandshakeFromFrameMatchesOnlyOwner(t *testing.T) {
	secrets := make([][]byte, 50)
	for i := range secrets {
		secrets[i] = make([]byte, 16)
		if _, err := rand.Read(secrets[i]); err != nil {
			t.Fatal(err)
		}
	}

	owner := 37
	client := Obfuscator{Secret: secrets[owner]}

	var wire bytes.Buffer
	sink := frameConn{r: bytes.NewReader(nil)}
	captured := &captureConn{frameConn: sink, buf: &wire}
	if _, err := client.SendHandshake(captured, 4); err != nil {
		t.Fatal(err)
	}

	frame, err := ReadHandshakeFrame(bytes.NewReader(wire.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	matched := -1
	for i, s := range secrets {
		dc, _, ok := Obfuscator{Secret: s}.HandshakeFromFrame(frame, sink)
		if !ok {
			continue
		}
		if matched != -1 {
			t.Fatalf("secret %d also matched after %d", i, matched)
		}
		if dc != 4 {
			t.Fatalf("dc=%d want 4", dc)
		}
		matched = i
	}

	if matched != owner {
		t.Fatalf("matched=%d want %d", matched, owner)
	}
}

type captureConn struct {
	frameConn
	buf *bytes.Buffer
}

func (c *captureConn) Write(p []byte) (int, error) { return c.buf.Write(p) }
