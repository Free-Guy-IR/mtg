package mtglib

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

type fakeConn struct{ r io.Reader }

func (f *fakeConn) Read(p []byte) (int, error)         { return f.r.Read(p) }
func (f *fakeConn) Write(p []byte) (int, error)        { return len(p), nil }
func (f *fakeConn) Close() error                       { return nil }
func (f *fakeConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (f *fakeConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (f *fakeConn) SetDeadline(t time.Time) error      { return nil }
func (f *fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (f *fakeConn) SetWriteDeadline(t time.Time) error { return nil }
func (f *fakeConn) CloseRead() error                   { return nil }
func (f *fakeConn) CloseWrite() error                  { return nil }

func TestConnRewindReplaysManyTimes(t *testing.T) {
	payload := []byte("CLIENT-HELLO-PAYLOAD-0123456789")

	conn := newConnRewind(&fakeConn{r: bytes.NewReader(payload)})

	first := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, first); err != nil {
		t.Fatalf("first read: %v", err)
	}

	if !bytes.Equal(first, payload) {
		t.Fatalf("first read got %q, want %q", first, payload)
	}

	for attempt := 2; attempt <= 50; attempt++ {
		conn.Rewind()

		got := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("attempt %d: %v (a consuming rewind loses the bytes after the first replay)", attempt, err)
		}

		if !bytes.Equal(got, payload) {
			t.Fatalf("attempt %d got %q, want %q", attempt, got, payload)
		}
	}
}
