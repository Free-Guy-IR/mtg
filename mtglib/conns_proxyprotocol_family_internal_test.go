package mtglib

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/9seconds/mtg/v2/internal/testlib"
	"github.com/pires/go-proxyproto"
	"github.com/stretchr/testify/mock"
)

func writeRecovering(conn *connProxyProtocol, payload []byte) (n int, recovered any, err error) {
	defer func() {
		recovered = recover()
	}()

	n, err = conn.Write(payload)

	return n, recovered, err
}

func writeThroughProxyProtocol(t *testing.T, sourceAddr, targetAddr net.Addr) (*proxyproto.Header, []byte) {
	t.Helper()

	source := &testlib.EssentialsConnMock{}
	source.On("RemoteAddr").Return(sourceAddr)

	written := &bytes.Buffer{}
	target := &testlib.EssentialsConnMock{}
	target.On("RemoteAddr").Return(targetAddr)
	target.On("Write", mock.AnythingOfType("[]uint8")).
		Return(0, nil).
		Run(func(args mock.Arguments) {
			written.Write(args.Get(0).([]byte)) //nolint: forcetypeassert
		})

	conn := newConnProxyProtocol(source, target)

	n, recovered, err := writeRecovering(conn, []byte{1, 2, 3})
	if recovered != nil || err != nil {
		t.Fatalf("Write failed: n=%d err=%v panic=%v", n, err, recovered)
	}

	reader := bufio.NewReader(written)

	header, err := proxyproto.Read(reader)
	if err != nil {
		t.Fatalf("cannot read the PROXY header back: %v", err)
	}

	payload, _ := io.ReadAll(reader)

	return header, payload
}

func TestConnProxyProtocolWriteMapsAnIPv4ClientForAnIPv6Target(t *testing.T) {
	t.Parallel()

	sourceAddr := &net.TCPAddr{IP: net.ParseIP("192.0.2.10").To4(), Port: 40000}
	targetAddr := &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}

	header, payload := writeThroughProxyProtocol(t, sourceAddr, targetAddr)

	if header.TransportProtocol != proxyproto.TCPv6 {
		t.Fatalf("header transport is %v, want TCPv6", header.TransportProtocol)
	}

	gotSource, gotTarget, ok := header.TCPAddrs()
	if !ok {
		t.Fatalf("header carries no TCP addresses")
	}

	if !gotSource.IP.Equal(net.ParseIP("::ffff:192.0.2.10")) || gotSource.Port != sourceAddr.Port {
		t.Fatalf("header source is %v, want the IPv4-mapped client [::ffff:192.0.2.10]:%d", gotSource, sourceAddr.Port)
	}

	if !gotTarget.IP.Equal(targetAddr.IP) || gotTarget.Port != targetAddr.Port {
		t.Fatalf("header target is %v, want %v", gotTarget, targetAddr)
	}

	if !bytes.Equal(payload, []byte{1, 2, 3}) {
		t.Fatalf("payload after the header is %v", payload)
	}
}

func TestConnProxyProtocolWriteMapsAnIPv4TargetForAnIPv6Client(t *testing.T) {
	t.Parallel()

	sourceAddr := &net.TCPAddr{IP: net.ParseIP("2001:db8::10"), Port: 40000}
	targetAddr := &net.TCPAddr{IP: net.ParseIP("192.0.2.1").To4(), Port: 443}

	header, payload := writeThroughProxyProtocol(t, sourceAddr, targetAddr)

	gotSource, gotTarget, ok := header.TCPAddrs()
	if !ok || !gotSource.IP.Equal(sourceAddr.IP) || !gotTarget.IP.Equal(targetAddr.IP) {
		t.Fatalf("header carries %v -> %v, want %v -> %v", gotSource, gotTarget, sourceAddr, targetAddr)
	}

	if !bytes.Equal(payload, []byte{1, 2, 3}) {
		t.Fatalf("payload after the header is %v", payload)
	}
}

func TestConnProxyProtocolWriteReturnsAnErrorForAHeaderItCannotEncode(t *testing.T) {
	t.Parallel()

	source := &testlib.EssentialsConnMock{}
	source.On("RemoteAddr").Return(&net.TCPAddr{IP: net.ParseIP("192.0.2.10").To4(), Port: 40000})

	target := &testlib.EssentialsConnMock{}
	target.On("RemoteAddr").Return(&net.TCPAddr{Port: 443})

	conn := newConnProxyProtocol(source, target)

	for attempt := range 2 {
		n, recovered, err := writeRecovering(conn, []byte{1, 2, 3})
		if recovered != nil {
			t.Fatalf("attempt %d: Write panicked on a header it cannot encode: %v", attempt, recovered)
		}

		if !errors.Is(err, proxyproto.ErrInvalidAddress) {
			t.Fatalf("attempt %d: Write returned %v, want an error wrapping %v", attempt, err, proxyproto.ErrInvalidAddress)
		}

		if n != 0 {
			t.Fatalf("attempt %d: Write reported %d bytes written", attempt, n)
		}
	}

	target.AssertNotCalled(t, "Write", mock.Anything)
}
