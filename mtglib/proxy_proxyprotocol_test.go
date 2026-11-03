package mtglib_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/antireplay"
	"github.com/9seconds/mtg/v2/events"
	"github.com/9seconds/mtg/v2/internal/proxyprotocol"
	"github.com/9seconds/mtg/v2/internal/utils"
	"github.com/9seconds/mtg/v2/ipblocklist"
	"github.com/9seconds/mtg/v2/logger"
	"github.com/9seconds/mtg/v2/mtglib"
	"github.com/9seconds/mtg/v2/network"
	"github.com/pires/go-proxyproto"
)

const proxyProtocolDeadline = 5 * time.Second

type recordingAllowlist struct {
	mu   sync.Mutex
	seen []string
	hit  chan string
}

func newRecordingAllowlist() *recordingAllowlist {
	return &recordingAllowlist{hit: make(chan string, 64)}
}

func (r *recordingAllowlist) Contains(ip net.IP) bool {
	r.mu.Lock()
	r.seen = append(r.seen, ip.String())
	r.mu.Unlock()

	r.hit <- ip.String()

	return false
}

func (r *recordingAllowlist) Run(time.Duration) {}
func (r *recordingAllowlist) Shutdown()         {}

func (r *recordingAllowlist) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.seen...)
}

type recordingEvents struct {
	mu     sync.Mutex
	events []mtglib.Event
}

func (r *recordingEvents) Send(_ context.Context, evt mtglib.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, evt)
}

func (r *recordingEvents) snapshot() []mtglib.Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]mtglib.Event(nil), r.events...)
}

func newProxyProtocolProxy(t *testing.T, allowlist mtglib.IPBlocklist, stream mtglib.EventStream) *mtglib.Proxy {
	t.Helper()

	dialer, err := network.NewDefaultDialer(0, 0)
	if err != nil {
		t.Fatalf("cannot build dialer: %v", err)
	}

	ntw, err := network.NewNetwork(dialer, "mtgtest", "1.1.1.1", 0)
	if err != nil {
		t.Fatalf("cannot build network: %v", err)
	}

	proxy, err := mtglib.NewProxy(mtglib.ProxyOpts{
		Secret:           mtglib.GenerateSecret("example.com"),
		PlainMode:        true,
		Network:          ntw,
		AntiReplayCache:  antireplay.NewNoop(),
		IPBlocklist:      ipblocklist.NewNoop(),
		IPAllowlist:      allowlist,
		EventStream:      stream,
		Logger:           logger.NewNoopLogger(),
		UseTestDCs:       true,
		HandshakeTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("cannot build proxy: %v", err)
	}

	return proxy
}

func newProxyProtocolListener(t *testing.T) net.Listener {
	t.Helper()

	base, err := utils.NewListener("127.0.0.1:0", 0, logger.NewNoopLogger())
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}

	listener := &proxyprotocol.ListenerAdapter{
		Listener: proxyproto.Listener{Listener: base},
	}

	t.Cleanup(func() { listener.Close() }) //nolint: errcheck

	return listener
}

func proxyHeaderBytes(t *testing.T, src, dst net.Addr) []byte {
	t.Helper()

	header := proxyproto.HeaderProxyFromAddrs(2, src, dst)
	if header.Command != proxyproto.PROXY {
		t.Fatalf("header for %T is not a PROXY command", src)
	}

	raw, err := header.Format()
	if err != nil {
		t.Fatalf("cannot format a proxy header: %v", err)
	}

	return raw
}

func rawProxyV2Header(family byte) []byte {
	raw := []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A, 0x21, family, 0x00, 0x0C}

	return append(raw, 192, 0, 2, 30, 192, 0, 2, 1, 0x0F, 0xA0, 0x01, 0xBB)
}

func sendRaw(t *testing.T, addr net.Addr, raw []byte) net.Conn {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr.String(), proxyProtocolDeadline)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}

	t.Cleanup(func() { conn.Close() }) //nolint: errcheck

	if _, err := conn.Write(raw); err != nil {
		t.Fatalf("cannot write a proxy header: %v", err)
	}

	return conn
}

func assertClosedByProxy(t *testing.T, conn net.Conn) {
	t.Helper()

	conn.SetReadDeadline(time.Now().Add(proxyProtocolDeadline)) //nolint: errcheck

	_, err := conn.Read(make([]byte, 1))

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("the proxy kept a connection open: %v", err)
	}

	if err == nil {
		t.Fatalf("the proxy sent data to a connection it must reject")
	}
}

type serveOutcome struct {
	err       error
	recovered any
}

func serveRecovering(proxy *mtglib.Proxy, listener net.Listener) <-chan serveOutcome {
	done := make(chan serveOutcome, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- serveOutcome{recovered: r}
			}
		}()

		done <- serveOutcome{err: proxy.Serve(listener)}
	}()

	return done
}

func TestServeRejectsNonTCPProxyProtocolSource(t *testing.T) {
	t.Parallel()

	dst := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443}
	bogus := []struct {
		name   string
		header []byte
	}{
		{"udp4", proxyHeaderBytes(t,
			&net.UDPAddr{IP: net.IPv4(192, 0, 2, 20), Port: 4000},
			&net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443})},
		{"udp6", proxyHeaderBytes(t,
			&net.UDPAddr{IP: net.ParseIP("2001:db8::20"), Port: 4000},
			&net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443})},
		{"unix", proxyHeaderBytes(t,
			&net.UnixAddr{Name: "/run/a", Net: "unix"},
			&net.UnixAddr{Name: "/run/b", Net: "unix"})},
		{"unixgram", proxyHeaderBytes(t,
			&net.UnixAddr{Name: "/run/a", Net: "unixgram"},
			&net.UnixAddr{Name: "/run/b", Net: "unixgram"})},
		{"inet without transport", rawProxyV2Header(0x10)},
		{"inet with unknown transport", rawProxyV2Header(0x13)},
	}

	allowlist := newRecordingAllowlist()
	proxy := newProxyProtocolProxy(t, allowlist, events.NewNoopStream())
	listener := newProxyProtocolListener(t)
	done := serveRecovering(proxy, listener)

	t.Cleanup(func() {
		listener.Close() //nolint: errcheck
		proxy.Shutdown()
	})

	for i, tc := range bogus {
		select {
		case outcome := <-done:
			t.Fatalf("Serve stopped before the %s header: panic=%v err=%v", tc.name, outcome.recovered, outcome.err)
		default:
		}

		conn := sendRaw(t, listener.Addr(), tc.header)
		good := &net.TCPAddr{IP: net.IPv4(192, 0, 2, byte(100+i)), Port: 5000}
		goodConn := sendRaw(t, listener.Addr(), proxyHeaderBytes(t, good, dst))

		select {
		case outcome := <-done:
			t.Fatalf("Serve stopped on a %s PROXY v2 source: panic=%v err=%v", tc.name, outcome.recovered, outcome.err)
		case ip := <-allowlist.hit:
			if ip != good.IP.String() {
				t.Fatalf("after a %s header the allowlist saw %q, want %q", tc.name, ip, good.IP.String())
			}
		case <-time.After(proxyProtocolDeadline):
			t.Fatalf("Serve did not reach the next connection after a %s header", tc.name)
		}

		assertClosedByProxy(t, conn)
		assertClosedByProxy(t, goodConn)
	}

	want := []string{"192.0.2.100", "192.0.2.101", "192.0.2.102", "192.0.2.103", "192.0.2.104", "192.0.2.105"}
	if got := allowlist.snapshot(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("allowlist saw %v, want only the TCP sources %v", got, want)
	}
}

type unixAddrConn struct {
	*net.TCPConn
}

func (unixAddrConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: "/run/a", Net: "unix"}
}

func TestServeConnRejectsNonTCPRemoteAddr(t *testing.T) {
	t.Parallel()

	inner, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}

	t.Cleanup(func() { inner.Close() }) //nolint: errcheck

	client, err := net.DialTimeout("tcp", inner.Addr().String(), proxyProtocolDeadline)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}

	t.Cleanup(func() { client.Close() }) //nolint: errcheck

	server, err := inner.AcceptTCP()
	if err != nil {
		t.Fatalf("cannot accept: %v", err)
	}

	stream := &recordingEvents{}
	proxy := newProxyProtocolProxy(t, newRecordingAllowlist(), stream)
	t.Cleanup(proxy.Shutdown)

	done := make(chan any, 1)

	go func() {
		defer func() { done <- recover() }()

		proxy.ServeConn(unixAddrConn{TCPConn: server})
	}()

	select {
	case recovered := <-done:
		if recovered != nil {
			t.Fatalf("ServeConn panicked on a non-TCP remote address: %v", recovered)
		}
	case <-time.After(proxyProtocolDeadline):
		t.Fatalf("ServeConn did not return for a non-TCP remote address")
	}

	assertClosedByProxy(t, client)

	if got := stream.snapshot(); len(got) != 0 {
		t.Fatalf("ServeConn started a stream for a non-TCP remote address: %#v", got)
	}
}
