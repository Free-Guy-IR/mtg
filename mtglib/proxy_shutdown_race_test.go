package mtglib_test

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/9seconds/mtg/v2/antireplay"
	"github.com/9seconds/mtg/v2/ipblocklist"
	"github.com/9seconds/mtg/v2/logger"
	"github.com/9seconds/mtg/v2/mtglib"
	"github.com/9seconds/mtg/v2/network"
)

const shutdownRaceDeadline = 20 * time.Second

type shutdownRaceAllowAll struct{}

func (shutdownRaceAllowAll) Contains(net.IP) bool { return true }
func (shutdownRaceAllowAll) Run(time.Duration)    {}
func (shutdownRaceAllowAll) Shutdown()            {}

type shutdownRaceListener struct {
	net.Listener

	remaining atomic.Int64
}

func (l *shutdownRaceListener) Accept() (net.Conn, error) {
	if l.remaining.Add(-1) < 0 {
		return nil, net.ErrClosed
	}

	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err //nolint: wrapcheck
	}

	shutdownRaceResetOnClose(conn)

	return conn, nil
}

func shutdownRaceResetOnClose(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetLinger(0) //nolint: errcheck
	}
}

type shutdownRaceMessages struct {
	mu       sync.Mutex
	messages []string
}

func (s *shutdownRaceMessages) add(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.messages = append(s.messages, message)
}

func (s *shutdownRaceMessages) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.messages...)
}

type shutdownRaceLogger struct {
	mtglib.Logger

	printed *shutdownRaceMessages
}

func (l shutdownRaceLogger) Named(string) mtglib.Logger            { return l }
func (l shutdownRaceLogger) BindInt(string, int) mtglib.Logger     { return l }
func (l shutdownRaceLogger) BindStr(string, string) mtglib.Logger  { return l }
func (l shutdownRaceLogger) BindJSON(string, string) mtglib.Logger { return l }

func (l shutdownRaceLogger) Printf(format string, args ...any) {
	l.printed.add(fmt.Sprintf(format, args...))
}

type shutdownRaceLedger struct {
	mu           sync.Mutex
	started      int
	finished     int
	booksClosed  bool
	startedAfter int
}

func (s *shutdownRaceLedger) Send(_ context.Context, evt mtglib.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch evt.(type) {
	case mtglib.EventStart:
		s.started++
		if s.booksClosed {
			s.startedAfter++
		}
	case mtglib.EventFinish:
		s.finished++
	}
}

func (s *shutdownRaceLedger) closeBooks() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.booksClosed = true

	return s.started, s.finished
}

func (s *shutdownRaceLedger) lateStarts() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.startedAfter
}

type shutdownRaceRun struct {
	ntw            mtglib.Network
	workerPanics   *shutdownRaceMessages
	ledgers        []*shutdownRaceLedger
	unbalanced     int
	shutdownPanics []string
}

func newShutdownRaceRun(t *testing.T) *shutdownRaceRun {
	t.Helper()

	dialer, err := network.NewDefaultDialer(0, 0)
	if err != nil {
		t.Fatalf("cannot build dialer: %v", err)
	}

	ntw, err := network.NewNetwork(dialer, "mtgtest", "1.1.1.1", 0)
	if err != nil {
		t.Fatalf("cannot build network: %v", err)
	}

	return &shutdownRaceRun{
		ntw:          ntw,
		workerPanics: &shutdownRaceMessages{},
	}
}

func (r *shutdownRaceRun) newProxy(t *testing.T, concurrency uint) (*mtglib.Proxy, *shutdownRaceLedger) {
	t.Helper()

	ledger := &shutdownRaceLedger{}
	r.ledgers = append(r.ledgers, ledger)

	proxy, err := mtglib.NewProxy(mtglib.ProxyOpts{
		Secret:          mtglib.GenerateSecret("example.com"),
		PlainMode:       true,
		Network:         r.ntw,
		AntiReplayCache: antireplay.NewNoop(),
		IPBlocklist:     ipblocklist.NewNoop(),
		IPAllowlist:     shutdownRaceAllowAll{},
		EventStream:     ledger,
		Logger:          shutdownRaceLogger{Logger: logger.NewNoopLogger(), printed: r.workerPanics},
		Concurrency:     concurrency,
		UseTestDCs:      true,
	})
	if err != nil {
		t.Fatalf("cannot build proxy: %v", err)
	}

	return proxy, ledger
}

func (r *shutdownRaceRun) closeRound(ledger *shutdownRaceLedger, shutdownPanic any) {
	if shutdownPanic != nil {
		r.shutdownPanics = append(r.shutdownPanics, fmt.Sprint(shutdownPanic))
	}

	if started, finished := ledger.closeBooks(); started != finished {
		r.unbalanced++
	}
}

func (r *shutdownRaceRun) verdict(t *testing.T) {
	t.Helper()

	time.Sleep(200 * time.Millisecond)

	lateRounds := 0

	for _, ledger := range r.ledgers {
		if ledger.lateStarts() > 0 {
			lateRounds++
		}
	}

	rounds := len(r.ledgers)

	if len(r.shutdownPanics) > 0 {
		t.Errorf("Shutdown panicked in %d of %d rounds, first: %s", len(r.shutdownPanics), rounds, r.shutdownPanics[0])
	}

	if r.unbalanced > 0 {
		t.Errorf("Shutdown returned while started streams were still running in %d of %d rounds", r.unbalanced, rounds)
	}

	if lateRounds > 0 {
		t.Errorf("streams started after Shutdown had returned in %d of %d rounds", lateRounds, rounds)
	}

	if messages := r.workerPanics.all(); len(messages) > 0 {
		t.Errorf("%d worker goroutines panicked, first: %s", len(messages), messages[0])
	}
}

func shutdownRaceShutdown(t *testing.T, proxy *mtglib.Proxy) any {
	t.Helper()

	recovered := make(chan any, 1)

	go func() {
		defer func() {
			recovered <- recover()
		}()

		proxy.Shutdown()
	}()

	select {
	case value := <-recovered:
		return value
	case <-time.After(shutdownRaceDeadline):
		t.Fatalf("Shutdown did not return within %s", shutdownRaceDeadline)
	}

	return nil
}

func shutdownRaceDial(addr string, count int) []net.Conn {
	conns := make([]net.Conn, count)

	var dialers sync.WaitGroup

	for i := range conns {
		dialers.Go(func() {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err == nil {
				shutdownRaceResetOnClose(conn)
				conns[i] = conn
			}
		})
	}

	dialers.Wait()

	return conns
}

func shutdownRaceConnected(conns []net.Conn) int {
	connected := 0

	for _, conn := range conns {
		if conn != nil {
			connected++
		}
	}

	return connected
}

func shutdownRaceCloseAll(conns []net.Conn) {
	for _, conn := range conns {
		if conn != nil {
			conn.Close() //nolint: errcheck
		}
	}
}

func shutdownRaceAcceptedPairs(t *testing.T, count int) ([]*net.TCPConn, []net.Conn) {
	t.Helper()

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}
	defer listener.Close() //nolint: errcheck

	accepted := make(chan *net.TCPConn, count)

	go func() {
		for range count {
			conn, err := listener.AcceptTCP()
			if err != nil {
				return
			}

			shutdownRaceResetOnClose(conn)
			accepted <- conn
		}
	}()

	clients := shutdownRaceDial(listener.Addr().String(), count)
	servers := make([]*net.TCPConn, 0, count)

	for range shutdownRaceConnected(clients) {
		select {
		case conn := <-accepted:
			servers = append(servers, conn)
		case <-time.After(shutdownRaceDeadline):
			t.Fatalf("accepted only %d of %d connections", len(servers), count)
		}
	}

	return servers, clients
}

func TestShutdownRightAfterServeReturnsWaitsForEveryHandedOffConnection(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		processors  int
		clients     int
		concurrency uint
	}{
		{name: "one processor", processors: 1, clients: 64},
		{name: "every processor few clients", clients: 4},
		{name: "every processor many clients", clients: 64},
		{name: "overloaded pool", clients: 64, concurrency: 4},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.processors > 0 {
				previous := runtime.GOMAXPROCS(testCase.processors)
				defer runtime.GOMAXPROCS(previous)
			}

			const rounds = 200

			run := newShutdownRaceRun(t)

			for round := range rounds {
				proxy, ledger := run.newProxy(t, testCase.concurrency)

				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatalf("cannot listen: %v", err)
				}

				conns := shutdownRaceDial(listener.Addr().String(), testCase.clients)
				counted := &shutdownRaceListener{Listener: listener}
				counted.remaining.Store(int64(shutdownRaceConnected(conns)))

				shutdownPanic := make(chan any, 1)

				go func() {
					defer func() {
						shutdownPanic <- recover()
					}()

					proxy.Serve(counted) //nolint: errcheck
					proxy.Shutdown()
				}()

				select {
				case value := <-shutdownPanic:
					run.closeRound(ledger, value)
				case <-time.After(shutdownRaceDeadline):
					t.Fatalf("round %d: Serve and Shutdown did not return", round)
				}

				listener.Close() //nolint: errcheck
				shutdownRaceCloseAll(conns)
			}

			run.verdict(t)
		})
	}
}

func TestServeConnRacingShutdownIsEitherWaitedForOrRefused(t *testing.T) {
	const (
		rounds  = 100
		callers = 32
	)

	run := newShutdownRaceRun(t)
	callerPanics := &shutdownRaceMessages{}
	leftOpen := 0

	for round := range rounds {
		proxy, ledger := run.newProxy(t, 0)
		servers, clients := shutdownRaceAcceptedPairs(t, callers)
		start := make(chan struct{})

		var callersDone sync.WaitGroup

		for _, conn := range servers {
			callersDone.Go(func() {
				defer func() {
					if value := recover(); value != nil {
						callerPanics.add(fmt.Sprint(value))
					}
				}()

				<-start
				proxy.ServeConn(conn)
			})
		}

		close(start)
		run.closeRound(ledger, shutdownRaceShutdown(t, proxy))

		returned := make(chan struct{})

		go func() {
			callersDone.Wait()
			close(returned)
		}()

		select {
		case <-returned:
		case <-time.After(shutdownRaceDeadline):
			t.Fatalf("round %d: ServeConn callers did not return after Shutdown", round)
		}

		for _, conn := range servers {
			if _, err := conn.Write([]byte{0}); err == nil {
				leftOpen++
			}
		}

		shutdownRaceCloseAll(clients)
	}

	run.verdict(t)

	if messages := callerPanics.all(); len(messages) > 0 {
		t.Errorf("ServeConn panicked %d times, first: %s", len(messages), messages[0])
	}

	if leftOpen > 0 {
		t.Errorf("ServeConn returned without closing %d connections", leftOpen)
	}
}

func TestServeOnAShutDownProxyReturnsWithoutAccepting(t *testing.T) {
	run := newShutdownRaceRun(t)
	proxy, ledger := run.newProxy(t, 0)

	if value := shutdownRaceShutdown(t, proxy); value != nil {
		t.Fatalf("Shutdown panicked: %v", value)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}
	defer listener.Close() //nolint: errcheck

	served := make(chan error, 1)

	go func() {
		served <- proxy.Serve(listener)
	}()

	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve on a shut-down proxy returned an error: %v", err)
		}
	case <-time.After(2 * time.Second):
		listener.Close() //nolint: errcheck
		<-served
		t.Fatalf("Serve kept accepting on a proxy that was already shut down")
	}

	if started, _ := ledger.closeBooks(); started != 0 {
		t.Fatalf("a shut-down proxy started %d streams", started)
	}
}
