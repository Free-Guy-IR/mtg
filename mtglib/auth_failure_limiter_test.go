package mtglib

import (
	"net"
	"testing"
	"time"
)

func TestAuthFailureLimiterBlocksAfterLimit(t *testing.T) {
	l := newAuthFailureLimiter(3, time.Minute, 10)
	ip := net.ParseIP("203.0.113.7")

	for i := 0; i < 3; i++ {
		if l.blocked(ip) {
			t.Fatalf("blocked too early at %d", i)
		}
		l.record(ip)
	}

	if !l.blocked(ip) {
		t.Fatal("expected block after limit")
	}

	if l.blocked(net.ParseIP("203.0.113.8")) {
		t.Fatal("unrelated ip must not be blocked")
	}
}

func TestAuthFailureLimiterWindowExpires(t *testing.T) {
	l := newAuthFailureLimiter(1, 10*time.Millisecond, 10)
	ip := net.ParseIP("203.0.113.9")
	l.record(ip)
	if !l.blocked(ip) {
		t.Fatal("expected block")
	}
	time.Sleep(20 * time.Millisecond)
	if l.blocked(ip) {
		t.Fatal("expected window to expire")
	}
}
