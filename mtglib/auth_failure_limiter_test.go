package mtglib

import (
	"net"
	"testing"
	"time"
)

func TestAuthFailureLimiterCostlyTripsFirst(t *testing.T) {
	l := newAuthFailureLimiter(100, 3, time.Minute, 10)
	ip := net.ParseIP("203.0.113.7")

	for i := 0; i < 3; i++ {
		if l.blocked(ip) {
			t.Fatalf("blocked too early at %d", i)
		}

		l.record(ip, true)
	}

	if !l.blocked(ip) {
		t.Fatal("expected block once the costly ceiling is reached")
	}
}

func TestAuthFailureLimiterCheapCeilingIsSeparate(t *testing.T) {
	l := newAuthFailureLimiter(3, 100, time.Minute, 10)
	ip := net.ParseIP("203.0.113.8")

	for i := 0; i < 3; i++ {
		l.record(ip, false)
	}

	if !l.blocked(ip) {
		t.Fatal("expected block once the cheap ceiling is reached")
	}

	if l.blocked(net.ParseIP("203.0.113.9")) {
		t.Fatal("unrelated ip must not be blocked")
	}
}

func TestAuthFailureLimiterWindowExpires(t *testing.T) {
	l := newAuthFailureLimiter(1, 1, 10*time.Millisecond, 10)
	ip := net.ParseIP("203.0.113.10")
	l.record(ip, true)

	if !l.blocked(ip) {
		t.Fatal("expected block")
	}

	time.Sleep(20 * time.Millisecond)

	if l.blocked(ip) {
		t.Fatal("expected the window to expire")
	}
}
