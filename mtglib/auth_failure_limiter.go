package mtglib

import (
	"net"
	"sync"
	"time"
)

const (
	authFailureCheapLimit  = 5000
	authFailureCostlyLimit = 200
	authFailureWindow      = time.Minute
	authFailureMaxIPs      = 65536
)

type authFailureEntry struct {
	cheap       int
	costly      int
	windowStart time.Time
}

type authFailureLimiter struct {
	mu          sync.Mutex
	cheapLimit  int
	costlyLimit int
	window      time.Duration
	maxIPs      int
	byIP        map[string]*authFailureEntry
}

func newAuthFailureLimiter(cheapLimit, costlyLimit int, window time.Duration, maxIPs int) *authFailureLimiter {
	return &authFailureLimiter{
		cheapLimit:  cheapLimit,
		costlyLimit: costlyLimit,
		window:      window,
		maxIPs:      maxIPs,
		byIP:        make(map[string]*authFailureEntry),
	}
}

func (l *authFailureLimiter) blocked(ip net.IP) bool {
	key := ip.String()
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.byIP[key]
	if !ok {
		return false
	}

	if now.Sub(entry.windowStart) > l.window {
		delete(l.byIP, key)

		return false
	}

	return entry.cheap >= l.cheapLimit || entry.costly >= l.costlyLimit
}

func (l *authFailureLimiter) record(ip net.IP, costly bool) {
	key := ip.String()
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.byIP[key]
	if !ok || now.Sub(entry.windowStart) > l.window {
		if !ok && len(l.byIP) >= l.maxIPs {
			for k, e := range l.byIP {
				if now.Sub(e.windowStart) > l.window {
					delete(l.byIP, k)
				}
			}

			if len(l.byIP) >= l.maxIPs {
				return
			}
		}

		entry = &authFailureEntry{windowStart: now}
		l.byIP[key] = entry
	}

	if costly {
		entry.costly++

		return
	}

	entry.cheap++
}
