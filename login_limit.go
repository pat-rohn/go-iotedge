package iotedge

import (
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"
)

const (
	// loginMaxFailures failed logins from one address start a lockout.
	loginMaxFailures = 5
	// loginBaseLockout is the first lockout; each further failure doubles it.
	loginBaseLockout = time.Minute
	loginMaxLockout  = 15 * time.Minute
	// loginForgetAfter drops an address's failures after this quiet time.
	loginForgetAfter = time.Hour
)

type loginRecord struct {
	failures    int
	lockedUntil time.Time
	last        time.Time
}

// loginLimiter slows down password guessing per client address.
type loginLimiter struct {
	mu      sync.Mutex
	records map[string]*loginRecord
	now     func() time.Time
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{records: map[string]*loginRecord{}, now: time.Now}
}

func (l *loginLimiter) lockout(failures int) time.Duration {
	steps := failures - loginMaxFailures
	if steps < 0 {
		return 0
	}
	d := time.Duration(float64(loginBaseLockout) * math.Pow(2, float64(min(steps, 10))))
	return min(d, loginMaxLockout)
}

// retryAfter reports how long the address must wait before trying again.
func (l *loginLimiter) retryAfter(ip string) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	for addr, r := range l.records { // forget quiet addresses
		if now.Sub(r.last) > loginForgetAfter {
			delete(l.records, addr)
		}
	}
	if r, ok := l.records[ip]; ok && now.Before(r.lockedUntil) {
		return r.lockedUntil.Sub(now)
	}
	return 0
}

func (l *loginLimiter) failed(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	r, ok := l.records[ip]
	if !ok {
		r = &loginRecord{}
		l.records[ip] = r
	}
	r.failures++
	r.last = now
	if d := l.lockout(r.failures); d > 0 {
		r.lockedUntil = now.Add(d)
		slog.Warn(fmt.Sprintf("%d failed logins from %s; locked for %v", r.failures, ip, d), "fnct", "loginLimiter")
	}
}

func (l *loginLimiter) succeeded(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.records, ip)
}
