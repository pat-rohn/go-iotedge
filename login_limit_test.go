package iotedge

// login_limit_test.go — S5 of doc/EDGE_SERVER_WORKBOOK.md: repeated failed
// logins from one address are slowed down.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func loginFrom(router http.Handler, password, remote string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(LoginRequest{Password: password})
	req := httptest.NewRequest(http.MethodPost, URILogin, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = remote
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestLoginIsLockedAfterRepeatedFailures(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	iot.loginLimiter.now = func() time.Time { return now }
	attacker, other := "192.0.2.80:1", "192.0.2.81:1"

	for i := 0; i < loginMaxFailures; i++ {
		if w := loginFrom(router, "wrong", attacker); w.Code != http.StatusUnauthorized {
			t.Fatalf("failure %d = %d, want 401", i+1, w.Code)
		}
	}
	w := loginFrom(router, testPassword, attacker)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") == "" {
		t.Fatalf("correct password while locked = %d (Retry-After %q), want 429", w.Code, w.Header().Get("Retry-After"))
	}
	if w := loginFrom(router, testPassword, other); w.Code != http.StatusFound {
		t.Errorf("another address is locked too: %d", w.Code)
	}

	now = now.Add(loginBaseLockout + time.Second)
	if w := loginFrom(router, "wrong", attacker); w.Code != http.StatusUnauthorized {
		t.Fatalf("attempt after the lockout = %d, want 401", w.Code)
	}
	// The next lockout is longer.
	if w := loginFrom(router, testPassword, attacker); w.Code != http.StatusTooManyRequests {
		t.Fatalf("after another failure = %d, want 429", w.Code)
	}
	now = now.Add(loginBaseLockout + time.Second)
	if w := loginFrom(router, testPassword, attacker); w.Code != http.StatusTooManyRequests {
		t.Fatalf("doubled lockout ended after the base lockout: %d", w.Code)
	}
	now = now.Add(loginBaseLockout)
	if w := loginFrom(router, testPassword, attacker); w.Code != http.StatusFound {
		t.Fatalf("login after the doubled lockout = %d, want 302", w.Code)
	}
	// Success resets the count.
	for i := 0; i < loginMaxFailures-1; i++ {
		loginFrom(router, "wrong", attacker)
	}
	if w := loginFrom(router, testPassword, attacker); w.Code != http.StatusFound {
		t.Fatalf("failures before a success were not reset: %d", w.Code)
	}
}

func TestLoginLockoutIsCapped(t *testing.T) {
	l := newLoginLimiter()
	for failures := loginMaxFailures; failures < loginMaxFailures+20; failures++ {
		if d := l.lockout(failures); d > loginMaxLockout {
			t.Fatalf("lockout after %d failures = %v, above %v", failures, d, loginMaxLockout)
		}
	}
	if d := l.lockout(loginMaxFailures + 20); d != loginMaxLockout {
		t.Fatalf("lockout does not reach the cap: %v", d)
	}
}
