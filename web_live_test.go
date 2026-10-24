package iotedge

// web_live_test.go — W1 of doc/EDGE_SERVER_WORKBOOK.md: the overview updates
// itself over Server-Sent Events.

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// readEvent reads one SSE event (name and joined data lines).
func readEvent(t *testing.T, r *bufio.Reader, timeout time.Duration) (string, string) {
	t.Helper()
	type result struct{ name, data string }
	done := make(chan result, 1)
	go func() {
		var name string
		var data []string
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				done <- result{}
				return
			}
			line = strings.TrimRight(line, "\r\n")
			switch {
			case line == "" && name != "":
				done <- result{name, strings.Join(data, "\n")}
				return
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				data = append(data, strings.TrimPrefix(line, "data: "))
			}
		}
	}()
	select {
	case res := <-done:
		return res.name, res.data
	case <-time.After(timeout):
		t.Fatal("no server-sent event in time")
		return "", ""
	}
}

func TestWebOverviewStreamsUpdates(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	iot.liveInterval = 50 * time.Millisecond
	server := httptest.NewServer(router)
	defer server.Close()
	cookie := login(t, router)

	// The page subscribes to the stream.
	page := serve(router, http.MethodGet, URIDashboard, nil, cookie).Body.String()
	if !strings.Contains(page, `sse-connect="/dashboard/events"`) || !strings.Contains(page, `sse-swap="overview"`) {
		t.Fatal("overview does not subscribe to /dashboard/events")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/dashboard/events", nil)
	req.AddCookie(cookie)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("events = %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	r := bufio.NewReader(resp.Body)
	name, data := readEvent(t, r, 2*time.Second)
	if name != "overview" || !strings.Contains(data, "Online") {
		t.Fatalf("first event = %q %q", name, data)
	}

	problem := "live problem " + uuid.NewString()
	serveIngest(router, URILogging, LogMessage{Device: "live-dev", Text: problem, Level: Error}, "", "192.0.2.70:1")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if name, data = readEvent(t, r, 3*time.Second); strings.Contains(data, problem) {
			return
		}
	}
	t.Fatalf("no update with the new error; last event %q", name)
}

func TestWebEventsNeedLogin(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	if w := serve(router, http.MethodGet, "/dashboard/events", nil); w.Code != http.StatusFound {
		t.Fatalf("events without login = %d, want redirect", w.Code)
	}
}

// An open stream must not hold up shutdown.
func TestSensorServerStopsWithOpenEventStream(t *testing.T) {
	iot := regressionIoT()
	iot.Port = freePort(t)
	iot.IoTConfig.Password = testPassword
	iot.IoTConfig.SessionSecret = "unit-test-session-secret-32-bytes!!"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- iot.StartSensorServer(ctx) }()
	base := fmt.Sprintf("http://127.0.0.1:%d", iot.Port)
	eventually(t, "server up", func() bool {
		resp, err := http.Get(base + URIVersion)
		if err == nil {
			resp.Body.Close()
		}
		return err == nil
	})
	// Keep the login's 302 response: it carries the session cookie.
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	loginResp, err := client.Post(base+URILogin, "application/json", strings.NewReader(`{"password":"`+testPassword+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	loginResp.Body.Close()
	req, _ := http.NewRequest(http.MethodGet, base+"/dashboard/events", nil)
	for _, c := range loginResp.Cookies() {
		req.AddCookie(c)
	}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("events: %v %v", err, resp)
	}
	defer resp.Body.Close()

	start := time.Now()
	cancel()
	if err := waitReturn(t, done, "StartSensorServer"); err != nil {
		t.Fatalf("StartSensorServer: %v", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("shutdown took %v with an open event stream", took)
	}
}
