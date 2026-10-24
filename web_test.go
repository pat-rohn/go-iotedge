package iotedge

// web_test.go — Phase 3 of doc/EDGE_SERVER_WORKBOOK.md: the dashboard as
// separate pages on a shared layout, with htmx served from the binary.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
)

var webPages = []string{"/dashboard", "/dashboard/devices", "/dashboard/logs", "/dashboard/measurements"}

func TestWebPagesNeedLogin(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	for _, page := range webPages {
		w := serve(router, http.MethodGet, page, nil)
		if w.Code != http.StatusFound || w.Header().Get("Location") != "/" {
			t.Errorf("GET %s without login = %d to %q, want redirect to /", page, w.Code, w.Header().Get("Location"))
		}
	}
}

func TestWebPagesShareNavigationAndMarkTheCurrentPage(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	cookie := login(t, router)
	for _, page := range webPages {
		w := serve(router, http.MethodGet, page, nil, cookie)
		if w.Code != http.StatusOK {
			t.Fatalf("GET %s = %d", page, w.Code)
		}
		body := w.Body.String()
		for _, link := range webPages {
			if !strings.Contains(body, `href="`+link+`"`) {
				t.Errorf("%s has no navigation link to %s", page, link)
			}
		}
		current := regexp.MustCompile(`href="` + regexp.QuoteMeta(page) + `"[^>]*aria-current="page"`)
		if !current.MatchString(body) {
			t.Errorf("%s does not mark itself as the current page", page)
		}
	}
}

var csrfMeta = regexp.MustCompile(`<meta name="csrf-token" content="([^"]+)"`)

func TestWebLayoutCarriesCSRFTokenAndLocalHtmx(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	cookie := login(t, router)
	w := serve(router, http.MethodGet, "/dashboard", nil, cookie)
	m := csrfMeta.FindStringSubmatch(w.Body.String())
	if m == nil || len(m[1]) < 32 {
		t.Fatalf("no CSRF token in the layout")
	}
	src := regexp.MustCompile(`<script src="(/static/js/htmx-[0-9.]+\.min\.js)"`).FindStringSubmatch(w.Body.String())
	if src == nil {
		t.Fatal("layout does not load htmx from /static/js")
	}
	req := httptest.NewRequest(http.MethodGet, src[1], nil)
	js := httptest.NewRecorder()
	router.ServeHTTP(js, req)
	body, _ := io.ReadAll(js.Body)
	if js.Code != http.StatusOK || !strings.Contains(string(body), "htmx") {
		t.Fatalf("GET %s = %d", src[1], js.Code)
	}
}

func TestWebOverviewShowsMonitoringAndLatestProblems(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	name := "web-ov-" + uuid.NewString()
	serveIngest(router, URIInitDevice, map[string]any{"Device": DeviceDesc{Name: name}}, "", "192.0.2.50:1")
	problem := "sensor failed " + uuid.NewString()
	serveIngest(router, URILogging, LogMessage{Device: name, Text: problem, Level: Error}, "", "192.0.2.50:1")
	cookie := login(t, router)
	body := serve(router, http.MethodGet, "/dashboard", nil, cookie).Body.String()
	for _, want := range []string{"Online", "Stale", "Never seen", problem} {
		if !strings.Contains(body, want) {
			t.Errorf("overview lacks %q", want)
		}
	}
}

func TestWebDevicesPageListsDevicesWithAddress(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	name := "web-dev-" + uuid.NewString()
	serveIngest(router, URIInitDevice, map[string]any{"Device": DeviceDesc{Name: name}}, "", "192.0.2.51:1")
	cookie := login(t, router)
	body := serve(router, http.MethodGet, "/dashboard/devices", nil, cookie).Body.String()
	if !strings.Contains(body, name) || !strings.Contains(body, `href="http://192.0.2.51/"`) {
		t.Fatal("devices page lacks the device or its address link")
	}
}

func TestWebLogsPageFiltersAndNamesLevels(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	name := "web-log-" + uuid.NewString()
	serveIngest(router, URILogging, LogMessage{Device: name, Text: "low battery", Level: Warning}, "", "192.0.2.52:1")
	serveIngest(router, URILogging, LogMessage{Device: name, Text: "just info", Level: Info}, "", "192.0.2.52:1")
	cookie := login(t, router)
	body := serve(router, http.MethodGet, "/dashboard/logs?log_device="+name+"&log_level=2", nil, cookie).Body.String()
	if !strings.Contains(body, "low battery") || strings.Contains(body, "just info") {
		t.Error("logs page does not filter by device and level")
	}
	if !strings.Contains(body, ">Warning<") {
		t.Error("logs page shows the level as a number, not its name")
	}
	if !regexp.MustCompile(`<option value="2" selected`).MatchString(body) {
		t.Error("level filter forgets the selected level")
	}
}
