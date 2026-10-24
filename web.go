package iotedge

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

// navPage is an entry of the dashboard navigation.
type navPage struct {
	Path  string
	Title string
	Icon  template.HTML
}

var dashboardNav = []navPage{
	{URIDashboard, "Overview", `<path d="M3 12l9-8 9 8"/><path d="M5 10v10h14V10"/>`},
	{URIDashboard + "/devices", "Devices", `<rect x="7" y="7" width="10" height="10" rx="1"/><path d="M9 7V5M12 7V5M15 7V5M9 19v-2M12 19v-2M15 19v-2M7 9H5M7 12H5M7 15H5M19 9h-2M19 12h-2M19 15h-2"/>`},
	{URIDashboard + "/measurements", "Measurements", `<path d="M4 19h16"/><path d="M5 15l4-5 4 3 6-7"/>`},
	{URIDashboard + "/logs", "Logs", `<path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2"/><rect x="9" y="3" width="6" height="4" rx="1"/><line x1="9" y1="12" x2="15" y2="12"/><line x1="9" y1="16" x2="13" y2="16"/>`},
	{URIDashboard + "/system", "System", `<circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3M4.9 4.9l2.1 2.1M17 17l2.1 2.1M4.9 19.1L7 17M17 7l2.1-2.1"/>`},
}

// pageData is what every page template gets; Data is the page's own.
type pageData struct {
	Nav     []navPage
	Current string
	Title   string
	CSRF    string
	Build   VersionInfo
	Data    any
}

var levelNames = map[Loglevel]string{Debug: "Debug", Info: "Info", Warning: "Warning", Error: "Error"}

var templateFuncs = template.FuncMap{
	"levelName": func(l Loglevel) string {
		if name, ok := levelNames[l]; ok {
			return name
		}
		return fmt.Sprint(int(l))
	},
	"statusName": func(status string) string {
		switch status {
		case "never_seen":
			return "Never seen"
		case "online":
			return "Online"
		case "stale":
			return "Stale"
		}
		return status
	},
	"pathEscape": url.PathEscape,
	"shortHash": func(h string) string {
		if len(h) > 12 {
			return h[:12]
		}
		return h
	},
	// Times are stored in UTC; pages show the server's local time, like the
	// charts show the browser's.
	"localTime":   func(t time.Time) string { return t.Local().Format("2006-01-02 15:04:05") },
	"localMinute": func(t time.Time) string { return t.Local().Format("2006-01-02 15:04") },
	"clockStatus": func(synced *bool) string {
		switch {
		case synced == nil:
			return "—"
		case *synced:
			return "Synced"
		}
		return "Not synced"
	},
	// dict builds a map for passing several values to a sub-template.
	"dict": func(kv ...any) map[string]any {
		m := make(map[string]any, len(kv)/2)
		for i := 0; i+1 < len(kv); i += 2 {
			m[fmt.Sprint(kv[i])] = kv[i+1]
		}
		return m
	},
	"int":   func(l Loglevel) int { return int(l) },
	"deref": func(l *Loglevel) Loglevel { return *l },
	"kb": func(b *int64) string {
		if b == nil {
			return "—"
		}
		return fmt.Sprintf("%.0f KB", float64(*b)/1024)
	},
	"duration": func(seconds *int64) string {
		if seconds == nil {
			return "—"
		}
		d := time.Duration(*seconds) * time.Second
		if d >= 24*time.Hour {
			return fmt.Sprintf("%d d %d h", int(d.Hours())/24, int(d.Hours())%24)
		}
		return fmt.Sprintf("%d h %d min", int(d.Hours()), int(d.Minutes())%60)
	},
}

var (
	pageTemplatesOnce sync.Once
	pageTemplates     map[string]*template.Template
)

// pageTemplate returns templates/pages/<name> parsed with the layout.
func pageTemplate(name string) *template.Template {
	pageTemplatesOnce.Do(func() {
		pageTemplates = map[string]*template.Template{}
		for _, page := range []string{"overview.html", "devices.html", "logs.html", "device.html", "notfound.html", "measurements.html", "system.html"} {
			pageTemplates[page] = template.Must(template.New(page).Funcs(templateFuncs).
				ParseFS(templatesFS, "templates/layout.html", "templates/pages/"+page))
		}
	})
	return pageTemplates[name]
}

// csrfToken returns the session's CSRF token, creating it on first use.
func csrfToken(c *gin.Context) string {
	session := sessions.Default(c)
	if token, ok := session.Get("csrf").(string); ok && token != "" {
		return token
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	token := hex.EncodeToString(b)
	session.Set("csrf", token)
	if err := session.Save(); err != nil {
		slog.Error(fmt.Sprintf("failed to save CSRF token: %v", err), "fnct", "csrfToken")
	}
	return token
}

// renderPage writes a dashboard page in the shared layout.
func (s *IoTEdge) renderPage(c *gin.Context, current, title, tmpl string, data any) {
	s.renderPageStatus(c, http.StatusOK, current, title, tmpl, data)
}

func (s *IoTEdge) renderPageStatus(c *gin.Context, status int, current, title, tmpl string, data any) {
	page := pageData{Nav: dashboardNav, Current: current, Title: title, CSRF: csrfToken(c), Build: CurrentVersion(), Data: data}
	// Render fully first: a template error must not leave a half page.
	var buf bytes.Buffer
	if err := pageTemplate(tmpl).ExecuteTemplate(&buf, "layout", page); err != nil {
		slog.Error(fmt.Sprintf("render %s: %v", tmpl, err), "fnct", "renderPage")
		c.String(http.StatusInternalServerError, "failed to render page")
		return
	}
	c.Data(status, "text/html; charset=utf-8", buf.Bytes())
}

func (s *IoTEdge) overviewPage(c *gin.Context) {
	data, err := s.overviewData()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	s.renderPage(c, URIDashboard, "Overview", "overview.html", data)
}

// overviewData is what the overview's live part shows.
func (s *IoTEdge) overviewData() (map[string]any, error) {
	monitoring, err := s.DeviceDB.GetMonitoringSummary(s.IoTConfig.Monitoring, time.Now())
	if err != nil {
		return nil, fmt.Errorf("failed to read monitoring summary")
	}
	var attention []DeviceMonitoringSummary
	for _, d := range monitoring.Devices {
		if d.Status != "online" {
			attention = append(attention, d)
		}
	}
	warning := Warning
	problems, err := s.LoggingDB.GetLogMessagesPage(10, 0, LogFilter{MinLevel: &warning})
	if err != nil {
		return nil, fmt.Errorf("failed to read logs")
	}
	return map[string]any{
		"Monitoring":            monitoring,
		"Attention":             attention,
		"Problems":              problems,
		"UnauthenticatedIngest": s.ingestAuth.unauthenticated(),
	}, nil
}

// defaultLiveInterval is how often the overview stream re-checks for changes.
const defaultLiveInterval = 5 * time.Second

// overviewEvents streams the overview's live part as Server-Sent Events,
// whenever it changes, until the client leaves or the server stops.
func (s *IoTEdge) overviewEvents(c *gin.Context) {
	interval := s.liveInterval
	if interval <= 0 {
		interval = defaultLiveInterval
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("X-Accel-Buffering", "no") // let a reverse proxy pass events through
	c.Status(http.StatusOK)
	var last string
	send := func() bool {
		data, err := s.overviewData()
		if err != nil {
			return true // keep the stream; try again next tick
		}
		var buf bytes.Buffer
		if err := pageTemplate("overview.html").ExecuteTemplate(&buf, "overview-live", data); err != nil {
			slog.Error(fmt.Sprintf("render overview-live: %v", err), "fnct", "overviewEvents")
			return false
		}
		if buf.String() == last {
			return true
		}
		last = buf.String()
		var event strings.Builder
		event.WriteString("event: overview\n")
		for _, line := range strings.Split(strings.TrimSpace(last), "\n") {
			event.WriteString("data: " + line + "\n")
		}
		event.WriteString("\n")
		if _, err := c.Writer.WriteString(event.String()); err != nil {
			return false
		}
		c.Writer.Flush()
		return true
	}
	if !send() {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	keepAlive := time.NewTicker(25 * time.Second)
	defer keepAlive.Stop()
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-ticker.C:
			if !send() {
				return
			}
		case <-keepAlive.C:
			if _, err := c.Writer.WriteString(": keep-alive\n\n"); err != nil {
				return
			}
			c.Writer.Flush()
		}
	}
}

func (s *IoTEdge) devicesPage(c *gin.Context) {
	devices, err := s.DeviceDB.GetDevicesConfigs()
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read devices")
		return
	}
	monitoring, err := s.DeviceDB.GetMonitoringSummary(s.IoTConfig.Monitoring, time.Now())
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read monitoring summary")
		return
	}
	byDevice := make(map[string]DeviceMonitoringSummary, len(monitoring.Devices))
	for _, d := range monitoring.Devices {
		byDevice[d.Device] = d
	}
	s.renderPage(c, URIDashboard+"/devices", "Devices", "devices.html", map[string]any{
		"Devices":            devices,
		"Monitoring":         monitoring,
		"MonitoringByDevice": byDevice,
	})
}

func (s *IoTEdge) logsPage(c *gin.Context) {
	offset, filter, err := parseLogRequest(c)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logs, err := s.LoggingDB.GetLogMessagesPage(100, offset, filter)
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read logs")
		return
	}
	devices, err := s.DeviceDB.GetDevices()
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read devices")
		return
	}
	data := map[string]any{
		"Logs":    logs,
		"Filter":  filter,
		"Query":   logFilterQuery(filter),
		"Devices": devices,
		"Levels":  []Loglevel{Debug, Info, Warning, Error},
	}
	if offset >= 100 {
		data["PrevOffset"] = fmt.Sprint(offset - 100)
	}
	if len(logs) == 100 {
		data["NextOffset"] = fmt.Sprint(offset + 100)
	}
	s.renderPage(c, URIDashboard+"/logs", "Logs", "logs.html", data)
}
