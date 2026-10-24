package iotedge

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
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
	{URIDashboard + "/logs", "Logs", `<path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2"/><rect x="9" y="3" width="6" height="4" rx="1"/><line x1="9" y1="12" x2="15" y2="12"/><line x1="9" y1="16" x2="13" y2="16"/>`},
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
	"int":   func(l Loglevel) int { return int(l) },
	"deref": func(l *Loglevel) Loglevel { return *l },
}

var (
	pageTemplatesOnce sync.Once
	pageTemplates     map[string]*template.Template
)

// pageTemplate returns templates/pages/<name> parsed with the layout.
func pageTemplate(name string) *template.Template {
	pageTemplatesOnce.Do(func() {
		pageTemplates = map[string]*template.Template{}
		for _, page := range []string{"overview.html", "devices.html", "logs.html"} {
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
	page := pageData{Nav: dashboardNav, Current: current, Title: title, CSRF: csrfToken(c), Build: CurrentVersion(), Data: data}
	c.Status(http.StatusOK)
	c.Header("Content-Type", "text/html; charset=utf-8")
	if err := pageTemplate(tmpl).ExecuteTemplate(c.Writer, "layout", page); err != nil {
		slog.Error(fmt.Sprintf("render %s: %v", tmpl, err), "fnct", "renderPage")
	}
}

func (s *IoTEdge) overviewPage(c *gin.Context) {
	monitoring, err := s.DeviceDB.GetMonitoringSummary(s.IoTConfig.Monitoring, time.Now())
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read monitoring summary")
		return
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
		c.String(http.StatusInternalServerError, "failed to read logs")
		return
	}
	s.renderPage(c, URIDashboard, "Overview", "overview.html", map[string]any{
		"Monitoring":            monitoring,
		"Attention":             attention,
		"Problems":              problems,
		"UnauthenticatedIngest": s.ingestAuth.unauthenticated(),
	})
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
