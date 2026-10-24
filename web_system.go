package iotedge

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
)

// serverStatus holds the running parts the system page reports on.
type serverStatus struct {
	started   time.Time
	broker    atomic.Pointer[mqttBroker]
	forwarder atomic.Pointer[forwarder]
}

func newServerStatus() *serverStatus {
	return &serverStatus{started: time.Now()}
}

// configWarning is a configuration finding; Level is "warning" or "info".
type configWarning struct {
	Level string
	Text  string
}

// configWarnings lists what in the configuration weakens the server.
func configWarnings(cfg IoTConfig) []configWarning {
	var out []configWarning
	warn := func(text string) { out = append(out, configWarning{"warning", text}) }
	info := func(text string) { out = append(out, configWarning{"info", text}) }
	switch cfg.Password {
	case "", defaultConfigPassword:
		warn("Password is empty or the shipped default: anyone who knows it can log in.")
	}
	switch cfg.SessionSecret {
	case "", defaultConfigSessionSecret:
		warn("SessionSecret is empty or the shipped default: login sessions can be forged.")
	}
	switch normalizeIngestAuthMode(cfg.Security.IngestAuth) {
	case IngestAuthOff:
		warn("Security.IngestAuth is off: any client on the network can send device data.")
	case IngestAuthWarn:
		info("Security.IngestAuth is warn: device data without the token is accepted and listed on the overview.")
	}
	if !cfg.Security.ProtectAdminRoutes {
		warn("Security.ProtectAdminRoutes is off: /device/configure and /sensor/configure accept changes without login.")
	}
	if cfg.MQTTRedirectAddress != "" && cfg.Forwarding.URL == "" {
		warn("MQTTRedirectAddress is deprecated: set Forwarding.URL and Forwarding.Token.")
	}
	info("The server speaks plain HTTP: put a TLS reverse proxy in front of it for access beyond the local network.")
	return out
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

func formatUptime(d time.Duration) string {
	d = d.Round(time.Minute)
	days := int(d.Hours()) / 24
	if days > 0 {
		return fmt.Sprintf("%d d %d h", days, int(d.Hours())%24)
	}
	return fmt.Sprintf("%d h %d min", int(d.Hours()), int(d.Minutes())%60)
}

func (s *IoTEdge) systemPage(c *gin.Context) {
	data := map[string]any{
		"Build":    CurrentVersion(),
		"Database": s.DeviceDB.Location(),
		"Warnings": configWarnings(s.IoTConfig),
	}
	if s.status != nil {
		data["Uptime"] = formatUptime(time.Since(s.status.started))
		if b := s.status.broker.Load(); b != nil {
			data["MQTT"] = map[string]any{"Port": b.port, "Info": b.server.Info.Clone()}
		}
		if f := s.status.forwarder.Load(); f != nil {
			data["Forwarding"] = f.status()
		}
	}
	if size, err := s.DeviceDB.Size(); err == nil {
		data["DatabaseSize"] = formatBytes(size)
	}
	if version, err := s.DeviceDB.SchemaVersion(); err == nil {
		data["SchemaVersion"] = version
	}
	if tags, err := s.DeviceDB.Tags(s.IoTConfig.TimeseriesTable); err == nil {
		data["Tags"] = len(tags)
	}
	if fw := s.IoTConfig.forwarding(); fw.URL != "" && data["Forwarding"] == nil {
		data["ForwardingConfigured"] = fw.URL
	}
	data["IngestAuth"] = strings.ToLower(normalizeIngestAuthMode(s.IoTConfig.Security.IngestAuth))
	s.renderPage(c, URIDashboard+"/system", "System", "system.html", data)
}
