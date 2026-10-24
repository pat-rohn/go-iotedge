package iotedge

import (
	"bytes"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

// maxChartPoints bounds the points per chart sent to the browser.
const maxChartPoints = 300

// chartRanges are the presets above the device page's charts.
var chartRanges = []struct {
	Key   string
	Label string
	Span  time.Duration
}{
	{"24h", "24 hours", 24 * time.Hour},
	{"7d", "7 days", 7 * 24 * time.Hour},
	{"30d", "30 days", 30 * 24 * time.Hour},
}

type chartPoint struct {
	At    time.Time
	Value float64
}

// chartData is one chart's series as the page's chart script reads it:
// Points are [unix milliseconds, value].
type chartData struct {
	Key    string
	Title  string
	Unit   string
	Points [][2]float64
}

// downsample averages points into at most max buckets of equal count.
func downsample(points []chartPoint, max int) []chartPoint {
	if len(points) <= max {
		return points
	}
	size := (len(points) + max - 1) / max
	out := make([]chartPoint, 0, max)
	for start := 0; start < len(points); start += size {
		end := min(start+size, len(points))
		var sum float64
		for _, p := range points[start:end] {
			sum += p.Value
		}
		mid := points[start+(end-start)/2].At
		out = append(out, chartPoint{At: mid, Value: sum / float64(end-start)})
	}
	return out
}

func toChart(key, title, unit string, points []chartPoint) chartData {
	c := chartData{Key: key, Title: title, Unit: unit, Points: [][2]float64{}}
	for _, p := range downsample(points, maxChartPoints) {
		c.Points = append(c.Points, [2]float64{float64(p.At.UnixMilli()), p.Value})
	}
	return c
}

// healthCharts turns the history into one chart per metric: their units
// differ, so they never share an axis.
func healthCharts(samples []HealthSample) []chartData {
	var rssi, heap, uptime []chartPoint
	for _, s := range samples {
		if s.WiFiRSSI != nil {
			rssi = append(rssi, chartPoint{s.At, float64(*s.WiFiRSSI)})
		}
		if s.FreeHeapBytes != nil {
			heap = append(heap, chartPoint{s.At, float64(*s.FreeHeapBytes) / 1024})
		}
		if s.UptimeSeconds != nil {
			uptime = append(uptime, chartPoint{s.At, float64(*s.UptimeSeconds) / 3600})
		}
	}
	return []chartData{
		toChart("rssi", "Wi-Fi signal", "dBm", rssi),
		toChart("heap", "Free heap", "KB", heap),
		toChart("uptime", "Uptime", "h", uptime),
	}
}

// csrfProtect rejects changes that do not carry the session's CSRF token
// (header X-CSRF-Token, which htmx sends, or form field csrf_token).
func (s *IoTEdge) csrfProtect(c *gin.Context) {
	want, _ := sessions.Default(c).Get("csrf").(string)
	got := c.GetHeader("X-CSRF-Token")
	if got == "" {
		got = c.PostForm("csrf_token")
	}
	if want == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "missing or invalid CSRF token"})
		return
	}
	c.Next()
}

// deviceView loads what the device page and its fragments show.
func (s *IoTEdge) deviceView(name string) (DeviceConfig, error) {
	devices, err := s.DeviceDB.GetDevicesConfigs()
	if err != nil {
		return DeviceConfig{}, err
	}
	for _, d := range devices {
		if d.Name == name {
			return d, nil
		}
	}
	return DeviceConfig{}, ErrDeviceNotFound
}

func (s *IoTEdge) devicePage(c *gin.Context) {
	name := c.Param("name")
	device, err := s.deviceView(name)
	if errors.Is(err, ErrDeviceNotFound) {
		s.renderPageStatus(c, http.StatusNotFound, URIDashboard+"/devices", "Device not found", "notfound.html",
			map[string]any{"What": "device", "Name": name, "Back": URIDashboard + "/devices"})
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read device")
		return
	}
	rangeKey, span := chartRanges[0].Key, chartRanges[0].Span
	for _, r := range chartRanges {
		if r.Key == c.Query("range") {
			rangeKey, span = r.Key, r.Span
		}
	}
	now := time.Now()
	history, err := s.DeviceDB.GetHealthHistory(name, now.Add(-span))
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read health history")
		return
	}
	restartDays := s.IoTConfig.Monitoring.RestartWindowDays
	restarts, err := s.DeviceDB.GetRestarts(name, now.AddDate(0, 0, -restartDays))
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read restarts")
		return
	}
	monitoring, err := s.DeviceDB.GetMonitoringSummary(s.IoTConfig.Monitoring, now)
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read monitoring summary")
		return
	}
	var status DeviceMonitoringSummary
	for _, d := range monitoring.Devices {
		if d.Device == name {
			status = d
		}
	}
	logs, err := s.LoggingDB.GetLogMessagesPage(10, 0, LogFilter{Device: name})
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read logs")
		return
	}
	var latest *HealthSample
	if len(history) > 0 {
		latest = &history[len(history)-1]
	}
	s.renderPage(c, URIDashboard+"/devices", device.Name, "device.html", map[string]any{
		"Device":      device,
		"Status":      status,
		"Latest":      latest,
		"Charts":      healthCharts(history),
		"Samples":     len(history),
		"Ranges":      chartRanges,
		"Range":       rangeKey,
		"Restarts":    restarts,
		"RestartDays": restartDays,
		"Logs":        logs,
		"CSRF":        csrfToken(c),
	})
}

// renderFragment writes one named template of a page, for htmx swaps.
func renderFragment(c *gin.Context, status int, page, fragment string, data any) {
	var buf bytes.Buffer
	if err := pageTemplate(page).ExecuteTemplate(&buf, fragment, data); err != nil {
		slog.Error(fmt.Sprintf("render %s/%s: %v", page, fragment, err), "fnct", "renderFragment")
		c.String(http.StatusInternalServerError, "failed to render")
		return
	}
	c.Data(status, "text/html; charset=utf-8", buf.Bytes())
}

func (s *IoTEdge) updateDeviceSettings(c *gin.Context) {
	device, err := s.deviceView(c.Param("name"))
	if errors.Is(err, ErrDeviceNotFound) {
		c.String(http.StatusNotFound, "device not found")
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read device")
		return
	}
	interval, errInterval := strconv.ParseFloat(c.PostForm("Interval"), 32)
	buffer, errBuffer := strconv.Atoi(c.PostForm("Buffer"))
	if errInterval != nil || errBuffer != nil || interval <= 0 || buffer < 1 {
		renderFragment(c, http.StatusBadRequest, "device.html", "device-settings",
			map[string]any{"Device": device, "Error": "Interval must be a positive number of seconds and buffer at least 1."})
		return
	}
	dev := Device{ID: device.ID, Name: device.Name, Description: device.Description, Interval: float32(interval), Buffer: buffer}
	if err := s.DeviceDB.Configure(dev); err != nil {
		c.String(http.StatusInternalServerError, "failed to save device")
		return
	}
	device.Interval, device.Buffer = dev.Interval, dev.Buffer
	renderFragment(c, http.StatusOK, "device.html", "device-settings", map[string]any{"Device": device, "Saved": true})
}

func (s *IoTEdge) updateSensorOffset(c *gin.Context) {
	device, err := s.deviceView(c.Param("name"))
	if errors.Is(err, ErrDeviceNotFound) {
		c.String(http.StatusNotFound, "device not found")
		return
	}
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read device")
		return
	}
	var sensor *Sensor
	for i := range device.Sensors {
		if device.Sensors[i].Name == c.Param("sensor") {
			sensor = &device.Sensors[i]
		}
	}
	if sensor == nil {
		c.String(http.StatusNotFound, "sensor not found")
		return
	}
	offset, err := strconv.ParseFloat(c.PostForm("SensorOffset"), 32)
	if err != nil {
		renderFragment(c, http.StatusBadRequest, "device.html", "sensor-row",
			map[string]any{"Device": device, "Sensor": sensor, "Error": "Offset must be a number."})
		return
	}
	sensor.SensorOffset = float32(offset)
	sensor.DeviceID = device.ID
	if err := s.DeviceDB.ConfigureSensor(*sensor); err != nil {
		c.String(http.StatusInternalServerError, "failed to save sensor")
		return
	}
	renderFragment(c, http.StatusOK, "device.html", "sensor-row", map[string]any{"Device": device, "Sensor": sensor, "Saved": true})
}
