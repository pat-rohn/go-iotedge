package iotedge

import (
	"fmt"
	"strings"
	"time"
)

const (
	defaultOnlineWindowMinutes = 10
	defaultRestartWindowDays   = 7
)

type MonitoringConfig struct {
	OnlineWindowMinutes int
	RestartWindowDays   int
	// HomeAssistantDiscovery opts in to publishing discovery and state for
	// server-computed entities (last seen, restart count) to the MQTT broker.
	HomeAssistantDiscovery bool
}

type DeviceHealthReport struct {
	Device           string `json:"device"`
	BootID           string `json:"boot_id"`
	FirmwareVersion  string `json:"firmware_version"`
	UptimeSeconds    *int64 `json:"uptime_seconds,omitempty"`
	FreeHeapBytes    *int64 `json:"free_heap_bytes,omitempty"`
	WiFiRSSI         *int   `json:"wifi_rssi,omitempty"`
	TimeSynced       *bool  `json:"time_synced,omitempty"`
	SensorAgeSeconds *int64 `json:"sensor_age_seconds,omitempty"`
}

type DeviceMonitoringSummary struct {
	Device          string     `json:"device"`
	Status          string     `json:"status"`
	LastSeen        *time.Time `json:"last_seen,omitempty"`
	FirmwareVersion string     `json:"firmware_version,omitempty"`
	Restarts        int        `json:"restarts"`
}

type MonitoringSummary struct {
	OnlineWindowMinutes int                       `json:"online_window_minutes"`
	RestartWindowDays   int                       `json:"restart_window_days"`
	Online              int                       `json:"online"`
	Stale               int                       `json:"stale"`
	NeverSeen           int                       `json:"never_seen"`
	Devices             []DeviceMonitoringSummary `json:"devices"`
}

func (c MonitoringConfig) normalized() MonitoringConfig {
	if c.OnlineWindowMinutes == 0 {
		c.OnlineWindowMinutes = defaultOnlineWindowMinutes
	}
	if c.RestartWindowDays == 0 {
		c.RestartWindowDays = defaultRestartWindowDays
	}
	return c
}

func (c MonitoringConfig) validate() error {
	if c.OnlineWindowMinutes <= 0 {
		return fmt.Errorf("Monitoring.OnlineWindowMinutes must be positive")
	}
	if c.RestartWindowDays <= 0 {
		return fmt.Errorf("Monitoring.RestartWindowDays must be positive")
	}
	return nil
}

func validateHealthReport(report DeviceHealthReport) error {
	if strings.TrimSpace(report.Device) == "" {
		return fmt.Errorf("device is required")
	}
	if len(report.Device) > 255 || len(report.BootID) > 255 || len(report.FirmwareVersion) > 255 {
		return fmt.Errorf("device, boot_id, and firmware_version must be at most 255 characters")
	}
	if report.UptimeSeconds != nil && *report.UptimeSeconds < 0 {
		return fmt.Errorf("uptime_seconds must not be negative")
	}
	if report.FreeHeapBytes != nil && *report.FreeHeapBytes < 0 {
		return fmt.Errorf("free_heap_bytes must not be negative")
	}
	if report.SensorAgeSeconds != nil && *report.SensorAgeSeconds < -1 {
		return fmt.Errorf("sensor_age_seconds must be -1 or greater")
	}
	return nil
}

func (devDB *DeviceDB) RecordDeviceHealth(report DeviceHealthReport, now time.Time) error {
	if err := validateHealthReport(report); err != nil {
		return err
	}

	if _, err := devDB.GetOrCreateDevice(DeviceDesc{Name: report.Device}); err != nil {
		return fmt.Errorf("ensure device: %w", err)
	}

	lastSeen := now.UTC()
	var upsert string
	if devDB.conf.UsePostgres {
		upsert = `INSERT INTO device_health
			(device_name, last_seen, firmware_version, uptime_seconds, free_heap_bytes, wifi_rssi, time_synced, sensor_age_seconds)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (device_name) DO UPDATE SET
			last_seen = EXCLUDED.last_seen, firmware_version = EXCLUDED.firmware_version,
			uptime_seconds = EXCLUDED.uptime_seconds, free_heap_bytes = EXCLUDED.free_heap_bytes,
			wifi_rssi = EXCLUDED.wifi_rssi, time_synced = EXCLUDED.time_synced,
			sensor_age_seconds = EXCLUDED.sensor_age_seconds`
	} else {
		upsert = `INSERT INTO device_health
			(device_name, last_seen, firmware_version, uptime_seconds, free_heap_bytes, wifi_rssi, time_synced, sensor_age_seconds)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (device_name) DO UPDATE SET
			last_seen = excluded.last_seen, firmware_version = excluded.firmware_version,
			uptime_seconds = excluded.uptime_seconds, free_heap_bytes = excluded.free_heap_bytes,
			wifi_rssi = excluded.wifi_rssi, time_synced = excluded.time_synced,
			sensor_age_seconds = excluded.sensor_age_seconds`
	}
	if _, err := devDB.DB.Exec(upsert, report.Device, lastSeen, report.FirmwareVersion, report.UptimeSeconds,
		report.FreeHeapBytes, report.WiFiRSSI, report.TimeSynced, report.SensorAgeSeconds); err != nil {
		return fmt.Errorf("upsert device health: %w", err)
	}
	if report.BootID == "" {
		return nil
	}

	insertRestart := `INSERT OR IGNORE INTO device_restarts (device_name, boot_id, occurred_at) VALUES (?, ?, ?)`
	if devDB.conf.UsePostgres {
		insertRestart = `INSERT INTO device_restarts (device_name, boot_id, occurred_at) VALUES ($1, $2, $3) ON CONFLICT (device_name, boot_id) DO NOTHING`
	}
	if _, err := devDB.DB.Exec(insertRestart, report.Device, report.BootID, lastSeen); err != nil {
		return fmt.Errorf("insert restart event: %w", err)
	}
	return nil
}

func (devDB *DeviceDB) RecordDeviceActivity(device string, now time.Time) error {
	if strings.TrimSpace(device) == "" {
		return fmt.Errorf("device is required")
	}
	if _, err := devDB.GetOrCreateDevice(DeviceDesc{Name: device}); err != nil {
		return fmt.Errorf("ensure device: %w", err)
	}
	query := `INSERT INTO device_health (device_name, last_seen) VALUES (?, ?)
		ON CONFLICT (device_name) DO UPDATE SET last_seen = excluded.last_seen`
	if devDB.conf.UsePostgres {
		query = `INSERT INTO device_health (device_name, last_seen) VALUES ($1, $2)
			ON CONFLICT (device_name) DO UPDATE SET last_seen = EXCLUDED.last_seen`
	}
	if _, err := devDB.DB.Exec(query, device, now.UTC()); err != nil {
		return fmt.Errorf("record device activity: %w", err)
	}
	return nil
}

func (devDB *DeviceDB) GetMonitoringSummary(config MonitoringConfig, now time.Time) (MonitoringSummary, error) {
	config = config.normalized()
	if err := config.validate(); err != nil {
		return MonitoringSummary{}, err
	}
	restartSince := now.UTC().AddDate(0, 0, -config.RestartWindowDays)
	query := `SELECT d.name, h.last_seen, COALESCE(h.firmware_version, ''), COUNT(r.id)
		FROM devices d
		LEFT JOIN device_health h ON h.device_name = d.name
		LEFT JOIN device_restarts r ON r.device_name = d.name AND r.occurred_at >= ?
		GROUP BY d.name, h.last_seen, h.firmware_version
		ORDER BY d.name`
	if devDB.conf.UsePostgres {
		query = `SELECT d.name, h.last_seen, COALESCE(h.firmware_version, ''), COUNT(r.id)
			FROM devices d
			LEFT JOIN device_health h ON h.device_name = d.name
			LEFT JOIN device_restarts r ON r.device_name = d.name AND r.occurred_at >= $1
			GROUP BY d.name, h.last_seen, h.firmware_version
			ORDER BY d.name`
	}
	rows, err := devDB.DB.Query(query, restartSince)
	if err != nil {
		return MonitoringSummary{}, fmt.Errorf("query monitoring summary: %w", err)
	}
	defer rows.Close()

	summary := MonitoringSummary{OnlineWindowMinutes: config.OnlineWindowMinutes, RestartWindowDays: config.RestartWindowDays}
	onlineSince := now.UTC().Add(-time.Duration(config.OnlineWindowMinutes) * time.Minute)
	for rows.Next() {
		var device, version string
		var lastSeen *time.Time
		var restarts int
		if err := rows.Scan(&device, &lastSeen, &version, &restarts); err != nil {
			return MonitoringSummary{}, fmt.Errorf("scan monitoring summary: %w", err)
		}
		status := "never_seen"
		if lastSeen != nil {
			status = "stale"
			if !lastSeen.Before(onlineSince) {
				status = "online"
			}
		}
		switch status {
		case "online":
			summary.Online++
		case "stale":
			summary.Stale++
		default:
			summary.NeverSeen++
		}
		summary.Devices = append(summary.Devices, DeviceMonitoringSummary{
			Device: device, Status: status, LastSeen: lastSeen, FirmwareVersion: version, Restarts: restarts,
		})
	}
	if err := rows.Err(); err != nil {
		return MonitoringSummary{}, fmt.Errorf("iterate monitoring summary: %w", err)
	}
	return summary, nil
}
