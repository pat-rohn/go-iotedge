package store

import (
	"strings"
	"time"
)

type DeviceDesc struct {
	Name        string
	Sensors     []string
	Description string
}

type Sensor struct {
	ID           int
	DeviceID     int
	Name         string
	SensorOffset float32
}

type Device struct {
	ID          int
	Name        string
	Interval    float32
	Buffer      int
	Description string
}

type DeviceConfig struct {
	ID          int
	Name        string
	Interval    float32
	Buffer      int
	Description string
	Sensors     []Sensor
	// Address is the IP the device last sent from; AddressSeenAt is when.
	Address       string
	AddressSeenAt *time.Time
}

// URL is the device's own web page, or "" when its address is unknown.
func (d DeviceConfig) URL() string {
	if d.Address == "" {
		return ""
	}
	host := d.Address
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return "http://" + host + "/"
}
