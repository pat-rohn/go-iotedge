package iotedge

import (
	"encoding/json"
	"testing"
	"time"
)

func TestHomeAssistantDiscoveryAndState(t *testing.T) {
	msgs := homeAssistantDiscovery("dev1")
	if len(msgs) != 2 {
		t.Fatalf("got %d discovery messages", len(msgs))
	}
	var cfg map[string]any
	if err := json.Unmarshal([]byte(msgs[0].Payload), &cfg); err != nil {
		t.Fatal(err)
	}
	if msgs[0].Topic != "homeassistant/sensor/dev1_last_seen/config" || cfg["unique_id"] != "enlightened_dev1_last_seen" {
		t.Fatalf("unexpected discovery: %s %v", msgs[0].Topic, cfg)
	}
	seen := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	st := homeAssistantState(DeviceMonitoringSummary{Device: "dev1", LastSeen: &seen, Restarts: 3})
	if len(st) != 2 || st[0].Payload != "2026-01-02T03:04:05Z" || st[1].Payload != "3" {
		t.Fatalf("unexpected state: %+v", st)
	}
	if got := homeAssistantState(DeviceMonitoringSummary{Device: "dev1"}); len(got) != 1 {
		t.Fatalf("never-seen device must not publish last_seen: %+v", got)
	}
}
