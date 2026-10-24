package store

import (
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }

func TestHealthHistoryIsRecordedInOrderAndPruned(t *testing.T) {
	db := openTestDB(t)
	db.SetHealthHistoryRetention(30 * 24 * time.Hour)
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	report := func(rssi int) DeviceHealthReport {
		return DeviceHealthReport{Device: "dev", BootID: "b1", FirmwareVersion: "fw",
			WiFiRSSI: ptr(rssi), FreeHeapBytes: ptr(int64(1000 + rssi)), UptimeSeconds: ptr(int64(60)), TimeSynced: ptr(true)}
	}
	for _, r := range []struct {
		at   time.Time
		rssi int
	}{
		{now.Add(-40 * 24 * time.Hour), -90}, // beyond retention, pruned by the next report
		{now.Add(-2 * time.Minute), -70},
		{now.Add(-1 * time.Minute), -72},
		{now, -75},
	} {
		if err := db.RecordDeviceHealth(report(r.rssi), r.at); err != nil {
			t.Fatal(err)
		}
	}

	all, err := db.GetHealthHistory("dev", now.Add(-60*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("history = %d samples, want 3 (the 40-day-old one pruned)", len(all))
	}
	for i, want := range []int{-70, -72, -75} {
		if all[i].WiFiRSSI == nil || *all[i].WiFiRSSI != want {
			t.Errorf("sample %d RSSI = %v, want %d", i, all[i].WiFiRSSI, want)
		}
	}
	if !all[2].At.Equal(now) || all[2].FreeHeapBytes == nil || *all[2].FreeHeapBytes != 925 {
		t.Errorf("last sample = %+v", all[2])
	}

	recent, _ := db.GetHealthHistory("dev", now.Add(-90*time.Second))
	if len(recent) != 2 {
		t.Errorf("history since 90 s ago = %d samples, want 2", len(recent))
	}
	if other, _ := db.GetHealthHistory("other", now.Add(-time.Hour)); len(other) != 0 {
		t.Errorf("history of another device = %d samples, want 0", len(other))
	}
}

func TestRestartsListsBootsNewestFirst(t *testing.T) {
	db := openTestDB(t)
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	for i, boot := range []string{"b1", "b2", "b2", "b3"} {
		r := DeviceHealthReport{Device: "dev", BootID: boot, FirmwareVersion: "fw"}
		if err := db.RecordDeviceHealth(r, now.Add(time.Duration(i)*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	restarts, err := db.GetRestarts("dev", now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(restarts) != 3 || restarts[0].BootID != "b3" || restarts[2].BootID != "b1" {
		t.Fatalf("restarts = %+v, want b3, b2, b1", restarts)
	}
}
