package iotedge

// web_measurements_test.go — W4 of doc/EDGE_SERVER_WORKBOOK.md: look at one
// tag's readings over a time range and export them as CSV.

import (
	"encoding/csv"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pat-rohn/timeseries"
)

func seedSeries(t *testing.T, iot *IoTEdge, tag string, values ...string) {
	t.Helper()
	now := time.Now().UTC()
	series := timeseries.TimeseriesImportStruct{Tag: tag}
	for i, v := range values {
		at := now.Add(-time.Duration(len(values)-i) * time.Hour)
		series.Timestamps = append(series.Timestamps, at.Format("2006-01-02T15:04:05.000Z"))
		series.Values = append(series.Values, v)
	}
	if err := iot.DeviceDB.InsertTimeseries(series, true, iot.IoTConfig.TimeseriesTable); err != nil {
		t.Fatal(err)
	}
}

func TestWebMeasurementsPageShowsATagsSeries(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	tag := "meas-" + uuid.NewString()
	seedSeries(t, iot, tag, "20", "22", "x", "24")
	cookie := login(t, router)

	body := serve(router, http.MethodGet, URIDashboard+"/measurements?tag="+tag+"&range=24h", nil, cookie).Body.String()
	if !strings.Contains(body, "</html>") {
		t.Fatal("measurements page is cut off")
	}
	if !regexp.MustCompile(`<option value="` + regexp.QuoteMeta(tag) + `" selected`).MatchString(body) {
		t.Error("the chosen tag is not selected in the picker")
	}
	for _, want := range []string{"Latest", ">24<", "Min", ">20<", "Max", "Average", ">22<", `id="measurement-data"`, "/dashboard/measurements.csv?", "tag=" + tag} {
		if !strings.Contains(body, want) {
			t.Errorf("measurements page lacks %q", want)
		}
	}

	empty := serve(router, http.MethodGet, URIDashboard+"/measurements", nil, cookie).Body.String()
	if !strings.Contains(empty, "Pick a tag") {
		t.Error("without a tag the page does not ask for one")
	}
}

func TestWebMeasurementsCSVExport(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	tag := "csv-" + uuid.NewString()
	seedSeries(t, iot, tag, "1.5", "x", "3")
	cookie := login(t, router)

	w := serve(router, http.MethodGet, URIDashboard+"/measurements.csv?tag="+tag+"&range=7d", nil, cookie)
	if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/csv") {
		t.Fatalf("CSV = %d %s", w.Code, w.Header().Get("Content-Type"))
	}
	if !strings.Contains(w.Header().Get("Content-Disposition"), "attachment") {
		t.Error("CSV is not offered as a download")
	}
	records, err := csv.NewReader(strings.NewReader(w.Body.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 || records[0][0] != "time" || records[0][1] != "value" {
		t.Fatalf("CSV records = %v", records)
	}
	if records[1][1] != "1.5" || records[2][1] != "" || records[3][1] != "3" {
		t.Errorf("CSV values = %v, want 1.5, empty for the null, 3", records[1:])
	}
	if _, err := time.Parse(time.RFC3339, records[1][0]); err != nil {
		t.Errorf("CSV time %q is not RFC3339: %v", records[1][0], err)
	}

	if w := serve(router, http.MethodGet, URIDashboard+"/measurements.csv?tag="+tag, nil); w.Code != http.StatusFound {
		t.Errorf("CSV without login = %d, want redirect", w.Code)
	}
}
