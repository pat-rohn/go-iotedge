package iotedge

import (
	"encoding/csv"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pat-rohn/timeseries"
)

// measurementRanges are the presets of the measurements page.
var measurementRanges = append(chartRanges[:len(chartRanges):len(chartRanges)],
	struct {
		Key   string
		Label string
		Span  time.Duration
	}{"1y", "1 year", 365 * 24 * time.Hour})

func measurementRange(key string) (string, time.Duration) {
	for _, r := range measurementRanges {
		if r.Key == key {
			return r.Key, r.Span
		}
	}
	return measurementRanges[0].Key, measurementRanges[0].Span
}

func formatNumber(v float64) string {
	return strconv.FormatFloat(math.Round(v*100)/100, 'f', -1, 64)
}

// seriesStats summarises the non-null values of a series.
func seriesStats(points []timeseries.Point) map[string]string {
	var count int
	var sum, lo, hi float64
	var latest *float64
	for _, p := range points {
		if p.Value == nil {
			continue
		}
		v := *p.Value
		if count == 0 || v < lo {
			lo = v
		}
		if count == 0 || v > hi {
			hi = v
		}
		sum += v
		count++
		latest = p.Value
	}
	if count == 0 {
		return nil
	}
	return map[string]string{
		"Latest":  formatNumber(*latest),
		"Min":     formatNumber(lo),
		"Max":     formatNumber(hi),
		"Average": formatNumber(sum / float64(count)),
		"Count":   strconv.Itoa(count),
	}
}

func (s *IoTEdge) measurementsPage(c *gin.Context) {
	table := s.IoTConfig.TimeseriesTable
	tags, err := s.DeviceDB.Tags(table)
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read tags")
		return
	}
	tag := c.Query("tag")
	rangeKey, span := measurementRange(c.Query("range"))
	data := map[string]any{
		"Tags":   tags,
		"Tag":    tag,
		"Range":  rangeKey,
		"Ranges": measurementRanges,
	}
	if tag != "" {
		now := time.Now()
		points, err := s.DeviceDB.Series(table, tag, now.Add(-span), now)
		if err != nil {
			c.String(http.StatusInternalServerError, "failed to read measurements")
			return
		}
		var values []chartPoint
		for _, p := range points {
			if p.Value != nil {
				values = append(values, chartPoint{p.Time, *p.Value})
			}
		}
		query := url.Values{"tag": {tag}, "range": {rangeKey}}
		data["Stats"] = seriesStats(points)
		data["Charts"] = []chartData{toChart("values", tag, "", values)}
		data["CSV"] = URIDashboard + "/measurements.csv?" + query.Encode()
	}
	s.renderPage(c, URIDashboard+"/measurements", "Measurements", "measurements.html", data)
}

// measurementsCSV exports a tag's readings in the range at full resolution.
func (s *IoTEdge) measurementsCSV(c *gin.Context) {
	tag := c.Query("tag")
	if tag == "" {
		c.String(http.StatusBadRequest, "tag is required")
		return
	}
	rangeKey, span := measurementRange(c.Query("range"))
	now := time.Now()
	points, err := s.DeviceDB.Series(s.IoTConfig.TimeseriesTable, tag, now.Add(-span), now)
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to read measurements")
		return
	}
	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, fmt.Sprintf("%s-%s.csv", tag, rangeKey)))
	w := csv.NewWriter(c.Writer)
	w.Write([]string{"time", "value"})
	for _, p := range points {
		value := ""
		if p.Value != nil {
			value = strconv.FormatFloat(*p.Value, 'f', -1, 64)
		}
		w.Write([]string{p.Time.UTC().Format(time.RFC3339Nano), value})
	}
	w.Flush()
}
