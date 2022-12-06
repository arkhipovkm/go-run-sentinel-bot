package clouddb

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api"
)

const (
	INFLUX_SENSOGRAM_NB_SIMULTANEOUS_CHUNKS = 3
	INFLUX_SENSOGRAM_CHUNK_SIZE             = 10000    // Empirically-chosen chunk size (lower menas more parallel request but higher server load; 10k seemed OK)
	INFLUX_SENSOGRAM_AGREGATE_WINDOW_MS     = 1000 / 1 // 1Hz fixed frequency
)

func GetRecentRuns(startStr, tsdbRef string, api api.QueryAPI) ([]*RunMetadata, error) {
	runs, err := fetchRuns(startStr, "", tsdbRef, nil, api)
	if err != nil {
		return runs, err
	}
	// Sort in time-ascending order
	sort.SliceStable(runs, func(i, j int) bool {
		return runs[i].TimestampStart < runs[j].TimestampStart
	})
	return runs, err
}

func GetItems(runUID string, tsdbRef string, api api.QueryAPI) ([]*ItemMetadata, error) {
	items, err := fetchItems("0", "", tsdbRef, &runUID, api)
	if err != nil {
		return items, err
	}
	return items, err
}

func fetchHIHSensorWindow(runUID, sensorName, startStr, stopStr string, api api.QueryAPI) ([]*FrameBase, error) {
	var err error
	var series []*FrameBase
	result, err := executeQuery(fmt.Sprintf(`
		from(bucket: "main-v2")
			|> range(start: %s, stop: %s)
			|> filter(fn: (r) => r["_measurement"] == "%s")
			|> filter(fn: (r) => (r["product"] == "hih" or r["product"] == "ak") and r["class"] == "%s")
			|> filter(fn: (r) => r["_field"] != "device.info" and r["_field"] != "calib.eac.exposure" and r["_field"] != "calib.eac.maxIntensity" and r["_field"] != "calib.eac.logs" and r["_field"] != "calib.eapda.logs" and r["_field"] != "calib.eapda.peakPositions" and r["_field"] != "calib.eapda.peaksRadius" and r["_field"] != "calib.eapda.image")
			|> filter(fn: (r) => r["section"] == "baseline")
			|> aggregateWindow(fn: mean,
							every: %dms,
							timeSrc: "_start",
							createEmpty: false)
			|> keep(columns: ["_time", "section", "_value"])
			|> group()
			|> sort(columns: ["_time"])
		`,
		startStr,
		stopStr,
		runUID,
		sensorName,
		INFLUX_SENSOGRAM_AGREGATE_WINDOW_MS,
	), api)
	if err != nil {
		return series, err
	}
	if result != nil {
		defer result.Close()
	}
	for result.Next() {
		frame := &FrameBase{}
		for k, v := range result.Record().Values() {
			switch k {
			case "_value":
				f, ok := v.(float64)
				if !ok {
					err = fmt.Errorf("error casting table value: %s - %v", k, v)
					return series, err
				}
				f = math.Round(f*1e4) / 1e4
				frame.Value = f //float32(f)
			case "_time":
				t, ok := v.(time.Time)
				if !ok {
					err = fmt.Errorf("error casting table value: %s - %v", k, v)
					return series, err
				}
				frame.Timestamp = int(t.UnixMilli())
			case "section":
				s, ok := v.(string)
				if !ok {
					err = fmt.Errorf("error casting table value: %s - %v", k, v)
					return series, err
				}
				frame.Section = s
				continue
			}
		}
		series = append(series, frame)
	}
	return series, err
}

func FetchItemHumidityFrames(runUID string, item *ItemMetadata, api api.QueryAPI) ([]*FrameBase, error) {
	var err error
	var frames []*FrameBase

	startStr := fmt.Sprint(int(math.Floor(float64(item.TimestampStart) / 1e3)))
	stopStr := fmt.Sprint(int(math.Ceil(float64(item.TimestampEnd) / 1e3)))
	if stopStr == "0" {
		stopStr = "now()"
	}

	// log.Printf("Fetching item from %s to %s", startStr, stopStr)

	frames, err = fetchHIHSensorWindow(
		runUID,
		"humidity",
		startStr,
		stopStr,
		api,
	)
	return frames, err
}
