package clouddb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api"
	// "go.advanced-analytics.http-server/common"
)

// Sends influxDB query and receives first byte, then returns the *api.QueryTableResult object.
// It does not actually read the response content ( to be read by result.Next())
func executeQuery(query string, api api.QueryAPI) (*api.QueryTableResult, error) {
	if os.Getenv("DEBUG_AA") != "" {
		log.Println(query)
	}
	t0 := time.Now()
	result, err := api.Query(context.Background(), query)
	if err != nil {
		return result, err
	}
	if result.Err() != nil {
		err = fmt.Errorf("query parsing error: %s", result.Err().Error())
		return result, err
	}
	t1 := time.Now()
	log.Printf("Query completed in %.2f ms\n", float64(t1.UnixMicro()-t0.UnixMicro())/float64(1e3))
	return result, err
}

// Fetch devices associated to a run. Returns fetched result
func fetchDevices(startStr string, stopStr string, tsdbRef string, runUID *string, api api.QueryAPI) ([]*DeviceMetadata, error) {
	var err error
	var deviceMetadatas []*DeviceMetadata

	if stopStr == "" {
		stopStr = "now()"
	}
	var effectiveStartStr string = startStr

	if runUID == nil && tsdbRef != "" {
		fileName := filepath.Join("storage", "cache", "devices", tsdbRef)
		err = ReadCache(fileName, &deviceMetadatas)
		if err != nil {
			log.Printf("Could not load cached deviceMetadatas for tsdbRef %s (%s): %s", tsdbRef, fileName, err.Error())
			err = nil
		}
		log.Printf("Loaded %d devices from cache (%s)", len(deviceMetadatas), fileName)
	}

	if len(deviceMetadatas) > 0 && startStr != "0" {
		startInt, err := parseStartStr(startStr)
		if err != nil {
			return deviceMetadatas, err
		}
		var effectiveDeviceMetadatas []*DeviceMetadata
		for _, deviceMetadata := range deviceMetadatas {
			if deviceMetadata.TimestampStart >= startInt {
				effectiveDeviceMetadatas = append(effectiveDeviceMetadatas, deviceMetadata)
			}
		}
		deviceMetadatas = effectiveDeviceMetadatas
		log.Printf("Effective number of devices from cache: %d", len(deviceMetadatas))
	}

	var lastStartMilli int
	if len(deviceMetadatas) > 0 {
		lastStartMilli = deviceMetadatas[len(deviceMetadatas)-1].TimestampStart
		effectiveStartStr = fmt.Sprint(lastStartMilli/1e3 + 1)
	}

	log.Printf("Fetching device metadatas from %s. Initial startStr: %s", effectiveStartStr, startStr)

	var filterRunID string
	if runUID != nil {
		filterRunID = fmt.Sprintf(`|> filter(fn: (r) => r["_measurement"] == "%s")`, *runUID)
	}

	if lastStartMilli > 0 && stopStr != "now()" {
		stopInt, err := parseStartStr(stopStr)
		if err != nil {
			return deviceMetadatas, err
		}
		if stopInt <= lastStartMilli {
			stopStr = "now()"
		}
	}

	result, err := executeQuery(fmt.Sprintf(
		`from(bucket: "main-v2")
			|> range(start: %s, stop: %s)
			%s
			|> filter(fn: (r) => r["_field"] == "device.info")
			|> keep(columns: ["_time", "_measurement", "_value", "device.Id", "host.name"])
			|> group()
			|> sort(columns: ["_time", "_measurement"])
		`,
		effectiveStartStr,
		stopStr,
		filterRunID,
	), api)
	if err != nil {
		return deviceMetadatas, err
	}
	if result != nil {
		defer result.Close()
	}

	if err != nil {
		return deviceMetadatas, err
	}

	var nRecords int
	for result.Next() {
		deviceMetadata := &DeviceMetadata{}
		record := result.Record()
		for k, v := range record.Values() {
			if v != nil {
				switch k {
				case "_time":
					t, ok := v.(time.Time)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return deviceMetadatas, err
					}
					deviceMetadata.TimestampStart = int(t.UnixMilli())
				case "_measurement":
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return deviceMetadatas, err
					}
					deviceMetadata.RunUID = s
				case "device.Id":
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return deviceMetadatas, err
					}
					deviceMetadata.ID = s
				case "host.name":
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return deviceMetadatas, err
					}
					deviceMetadata.HostName = s
				case "_value":
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return deviceMetadatas, err
					}
					var deviceInfo DeviceInfo
					json.Unmarshal([]byte(s), &deviceInfo)
					deviceMetadata.Info = &deviceInfo
				}
			}
		}
		deviceMetadatas = append(deviceMetadatas, deviceMetadata)
		nRecords++
	}

	log.Printf("Fetched %d deviceMetadatas", nRecords)

	// Update the cache
	// Only update it if startStr is explicitely set to 0
	// and if at least some records have beeen fetched (otherwize avoid rewriting the cache)
	if runUID == nil && startStr == "0" && tsdbRef != "" && nRecords > 0 {
		fileName := filepath.Join("storage", "cache", "devices", tsdbRef)
		err = WriteCache(fileName, deviceMetadatas)
		if err != nil {
			log.Printf("Could not write deviceMetadatas to cache for tsdbRef: %s", tsdbRef)
			err = nil
		} else {
			log.Printf("Updated cache for deviceMetadatas for tsdbRef: %s", tsdbRef)
		}
	} else {
		log.Println("Avoiding updating the deviceMetadatas cache")
	}

	return deviceMetadatas, err
}

// Fetch devices associated to a run.
// Wrrites the result and the error to the provided channels
func fetchDevicesChan(startStr string, stopStr string, tsdbRef string, runUID *string, api api.QueryAPI, ch chan []*DeviceMetadata, errCh chan error) {
	devices, err := fetchDevices(startStr, stopStr, tsdbRef, runUID, api)
	errCh <- err
	ch <- devices
}

// Fetch runs (normally one run because run_uid is unique) associated to runUID.
// Returns fetched runs
func fetchRuns(startStr string, stopStr string, tsdbRef string, runUID *string, api api.QueryAPI) ([]*RunMetadata, error) {
	var err error
	var runMetadatas []*RunMetadata

	if stopStr == "" {
		stopStr = "now()"
	}

	var effectiveStartStr string = startStr

	if runUID == nil && tsdbRef != "" {
		fileName := filepath.Join("storage", "cache", "runs", tsdbRef)
		err = ReadCache(fileName, &runMetadatas)
		if err != nil {
			log.Printf("Could not load cached runMetadatas for tsdbRef  %s: %s", tsdbRef, err.Error())
			err = nil
		}
	}

	if len(runMetadatas) > 0 && startStr != "0" {
		startInt, err := parseStartStr(startStr)
		if err != nil {
			return runMetadatas, err
		}
		var effectiveRunMetadatas []*RunMetadata
		for _, deviceMetadata := range runMetadatas {
			if deviceMetadata.TimestampStart >= startInt {
				effectiveRunMetadatas = append(effectiveRunMetadatas, deviceMetadata)
			}
		}
		runMetadatas = effectiveRunMetadatas
		log.Printf("Effective number of items from cache: %d", len(runMetadatas))
	}

	var lastStartMilli int
	if len(runMetadatas) > 0 {
		lastStartMilli = runMetadatas[len(runMetadatas)-1].TimestampStart // Assuming they are already sorted by timestamp
		effectiveStartStr = fmt.Sprint(lastStartMilli/1e3 + 1)            // +1 second to avoid fetching the already existitng one
	}

	log.Printf("Fetching run metadatas from %s. Initial startStr: %s", effectiveStartStr, startStr)

	var filterRunID string
	if runUID != nil {
		filterRunID = fmt.Sprintf(`|> filter(fn: (r) => r["_measurement"] == "%s")`, *runUID)
	}

	if lastStartMilli > 0 && stopStr != "now()" {
		stopInt, err := parseStartStr(stopStr)
		if err != nil {
			return runMetadatas, err
		}
		if stopInt <= lastStartMilli {
			stopStr = "now()"
		}
	}

	result, err := executeQuery(fmt.Sprintf(
		`from(bucket: "main-v2")
			|> range(start: %s, stop: %s)
			%s
			|> filter(fn: (r) => r["class"] == "run")
			|> filter(fn: (r) => r["_value"] == 1 or r["_value"] == 0)
			|> keep(columns: ["_measurement", "run", "_value", "_time", "name", "user.protocol-name", "host.swVersion", "device.Id"])
			|> group()
			|> sort(columns: ["_time", "_measurement"])
		`,
		effectiveStartStr,
		stopStr,
		filterRunID,
	), api)
	if err != nil {
		return runMetadatas, err
	}
	if result != nil {
		defer result.Close()
	}

	if err != nil {
		return runMetadatas, err
	}

	var nRecords int
	for result.Next() {
		runMetadata := &RunMetadata{}
		record := result.Record()

		for k, v := range record.Values() {
			if v != nil {
				if k == "_measurement" {
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return runMetadatas, err
					}
					runMetadata.UID = s
				}
				if k == "run" {
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return runMetadatas, err
					}
					runMetadata.ID = s
				}
				if k == "user.protocol-name" {
					s, ok := v.(string)

					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return runMetadatas, err
					}
					runMetadata.ProtocolName = s
				}
				if k == "_time" {
					t, ok := v.(time.Time)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return runMetadatas, err
					}
					runMetadata.TimestampStart = int(t.UnixMilli())
				}
				if k == "name" {
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return runMetadatas, err
					}
					s = strings.ReplaceAll(s, "\\", "")
					runMetadata.Name = s
				}
				if k == "host.swVersion" {
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return runMetadatas, err
					}
					s = strings.ReplaceAll(s, "\\", "")
					runMetadata.SWVersion = s
				}
				if k == "_value" {
					vInt, ok := v.(int64)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return runMetadatas, err
					}
					runMetadata.value = int(vInt)
				}
			}
		}

		if runMetadata.value == 1 {
			runMetadatas = append(runMetadatas, runMetadata)
			nRecords++
		} else {
			for _, previousRun := range runMetadatas {
				if runMetadata.UID == previousRun.UID {
					previousRun.TimestampEnd = runMetadata.TimestampStart
					previousRun.IsFinished = true
					break
				}
			}
		}
	}

	log.Printf("Fetched %d runMetadatas", nRecords)

	// Update the cache
	// Only update it if startStr is explicitely set to 0
	// and if at least some records have beeen fetched (otherwize avoid rewriting the cache)
	if runUID == nil && startStr == "0" && tsdbRef != "" && nRecords > 0 {
		fileName := filepath.Join("storage", "cache", "runs", tsdbRef)
		err = WriteCache(fileName, runMetadatas)
		if err != nil {
			log.Printf("Could not write runMetadatas to cache for tsdbRef %s: %s", tsdbRef, err.Error())
			err = nil
		} else {
			log.Printf("Updated cache for runMetadatas for tsdbRef: %s", tsdbRef)
		}
	} else {
		log.Println("Avoiding updating the runMetadatas cache")
	}

	return runMetadatas, err
}

// Fetch runs (normally one run because run_uid is unique) associated to runUID.
// Sends the result and the error to the provided channels
func fetchRunsChan(startStr string, stopStr string, tsdbRef string, runUID *string, api api.QueryAPI, ch chan []*RunMetadata, errCh chan error) {
	runs, err := fetchRuns(startStr, stopStr, tsdbRef, runUID, api)
	errCh <- err
	ch <- runs
}

// Fetch items (start of each item) associated to runUID
// Returns the result
func fetchItems(startStr string, stopStr string, tsdbRef string, runUID *string, api api.QueryAPI) ([]*ItemMetadata, error) {
	var err error
	var itemMetadatas []*ItemMetadata

	if stopStr == "" {
		stopStr = "now()"
	}

	var effectiveStartStr string = startStr

	if runUID == nil && tsdbRef != "" {
		fileName := filepath.Join("storage", "cache", "items", tsdbRef)
		err = ReadCache(fileName, &itemMetadatas)
		if err != nil {
			log.Printf("Could not load cached itemMetadatas for tsdbRef %s: %s", tsdbRef, err.Error())
			err = nil
		}
	}

	if len(itemMetadatas) > 0 && startStr != "0" {
		startInt, err := parseStartStr(startStr)
		if err != nil {
			return itemMetadatas, err
		}
		var effectiveItemMetadatas []*ItemMetadata
		for _, deviceMetadata := range itemMetadatas {
			if deviceMetadata.TimestampStart >= startInt {
				effectiveItemMetadatas = append(effectiveItemMetadatas, deviceMetadata)
			}
		}
		itemMetadatas = effectiveItemMetadatas
		log.Printf("Effective number of items from cache: %d", len(itemMetadatas))
	}

	var lastStartMilli = 0
	if len(itemMetadatas) > 0 {
		lastStartMilli = itemMetadatas[len(itemMetadatas)-1].TimestampStart // Assuming they are already sorted by timestamp
		effectiveStartStr = fmt.Sprint(lastStartMilli/1e3 + 1)              // +1 second to avoid fetching the already existitng one
	}

	log.Printf("Fetching item metadatas from %s. Initial startStr: %s", effectiveStartStr, startStr)

	var filterRunID string
	if runUID != nil {
		filterRunID = fmt.Sprintf(`|> filter(fn: (r) => r["_measurement"] == "%s")`, *runUID)
	}

	if lastStartMilli > 0 && stopStr != "now()" {
		stopInt, err := parseStartStr(stopStr)
		if err != nil {
			return itemMetadatas, err
		}
		if stopInt <= lastStartMilli {
			stopStr = "now()"
		}
	}

	// Item start: 	value:1
	// Item stop: 	value:0
	result, err := executeQuery(fmt.Sprintf(
		`from(bucket: "main-v2")
			|> range(start: %s, stop: %s)
			%s
			|> filter(fn: (r) => r["class"] == "item")
			|> group()
			|> keep(fn: (column) =>
				column == "_measurement" or column == "name" or
				column == "cycle" or column == "item" or column == "_time" or column =~ /user.*/
				or column == "description"
				or column == "_value"
			)
			|> sort(columns: ["_time", "_measurement"])
		`,
		effectiveStartStr,
		stopStr,
		filterRunID,
	), api)
	if err != nil {
		return itemMetadatas, err
	}
	if result != nil {
		defer result.Close()
	}

	if err != nil {
		return itemMetadatas, err
	}

	var nRecords int
	for result.Next() {
		itemMetadata := &ItemMetadata{}
		record := result.Record()
		for k, v := range record.Values() {
			if v != nil {
				switch k {
				case "cycle":
					cStr, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return itemMetadatas, err
					}
					cInt, err := strconv.Atoi(cStr)
					if err != nil {
						return itemMetadatas, err
					}
					itemMetadata.Cycle = cInt
				case "item":
					cStr, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return itemMetadatas, err
					}
					cInt, err := strconv.Atoi(cStr)
					if err != nil {
						return itemMetadatas, err
					}
					itemMetadata.Index = cInt
				case "name":
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return itemMetadatas, err
					}
					s = strings.ReplaceAll(s, "\\", "")
					itemMetadata.Name = s
				case "description":
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return itemMetadatas, err
					}
					s = strings.ReplaceAll(s, "\\", "")
					itemMetadata.Description = s
				case "_measurement":
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return itemMetadatas, err
					}
					itemMetadata.RunUID = s
				case "_time":
					t, ok := v.(time.Time)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return itemMetadatas, err
					}
					itemMetadata.TimestampStart = int(t.UnixMilli())
				case "_value":
					vInt, ok := v.(int64)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return itemMetadatas, err
					}
					itemMetadata.value = int(vInt)
				}
				if strings.Contains(k, "user.") {
					k = strings.ReplaceAll(k, "user.", "")
					s, ok := v.(string)
					if !ok {
						err = fmt.Errorf("error casting table value: %s - %v", k, v)
						return itemMetadatas, err
					}
					if s == "@" {
						itemMetadata.UserTags = append(itemMetadata.UserTags, k)
					} else {
						s = strings.ReplaceAll(s, "\\", "")
						itemMetadata.UserTags = append(itemMetadata.UserTags, fmt.Sprintf("%s:%s", k, s))
					}
				}
			}
		}
		if itemMetadata.value == 1 {
			itemMetadatas = append(itemMetadatas, itemMetadata)
			nRecords++
		} else {
			for _, previousItemMetadata := range itemMetadatas {
				if itemMetadata.RunUID == previousItemMetadata.RunUID && itemMetadata.Index == previousItemMetadata.Index && itemMetadata.Cycle == previousItemMetadata.Cycle {
					previousItemMetadata.TimestampEnd = itemMetadata.TimestampStart
					if previousItemMetadata.Name == "@end" {
						previousItemMetadata.Name = itemMetadata.Name
					}
					if previousItemMetadata.Description == "@end" {
						previousItemMetadata.Description = itemMetadata.Description
					}
					break
				}
			}
		}
		nRecords++
	}

	log.Printf("Fetched %d itemMetadatas", nRecords)

	// Update the cache
	// Only update it if startStr is explicitely set to 0
	// and if at least some records have beeen fetched (otherwize avoid rewriting the cache)
	if runUID == nil && startStr == "0" && tsdbRef != "" && nRecords > 0 {
		fileName := filepath.Join("storage", "cache", "items", tsdbRef)
		err = WriteCache(fileName, itemMetadatas)
		if err != nil {
			log.Printf("Could not write itemMetadatas to cache for tsdbRef: %s", tsdbRef)
			err = nil
		} else {
			log.Printf("Updated cache for itemMetadatas for tsdbRef: %s", tsdbRef)
		}
	} else {
		log.Println("Avoiding updating the itemMetadatas cache")
	}

	return itemMetadatas, err
}

// Fetch items (start of each item) associated to runUID.
// Sends the result and error to channels
func fetchItemsChan(startStr string, stopStr string, tsdbRef string, runUID *string, api api.QueryAPI, ch chan []*ItemMetadata, errCh chan error) {
	items, err := fetchItems(startStr, stopStr, tsdbRef, runUID, api)
	errCh <- err
	ch <- items
}

// List all runs in a bucket starting from *start*.
// Returns the result
func listRuns(startStr string, stopStr string, tsdbRef string, api api.QueryAPI) ([]*RunMetadata, error) {
	var err error

	var runMetadatas []*RunMetadata
	var itemMetadatas []*ItemMetadata
	var deviceMetadatas []*DeviceMetadata

	var runsCh chan []*RunMetadata = make(chan []*RunMetadata, 1)
	var itemsCh chan []*ItemMetadata = make(chan []*ItemMetadata, 1)
	var devicesCh chan []*DeviceMetadata = make(chan []*DeviceMetadata, 1)

	var errCh chan error = make(chan error, 3)

	go fetchRunsChan(startStr, stopStr, tsdbRef, nil, api, runsCh, errCh)
	go fetchItemsChan(startStr, stopStr, tsdbRef, nil, api, itemsCh, errCh)
	go fetchDevicesChan(startStr, stopStr, tsdbRef, nil, api, devicesCh, errCh)

	runMetadatas = <-runsCh
	itemMetadatas = <-itemsCh
	deviceMetadatas = <-devicesCh

	for i := 0; i < 3; i++ {
		err = <-errCh
		if err != nil {
			return runMetadatas, err
		}
	}

	var itemToRunUIDMap map[string][]*ItemMetadata = make(map[string][]*ItemMetadata)
	for _, itemMetadata := range itemMetadatas {
		if itemToRunUIDMap[itemMetadata.RunUID] == nil {
			itemToRunUIDMap[itemMetadata.RunUID] = []*ItemMetadata{}
		}
		itemToRunUIDMap[itemMetadata.RunUID] = append(itemToRunUIDMap[itemMetadata.RunUID], itemMetadata)
	}

	var deviceToRunUIDMap map[string][]*DeviceMetadata = make(map[string][]*DeviceMetadata)
	for _, deviceMetadata := range deviceMetadatas {
		if deviceToRunUIDMap[deviceMetadata.RunUID] == nil {
			deviceToRunUIDMap[deviceMetadata.RunUID] = []*DeviceMetadata{}
		}
		deviceToRunUIDMap[deviceMetadata.RunUID] = append(deviceToRunUIDMap[deviceMetadata.RunUID], deviceMetadata)
	}

	for _, runMetadata := range runMetadatas {
		if itemToRunUIDMap[runMetadata.UID] != nil {
			runMetadata.RecordsCount = len(itemToRunUIDMap[runMetadata.UID])

			var uniqueItemNamesMap map[string]string = make(map[string]string)
			var uniqueUserTagsMap map[string]string = make(map[string]string)
			var uniqueCyclesMap map[int]int = make(map[int]int)

			var itemMetadata *ItemMetadata
			for _, itemMetadata = range itemToRunUIDMap[runMetadata.UID] {
				uniqueItemNamesMap[itemMetadata.Name] = ""
				uniqueCyclesMap[itemMetadata.Cycle] = 0
				for _, t := range itemMetadata.UserTags {
					uniqueUserTagsMap[t] = ""
				}
			}

			// If TimestampEnd is 0, use last item metadata to deduce timestampEnd
			if runMetadata.TimestampEnd == 0 {
				runMetadata.TimestampEnd = itemMetadata.TimestampStart
			}

			var uniqueItemNamesSlice []string = make([]string, 0)
			var uniqueUserTagsSlice []string = make([]string, 0)
			var uniqueCyclesSlice []int = make([]int, 0)

			for k := range uniqueItemNamesMap {
				uniqueItemNamesSlice = append(uniqueItemNamesSlice, k)
			}
			for k := range uniqueUserTagsMap {
				uniqueUserTagsSlice = append(uniqueUserTagsSlice, k)
			}
			for k := range uniqueCyclesMap {
				uniqueCyclesSlice = append(uniqueCyclesSlice, k)
			}

			sort.Strings(uniqueItemNamesSlice)
			sort.Strings(uniqueUserTagsSlice)
			sort.Ints(uniqueCyclesSlice)

			runMetadata.ItemNames = uniqueItemNamesSlice
			runMetadata.Cycles = uniqueCyclesSlice
			runMetadata.Tags = uniqueUserTagsSlice
		}

		if deviceToRunUIDMap[runMetadata.UID] != nil && len(deviceToRunUIDMap[runMetadata.UID]) > 0 {
			runMetadata.Device = deviceToRunUIDMap[runMetadata.UID][0]
		}
	}

	var effectiveRunMetadatas []*RunMetadata
	for _, runMetadata := range runMetadatas {
		if runMetadata.RecordsCount == 0 ||
			runMetadata.ProtocolName == "phasisCalibration" ||
			runMetadata.ProtocolName == "warm_up" {
			continue
		}
		effectiveRunMetadatas = append(effectiveRunMetadatas, runMetadata)
	}

	return effectiveRunMetadatas, err
}

// Appends the given frame to correct recor in a []*Record
// Uses timestamps to find the appropriate record
// Logs a warning if a record found by the timestamp has different item_index or cycle
// (it indicates incoherence). Will still add the frame to the record found by timestamp
func dispatchFrameToRecords(records []*Record, frame *SensogramFrame) error {
	var err error
	for i := 0; i < len(records); i++ {
		currentRecord := records[i]

		if i == len(records)-1 {
			// Case of last record in the run
			currentRecord.Frames = append(currentRecord.Frames, frame)
			if currentRecord.Spots == nil {
				currentRecord.Spots = &frame.Spots
			}
			return err
		}

		nextRecord := records[i+1]

		if frame.Timestamp >= currentRecord.Item.TimestampStart && frame.Timestamp+INFLUX_SENSOGRAM_AGREGATE_WINDOW_MS < nextRecord.Item.TimestampStart {
			if currentRecord.Item.Index != frame.ItemIndex {
				log.Printf(
					"item indices of frame (%d) and the found record (%d) do not correspond; time (ms) from the start of the current record: %d; time (ms) until the start of the next record: %d",
					frame.ItemIndex,
					currentRecord.Item.Index,
					frame.Timestamp-currentRecord.Item.TimestampStart,
					frame.Timestamp-nextRecord.Item.TimestampStart,
				)
				// continue
			}
			if currentRecord.Item.Cycle != frame.Cycle {
				log.Printf(
					"cycles of frame (%d) and the found record (%d) do not correspond; time (ms) from the start of the current record: %d; time (ms) until the start of the next record: %d",
					frame.Cycle,
					currentRecord.Item.Cycle,
					frame.Timestamp-currentRecord.Item.TimestampStart,
					frame.Timestamp-nextRecord.Item.TimestampStart,
				)
				// continue
			}
			currentRecord.Frames = append(currentRecord.Frames, frame)
			if currentRecord.Spots == nil {
				currentRecord.Spots = &frame.Spots
			}
			return err
		}
	}
	return err
}

// Fetches amplifier's sensor data
// Returns the sensor's data timeseries
func fetchAmplifierSensor(runUID, sensorName string, api api.QueryAPI) ([]*FrameBase, error) {
	var err error
	var series []*FrameBase
	result, err := executeQuery(fmt.Sprintf(`
		from(bucket: "main-v2")
			|> range(start: 0)
			|> filter(fn: (r) => r["_measurement"] == "%s")
			|> filter(fn: (r) => r["product"] == "amplifier" and r["class"] == "%s")
			|> filter(fn: (r) => r["_field"] != "device.info" and r["_field"] != "calib.eac.exposure" and r["_field"] != "calib.eac.maxIntensity" and r["_field"] != "calib.eac.logs" and r["_field"] != "calib.eapda.logs" and r["_field"] != "calib.eapda.peakPositions" and r["_field"] != "calib.eapda.peaksRadius" and r["_field"] != "calib.eapda.image")
			|> filter(fn: (r) => r["section"] == "baseline" or r["section"] == "analyte" or r["section"] == "desorption")
			|> aggregateWindow(fn: mean,
							every: %dms,
							timeSrc: "_start",
							createEmpty: false)
			|> keep(columns: ["_time", "item", "cycle", "_measurement", "section", "_value"])
			|> group()
			|> sort(columns: ["_time"])
		`,
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
				f = math.Round(f*1e2) / 1e2
				frame.Value = f //float32(f)
			case "_time":
				t, ok := v.(time.Time)
				if !ok {
					err = fmt.Errorf("error casting table value: %s - %v", k, v)
					return series, err
				}
				frame.Timestamp = int(t.UnixMilli())
			}
		}
		series = append(series, frame)
	}
	return series, err
}

// Fetches amplifier's sensor data
// Sensds the result and the error to provided channels
func fetchAmplifierSensorChan(runUID, sensorName string, api api.QueryAPI, seriesCh chan []*FrameBase, errCh chan error) {
	series, err := fetchAmplifierSensor(runUID, sensorName, api)
	errCh <- err
	seriesCh <- series
}

// Fetches HIH sensor data
// Returns the sensor's data timeseries
func fetchHIHSensor(runUID, sensorName string, api api.QueryAPI) ([]*FrameBase, error) {
	var err error
	var series []*FrameBase
	result, err := executeQuery(fmt.Sprintf(`
		from(bucket: "main-v2")
			|> range(start: 0)
			|> filter(fn: (r) => r["_measurement"] == "%s")
			|> filter(fn: (r) => (r["product"] == "hih" or r["product"] == "ak") and r["class"] == "%s")
			|> filter(fn: (r) => r["_field"] != "device.info" and r["_field"] != "calib.eac.exposure" and r["_field"] != "calib.eac.maxIntensity" and r["_field"] != "calib.eac.logs" and r["_field"] != "calib.eapda.logs" and r["_field"] != "calib.eapda.peakPositions" and r["_field"] != "calib.eapda.peaksRadius" and r["_field"] != "calib.eapda.image")
			|> filter(fn: (r) => r["section"] == "baseline" or r["section"] == "analyte" or r["section"] == "desorption")
			|> aggregateWindow(fn: mean,
							every: %dms,
							timeSrc: "_start",
							createEmpty: false)
			|> keep(columns: ["_time", "item", "cycle", "_measurement", "section", "_value"])
			|> group()
			|> sort(columns: ["_time"])
		`,
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
			}
		}
		series = append(series, frame)
	}
	return series, err
}

// Fetches HIH sensor data
// Sensds the result and the error to provided channels
func fetchHIHSensorChan(runUID, sensorName string, api api.QueryAPI, seriesCh chan []*FrameBase, errCh chan error) {
	series, err := fetchHIHSensor(runUID, sensorName, api)
	errCh <- err
	seriesCh <- series
}

// Fetches SDOK sensor data
// Returns the sensor's data timeseries
func fetchSDOKSensor(runUID, productName string, sensorName string, api api.QueryAPI) ([]*FrameBase, error) {
	var err error
	var series []*FrameBase
	result, err := executeQuery(fmt.Sprintf(`
		from(bucket: "main-v2")
			|> range(start: 0)
			|> filter(fn: (r) => r["_measurement"] == "%s")
			|> filter(fn: (r) => (r["product"] == "%s") and r["class"] == "%s")
			|> filter(fn: (r) => r["_field"] != "device.info" and r["_field"] != "calib.eac.exposure" and r["_field"] != "calib.eac.maxIntensity" and r["_field"] != "calib.eac.logs" and r["_field"] != "calib.eapda.logs" and r["_field"] != "calib.eapda.peakPositions" and r["_field"] != "calib.eapda.peaksRadius" and r["_field"] != "calib.eapda.image")
			|> filter(fn: (r) => r["section"] == "baseline" or r["section"] == "analyte" or r["section"] == "desorption")
			|> aggregateWindow(fn: mean,
							every: %dms,
							timeSrc: "_start",
							createEmpty: false)
			|> keep(columns: ["_time", "item", "cycle", "_measurement", "section", "_value"])
			|> group()
			|> sort(columns: ["_time"])
		`,
		runUID,
		productName,
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
				frame.Value = f
			case "_time":
				t, ok := v.(time.Time)
				if !ok {
					err = fmt.Errorf("error casting table value: %s - %v", k, v)
					return series, err
				}
				frame.Timestamp = int(t.UnixMilli())
			}
		}
		series = append(series, frame)
	}
	return series, err
}

// Fetches SDOK sensor data
// Sensds the result and the error to provided channels
func fetchSDOKSensorChan(runUID, productName string, sensorName string, api api.QueryAPI, seriesCh chan []*FrameBase, errCh chan error) {
	series, err := fetchSDOKSensor(runUID, productName, sensorName, api)
	errCh <- err
	seriesCh <- series
}

// Fetches all frames' timestamps.
// Used as a preflight request to verify frames length
// coherence with aux sensor data and to properly chunk requested full frames.
// Returns the result
func fetchTimestamps(runUID string, api api.QueryAPI) ([]int, error) {
	var err error
	var frameTimestamps []int

	result, err := executeQuery(fmt.Sprintf(`
		from(bucket: "main-v2")
			|> range(start: 0, stop: now())
			|> filter(fn: (r) => r["_measurement"] == "%s")
			|> filter(fn: (r) => r["_field"] == "A0" and r["product"] == "osense")
			|> filter(fn: (r) => r["_field"] != "device.info" and r["_field"] != "calib.eac.exposure" and r["_field"] != "calib.eac.maxIntensity" and r["_field"] != "calib.eac.logs" and r["_field"] != "calib.eapda.logs" and r["_field"] != "calib.eapda.peakPositions" and r["_field"] != "calib.eapda.peaksRadius" and r["_field"] != "calib.eapda.image")
			|> filter(fn: (r) => r["section"] == "baseline" or r["section"] == "analyte" or r["section"] == "desorption")
			|> aggregateWindow(fn: mean,
							every: %dms,
							timeSrc: "_start",
							createEmpty: false)
			|> keep(columns: ["_time", "item", "cycle", "_measurement", "section", "_value"])
			|> keep(columns: ["_time"])
			|> group()
			|> sort(columns: ["_time"])
		`,
		runUID,
		INFLUX_SENSOGRAM_AGREGATE_WINDOW_MS,
	), api)
	if err != nil {
		return frameTimestamps, err
	}
	if result != nil {
		defer result.Close()
	}

	for result.Next() {
		t := result.Record().Time()
		frameTimestamps = append(frameTimestamps, int(t.UnixNano()))
	}

	return frameTimestamps, err
}

// Fetches all frames' timestamps.
// Used as a preflight request to verify frames length
// coherence with aux sensor data and to properly chunk requested full frames.
// Sensds the result and the error to provided channels
func fetchTimestampsChan(runUID string, api api.QueryAPI, seriesCh chan []int, errCh chan error) {
	series, err := fetchTimestamps(runUID, api)
	errCh <- err
	seriesCh <- series
}

// Fetches a chunk of sensogram frames
// Returns chunk's hashCode
func _fetchRecordsChunk(runUID string, start int, stop string, api api.QueryAPI) (string, error) {
	// start - integer - unix nano
	var err error
	var frames []*SensogramFrame

	chunkName := fmt.Sprintf("%s_%d_%s", runUID, start, stop)
	chunkHashCode, err := HashString(chunkName)
	if err != nil {
		return chunkHashCode, err
	}
	if chunkHashCode != "" && stop != "now()" {
		fileName := filepath.Join("storage", "cache", "chunks", chunkHashCode)
		f, err := os.Open(fileName)
		if err != nil {
			log.Printf("Error reading chunk (%s) cache: %s", chunkName, err.Error())
			err = nil
		} else {
			log.Printf("Successfully read cache for the chunk: %s - %s. Frames: %d", chunkName, chunkHashCode, len(frames))
			return chunkHashCode, err
		}
		defer f.Close()
	}

	result, err := executeQuery(fmt.Sprintf(`
		from(bucket: "main-v2")
			|> range(start: time(v: %d), stop: %s)
			|> filter(fn: (r) => r["_measurement"] == "%s")
			|> filter(fn: (r) => r["product"] == "osense")
			|> filter(fn: (r) => r["_field"] != "device.info" and r["_field"] != "calib.eac.exposure" and r["_field"] != "calib.eac.maxIntensity" and r["_field"] != "calib.eac.logs" and r["_field"] != "calib.eapda.logs" and r["_field"] != "calib.eapda.peakPositions" and r["_field"] != "calib.eapda.peaksRadius" and r["_field"] != "calib.eapda.image")
			|> filter(fn: (r) => r["section"] == "baseline" or r["section"] == "analyte" or r["section"] == "desorption")
		    |> aggregateWindow(fn: mean,
							  every: %dms,
							  timeSrc: "_start",
							  createEmpty: false)
			|> group()
			|> pivot(rowKey:["_time", "item", "cycle", "_measurement", "section"],
					 columnKey: ["class", "_field"],
					 valueColumn: "_value")
			|> group()
			|> sort(columns: ["_time"])
		`,
		start,
		stop,
		runUID,
		INFLUX_SENSOGRAM_AGREGATE_WINDOW_MS,
	), api)
	if err != nil {
		return chunkHashCode, err
	}
	if result != nil {
		defer result.Close()
	}
	var frame *SensogramFrame
	for result.Next() {
		rec := result.Record()

		t := int(rec.Time().UnixMilli())
		frame = &SensogramFrame{
			Timestamp: t,
		}

		// if i >= nFrames {
		// 	log.Printf("Breaking the frames parsing loop as the counter achieves the nFrames: %d/%d", i, nFrames)
		// 	break
		// }

		type SpotValue struct {
			Spot  string
			Value *float64
		}
		var spotValueSlice []*SpotValue

		for k, v := range rec.Values() {
			if v == nil {
				continue
			}
			switch k {
			case "item":
				cStr, ok := v.(string)
				if !ok {
					err = fmt.Errorf("error casting table value: %s - %v", k, v)
					return chunkHashCode, err
				}
				cInt, err := strconv.Atoi(cStr)
				if err != nil {
					return chunkHashCode, err
				}
				frame.ItemIndex = cInt
				continue
			case "cycle":
				cStr, ok := v.(string)
				if !ok {
					err = fmt.Errorf("error casting table value: %s - %v", k, v)
					return chunkHashCode, err
				}
				cInt, err := strconv.Atoi(cStr)
				if err != nil {
					return chunkHashCode, err
				}
				frame.Cycle = cInt
				continue
			case "section":
				s, ok := v.(string)
				if !ok {
					err = fmt.Errorf("error casting table value: %s - %v", k, v)
					return chunkHashCode, err
				}
				frame.Section = s
				continue
			case "_measurement":
				continue
			case "result":
				continue
			case "table":
				continue
			case "_time":
				continue
			default:
				f, ok := v.(float64)
				if !ok {
					err = fmt.Errorf("error casting table value: %s - %v", k, v)
					return chunkHashCode, err
				}
				// f32 := float32(f)
				f = math.Round(f*1e4) / 1e4
				spotValueSlice = append(spotValueSlice, &SpotValue{
					Spot:  k,
					Value: &f,
				})
				continue
			}
		}
		if len(spotValueSlice) == 0 {
			continue
		}
		sort.SliceStable(spotValueSlice, func(i, j int) bool {
			return spotValueSlice[i].Spot < spotValueSlice[j].Spot
		})
		for _, sv := range spotValueSlice {
			frame.Spots = append(frame.Spots, sv.Spot)
			frame.Data = append(frame.Data, *sv.Value)
		}
		frames = append(frames, frame)
	}

	if chunkHashCode != "" {
		fileName := filepath.Join("storage", "cache", "chunks", chunkHashCode)
		err = WriteCache(fileName, frames)
		if err != nil {
			log.Printf("Error writing chunk (%s) cache: %s", chunkName, err.Error())
			err = nil
		} else {
			log.Printf("Wrote chunk cache: %s - %s", chunkName, chunkHashCode)
		}
	}

	return chunkHashCode, err
}

// Fetches a chunk of sensogram frames
// Sends result and error to channels
func _fetchRecordsChunkChan(
	runUID string,
	start int,
	stop string,
	n int,
	api api.QueryAPI,
	ch chan *ChunkedResult,
	progressCh chan float32,
	progressLevel float32,
	asyncRequestsQueue chan bool,
) {
	chunkHashCode, err := _fetchRecordsChunk(runUID, start, stop, api)
	ch <- &ChunkedResult{
		N:        n,
		HashCode: chunkHashCode,
		// Frames: frames,
		Err: err,
	}
	// Consume one to signal job is done
	<-asyncRequestsQueue
	log.Printf("Async request queue length on consuming: %d; Chunk nb: %d", len(asyncRequestsQueue), n)

	// Send progress ratio to progress channel (if it's not nil)
	if progressCh != nil {
		progressCh <- progressLevel
		log.Printf("Sent progress: %v", progressLevel)
	}
}

// Fetch records of the provided runUID
// Returns []*Record which is to be simplified before sending back
func fetchRecords(runUID string, api api.QueryAPI, progressCh chan float32, isExternallyHosted bool) ([]*Record, error) {
	var err error
	var records []*Record

	var runCh chan []*RunMetadata = make(chan []*RunMetadata, 1)
	var itemsCh chan []*ItemMetadata = make(chan []*ItemMetadata, 1)
	var errCh chan error = make(chan error, 2)

	go fetchRunsChan("0", "", "", &runUID, api, runCh, errCh)
	go fetchItemsChan("0", "", "", &runUID, api, itemsCh, errCh)

	runMetadatas := <-runCh
	itemMetadatas := <-itemsCh

	for i := 0; i < 2; i++ {
		err = <-errCh
		if err != nil {
			return records, err
		}
	}

	if len(runMetadatas) != 1 {
		err = fmt.Errorf("invalid number of run metadatas for a single run: %d", len(runMetadatas))
		return records, err
	}
	runMetadata := runMetadatas[0]
	log.Printf("run (%s) finished: %v", runMetadata.UID, runMetadata.IsFinished)

	// TimestampEnd is the time of the last measurement
	// If TimestampEnd is 0, use last item metadata to deduce timestampEnd
	if runMetadata.TimestampEnd == 0 {
		if os.Getenv("DEBUG_AA") != "" {
			log.Printf("'Unfinished' run: %s", runMetadata.UID)
		}
		runMetadata.TimestampEnd = itemMetadatas[len(itemMetadatas)-1].TimestampStart
	}

	var allItemsConcatenated string
	for _, itemMetadata := range itemMetadatas {
		allItemsConcatenated += fmt.Sprintf("%d%d", itemMetadata.Index, itemMetadata.Cycle)
	}

	log.Printf("Fetched %d item metadatas for run %s.", len(itemMetadatas), runUID)

	if len(records) != 0 {
		return records, err
	}

	errCh = make(chan error, 13)
	var devCh chan []*DeviceMetadata = make(chan []*DeviceMetadata, 1)
	var timeCh chan []int = make(chan []int, 1)
	var humCh chan []*FrameBase = make(chan []*FrameBase, 1)
	var tempCh chan []*FrameBase = make(chan []*FrameBase, 1)
	var ampTempCh chan []*FrameBase = make(chan []*FrameBase, 1)
	var sunriseCo2Chan chan []*FrameBase = make(chan []*FrameBase, 1)
	var pidVocChan chan []*FrameBase = make(chan []*FrameBase, 1)
	var zephyrAirflowChan chan []*FrameBase = make(chan []*FrameBase, 1)
	var pms1Chan chan []*FrameBase = make(chan []*FrameBase, 1)
	var pms25Chan chan []*FrameBase = make(chan []*FrameBase, 1)
	var pms10Chan chan []*FrameBase = make(chan []*FrameBase, 1)
	var bmeVocChan chan []*FrameBase = make(chan []*FrameBase, 1)
	var bmeCo2Chan chan []*FrameBase = make(chan []*FrameBase, 1)

	go fetchDevicesChan("0", "", "", &runUID, api, devCh, errCh)
	go fetchTimestampsChan(runUID, api, timeCh, errCh)
	go fetchHIHSensorChan(runUID, "humidity", api, humCh, errCh)
	go fetchHIHSensorChan(runUID, "temperature", api, tempCh, errCh)
	go fetchAmplifierSensorChan(runUID, "temperature", api, ampTempCh, errCh)

	// fetch SDOK sensors : series might be empty
	go fetchSDOKSensorChan(runUID, "sunrise", "co2", api, sunriseCo2Chan, errCh)
	go fetchSDOKSensorChan(runUID, "pid-ah2", "voc", api, pidVocChan, errCh)
	go fetchSDOKSensorChan(runUID, "zephyr-haf", "airflow", api, zephyrAirflowChan, errCh)
	go fetchSDOKSensorChan(runUID, "pms7003", "pm-1", api, pms1Chan, errCh)
	go fetchSDOKSensorChan(runUID, "pms7003", "pm-2.5", api, pms25Chan, errCh)
	go fetchSDOKSensorChan(runUID, "pms7003", "pm-10", api, pms10Chan, errCh)
	go fetchSDOKSensorChan(runUID, "bme680", "voc", api, bmeVocChan, errCh)
	go fetchSDOKSensorChan(runUID, "bme680", "co2", api, bmeCo2Chan, errCh)

	humiditySeries := <-humCh
	temperatureSeries := <-tempCh
	frameTimestamps := <-timeCh
	deviceMetadatas := <-devCh
	amplifierTemperatureSeries := <-ampTempCh

	sunriseCo2Series := <-sunriseCo2Chan
	pidVocSeries := <-pidVocChan
	zephyrAirflowSeries := <-zephyrAirflowChan
	pms1Series := <-pms1Chan
	pms25Series := <-pms25Chan
	pms10Series := <-pms10Chan
	bmeVocSeries := <-bmeVocChan
	bmeCo2Series := <-bmeCo2Chan

	for i := 0; i < 13; i++ { // 13 is the number of parallel goroutimes (counted manually)
		err = <-errCh
		if err != nil {
			return records, err
		}
	}

	if len(deviceMetadatas) != 1 {
		err = fmt.Errorf("invalid number of device metadatas for a single run: %d", len(deviceMetadatas))
		log.Println(deviceMetadatas)
		return records, err
	}
	deviceMetadata := deviceMetadatas[0]
	log.Printf("device: %s", deviceMetadata.ID)

	nFrames := len(frameTimestamps)
	log.Printf("Sensograms of run %s contain %d frames after subsampling", runUID, nFrames)

	if nFrames == 0 {
		err = fmt.Errorf("zero frames associated with the run %s", runUID)
		return records, err
	}

	if len(humiditySeries) == len(temperatureSeries) && len(humiditySeries) == nFrames {
		log.Printf("Sensogram, humidity and temperature series lengths are equal: %d", nFrames)
	} else {
		log.Printf("Warning: humidity (%d) and temperature (%d) series and sensograms (%d) have differnet length", len(humiditySeries), len(temperatureSeries), nFrames)
	}

	for _, itemMetadata := range itemMetadatas {
		itemMetadata.RunID = runMetadata.ID
		itemMetadata.DeviceID = deviceMetadata.ID
		record := &Record{
			Run:    runMetadata,
			Device: deviceMetadata,
			Item:   itemMetadata,
			Frames: make([]*SensogramFrame, 0),
		}
		// log.Println(record.Item.Index, record.Item.Cycle)
		records = append(records, record)
	}

	log.Printf("Constructed %d record objects out of %d item metadatas", len(records), len(itemMetadatas))

	var effectiveInfluxSensogramChunkSize = INFLUX_SENSOGRAM_CHUNK_SIZE
	nChunks := (nFrames / effectiveInfluxSensogramChunkSize) + 1
	log.Println("nChunks", nChunks, nFrames, effectiveInfluxSensogramChunkSize)

	var nSimultaneousRequests int = nChunks
	var effectiveNbSimulteneousChunks = INFLUX_SENSOGRAM_NB_SIMULTANEOUS_CHUNKS
	if isExternallyHosted {
		effectiveNbSimulteneousChunks *= 3
	}
	if nSimultaneousRequests > effectiveNbSimulteneousChunks {
		nSimultaneousRequests = effectiveNbSimulteneousChunks
	}

	var asyncRequestsQueue chan bool = make(chan bool, nSimultaneousRequests)

	var chunkedResultCh chan *ChunkedResult = make(chan *ChunkedResult, nChunks)
	var iChunk int
	for i := 0; i < nFrames; i += effectiveInfluxSensogramChunkSize {
		var _stop string
		if i+effectiveInfluxSensogramChunkSize < nFrames {
			_stop = fmt.Sprintf("time(v: %d)", frameTimestamps[i+effectiveInfluxSensogramChunkSize])
		} else {
			_stop = "now()"
		}
		// Add an element to signal job has been started
		// Will block until the queue has a vacant place
		asyncRequestsQueue <- true
		log.Printf("Async request queue length on adding: %d", len(asyncRequestsQueue))
		go _fetchRecordsChunkChan(
			runUID,
			frameTimestamps[i], //start
			_stop,
			iChunk,
			api,
			chunkedResultCh,
			progressCh,
			float32(nChunks), // always send total number of chunks
			asyncRequestsQueue,
		)
		iChunk++
	}

	log.Printf("Total chunk requests sent: %d. Total requested frames: %d", iChunk, iChunk*effectiveInfluxSensogramChunkSize)

	var chunkedResultHashCodeSlice []string = make([]string, nChunks)
	var i int
	for cr := range chunkedResultCh {
		if cr.Err != nil {
			return records, cr.Err
		}
		chunkedResultHashCodeSlice[cr.N] = cr.HashCode
		i++
		if i == nChunks {
			break
		}
	}

	var iFrame int

	var lastHFrameIdx int = -1
	var lastTFrameIdx int = -1
	var lastAmpTFrameIdx int = -1

	var lastSunriseCo2FrameIdx int = -1
	var lastPidVocFrameIdx int = -1
	var lastZephyrAirflowFrameIdx int = -1
	var lastPms1FrameIdx int = -1
	var lastPms25FrameIdx int = -1
	var lastPms10FrameIdx int = -1
	var lastBmeVocFrameIdx int = -1
	var lastBmeCo2FrameIdx int = -1

	for _, chunkHashCode := range chunkedResultHashCodeSlice {
		fileName := filepath.Join("storage", "cache", "chunks", chunkHashCode)
		var chunkOfFrames []*SensogramFrame
		err = ReadCache(fileName, &chunkOfFrames)
		if err != nil {
			return records, err
		}
		for _, frame := range chunkOfFrames {
			if lastHFrameIdx+1 < len(humiditySeries) {
				frame.Humidity, lastHFrameIdx = findNearestInSeries(frame, humiditySeries, lastHFrameIdx)
			}

			if lastTFrameIdx+1 < len(temperatureSeries) {
				frame.Temperature, lastTFrameIdx = findNearestInSeries(frame, temperatureSeries, lastTFrameIdx)
			}

			if lastAmpTFrameIdx+1 < len(amplifierTemperatureSeries) {
				frame.ThermodesorptionTemperature, lastAmpTFrameIdx = findNearestInSeries(frame, amplifierTemperatureSeries, lastAmpTFrameIdx)
			}

			// SDOK sensors may be empty
			if len(sunriseCo2Series) != 0 && lastSunriseCo2FrameIdx+1 < len(sunriseCo2Series) {
				frame.SunriseCo2, lastSunriseCo2FrameIdx = findNearestInSeries(frame, sunriseCo2Series, lastSunriseCo2FrameIdx)
			}
			if len(pidVocSeries) != 0 && lastPidVocFrameIdx+1 < len(pidVocSeries) {
				frame.PidVoc, lastPidVocFrameIdx = findNearestInSeries(frame, pidVocSeries, lastPidVocFrameIdx)
			}
			if len(zephyrAirflowSeries) != 0 && lastZephyrAirflowFrameIdx+1 < len(zephyrAirflowSeries) {
				frame.ZephyrAirflow, lastZephyrAirflowFrameIdx = findNearestInSeries(frame, zephyrAirflowSeries, lastZephyrAirflowFrameIdx)
			}
			if len(pms1Series) != 0 && lastPms1FrameIdx+1 < len(pms1Series) {
				frame.Pms1, lastPms1FrameIdx = findNearestInSeries(frame, pms1Series, lastPms1FrameIdx)
			}
			if len(pms25Series) != 0 && lastPms25FrameIdx+1 < len(pms25Series) {
				frame.Pms25, lastPms25FrameIdx = findNearestInSeries(frame, pms25Series, lastPms25FrameIdx)
			}
			if len(pms10Series) != 0 && lastPms10FrameIdx+1 < len(pms10Series) {
				frame.Pms10, lastPms10FrameIdx = findNearestInSeries(frame, pms10Series, lastPms10FrameIdx)
			}
			if len(bmeVocSeries) != 0 && lastBmeVocFrameIdx+1 < len(bmeVocSeries) {
				frame.BmeVoc, lastBmeVocFrameIdx = findNearestInSeries(frame, bmeVocSeries, lastBmeVocFrameIdx)
			}
			if len(bmeCo2Series) != 0 && lastBmeCo2FrameIdx+1 < len(bmeCo2Series) {
				frame.BmeCo2, lastBmeCo2FrameIdx = findNearestInSeries(frame, bmeCo2Series, lastBmeCo2FrameIdx)
			}

			err = dispatchFrameToRecords(records, frame)
			if err != nil {
				return records, err
			}
			iFrame++
		}
	}

	if os.Getenv("DEBUG_AA") != "" {
		var totalDispatchedFrames int
		for _, record := range records {
			log.Printf("Record %s #%d: %d frames", record.Item.Name, record.Item.Cycle, len(record.Frames))
			totalDispatchedFrames += len(record.Frames)
		}
		log.Printf("Total dispatched frames: %d. Expected frames: %d. Frames difference: %d", totalDispatchedFrames, iFrame, iFrame-totalDispatchedFrames)
	}

	return records, err
}

// Same as fetchRecords but sends the result and error to provided channels
func fetchRecordsChan(runUID string, api api.QueryAPI, ch chan []*Record, errChan chan error, progressCh chan float32, isExternallyHosted bool) {
	records, err := fetchRecords(runUID, api, progressCh, isExternallyHosted)
	errChan <- err
	ch <- records
}
