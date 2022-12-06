package clouddb

import (
	"crypto/md5"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api"
)

func transpose(slice [][]float64) ([][]float64, error) {
	var err error
	var result [][]float64
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in transpose", r)
			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown panic")
			}
			// invalidate rep
			result = nil
			// return the modified err and rep
		}
	}()

	xl := len(slice[0])
	yl := len(slice)
	result = make([][]float64, xl)
	for i := range result {
		result[i] = make([]float64, yl)
	}
	for i := 0; i < xl; i++ {
		for j := 0; j < yl; j++ {
			result[i][j] = slice[j][i]
		}
	}
	return result, err
}

// Finds a frame from a series of frames which is nearest (in time) to a given reference frame
// Does the similar job of pandas.merge_asof https://pandas.pydata.org/docs/reference/api/pandas.merge_asof.html() which is used in arydbc.
// Returns the value of the found frame in series and its index
func findNearestInSeries(referenceFrame *SensogramFrame, series []*FrameBase, lastIdx int) (*float64, int) {
	var value *float64
	if lastIdx == -1 {
		value = &series[0].Value
		lastIdx++
	} else {
		currentSeriesFrame := series[lastIdx]
		nextSeriesFrame := series[lastIdx+1]
		currentDT := math.Abs(float64(referenceFrame.Timestamp - currentSeriesFrame.Timestamp))
		nextDT := math.Abs(float64(referenceFrame.Timestamp - nextSeriesFrame.Timestamp))
		if currentDT < nextDT {
			value = &currentSeriesFrame.Value
		} else {
			value = &nextSeriesFrame.Value
			lastIdx++
		}
	}
	return value, lastIdx
}

// parseStartStr parses the influxdb relative start string (e.g. "-3mo")
// into absolute timestamp
func parseStartStr(startStr string) (int, error) {
	var err error
	var startInt int //in milliseconds
	// Parse startStr to deduce the effectiveStartTimeMilli
	// Do not parse if empty or if it is a simple "0"
	if startStr != "" && startStr != "0" {
		re := regexp.MustCompile(`(-[0-9]*)(\w+)$`) // e.g. "-3mo"
		groups := re.FindStringSubmatch(startStr)
		if len(groups) < 3 {
			err = fmt.Errorf("could not parse startStr correctly: %s, %v", startStr, groups)
			return startInt, err
		}
		coefStr := groups[1]
		litStr := groups[2]
		coefInt, err := strconv.Atoi(coefStr)
		if err != nil {
			return startInt, err
		}
		if coefInt >= 0 {
			err = fmt.Errorf("startStr's coefficient is non-negative: %d", coefInt)
			return startInt, err
		}
		var duration time.Duration
		switch litStr {
		case "h":
			duration = time.Duration(coefInt) * time.Hour
		case "d":
			duration = time.Duration(coefInt) * 24 * time.Hour
		case "w":
			duration = time.Duration(coefInt) * 7 * 24 * time.Hour
		case "mo":
			duration = time.Duration(coefInt) * 31 * 24 * time.Hour
		case "yr":
			duration = time.Duration(coefInt) * 366 * 24 * time.Hour
		}
		startInt = int(time.Now().Add(duration).UnixMilli())
	}
	return startInt, err
}

// getRunsFullRecords returns full sensogram records given a list of runUIDs
// To query a single run, simply call this method with single-element list: []string{"run-id"}
// Used in all the encoding variants of corresponding callbacks (JSON, GOB, RemoteHandle), but not the classic Handle
func getRunsFullRecords(runUIDs []string, api api.QueryAPI, progressCh chan float32, isExternallyHosted bool) ([]*Record, error) {
	var err error
	var allRecords []*Record

	// Syncronous fetch run by run - to avoid memory over-consumption
	// Accumulates all records in one variable, though.. not quite memory-efficient
	for _, runUID := range runUIDs {
		records, err := fetchRecords(runUID, api, progressCh, isExternallyHosted)
		if err != nil {
			return allRecords, err
		}
		allRecords = append(allRecords, records...)
	}

	return allRecords, err
}

func ReadCache(filename string, obj interface{}) error {
	var err error
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	err = dec.Decode(obj)
	return err
}

func WriteCache(filename string, obj interface{}) error {
	var err error
	newFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(newFile)
	err = enc.Encode(obj)
	return err
}

func HashString(s string) (string, error) {
	var err error
	var bs string
	h := md5.New()
	_, err = h.Write([]byte(s))
	if err != nil {
		return bs, err
	}
	bs = fmt.Sprintf("%x", h.Sum(nil))
	return bs, err
}
