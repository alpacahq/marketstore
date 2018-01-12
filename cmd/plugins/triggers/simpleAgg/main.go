// SimpleAgg implements a trigger to downsample base timeframe data
// and write to disk.  Underlying data schema is expected at least
// - Open:float32
// - High:float32
// - Low:float32
// - Close:float32
// optionally,
// - Volume:one of float32, float64, or int32
//
// Example:
// 	triggers:
// 	  - module: simpleAgg.so
// 	    on: */1Min/OHLCV
// 	    config:
// 	      filter: "nasdaq"
// 	      destinations:
// 	        - 5Min
// 	        - 15Min
// 	        - 1H
// 	        - 1D
//
// destinations are downsample target time windows.  Optionally, if filter
// is set to "nasdaq", it filters the scan data by NASDAQ market hours.
package main

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"

	"github.com/alpacahq/marketstore/catalog"
	"github.com/alpacahq/marketstore/cmd/plugins/triggers/simpleAgg/calendar"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/trigger"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
)

type SimpleAggTrigger struct {
	config       map[string]interface{}
	destinations []string
	// filter by market hours if this is "nasdaq"
	filter string
}

var _ trigger.Trigger = &SimpleAggTrigger{}

var loadError = errors.New("plugin load error")

func NewTrigger(config map[string]interface{}) (trigger.Trigger, error) {
	glog.Infof("NewTrigger")

	destIns, ok := config["destinations"]
	if !ok {
		glog.Errorf("no destinations are configured")
		return nil, loadError
	}

	params, ok := destIns.([]interface{})
	if !ok {
		glog.Errorf("destinations do not look like an array")
		return nil, loadError
	}
	destinations := []string{}
	for _, ifval := range params {
		timeframe, ok := ifval.(string)
		if !ok {
			glog.Errorf("destination %v does not look like string", ifval)
		}
		destinations = append(destinations, timeframe)
	}
	glog.Infof("%d destination(s) configured", len(destinations))
	filterVal, ok := config["filter"]
	filter := ""
	if ok {
		filter = filterVal.(string)
	}
	return &SimpleAggTrigger{
		config:       config,
		destinations: destinations,
		filter:       filter,
	}, nil
}

func (s *SimpleAggTrigger) Fire(keyPath string, indexes []int64) {
	headIndex := indexes[0]
	tailIndex := indexes[len(indexes)-1]

	for _, timeframe := range s.destinations {
		s.processFor(timeframe, keyPath, headIndex, tailIndex)
	}
}

func (s *SimpleAggTrigger) processFor(timeframe, keyPath string, headIndex, tailIndex int64) {
	theInstance := executor.ThisInstance
	catalogDir := theInstance.CatalogDir
	elements := strings.Split(keyPath, "/")
	tbkString := strings.Join(elements[:len(elements)-1], "/")
	tf := utils.NewTimeframe(elements[1])
	fileName := elements[len(elements)-1]
	year, _ := strconv.Atoi(strings.Replace(fileName, ".bin", "", 1))
	tbk := io.NewTimeBucketKey(tbkString)
	headTs := io.IndexToTime(headIndex, int64(tf.PeriodsPerDay()), int16(year))
	tailTs := io.IndexToTime(tailIndex, int64(tf.PeriodsPerDay()), int16(year))
	timeWindow := utils.CandleDurationFromString(timeframe)
	start := timeWindow.Truncate(headTs)
	end := timeWindow.Ceil(tailTs)
	// TODO: this is not needed once we support "<" operator
	end = end.Add(-time.Second)

	targetTbkString := elements[0] + "/" + timeframe + "/" + elements[2]
	targetTbk := io.NewTimeBucketKey(targetTbkString)

	// Scan
	q := planner.NewQuery(catalogDir)
	q.AddTargetKey(tbk)
	q.SetRange(start, end)

	// decide whether to apply market-hour filter
	applyingFilter := false
	if s.filter == "nasdaq" && timeWindow.Duration() >= 24*time.Hour {
		calendarTz := calendar.Nasdaq.Tz()
		if utils.InstanceConfig.Timezone.String() != calendarTz.String() {
			glog.Errorf("misconfiguration... system must be configure in %s", calendarTz)
		} else {
			q.AddTimeQual(calendar.Nasdaq.IsMarketOpen)
			applyingFilter = true
		}
	}
	parsed, err := q.Parse()
	if err != nil {
		glog.Errorf("%v", err)
		return
	}
	scanner, err := executor.NewReader(parsed)
	if err != nil {
		glog.Errorf("%v", err)
		return
	}
	csm, _, err := scanner.Read()
	if err != nil {
		glog.Errorf("%v", err)
		return
	}
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		if !applyingFilter {
			// Nothing in there... really?
			glog.Errorf("result is empty for %s -> %s", tbk, targetTbk)
		}
		return
	}
	// calculate aggregated values
	outCs := aggregate(cs, targetTbk)
	outCsm := io.NewColumnSeriesMap()
	outCsm.AddColumnSeries(*targetTbk, outCs)

	if err := executor.WriteCSM(outCsm, false); err != nil {
		glog.Errorf("failed to wriet CSM: %v", err)
	}
}

func aggregate(cs *io.ColumnSeries, tbk *io.TimeBucketKey) *io.ColumnSeries {
	timeWindow := utils.CandleDurationFromString(tbk.GetItemInCategory("Timeframe"))

	params := []accumParam{
		accumParam{"Open", "first", "Open"},
		accumParam{"High", "max", "High"},
		accumParam{"Low", "min", "Low"},
		accumParam{"Close", "last", "Close"},
	}
	if cs.Exists("Volume") {
		params = append(params, accumParam{"Volume", "sum", "Volume"})
	}
	accumGroup := newAccumGroup(cs, params)

	ts := cs.GetTime()
	outEpoch := make([]int64, 0)

	groupKey := timeWindow.Truncate(ts[0])
	groupStart := 0
	// accumulate inputs.  Since the input is ordered by
	// time, it is just to slice by correct boundaries
	for i, t := range ts {
		if !timeWindow.IsWithin(t, groupKey) {
			// Emit new row and re-init aggState
			outEpoch = append(outEpoch, groupKey.Unix())
			accumGroup.apply(groupStart, i)
			groupKey = timeWindow.Truncate(t)
			groupStart = i
		}
	}
	// accumulate any remaining values if not yet
	if groupStart < len(ts)-1 {
		outEpoch = append(outEpoch, groupKey.Unix())
		accumGroup.apply(groupStart, len(ts))
	}

	// finalize output
	outCs := io.NewColumnSeries()
	outCs.AddColumn("Epoch", outEpoch)
	accumGroup.addColumns(outCs)

	return outCs
}

func getWriter(theInstance *executor.InstanceMetadata, tbk *io.TimeBucketKey, year int16, dataShapes []io.DataShape) (*executor.Writer, error) {
	catalogDir := theInstance.CatalogDir
	tbi, err := catalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	// TODO: refactor to common code
	// TODO: check existing file with new dataShapes
	if err != nil {
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			return nil, err
		}

		tbi = io.NewTimeBucketInfo(
			*tf, tbk.GetPathToYearFiles(catalogDir.GetPath()),
			"Created By Trigger", year,
			dataShapes, io.FIXED,
		)
		err = catalogDir.AddTimeBucket(tbk, tbi)
		if err != nil {
			if _, ok := err.(catalog.FileAlreadyExists); !ok {
				return nil, err
			}
		}
	}
	q := planner.NewQuery(theInstance.CatalogDir)
	q.AddTargetKey(tbk)
	parsed, err := q.Parse()
	if err != nil {
		return nil, err
	}
	return executor.NewWriter(parsed, theInstance.TXNPipe, theInstance.CatalogDir)
}

func main() {
}
