// OnDiskAgg implements a trigger to downsample base timeframe data
// and write to disk.  Underlying data schema is expected at least
// - Open:float32 or float64
// - High:float32 or float64
// - Low:float32 or float64
// - Close:float32 or float64
// optionally,
// - Volume:one of float32, float64, or int32
//
// Example:
// 	triggers:
// 	  - module: ondiskagg.so
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
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"

	"github.com/alpacahq/marketstore/contrib/ondiskagg/calendar"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/trigger"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
)

// AggTriggerConfig is the configuration for OnDiskAggTrigger you can define in
// marketstore's config file under triggers extension.
type AggTriggerConfig struct {
	Destinations []string `json:"destinations"`
	Filter       string   `json:"filter"`
}

// OnDiskAggTrigger is the main trigger.
type OnDiskAggTrigger struct {
	config       map[string]interface{}
	destinations []string
	// filter by market hours if this is "nasdaq"
	filter string
}

var _ trigger.Trigger = &OnDiskAggTrigger{}

var loadError = errors.New("plugin load error")

func recast(config map[string]interface{}) *AggTriggerConfig {
	data, _ := json.Marshal(config)
	ret := AggTriggerConfig{}
	json.Unmarshal(data, &ret)
	return &ret
}

// NewTrigger returns a new on-disk aggregate trigger based on the configuration.
func NewTrigger(conf map[string]interface{}) (trigger.Trigger, error) {
	config := recast(conf)
	if len(config.Destinations) == 0 {
		glog.Errorf("no destinations are configured")
		return nil, loadError
	}

	glog.Infof("%d destination(s) configured", len(config.Destinations))
	filter := config.Filter
	if filter != "" && filter != "nasdaq" {
		glog.Infof("filter value \"%s\" is not recognized", filter)
		filter = ""
	}
	return &OnDiskAggTrigger{
		config:       conf,
		destinations: config.Destinations,
		filter:       filter,
	}, nil
}

func minInt64(values []int64) int64 {
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func maxInt64(values []int64) int64 {
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

// Fire implements trigger interface.
func (s *OnDiskAggTrigger) Fire(keyPath string, indexes []int64) {
	headIndex := minInt64(indexes)
	tailIndex := maxInt64(indexes)

	for _, timeframe := range s.destinations {
		s.processFor(timeframe, keyPath, headIndex, tailIndex)
	}
}

func (s *OnDiskAggTrigger) processFor(timeframe, keyPath string, headIndex, tailIndex int64) {
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
	outEpoch = append(outEpoch, groupKey.Unix())
	accumGroup.apply(groupStart, len(ts))

	// finalize output
	outCs := io.NewColumnSeries()
	outCs.AddColumn("Epoch", outEpoch)
	accumGroup.addColumns(outCs)

	return outCs
}

func main() {
}
