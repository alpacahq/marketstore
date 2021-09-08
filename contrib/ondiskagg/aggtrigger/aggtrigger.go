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
package aggtrigger

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/frontend"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models"
	modelsenum "github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
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
	destinations timeframes
	// filter by market hours if this is "nasdaq"
	filter   string
	aggCache *sync.Map
}

var (
	_         trigger.Trigger = &OnDiskAggTrigger{}
)

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
		log.Warn("no destinations are configured\n")
		return nil, fmt.Errorf("plugin load error")
	}

	log.Info("%d destination(s) configured\n", len(config.Destinations))

	filter := config.Filter
	if filter != "" && filter != "nasdaq" {
		log.Error("filter value \"%s\" is not recognized\n", filter)
		filter = ""
	}

	var tfs timeframes

	for _, dest := range config.Destinations {
		tf := utils.TimeframeFromString(dest)
		if tf == nil {
			log.Fatal("invalid destination: %s", dest)
			return nil, errors.New("please specify valid timeframe for 'destinations' " +
				"in the aggtrigger config. dest=" + dest)
		}
		tfs = append(tfs, *tf)
	}

	return &OnDiskAggTrigger{
		config:       conf,
		destinations: tfs,
		filter:       filter,
		aggCache:     &sync.Map{},
	}, nil
}

// Fire implements trigger interface.
func (s *OnDiskAggTrigger) Fire(keyPath string, records []trigger.Record) {
	elements := strings.Split(keyPath, "/")
	tf := utils.NewTimeframe(elements[1])
	fileName := elements[len(elements)-1]
	year, _ := strconv.Atoi(strings.Replace(fileName, ".bin", "", 1))
	tbk := io.NewTimeBucketKey(strings.Join(elements[:len(elements)-1], "/"))

	head := io.IndexToTime(
		records[0].Index(),
		tf.Duration,
		int16(year))

	tail := io.IndexToTime(
		records[len(records)-1].Index(),
		tf.Duration,
		int16(year))

	// query the upper bound since it will contain the most candles
	window := utils.CandleDurationFromString(s.destinations.UpperBound().String)

	// check if we have a valid cache, if not, re-query
	if v, ok := s.aggCache.Load(tbk.String()); ok {
		c := v.(*cachedAgg)

		if !c.Valid(tail, head) {
			s.aggCache.Delete(tbk.String())

			goto Query
		}

		cs := trigger.RecordsToColumnSeries(
			*tbk, c.cs.GetDataShapes(),
			tf.Duration, int16(year), records)

		cs = io.ColumnSeriesUnion(cs, &c.cs)

		s.write(tbk, cs, tail, head, elements)

		return
	}

Query:
	csm, err := s.query(tbk, window, head, tail)
	if err != nil || csm == nil {
		log.Error("query error for %v (%v)\n", tbk.String(), err)
		return
	}

	cs := (*csm)[*tbk]

	if cs != nil {
		s.write(tbk, cs, tail, head, elements)
	}

	return
}

func (s *OnDiskAggTrigger) write(
	tbk *io.TimeBucketKey,
	cs *io.ColumnSeries,
	tail, head time.Time,
	elements []string) {

	for _, dest := range s.destinations {
		symbol := elements[0]
		attributeGroup := elements[2]
		if elements[2] == "TRADE" {
			attributeGroup = "OHLCV"
		}
		aggTbk := io.NewTimeBucketKeyFromString(symbol + "/" + dest.String + "/" + attributeGroup)

		if err := s.writeAggregates(aggTbk, tbk, *cs, dest, head, tail, symbol); err != nil {
			log.Error(
				"failed to write %v aggregates (%v)\n",
				tbk.String(),
				err)
			return
		}
	}
}

type cachedAgg struct {
	cs         io.ColumnSeries
	tail, head time.Time
}

func (c *cachedAgg) Valid(tail, head time.Time) bool {
	return tail.Unix() >= c.tail.Unix() && head.Unix() <= c.head.Unix()
}

func (s *OnDiskAggTrigger) writeAggregates(
	aggTbk, baseTbk *io.TimeBucketKey,
	cs io.ColumnSeries,
	dest utils.Timeframe,
	head, tail time.Time,
	symbol string) error {

	csm := io.NewColumnSeriesMap()

	window := utils.CandleDurationFromString(dest.String)
	start := window.Truncate(head).Unix()
	end := window.Ceil(tail).Add(-time.Second).Unix()

	slc, err := io.SliceColumnSeriesByEpoch(cs, &start, &end)
	if err != nil {
		return err
	}

	if len(slc.GetEpoch()) == 0 {
		return nil
	}

	// decide whether to apply market-hour filter
	applyingFilter := false
	if s.filter == "nasdaq" && window.Duration() >= utils.Day {
		calendarTz := calendar.Nasdaq.Tz()
		if utils.InstanceConfig.Timezone.String() != calendarTz.String() {
			log.Warn("misconfiguration... system must be configure in %s\n", calendarTz)
		} else {
			applyingFilter = true
		}
	}

	// store when writing for upper bound
	if dest.Duration == s.destinations.UpperBound().Duration {
		defer func() {
			t := window.Truncate(tail)
			tEpoch := t.Unix()
			h := time.Unix(end, 0)

			cacheSlc, _ := io.SliceColumnSeriesByEpoch(cs, &tEpoch, &end)

			s.aggCache.Store(baseTbk.String(), &cachedAgg{
				cs:   cacheSlc,
				tail: t,
				head: h,
			})
		}()
	}

	// apply the filter
	if applyingFilter {
		tqSlc := slc.ApplyTimeQual(calendar.Nasdaq.EpochIsMarketOpen)

		// normally this will always be true, but when there are random bars
		// on the weekend, it won't be, so checking to avoid panic
		if len(tqSlc.GetEpoch()) > 0 {
			csm.AddColumnSeries(*aggTbk, aggregate(tqSlc, aggTbk, baseTbk, symbol))
		}
	} else {
		csm.AddColumnSeries(*aggTbk, aggregate(&slc, aggTbk, baseTbk, symbol))
	}

	return executor.WriteCSM(csm, false)
}

func aggregate(cs *io.ColumnSeries, aggTbk, baseTbk *io.TimeBucketKey, symbol string) *io.ColumnSeries {
	timeWindow := utils.CandleDurationFromString(aggTbk.GetItemInCategory("Timeframe"))
	var params []accumParam

	suffix := fmt.Sprintf("/%s/%s", models.TradeTimeframe, models.TradeSuffix)
	if strings.HasSuffix(baseTbk.GetItemKey(), suffix) {
		// Ticks to bars
		trades := models.NewTrade(symbol, cs.Len())
		epochs := cs.GetEpoch()
		nanos := cs.GetColumn("Nanoseconds").([]int32)
		prices := cs.GetColumn("Price").([]float64)
		sizes := cs.GetColumn("Size").([]uint64)
		exchanges := cs.GetColumn("Exchange").([]byte)
		tapeids := cs.GetColumn("TapeID").([]byte)
		cond1 := cs.GetColumn("Cond1").([]byte)
		cond2 := cs.GetColumn("Cond2").([]byte)
		cond3 := cs.GetColumn("Cond3").([]byte)
		cond4 := cs.GetColumn("Cond4").([]byte)
		for i := range epochs {
			condition := []modelsenum.TradeCondition{
				modelsenum.TradeCondition(cond1[i]),
				modelsenum.TradeCondition(cond2[i]),
				modelsenum.TradeCondition(cond3[i]),
				modelsenum.TradeCondition(cond4[i]),
			}
			trades.Add(
				epochs[i], int(nanos[i]),
				modelsenum.Price(prices[i]),
				modelsenum.Size(sizes[i]),
				modelsenum.Exchange(exchanges[i]),
				modelsenum.Tape(tapeids[i]),
				condition...)
		}

		bar := models.FromTrades(trades, symbol, timeWindow.String)
		cs := bar.GetCs()

		return cs
	} else {
		// bars to bars
		params = []accumParam{
			{"Open", "first", "Open"},
			{"High", "max", "High"},
			{"Low", "min", "Low"},
			{"Close", "last", "Close"},
		}
		if cs.Exists("Volume") {
			params = append(params, accumParam{"Volume", "sum", "Volume"})
		}

		accumGroup := newAccumGroup(cs, params)

		ts, _ := cs.GetTime()
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
}

func (s *OnDiskAggTrigger) query(
	tbk *io.TimeBucketKey,
	window *utils.CandleDuration,
	head, tail time.Time) (*io.ColumnSeriesMap, error) {

	cDir := executor.ThisInstance.CatalogDir

	start := window.Truncate(head)

	// TODO: adding 1 second is not needed once we support "<" operator
	end := window.Ceil(tail).Add(-time.Second)

	// Scan
	qs := frontend.NewQueryService(cDir)
	csm, err := qs.ExecuteQuery(tbk, start, end, 0, false, nil)
	if err != nil {
		return nil, err
	}

	return &csm, nil
}
