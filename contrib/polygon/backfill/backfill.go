package backfill

import (
	"fmt"
	"math"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/worker"

	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const defaultFormat = "2006-01-02"

type ConsolidatedUpdateInfo struct {
	UpdateHighLow bool
	UpdateLast    bool
	UpdateVolume  bool
}

var WriteTime time.Duration
var ApiCallTime time.Duration
var WaitTime time.Duration
var NoIngest bool

// https://polygon.io/glossary/us/stocks/conditions-indicators
var ConditionToUpdateInfo = map[int]ConsolidatedUpdateInfo{
	0: {true, true, true},   // Regular Sale
	1: {true, true, true},   // Acquisition
	2: {false, false, true}, // Average Price Trade
	3: {true, true, true},   // Automatic Execution
	4: {true, true, true},   // Bunched Trade
	5: {true, false, true},  // Bunched Sold Trade
	// 6: {?, ?, ? },  //  CAP Election
	7:  {false, false, true}, // Cash Sale
	8:  {true, true, true},   // Closing Prints
	9:  {true, true, true},   // Cross Trade
	10: {true, false, true},  // Derivatively Priced
	11: {true, true, true},   // Distribution
	//	12: {false, false, true},  // XXX: Form T is disabled with the purpose to include extended hours data in mkts
	13: {false, false, true},  // Extended Trading Hours (Sold Out of Sequence)
	14: {true, true, true},    // Intermarket Sweep
	15: {false, false, false}, // Market Center Official Close
	16: {false, false, false}, // Market Center Official Open
	// 17: {?, ?, ?}, // Market Center Opening Trade
	// 18: {?, ?, ?}, // Market Center Reopening Trade
	// 19: {?, ?, ?}, // Market Center Closing Trade
	20: {false, false, true}, // Next Day
	21: {false, false, true}, // Price Variation Trade
	22: {true, false, true},  // Prior Reference Price
	23: {true, true, true},   // Rule 155 Trade (AMEX)
	// 24: {?, ?, ?}, // Rule 127 NYSE
	25: {true, true, true}, // Opening Prints
	// 26: {?, ?, ?}, // Opened
	27: {true, true, true},  // Stopped Stock (Regular Trade)
	28: {true, true, true},  // Re-Opening Prints
	29: {true, false, true}, // Seller
	30: {true, true, true},  // Sold Last
	// 32: {?, ?, ?}, // Sold Out
	33: {true, false, true}, // Sold (out of Sequence)
	34: {true, true, true},  // Split Trade
	// 35: {?, ?, ?},  // Stock option
	36: {true, true, true},   // Yellow Flag Regular Trade
	37: {false, false, true}, // Odd Lot Trade
	38: {true, true, false},  // Corrected Consolidated Close (per listing market)
	// 39: {?, ?, ?}, // Unknown
	// 40: {?, ?, ?}, // Held
	// 41: {?, ?, ?}, // Trade Thru Exempt
	// 42: {?, ?, ?}, // NonEligible
	// 43: {?, ?, ?}, // NonEligible Extended
	// 44: {?, ?, ?}, // Cancelled
	// 45: {?, ?, ?}, // Recovery
	// 46: {?, ?, ?}, // Correction
	// 47: {?, ?, ?}, // As of
	// 48: {?, ?, ?}, // As of Correction
	// 49: {?, ?, ?}, // As of Cancel
	// 50: {?, ?, ?}, // OOB
	// 51: {?, ?, ?}, // Summary
	52: {false, false, true}, // Contingent Trade
	53: {false, false, true}, // Qualified Contingent Trade ("QCT")
	// 54: {?, ?, ?}, // Errored
	// 55: {?, ?, ?}, // OPENING_REOPENING_TRADE_DETAIL
	// 56: {TBD, TBD, TBD}, // Placeholder
	// 59: {TBD, TBD, TBD}, // Placeholder for 611 exempt
}

var (
	// NY timezone
	NY, _     = time.LoadLocation("America/New_York")
	ErrRetry  = fmt.Errorf("retry error")
	BackfillM *sync.Map
)

func Bars(symbol string, from, to time.Time, batchSize int, unadjusted bool, writerWP *worker.WorkerPool) (err error) {
	if from.IsZero() {
		from = time.Date(2014, 1, 1, 0, 0, 0, 0, NY)
	}

	if to.IsZero() {
		to = time.Now()
	}
	t := time.Now()
	resp, err := api.GetHistoricAggregates(symbol, "minute", 1, from, to, &batchSize, unadjusted)
	if err != nil {
		return err
	}
	ApiCallTime += time.Now().Sub(t)

	if NoIngest {
		return nil
	}

	if len(resp.Results) == 0 {
		return nil
	}

	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/OHLCV")
	csm := io.NewColumnSeriesMap()

	epoch := make([]int64, len(resp.Results))
	open := make([]float32, len(resp.Results))
	high := make([]float32, len(resp.Results))
	low := make([]float32, len(resp.Results))
	close := make([]float32, len(resp.Results))
	volume := make([]int32, len(resp.Results))

	for i, bar := range resp.Results {
		timestamp := bar.EpochMilliseconds / 1000
		if time.Unix(timestamp, 0).After(to) || time.Unix(timestamp, 0).Before(from) {
			// polygon sometime returns inconsistent data
			continue
		}
		epoch[i] = timestamp
		open[i] = float32(bar.Open)
		high[i] = float32(bar.High)
		low[i] = float32(bar.Low)
		close[i] = float32(bar.Close)
		volume[i] = int32(bar.Volume)
	}

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", close)
	cs.AddColumn("Volume", volume)
	csm.AddColumnSeries(*tbk, cs)

	t = time.Now()
	writerWP.Do(func() {
		tt := time.Now()
		err = executor.WriteCSM(csm, false)
		if err != nil {
			log.Warn("[polygon] failed to write bars for %v (%v) %s - %s", symbol, err, from, to)
		}
		WriteTime += time.Now().Sub(tt)
	})

	WaitTime += time.Now().Sub(t)

	return nil
}

func intInSlice(s int, l []int) bool {
	for _, item := range l {
		if s == item {
			return true
		}
	}
	return false
}

func BuildBarsFromTrades(symbol string, date time.Time, exchangeIDs []int, batchSize int) error {
	resp, err := api.GetHistoricTrades(symbol, date.Format(defaultFormat), batchSize)
	if err != nil {
		return err
	}

	csm := tradesToBars(resp.Results, symbol, exchangeIDs)
	if csm == nil {
		return nil
	}

	if err = executor.WriteCSM(csm, false); err != nil {
		return err
	}

	return nil
}

func conditionToUpdateInfo(tick api.TradeTick) ConsolidatedUpdateInfo {
	r := ConsolidatedUpdateInfo{true, true, true}

	for _, condition := range tick.Conditions {
		if val, ok := ConditionToUpdateInfo[condition]; ok {
			r.UpdateHighLow = r.UpdateHighLow && val.UpdateHighLow
			r.UpdateLast = r.UpdateLast && val.UpdateLast
			r.UpdateVolume = r.UpdateVolume && val.UpdateVolume
		}
	}

	return r
}

func tradesToBars(ticks []api.TradeTick, symbol string, exchangeIDs []int) io.ColumnSeriesMap {
	var csm io.ColumnSeriesMap

	if len(ticks) == 0 {
		return csm
	}

	var epoch int64
	var open, high, low, close_ float32
	var volume, tickCnt int32

	epochs := make([]int64, 1440)
	opens := make([]float32, 1440)
	highs := make([]float32, 1440)
	lows := make([]float32, 1440)
	closes := make([]float32, 1440)
	volumes := make([]int32, 1440)
	tickCnts := make([]int32, 1440)

	barIdx := 0
	lastBucketTimestamp := time.Time{}

	storeAggregates := func() {
		// Store the minute aggregate
		epochs[barIdx] = epoch
		opens[barIdx] = open
		highs[barIdx] = high
		lows[barIdx] = low
		closes[barIdx] = close_
		volumes[barIdx] = volume
		tickCnts[barIdx] = tickCnt

		barIdx++
	}

	// FIXME: The daily close bars are not handled correctly:
	// We are aggregating from ticks to minutes then from minutes to daily prices.
	// The current routine correctly aggregates ticks to minutes.
	// The daily close price however should be the tick set with conditions
	// 'Closing Prints' & 'Trade Thru Exempt' (8 & 15), generally sent 2-5 minutes
	// after the official market close time. Given the daily roll-up is using minute data,
	// the close tick will be aggregated  and impossible to extract from the minutely bar.
	// In order to solve this, the daily close price should explicitly be stored and used
	// in the daily roll-up calculation. This would require substantial refactor.
	// The current solution therefore is just a reasonable approximation of the daily close price.
	for _, tick := range ticks {
		if !intInSlice(tick.Exchange, exchangeIDs) {
			continue
		}

		price := float32(tick.Price)
		timestamp := time.Unix(0, tick.SipTimestamp)
		bucketTimestamp := timestamp.Truncate(time.Minute)

		if bucketTimestamp.Before(lastBucketTimestamp) {
			log.Warn("[polygon] got an out-of-order tick for %v @ %v, skipping", symbol, timestamp)
			continue
		}

		if !lastBucketTimestamp.Equal(bucketTimestamp) {
			if open != 0 && volume != 0 {
				storeAggregates()
			}

			lastBucketTimestamp = bucketTimestamp
			epoch = bucketTimestamp.Unix()
			open = 0
			high = 0
			low = math.MaxFloat32
			close_ = 0
			volume = 0
			tickCnt = 0
		}

		tickCnt += 1

		updateInfo := conditionToUpdateInfo(tick)

		if !updateInfo.UpdateLast && !updateInfo.UpdateHighLow && !updateInfo.UpdateVolume {
			continue
		}

		if updateInfo.UpdateHighLow {
			if high < price {
				high = price
			}
			if low > price {
				low = price
			}
		}

		if updateInfo.UpdateLast {
			if open == 0 {
				open = price
			}
			close_ = price
		}

		if updateInfo.UpdateVolume {
			volume += int32(tick.Size)
		}
	}

	if open != 0 && volume != 0 {
		storeAggregates()
	}

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs[:barIdx])
	cs.AddColumn("Open", opens[:barIdx])
	cs.AddColumn("High", highs[:barIdx])
	cs.AddColumn("Low", lows[:barIdx])
	cs.AddColumn("Close", closes[:barIdx])
	cs.AddColumn("Volume", volumes[:barIdx])
	cs.AddColumn("TickCnt", tickCnts[:barIdx])

	csm = io.NewColumnSeriesMap()
	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/OHLCV")
	csm.AddColumnSeries(*tbk, cs)

	return csm
}

func Trades(symbol string, from time.Time, to time.Time, batchSize int, writerWP *worker.WorkerPool) error {
	trades := make([]api.TradeTick, 0)
	t := time.Now()
	for date := from; to.After(date); date = date.Add(24 * time.Hour) {
		if calendar.Nasdaq.IsMarketDay(date) {
			resp, err := api.GetHistoricTrades(symbol, date.Format(defaultFormat), batchSize)
			if err != nil {
				return err
			}
			trades = append(trades, resp.Results...)
		}
	}
	ApiCallTime += time.Now().Sub(t)

	if NoIngest {
		return nil
	}

	if len(trades) > 0 {
		csm := io.NewColumnSeriesMap()
		tbk := io.NewTimeBucketKeyFromString(symbol + "/1Sec/TRADE")
		cs := io.NewColumnSeries()

		epoch := make([]int64, len(trades))
		nanos := make([]int32, len(trades))
		price := make([]float32, len(trades))
		size := make([]int32, len(trades))

		for i, tick := range trades {
			timestamp := time.Unix(0, tick.SipTimestamp)
			bucketTimestamp := timestamp.Truncate(time.Second)
			epoch[i] = bucketTimestamp.Unix()
			nanos[i] = int32(timestamp.UnixNano() - bucketTimestamp.UnixNano())
			price[i] = float32(tick.Price)
			size[i] = int32(tick.Size)
		}

		cs.AddColumn("Epoch", epoch)
		cs.AddColumn("Nanoseconds", nanos)
		cs.AddColumn("Price", price)
		cs.AddColumn("Size", size)
		csm.AddColumnSeries(*tbk, cs)

		t = time.Now()
		writerWP.Do(func() {
			tt := time.Now()
			err := executor.WriteCSM(csm, true)
			if err != nil {
				log.Warn("[polygon] failed to write trades for %v (%v) between %s and %s ", symbol, err, from.String(), to.String())
			}
			WriteTime += time.Now().Sub(tt)
		})
		WaitTime += time.Now().Sub(t)
	}

	return nil
}

func Quotes(symbol string, from, to time.Time, batchSize int, writerWP *worker.WorkerPool) error {
	// FIXME: This function is broken with the following problems:
	//  - Callee (backfiller.go) wrongly checks the market day (checks for the day after)
	//  - Callee always specifies one day worth of data, pointless to do a for loop
	//  - Retry mechanism on GetHistoricQuotes calls Bars()
	//  - Underlying GetHistoricQuotes uses Polygon API v1 which is deprecated.
	quotes := make([]api.QuoteTick, 0)

	t := time.Now()
	for date := from; to.After(date); date = date.Add(24 * time.Hour) {
		if calendar.Nasdaq.IsMarketDay(date) {
			resp, err := api.GetHistoricQuotes(symbol, date.Format(defaultFormat), batchSize)
			if err != nil {
				return err
			}
			quotes = append(quotes, resp.Ticks...)
		}
	}
	ApiCallTime += time.Now().Sub(t)

	if NoIngest {
		return nil
	}

	if len(quotes) > 0 {
		csm := io.NewColumnSeriesMap()
		tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/QUOTE")
		cs := io.NewColumnSeries()

		epoch := make([]int64, len(quotes))
		nanos := make([]int32, len(quotes))
		bidPrice := make([]float32, len(quotes))
		bidSize := make([]int32, len(quotes))
		askPrice := make([]float32, len(quotes))
		askSize := make([]int32, len(quotes))

		for i, tick := range quotes {
			timestamp := time.Unix(0, 1000*1000*tick.Timestamp)

			epoch[i] = timestamp.Unix()
			nanos[i] = int32(timestamp.Nanosecond())
			bidPrice[i] = float32(tick.BidPrice)
			bidSize[i] = int32(tick.BidSize)
			askPrice[i] = float32(tick.BidPrice)
			askSize[i] = int32(tick.AskSize)
		}

		cs.AddColumn("Epoch", epoch)
		cs.AddColumn("Nanoseconds", nanos)
		cs.AddColumn("BidPrice", bidPrice)
		cs.AddColumn("AskPrice", askPrice)
		cs.AddColumn("BidSize", bidSize)
		cs.AddColumn("AskSize", askSize)
		csm.AddColumnSeries(*tbk, cs)

		t = time.Now()
		writerWP.Do(func() {
			tt := time.Now()
			err := executor.WriteCSM(csm, true)
			if err != nil {
				log.Warn("[polygon] failed to write trades for %v (%v) between %s and %s ", symbol, err, from.String(), to.String())
			}
			WriteTime += time.Now().Sub(tt)
		})
		WaitTime += time.Now().Sub(t)
	}

	return nil
}
