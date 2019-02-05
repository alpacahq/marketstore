package backfill

import (
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/contrib/polygon/api"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
)

var (
	// NY timezone
	NY, _    = time.LoadLocation("America/New_York")
	format   = "2006-01-02"
	ErrRetry = fmt.Errorf("retry error")
)

func Bars(symbol string, from, to time.Time) (err error) {
	if from.IsZero() {
		from = time.Date(2014, 1, 1, 0, 0, 0, 0, NY)
	}

	if to.IsZero() {
		to = time.Now()
	}

	resp, err := api.GetHistoricAggregates(symbol, "minute", from, to, nil)
	if err != nil {
		if strings.Contains(err.Error(), "GOAWAY") {
			<-time.After(5 * time.Second)
			return Bars(symbol, from, to)
		}

		return err
	}

	if len(resp.Ticks) == 0 {
		return
	}

	tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/OHLCV")
	csm := io.NewColumnSeriesMap()

	epoch := make([]int64, len(resp.Ticks))
	open := make([]float32, len(resp.Ticks))
	high := make([]float32, len(resp.Ticks))
	low := make([]float32, len(resp.Ticks))
	close := make([]float32, len(resp.Ticks))
	volume := make([]int32, len(resp.Ticks))

	for i, bar := range resp.Ticks {
		epoch[i] = bar.EpochMilliseconds / 1000
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

	return executor.WriteCSM(csm, false)
}

func Trades(symbol string, from, to time.Time) error {
	if from.IsZero() {
		from = time.Date(2014, 1, 1, 0, 0, 0, 0, NY)
	}

	if to.IsZero() {
		to = time.Now()
	}

	for {
		resp, err := api.GetHistoricTrades(symbol, from.Format("2006-01-02"))
		if err != nil {
			if strings.Contains(err.Error(), "GOAWAY") {
				<-time.After(5 * time.Second)
				return Bars(symbol, from, to)
			}

			return err
		}

		if len(resp.Ticks) > 0 {

			csm := io.NewColumnSeriesMap()
			tbk := io.NewTimeBucketKeyFromString(symbol + "/1Min/TRADE")
			cs := io.NewColumnSeries()

			epoch := make([]int64, len(resp.Ticks))
			nanos := make([]int32, len(resp.Ticks))
			price := make([]float32, len(resp.Ticks))
			size := make([]int32, len(resp.Ticks))

			for i, tick := range resp.Ticks {
				timestamp := time.Unix(0, 1000*1000*tick.Timestamp)
				bucketTimestamp := timestamp.Truncate(time.Minute)

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

			if err = executor.WriteCSM(csm, true); err != nil {
				return err
			}
		}

		from = from.AddDate(0, 0, 1)

		if from.After(to) {
			break
		}
	}

	return nil
}

func Quotes(symbol string, from, to time.Time) error {
	if from.IsZero() {
		from = time.Date(2014, 1, 1, 0, 0, 0, 0, NY)
	}

	if to.IsZero() {
		to = time.Now()
	}

	var (
		csm io.ColumnSeriesMap
		cs  *io.ColumnSeries
		tbk *io.TimeBucketKey

		epoch    []int64
		nanos    []int32
		bidPrice []float32
		bidSize  []int32
		askPrice []float32
		askSize  []int32

		err  error
		resp *api.HistoricQuotes
	)

	for {
		if resp, err = api.GetHistoricQuotes(symbol, from.Format("2006-01-02")); err != nil {
			if strings.Contains(err.Error(), "GOAWAY") {
				<-time.After(5 * time.Second)
				return Bars(symbol, from, to)
			}

			return err
		}

		if len(resp.Ticks) > 0 {
			csm = io.NewColumnSeriesMap()
			tbk = io.NewTimeBucketKeyFromString(symbol + "/1Min/QUOTE")
			cs = io.NewColumnSeries()

			epoch = make([]int64, len(resp.Ticks))
			nanos = make([]int32, len(resp.Ticks))
			bidPrice = make([]float32, len(resp.Ticks))
			bidSize = make([]int32, len(resp.Ticks))
			askPrice = make([]float32, len(resp.Ticks))
			askSize = make([]int32, len(resp.Ticks))

			for i, tick := range resp.Ticks {
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

			if err = executor.WriteCSM(csm, true); err != nil {
				return err
			}
		}

		from = from.AddDate(0, 0, 1)

		if from.After(to) {
			break
		}
	}

	return nil
}
