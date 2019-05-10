package writer

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/pkg/errors"
	"time"
)

// QuotesRangeWriter is an interface to write the historical daily chart data to the marketstore
type QuotesRangeWriter interface {
	Write(quotesRange api.GetQuotesRangeResponse) error
}

// MarketStoreQuotesRangeWriter is an implementation of the QuotesRangeWriter interface
type MarketStoreQuotesRangeWriter struct {
	Timeframe string
}

// Write converts the Response of the GetQuotesRange API to a ColumnSeriesMap and write it to the local marketstore server.
func (msw MarketStoreQuotesRangeWriter) Write(quotesRange api.GetQuotesRangeResponse) error {
	// convert Quotes Data to CSM (ColumnSeriesMap)
	csm, err := msw.convertToCSM(quotesRange)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to create CSM from Quotes Data. %v", quotesRange))
	}

	// write CSM to marketstore
	if err := msw.writeToMarketStore(csm); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write the data to marketstore. %v", csm))
	}

	return nil
}

func (msw *MarketStoreQuotesRangeWriter) convertToCSM(resp api.GetQuotesRangeResponse) (io.ColumnSeriesMap, error) {
	csm := io.NewColumnSeriesMap()
	var epochs []int64
	var opens []float32
	var closes []float32
	var highs []float32
	var lows []float32
	var volumes []float32

	for _, eq := range resp.ArrayOfEndOfDayQuote {
		// skip the symbol which date is empty string and cannot be parsed,
		// which means the symbols have never been executed
		if time.Time(eq.Date) == (time.Time{}) {
			continue
		}

		epochs = append(epochs, time.Time(eq.Date).Unix())
		opens = append(opens, eq.Open)
		closes = append(closes, eq.Close)
		highs = append(highs, eq.High)
		lows = append(lows, eq.Low)
		volumes = append(volumes, eq.Volume)
	}

	tbk := io.NewTimeBucketKey(resp.Security.Symbol + "/" + msw.Timeframe + "/OHLCV")
	cs := msw.newColumnSeries(epochs, opens, closes, highs, lows, volumes)
	csm.AddColumnSeries(*tbk, cs)
	return csm, nil
}

func (msw MarketStoreQuotesRangeWriter) newColumnSeries(epochs []int64, opens, closes, highs, lows, volumes []float32) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Open", opens)
	cs.AddColumn("Close", closes)
	cs.AddColumn("High", highs)
	cs.AddColumn("Low", lows)
	cs.AddColumn("Volume", volumes)

	return cs
}

func (msw MarketStoreQuotesRangeWriter) writeToMarketStore(csm io.ColumnSeriesMap) error {
	// no new data to write
	if len(csm) == 0 {
		return nil
	}
	return executor.WriteCSM(csm, false)
}
