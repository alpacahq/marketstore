package writer

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/pkg/errors"
)

// QuotesRangeWriter is an interface to write the historical daily chart data to the marketstore
type QuotesRangeWriter interface {
	Write(quotesRange api.GetQuotesRangeResponse) error
}

// QuotesRangeWriterImpl is an implementation of the QuotesRangeWriter interface
type QuotesRangeWriterImpl struct {
	MarketStoreWriter MarketStoreWriter
	Timeframe         string
}

// Write converts the Response of the GetQuotesRange API to a ColumnSeriesMap and write it to the local marketstore server.
func (q *QuotesRangeWriterImpl) Write(quotesRange api.GetQuotesRangeResponse) error {
	// convert Quotes Data to CSM (ColumnSeriesMap)
	csm, err := q.convertToCSM(quotesRange)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to create CSM from Quotes Data. %v", quotesRange))
	}

	// write CSM to marketstore
	if err := q.MarketStoreWriter.Write(csm); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write the data to marketstore. %v", csm))
	}

	return nil
}

func (q *QuotesRangeWriterImpl) convertToCSM(resp api.GetQuotesRangeResponse) (io.ColumnSeriesMap, error) {
	csm := io.NewColumnSeriesMap()
	var epochs []int64
	var opens []float32
	var closes []float32
	var highs []float32
	var lows []float32
	var previousCloses []float32
	var volumes []int64

	for _, eq := range resp.ArrayOfEndOfDayQuote {
		// skip the symbol which date is empty string and cannot be parsed,
		// which means the symbols have never been executed
		if time.Time(eq.Date) == (time.Time{}) {
			continue
		}

		// When Volume is 0, xignite getQuotesRange API returns data with open:0, close:0, high:0, low:0.
		// we don't write the zero data to marketstore.
		if eq.Volume == 0 {
			continue
		}
		epochs = append(epochs, time.Time(eq.Date).In(time.UTC).Unix())
		opens = append(opens, eq.Open)
		closes = append(closes, eq.Close)
		highs = append(highs, eq.High)
		lows = append(lows, eq.Low)
		previousCloses = append(previousCloses, eq.PreviousClose)
		volumes = append(volumes, eq.Volume)
	}

	tbk := io.NewTimeBucketKey(resp.Security.Symbol + "/" + q.Timeframe + "/OHLCV")
	cs := q.newColumnSeries(epochs, opens, closes, highs, lows, previousCloses, volumes)
	csm.AddColumnSeries(*tbk, cs)
	return csm, nil
}

func (q QuotesRangeWriterImpl) newColumnSeries(
	epochs []int64, opens, closes, highs, lows, previousCloses []float32, volumes []int64,
) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Open", opens)
	cs.AddColumn("Close", closes)
	cs.AddColumn("High", highs)
	cs.AddColumn("Low", lows)
	cs.AddColumn("PreviousClose", previousCloses)
	cs.AddColumn("Volume", volumes)

	return cs
}
