package writer

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// QuotesRangeWriter is an interface to write the historical daily chart data to the marketstore.
type QuotesRangeWriter interface {
	Write(symbol string, quotes []api.EndOfDayQuote, isIndexSymbol bool) error
}

// QuotesRangeWriterImpl is an implementation of the QuotesRangeWriter interface.
type QuotesRangeWriterImpl struct {
	MarketStoreWriter MarketStoreWriter
	Timeframe         string
}

// Write converts the Response of the QuickEquityHistorical/GetQuotesRange API
// to a ColumnSeriesMap and write it to the local marketstore server.
func (q *QuotesRangeWriterImpl) Write(symbol string, quotes []api.EndOfDayQuote, isIndexSymbol bool) error {
	// convert Quotes Data to CSM (ColumnSeriesMap)
	csm := q.convertToCSM(symbol, quotes, isIndexSymbol)

	// write CSM to marketstore
	if err := q.MarketStoreWriter.Write(csm); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write the data to marketstore. %v", csm))
	}

	return nil
}

func (q *QuotesRangeWriterImpl) convertToCSM(symbol string, quotes []api.EndOfDayQuote, isIndexSymbol bool,
) io.ColumnSeriesMap {
	csm := io.NewColumnSeriesMap()
	var epochs []int64
	var opens []float32
	var closes []float32
	var highs []float32
	var lows []float32
	var previousCloses []float32
	var volumes []int64
	var exchangeOfficialCloses []float32
	var previousExchangeOfficialClose []float32
	var changeFromPreviousClose []float32
	var percentChangeFromPreviousClose []float32

	for _, eq := range quotes {
		// skip the symbol which date is empty string and cannot be parsed,
		// which means the symbols have never been executed
		if time.Time(eq.Date) == (time.Time{}) {
			continue
		}

		// When Volume is 0, xignite getQuotesRange API returns data with open:0, close:0, high:0, low:0.
		// we don't write the zero data to marketstore.
		if !isIndexSymbol && eq.Volume == 0 {
			continue
		}
		epochs = append(epochs, time.Time(eq.Date).In(time.UTC).Unix())
		opens = append(opens, eq.Open)
		closes = append(closes, eq.Close)
		highs = append(highs, eq.High)
		lows = append(lows, eq.Low)
		previousCloses = append(previousCloses, eq.PreviousClose)
		volumes = append(volumes, eq.Volume)
		exchangeOfficialCloses = append(exchangeOfficialCloses, eq.ExchangeOfficialClose)
		previousExchangeOfficialClose = append(previousExchangeOfficialClose, eq.PreviousExchangeOfficialClose)
		changeFromPreviousClose = append(changeFromPreviousClose, eq.ChangeFromPreviousClose)
		percentChangeFromPreviousClose = append(percentChangeFromPreviousClose, eq.PercentChangeFromPreviousClose)
	}

	// to avoid that empty array is added to csm when all data are Volume=0 and there is no data to write
	if len(epochs) == 0 {
		// no data to write.
		return csm
	}

	tbk := io.NewTimeBucketKey(symbol + "/" + q.Timeframe + "/OHLCV")
	cs := q.newColumnSeries(epochs, opens, closes, highs, lows, previousCloses, exchangeOfficialCloses,
		previousExchangeOfficialClose, changeFromPreviousClose, percentChangeFromPreviousClose, volumes)
	csm.AddColumnSeries(*tbk, cs)
	return csm
}

func (q QuotesRangeWriterImpl) newColumnSeries(
	epochs []int64, opens, closes, highs, lows, previousCloses, exchangeOfficialCloses,
	previousExchangeOfficialClose, changeFromPreviousClose, percentChangeFromPreviousClose []float32, volumes []int64,
) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Open", opens)
	cs.AddColumn("Close", closes)
	cs.AddColumn("High", highs)
	cs.AddColumn("Low", lows)
	cs.AddColumn("ExchangeOfficialClose", exchangeOfficialCloses)
	cs.AddColumn("PreviousClose", previousCloses)
	cs.AddColumn("Volume", volumes)
	cs.AddColumn("PreviousExchangeOfficialClose", previousExchangeOfficialClose)
	cs.AddColumn("ChangeFromPreviousClose", changeFromPreviousClose)
	cs.AddColumn("PercentChangeFromPreviousClose", percentChangeFromPreviousClose)

	return cs
}
