package writer

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// QuotesWriter is an interface to write the realtime stock data to the marketstore.
type QuotesWriter interface {
	Write(resp api.GetQuotesResponse) error
}

// QuotesWriterImpl is an implementation of the QuotesWriter interface.
type QuotesWriterImpl struct {
	MarketStoreWriter MarketStoreWriter
	Timeframe         string
	// QuotesWriterImpl writes data with the timezone
	Timezone *time.Location
}

// Write converts the Response of the GetQuotes API to a ColumnSeriesMap and write it to the local marketstore server.
func (q QuotesWriterImpl) Write(quotes api.GetQuotesResponse) error {
	// convert Quotes Data to CSM (ColumnSeriesMap)
	csm := q.convertToCSM(quotes)

	// write CSM to marketstore
	if err := q.MarketStoreWriter.Write(csm); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write the data to marketstore. %v", csm))
	}

	log.Debug("Data has been saved to marketstore successfully.")
	return nil
}

func (q *QuotesWriterImpl) convertToCSM(response api.GetQuotesResponse) io.ColumnSeriesMap {
	csm := io.NewColumnSeriesMap()

	for _, eq := range response.ArrayOfEquityQuote {
		// skip the symbol which LastMarketDate is empty
		// which means the symbols are not listed yet
		if eq.Outcome != api.SuccessOutcome || time.Time(eq.Quote.LastMarketDate) == (time.Time{}) {
			continue
		}

		// choose a latest time among askDateTime, BidDatetime and DateTime
		latestDateTime := getLatestTime(
			time.Time(eq.Quote.DateTime),
			time.Time(eq.Quote.AskDateTime),
			time.Time(eq.Quote.BidDateTime),
		)

		// adjust the time to UTC and set the timezone the same way as the marketstore config
		UTCOffset := time.Duration(-1*eq.Quote.UTCOffSet) * time.Hour
		latestDateTime = latestDateTime.Add(UTCOffset).In(q.Timezone)

		cs := q.newColumnSeries(latestDateTime.Unix(), eq)
		tbk := io.NewTimeBucketKey(eq.Security.Symbol + "/" + q.Timeframe + "/TICK")
		csm.AddColumnSeries(*tbk, cs)
	}

	return csm
}

func (q QuotesWriterImpl) newColumnSeries(epoch int64, eq api.EquityQuote) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Ask", []float32{eq.Quote.Ask})
	cs.AddColumn("AskSize", []float32{eq.Quote.AskSize})
	cs.AddColumn("Bid", []float32{eq.Quote.Bid})
	cs.AddColumn("BidSize", []float32{eq.Quote.BidSize})
	cs.AddColumn("Last", []float32{eq.Quote.Last})
	cs.AddColumn("LastSize", []float32{eq.Quote.LastSize})
	cs.AddColumn("DateTime", []int64{time.Time(eq.Quote.DateTime).Unix()})
	cs.AddColumn("Open", []float32{eq.Quote.Open})
	cs.AddColumn("High", []float32{eq.Quote.High})
	cs.AddColumn("Low", []float32{eq.Quote.Low})
	cs.AddColumn("Close", []float32{eq.Quote.Close})
	cs.AddColumn("Volume", []int64{eq.Quote.Volume})
	cs.AddColumn("PreviousClose", []float32{eq.Quote.PreviousClose})
	cs.AddColumn("ExchangeOfficialClose", []float32{eq.Quote.ExchangeOfficialClose})
	cs.AddColumn("PreviousExchangeOfficialClose", []float32{eq.Quote.PreviousExchangeOfficialClose})
	cs.AddColumn("ChangeFromPreviousClose", []float32{eq.Quote.ChangeFromPreviousClose})
	cs.AddColumn("PercentChangeFromPreviousClose", []float32{eq.Quote.PercentChangeFromPreviousClose})
	return cs
}

// getLatestTime return the latest time among 3 datetimes.
func getLatestTime(dt1, dt2, dt3 time.Time) time.Time {
	u1, u2, u3 := dt1.Unix(), dt2.Unix(), dt3.Unix()
	switch {
	case u1 > u2:
		if u1 > u3 {
			return dt1
		}
		return dt3
	case u2 > u3:
		return dt2
	default:
		return dt3
	}
}
