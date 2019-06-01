package writer

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/pkg/errors"
	"time"
)

// QuotesWriter is an interface to write the realtime stock data to the marketstore
type QuotesWriter interface {
	Write(resp api.GetQuotesResponse) error
}

// QuotesWriterImpl is an implementation of the QuotesWriter interface
type QuotesWriterImpl struct {
	MarketStoreWriter MarketStoreWriter
	Timeframe         string
}

// Write converts the Response of the GetQuotes API to a ColumnSeriesMap and write it to the local marketstore server.
func (q QuotesWriterImpl) Write(quotes api.GetQuotesResponse) error {
	// convert Quotes Data to CSM (ColumnSeriesMap)
	csm, err := q.convertToCSM(quotes)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to create CSM from Quotes Data. %v", quotes))
	}

	// write CSM to marketstore
	if err := q.MarketStoreWriter.Write(csm); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write the data to marketstore. %v", csm))
	}

	log.Debug("Data has been saved to marketstore successfully.")
	return nil
}

func (q *QuotesWriterImpl) convertToCSM(response api.GetQuotesResponse) (io.ColumnSeriesMap, error) {
	csm := io.NewColumnSeriesMap()

	for _, eq := range response.ArrayOfEquityQuote {
		// skip the symbol which Ask/Bid time is empty string and cannot be parsed,
		// which means the symbols have never been executed
		if eq.Outcome != "Success" || time.Time(eq.Quote.AskDateTime) == (time.Time{}) || time.Time(eq.Quote.BidDateTime) == (time.Time{}) {
			continue
		}

		// choose a latest time among askDateTime, BidDatetime and DateTime
		var latestDateTime = getLatestTime(
			time.Time(eq.Quote.DateTime),
			time.Time(eq.Quote.AskDateTime),
			time.Time(eq.Quote.BidDateTime),
		)

		//if !q.needToWrite(eq.Security.Symbol, dateTime) {
		//	continue
		//}

		cs := q.newColumnSeries(latestDateTime.Unix(), eq.Quote.Ask, eq.Quote.Bid, eq.Quote.Last)
		tbk := io.NewTimeBucketKey(eq.Security.Symbol + "/" + q.Timeframe + "/TICK")
		csm.AddColumnSeries(*tbk, cs)
	}

	return csm, nil
}

func (q QuotesWriterImpl) newColumnSeries(epoch int64, ask, bid, last float32) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Ask", []float32{ask})
	cs.AddColumn("Bid", []float32{bid})
	cs.AddColumn("Last", []float32{last})
	return cs
}

// getLatestTime return the latest time among 3 datetimes
func getLatestTime(dt1, dt2, dt3 time.Time) time.Time {
	u1, u2, u3 := dt1.Unix(), dt2.Unix(), dt3.Unix()
	if u1 > u2 {
		if u1 > u3 {
			return dt1
		} else {
			return dt3
		}
	} else if u2 > u3 {
		return dt2
	} else {
		return dt3
	}
}
