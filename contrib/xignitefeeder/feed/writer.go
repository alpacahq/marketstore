package feed

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type QuotesWriter interface {
	Write(resp api.GetQuotesResponse) error
}

type MarketStoreQuotesWriter struct {
	// Key: symbol (string), Value: last execution time (time.Time).
	LastExecutionTimes sync.Map
	Timeframe          string
}

func (msw MarketStoreQuotesWriter) Write(quotes api.GetQuotesResponse) error {
	// convert Quotes Data to CSM (ColumnSeriesMap)
	csm, err := msw.convertToCSM(quotes)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to create CSM from Quotes Data. %v", quotes))
	}

	// write CSM to marketstore
	if err := msw.WriteToMarketStore(csm); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write TICK data to marketstore. %v", csm))
	}

	return nil
}

func (msw *MarketStoreQuotesWriter) convertToCSM(response api.GetQuotesResponse) (io.ColumnSeriesMap, error) {
	csm := io.NewColumnSeriesMap()

	for _, eq := range response.ArrayOfEquityQuote {
		// skip the symbol which execution time is empty string and cannot be parsed,
		// which means hey had never been executed
		if eq.Outcome != "Success" || time.Time(eq.Quote.DateTime) == (time.Time{}) {
			continue
		}

		// check if this data has already been written to Marketstore
		executionTime := time.Time(eq.Quote.DateTime)

		//if !msw.needToWrite(eq.Security.Symbol, executionTime) {
		//	continue
		//}

		cs := msw.newColumnSeries(executionTime.Unix(), eq.Quote.Ask, eq.Quote.Bid)
		tbk := io.NewTimeBucketKey(eq.Security.Symbol + "/" + msw.Timeframe + "/TICK")
		csm.AddColumnSeries(*tbk, cs)
	}

	return csm, nil
}

// if the tick data for the last execution has already been written before, skip it
func (msw *MarketStoreQuotesWriter) needToWrite(symbol string, executionTime time.Time) bool {
	if lastExecutionTime, ok := msw.LastExecutionTimes.Load(symbol); ok && lastExecutionTime.(time.Time).Equal(executionTime) {
		return false
	}

	msw.LastExecutionTimes.Store(symbol, executionTime)
	return true
}

func (msw MarketStoreQuotesWriter) newColumnSeries(epoch int64, ask float32, bid float32) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Ask", []float32{ask})
	cs.AddColumn("Bid", []float32{bid})

	return cs
}

func (msw MarketStoreQuotesWriter) WriteToMarketStore(csm io.ColumnSeriesMap) error {
	// no new data to write
	if len(csm) == 0 {
		return nil
	}
	return executor.WriteCSM(csm, false)
}
