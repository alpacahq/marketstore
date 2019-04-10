package feed

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/pkg/errors"
	"time"
)

const (
	// layout of Datetime string returned from Xignite API
	XigniteDateTimeLayout = "2006/01/02 15:04:05"
)

// Worker is the main worker instance.  It implements bgworker.Run().
type Worker struct {
	APIClient         api.Client
	MarketTimeChecker MarketTimeChecker
	CSMWriter         CSMWriter
	Timeframe         string
	Identifiers       []string
	Interval          int
	// Key: symbol, Value: last execution time
	LastExecutionTimes map[string]time.Time
}

// Run() runs forever to get TICK for each configured symbol every second from Xignite API,
// and writes in marketstore data format.  Even in case any error is returned from Xignite,
// it calls the API after a second.
func (w *Worker) Run() {

	for {
		// try to get stock data and write them every second
		err := w.try()
		if err != nil {
			fmt.Println(err)
		}
		time.Sleep(time.Duration(w.Interval) * time.Second)
	}
}

// try calls GetQuotes endpoint of Xignite API, convert the API response to a ColumnSeriesMap and write it to marketstore
func (w *Worker) try() error {
	// check if it needs to work now
	if !w.MarketTimeChecker.isOpen(time.Now()) {
		return nil
	}

	// call Xignite API
	response, err := w.APIClient.GetRealTimeQuotes(w.Identifiers)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to get data from Xignite API. %v", w.Identifiers))
	}

	// convert API response to CSM (ColumnSeriesMap)
	csm, err := w.convertToColumnSeriesMap(response)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to create CSM from API response. %v", response))
	}

	// no new data to write
	if len(csm) == 0 {
		return nil
	}

	// write CSM to marketstore
	err = w.CSMWriter.Write(csm)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write TICK data to marketstore. %v", csm))
	}

	return nil
}

func (w *Worker) convertToColumnSeriesMap(response api.GetQuotesResponse) (io.ColumnSeriesMap, error) {
	csm := io.NewColumnSeriesMap()

	for _, eq := range response.ArrayOfEquityQuote {
		if eq.Outcome != "Success" {
			continue
		}

		executionTime, err := time.Parse(XigniteDateTimeLayout, eq.Quote.DateTime)
		if err != nil {
			return nil, err
		}

		if !w.needToWrite(eq.Security.Symbol, executionTime) {
			continue
		}

		cs := NewColumnSeries(executionTime.Unix(), eq.Quote.Ask, eq.Quote.Bid)
		tbk := io.NewTimeBucketKey(eq.Security.Symbol + "/" + w.Timeframe + "/TICK")
		csm.AddColumnSeries(*tbk, cs)
	}

	return csm, nil
}

// if the tick data for the last execution has already been written before, skip it
func (w *Worker) needToWrite(symbol string, executionTime time.Time) bool {
	if lastExecutionTime, ok := w.LastExecutionTimes[symbol]; ok && lastExecutionTime.Equal(executionTime) {
		return false
	}

	w.LastExecutionTimes[symbol] = executionTime
	return true
}

func NewColumnSeries(epoch int64, ask float32, bid float32) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Ask", []float32{ask})
	cs.AddColumn("Bid", []float32{bid})

	return cs
}

// ConvertDateTimeToEpoch returns an epoch time [seconds] converted from the date string in XigniteDateTimeLayout
func ConvertDateTimeToEpoch(datetime string) (epoch int64, err error) {
	dt, err := time.Parse(XigniteDateTimeLayout, datetime)
	if err != nil {
		return 0, errors.Wrap(err, fmt.Sprintf("failed to parse datetime string. %v", datetime))
	}
	return dt.Unix(), nil
}
