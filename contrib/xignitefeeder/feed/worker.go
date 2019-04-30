package feed

import (
	"fmt"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/contrib/xignitefeeder/writer"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/pkg/errors"
	"time"
)

// Worker is the main worker instance.  It implements bgworker.Run().
type Worker struct {
	MarketTimeChecker MarketTimeChecker
	APIClient         api.Client
	SymbolManager     *symbols.Manager
	QuotesWriter      writer.QuotesWriter
	Interval          int
}

// Run() runs forever to get TICK for each symbol in the target exchanges by Xignite API periodically,
// and writes to local marketstore.  It ignores errors returned from Xignite.
func (w *Worker) Run() {
	for {
		// try to get stock data and write them every second
		go w.tryPrintErr()
		time.Sleep(time.Duration(w.Interval) * time.Second)
	}
}

func (w *Worker) tryPrintErr() {
	if err := w.try(); err != nil {
		log.Error(err.Error())
	}
}

// try calls GetQuotes endpoint of Xignite API, convert the API response to a ColumnSeriesMap and write it to marketstore
func (w *Worker) try() error {
	// check if it needs to work now
	if !w.MarketTimeChecker.isOpen(time.Now().UTC()) {
		return nil
	}
	// call Xignite API to get Quotes data
	identifiers := w.SymbolManager.GetAllIdentifiers()
	response, err := w.APIClient.GetRealTimeQuotes(identifiers)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to get data from Xignite API. %v", identifiers))
	}

	// write Quotes data
	err = w.QuotesWriter.Write(response)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write quotes data."))
	}

	return nil
}
