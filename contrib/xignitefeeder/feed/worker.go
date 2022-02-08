package feed

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/symbols"
	"github.com/alpacahq/marketstore/v4/contrib/xignitefeeder/writer"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Worker is the main worker instance.  It implements bgworker.Run().
type Worker struct {
	MarketTimeChecker MarketTimeChecker
	APIClient         api.Client
	SymbolManager     symbols.Manager
	QuotesWriter      writer.QuotesWriter
	Interval          int
}

// Run runs forever to get quotes data for each symbol in the target exchanges using Xignite API periodically,
// and writes the data to the local marketstore server.
func (w *Worker) Run() {
	ctx := context.Background()
	for {
		// try to get the data and write them every second
		go w.tryPrintErr(ctx)
		time.Sleep(time.Duration(w.Interval) * time.Second)
	}
}

// tryPrintErr tries and write the error log.
func (w *Worker) tryPrintErr(ctx context.Context) {
	if err := w.try(ctx); err != nil {
		log.Error(err.Error())
	}

	defer func() {
		err := recover()
		if err != nil {
			log.Error("Panic occurred:", err)
		}
	}()
}

// try calls GetQuotes endpoint of Xignite API,
// convert the API response to a ColumnSeriesMap and write it to the marketstore.
func (w *Worker) try(ctx context.Context) error {
	// check if it needs to work now
	if !w.MarketTimeChecker.IsOpen(time.Now().UTC()) {
		return nil
	}
	// call Xignite API to get Quotes data
	identifiers := w.SymbolManager.GetAllIdentifiers()
	response, err := w.APIClient.GetRealTimeQuotes(ctx, identifiers)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to get data from Xignite API. %v", identifiers))
	}

	// write Quotes data
	err = w.QuotesWriter.Write(response)
	if err != nil {
		return errors.Wrap(err, "failed to write quotes data.")
	}

	return nil
}
