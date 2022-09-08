package feed

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/symbols"
	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/writer"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Worker is the main worker instance.  It implements bgworker.Run().
type Worker struct {
	MarketTimeChecker MarketTimeChecker
	APIClient         GetSnapShotsAPIClient
	SymbolManager     symbols.Manager
	SnapshotWriter    writer.SnapshotWriter
	BarWriter         writer.BarWriter
	Interval          int
}

type GetSnapShotsAPIClient interface {
	GetSnapshots(symbols []string) (map[string]*api.Snapshot, error)
}

// Run runs forever to get quotes data for each symbol in the target exchanges using Alpaca API periodically,
// and writes the data to the local marketstore server.
func (w *Worker) Run() {
	for {
		// try to get the data and write them every second
		go w.tryPrintErr()
		time.Sleep(time.Duration(w.Interval) * time.Second)
	}
}

// tryPrintErr tries and write the error log.
func (w *Worker) tryPrintErr() {
	if err := w.try(); err != nil {
		log.Error(err.Error())
	}

	// nolint:gocritic // (unnecessaryDefer) this defer is for recovering from panic
	defer func() {
		err := recover()
		if err != nil {
			log.Error("Panic occurred:", err)
		}
	}()
}

// try calls GetSnapshots endpoint of Alpaca API,
// convert the API response to a ColumnSeriesMap and write it to the marketstore.
func (w *Worker) try() error {
	// check if it needs to work now
	if !w.MarketTimeChecker.IsOpen(time.Now().UTC()) {
		return nil
	}
	// call Alpaca API to get Quotes data
	symbls := w.SymbolManager.GetAllSymbols()
	snapshots, err := w.APIClient.GetSnapshots(symbls)
	if err != nil {
		return fmt.Errorf("failed to get snapshot from Alpaca API. len(symbols)=%v: %w",
			len(symbls), err,
		)
	}
	log.Info("successfully got snapshot data from Alpaca API. len(symbols)=%v", len(symbls))

	// write SnapShot data
	err = w.SnapshotWriter.Write(snapshots)
	if err != nil {
		return errors.Wrap(err, "failed to write quotes data")
	}

	return nil
}
