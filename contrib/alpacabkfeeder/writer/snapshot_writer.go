package writer

import (
	"fmt"
	"time"

	v2 "github.com/alpacahq/alpaca-trade-api-go/v2"
	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// SnapshotWriter is an interface to write the realtime stock data to the marketstore.
type SnapshotWriter interface {
	Write(snapshots map[string]*v2.Snapshot) error
}

// SnapshotWriterImpl is an implementation of the SnapshotWriter interface.
type SnapshotWriterImpl struct {
	MarketStoreWriter MarketStoreWriter
	Timeframe         string
	// SnapshotWriterImpl writes data with the timezone
	Timezone *time.Location
}

// Write converts the map(key:symbol, value:snapshot) to a ColumnSeriesMap and write it to the local marketstore server.
func (q SnapshotWriterImpl) Write(snapshots map[string]*v2.Snapshot) error {
	// convert Snapshot Data to CSM (ColumnSeriesMap)
	csm, err := q.convertToCSM(snapshots)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to create CSM from Snapshot Data. %v", snapshots))
	}

	// write CSM to marketstore
	if err := q.MarketStoreWriter.Write(csm); err != nil {
		return fmt.Errorf("failed to write data to marketstore. %w", err)
	}

	log.Debug("Data has been saved to marketstore successfully.")
	return nil
}

func (q *SnapshotWriterImpl) convertToCSM(snapshots map[string]*v2.Snapshot) (io.ColumnSeriesMap, error) {
	csm := io.NewColumnSeriesMap()

	for symbol, snapshot := range snapshots {
		if snapshot == nil || snapshot.LatestQuote == nil || snapshot.LatestTrade == nil {
			continue
		}

		// adjust the time to UTC and set the timezone the same way as the marketstore config
		latestTime := snapshot.LatestQuote.Timestamp.In(q.Timezone)

		cs := q.newColumnSeries(latestTime.Unix(), snapshot)
		tbk := io.NewTimeBucketKey(symbol + "/" + q.Timeframe + "/TICK")
		csm.AddColumnSeries(*tbk, cs)
	}

	return csm, nil
}

func (q SnapshotWriterImpl) newColumnSeries(epoch int64, ss *v2.Snapshot) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Ask", []float64{ss.LatestQuote.AskPrice})
	cs.AddColumn("AskSize", []uint32{ss.LatestQuote.AskSize})
	cs.AddColumn("Bid", []float64{ss.LatestQuote.BidPrice})
	cs.AddColumn("BidSize", []uint32{ss.LatestQuote.BidSize})
	return cs
}
