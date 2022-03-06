package writer

import (
	"fmt"
	"time"

	v2 "github.com/alpacahq/alpaca-trade-api-go/v2"

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
	csm := q.convertToCSM(snapshots)

	// write CSM to marketstore
	if err := q.MarketStoreWriter.Write(csm); err != nil {
		return fmt.Errorf("failed to write data to marketstore. %w", err)
	}

	log.Debug("Data has been saved to marketstore successfully.")
	return nil
}

func (q *SnapshotWriterImpl) convertToCSM(snapshots map[string]*v2.Snapshot) io.ColumnSeriesMap {
	csm := io.NewColumnSeriesMap()

	for symbol, snapshot := range snapshots {
		if snapshot == nil || snapshot.LatestQuote == nil || snapshot.LatestTrade == nil {
			continue
		}

		// adjust the time to UTC and set the timezone the same way as the marketstore config
		latestTime := latestTime(
			snapshot.LatestQuote.Timestamp,
			snapshot.LatestTrade.Timestamp,
		).In(q.Timezone)

		// These additional fields are not always provided.
		// fill empty data to keep the number of columns in the CSM
		if snapshot.DailyBar == nil {
			snapshot.DailyBar = &v2.Bar{}
		}
		if snapshot.PrevDailyBar == nil {
			snapshot.PrevDailyBar = &v2.Bar{}
		}
		if snapshot.MinuteBar == nil {
			snapshot.MinuteBar = &v2.Bar{}
		}
		cs := q.newColumnSeries(latestTime.Unix(), snapshot)
		tbk := io.NewTimeBucketKey(symbol + "/" + q.Timeframe + "/TICK")
		csm.AddColumnSeries(*tbk, cs)
	}

	return csm
}

func latestTime(time1, time2 time.Time) time.Time {
	if time1.After(time2) {
		return time1
	}
	return time2
}

func (q SnapshotWriterImpl) newColumnSeries(epoch int64, ss *v2.Snapshot) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("QuoteTimestamp", []int64{ss.LatestQuote.Timestamp.In(q.Timezone).Unix()})
	cs.AddColumn("Ask", []float64{ss.LatestQuote.AskPrice})
	cs.AddColumn("AskSize", []uint32{ss.LatestQuote.AskSize})
	cs.AddColumn("Bid", []float64{ss.LatestQuote.BidPrice})
	cs.AddColumn("BidSize", []uint32{ss.LatestQuote.BidSize})
	cs.AddColumn("TradeTimestamp", []int64{ss.LatestTrade.Timestamp.In(q.Timezone).Unix()})
	cs.AddColumn("Price", []float64{ss.LatestTrade.Price})
	cs.AddColumn("Size", []uint32{ss.LatestTrade.Size})
	cs.AddColumn("DailyTimestamp", []int64{ss.DailyBar.Timestamp.In(q.Timezone).Unix()})
	cs.AddColumn("Open", []float64{ss.DailyBar.Open})
	cs.AddColumn("High", []float64{ss.DailyBar.High})
	cs.AddColumn("Low", []float64{ss.DailyBar.Low})
	cs.AddColumn("Close", []float64{ss.DailyBar.Close})
	cs.AddColumn("Volume", []uint64{ss.DailyBar.Volume})
	cs.AddColumn("MinuteTimestamp", []int64{ss.MinuteBar.Timestamp.In(q.Timezone).Unix()})
	cs.AddColumn("MinuteOpen", []float64{ss.MinuteBar.Open})
	cs.AddColumn("MinuteHigh", []float64{ss.MinuteBar.High})
	cs.AddColumn("MinuteLow", []float64{ss.MinuteBar.Low})
	cs.AddColumn("MinuteClose", []float64{ss.MinuteBar.Close})
	cs.AddColumn("MinuteVolume", []uint64{ss.MinuteBar.Volume})
	cs.AddColumn("PreviousTimestamp", []int64{ss.PrevDailyBar.Timestamp.In(q.Timezone).Unix()})
	cs.AddColumn("PreviousOpen", []float64{ss.PrevDailyBar.Open})
	cs.AddColumn("PreviousHigh", []float64{ss.PrevDailyBar.High})
	cs.AddColumn("PreviousLow", []float64{ss.PrevDailyBar.Low})
	cs.AddColumn("PreviousClose", []float64{ss.PrevDailyBar.Close})
	cs.AddColumn("PreviousVolume", []uint64{ss.PrevDailyBar.Volume})
	return cs
}
