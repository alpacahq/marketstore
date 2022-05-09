package writer

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// SnapshotWriter is an interface to write the realtime stock data to the marketstore.
type SnapshotWriter interface {
	Write(snapshots map[string]*api.Snapshot) error
}

// SnapshotWriterImpl is an implementation of the SnapshotWriter interface.
type SnapshotWriterImpl struct {
	MarketStoreWriter MarketStoreWriter
	Timeframe         string
	// SnapshotWriterImpl writes data with the timezone
	Timezone *time.Location
}

// Write converts the map(key:symbol, value:snapshot) to a ColumnSeriesMap and write it to the local marketstore server.
func (q SnapshotWriterImpl) Write(snapshots map[string]*api.Snapshot) error {
	// convert Snapshot Data to CSM (ColumnSeriesMap)
	csm := q.convertToCSM(snapshots)

	// write CSM to marketstore
	if err := q.MarketStoreWriter.Write(csm); err != nil {
		return fmt.Errorf("failed to write data to marketstore. %w", err)
	}

	log.Debug("Data has been saved to marketstore successfully.")
	return nil
}

func (q *SnapshotWriterImpl) convertToCSM(snapshots map[string]*api.Snapshot) io.ColumnSeriesMap {
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
			snapshot.DailyBar = &api.Bar{}
		}
		if snapshot.PrevDailyBar == nil {
			snapshot.PrevDailyBar = &api.Bar{}
		}
		if snapshot.MinuteBar == nil {
			snapshot.MinuteBar = &api.Bar{}
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

func (q SnapshotWriterImpl) newColumnSeries(epoch int64, ss *api.Snapshot) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("QuoteTimestamp", []int64{ss.LatestQuote.Timestamp.In(q.Timezone).Unix()})
	cs.AddColumn("Ask", []float32{float32(ss.LatestQuote.AskPrice)})
	cs.AddColumn("AskSize", []uint32{ss.LatestQuote.AskSize})
	cs.AddColumn("Bid", []float32{float32(ss.LatestQuote.BidPrice)})
	cs.AddColumn("BidSize", []uint32{ss.LatestQuote.BidSize})
	cs.AddColumn("TradeTimestamp", []int64{ss.LatestTrade.Timestamp.In(q.Timezone).Unix()})
	cs.AddColumn("Price", []float32{float32(ss.LatestTrade.Price)})
	cs.AddColumn("Size", []uint32{ss.LatestTrade.Size})
	cs.AddColumn("DailyTimestamp", []int64{ss.DailyBar.Timestamp.In(q.Timezone).Unix()})
	cs.AddColumn("Open", []float32{float32(ss.DailyBar.Open)})
	cs.AddColumn("High", []float32{float32(ss.DailyBar.High)})
	cs.AddColumn("Low", []float32{float32(ss.DailyBar.Low)})
	cs.AddColumn("Close", []float32{float32(ss.DailyBar.Close)})
	cs.AddColumn("Volume", []uint64{ss.DailyBar.Volume})
	cs.AddColumn("MinuteTimestamp", []int64{ss.MinuteBar.Timestamp.In(q.Timezone).Unix()})
	cs.AddColumn("MinuteOpen", []float32{float32(ss.MinuteBar.Open)})
	cs.AddColumn("MinuteHigh", []float32{float32(ss.MinuteBar.High)})
	cs.AddColumn("MinuteLow", []float32{float32(ss.MinuteBar.Low)})
	cs.AddColumn("MinuteClose", []float32{float32(ss.MinuteBar.Close)})
	cs.AddColumn("MinuteVolume", []uint64{ss.MinuteBar.Volume})
	cs.AddColumn("PreviousTimestamp", []int64{ss.PrevDailyBar.Timestamp.In(q.Timezone).Unix()})
	cs.AddColumn("PreviousOpen", []float32{float32(ss.PrevDailyBar.Open)})
	cs.AddColumn("PreviousHigh", []float32{float32(ss.PrevDailyBar.High)})
	cs.AddColumn("PreviousLow", []float32{float32(ss.PrevDailyBar.Low)})
	cs.AddColumn("PreviousClose", []float32{float32(ss.PrevDailyBar.Close)})
	cs.AddColumn("PreviousVolume", []uint64{ss.PrevDailyBar.Volume})
	return cs
}
