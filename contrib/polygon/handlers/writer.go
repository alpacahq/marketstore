package handlers

import (
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type trade struct {
	epoch int64
	nanos int32
	px    float64
	sz    uint64
}

type quote struct {
	epoch int64   // 8
	nanos int32   // 4
	bidPx float64 // 4
	askPx float64 // 4
	bidSz uint64  // 4
	askSz uint64  // 4
}

func Write(writeMap map[io.TimeBucketKey]interface{}) {
	// preallocate the data structures for re-use
	var (
		csm   io.ColumnSeriesMap
		epoch []int64
		nanos []int32
		bidPx []float64
		askPx []float64
		px    []float64
		bidSz []uint64
		askSz []uint64
		sz    []uint64
	)
	csm = io.NewColumnSeriesMap()
	for tbk, bucket := range writeMap {
		switch b := bucket.(type) {
		case []*quote:
			for _, q := range b {
				epoch = append(epoch, q.epoch)
				nanos = append(nanos, q.nanos)
				bidPx = append(bidPx, q.bidPx)
				askPx = append(askPx, q.askPx)
				bidSz = append(bidSz, q.bidSz)
				askSz = append(askSz, q.askSz)
			}
			if len(epoch) > 0 {
				csm.AddColumn(tbk, "Epoch", epoch)
				csm.AddColumn(tbk, "Nanoseconds", nanos)
				csm.AddColumn(tbk, "BidPrice", bidPx)
				csm.AddColumn(tbk, "AskPrice", askPx)
				csm.AddColumn(tbk, "BidSize", bidSz)
				csm.AddColumn(tbk, "AskSize", askSz)
				// trim the slices
				epoch = epoch[:0]
				nanos = nanos[:0]
				bidPx = bidPx[:0]
				bidSz = bidSz[:0]
				askPx = bidPx[:0]
				askSz = askSz[:0]
			}
		case []*trade:
			for _, t := range b {
				epoch = append(epoch, t.epoch)
				nanos = append(nanos, t.nanos)
				px = append(px, t.px)
				sz = append(sz, t.sz)
			}
			if len(epoch) > 0 {
				csm.AddColumn(tbk, "Epoch", epoch)
				csm.AddColumn(tbk, "Nanoseconds", nanos)
				csm.AddColumn(tbk, "Price", px)
				csm.AddColumn(tbk, "Size", sz)

				// trim the slices
				epoch = epoch[:0]
				nanos = nanos[:0]
				px = px[:0]
				sz = sz[:0]
			}
		}
	}

	if err := executor.WriteCSM(csm, true); err != nil {
		log.Error("[polygon] failed to write csm (%v)", err)
	}
}

func writeTrades(writeMap map[io.TimeBucketKey][]*trade) {
	// preallocate the data structures for re-use
	var (
		csm   io.ColumnSeriesMap
		epoch []int64
		nanos []int32
		px    []float64
		sz    []uint64
	)
	csm = io.NewColumnSeriesMap()
	for tbk, trades := range writeMap {
		for _, tr := range trades {
			epoch = append(epoch, tr.epoch)
			nanos = append(nanos, tr.nanos)
			px = append(px, tr.px)
			sz = append(sz, tr.sz)
		}
		if len(epoch) > 0 {
			csm.AddColumn(tbk, "Epoch", epoch)
			csm.AddColumn(tbk, "Nanoseconds", nanos)
			csm.AddColumn(tbk, "Price", px)
			csm.AddColumn(tbk, "Size", sz)

			// trim the slices
			epoch = epoch[:0]
			nanos = nanos[:0]
			px = px[:0]
			sz = sz[:0]
		}
	}

	if err := executor.WriteCSM(csm, true); err != nil {
		log.Error("[polygon] failed to write trades csm (%v)", err)
	}
}

func writeQuotes(writeMap map[io.TimeBucketKey][]*quote) {
	// preallocate the data structures for re-use
	var (
		csm   io.ColumnSeriesMap
		epoch []int64
		nanos []int32
		bidPx []float64
		askPx []float64
		bidSz []uint64
		askSz []uint64
	)
	csm = io.NewColumnSeriesMap()
	for tbk, bucket := range writeMap {
			for _, q := range bucket {
				epoch = append(epoch, q.epoch)
				nanos = append(nanos, q.nanos)
				bidPx = append(bidPx, q.bidPx)
				askPx = append(askPx, q.askPx)
				bidSz = append(bidSz, q.bidSz)
				askSz = append(askSz, q.askSz)
			}
			if len(epoch) > 0 {
				csm.AddColumn(tbk, "Epoch", epoch)
				csm.AddColumn(tbk, "Nanoseconds", nanos)
				csm.AddColumn(tbk, "BidPrice", bidPx)
				csm.AddColumn(tbk, "AskPrice", askPx)
				csm.AddColumn(tbk, "BidSize", bidSz)
				csm.AddColumn(tbk, "AskSize", askSz)
				// trim the slices
				epoch = epoch[:0]
				nanos = nanos[:0]
				bidPx = bidPx[:0]
				bidSz = bidSz[:0]
				askPx = bidPx[:0]
				askSz = askSz[:0]
			}
	}

	if err := executor.WriteCSM(csm, true); err != nil {
		log.Error("[polygon] failed to write quotes csm (%v)", err)
	}
}
