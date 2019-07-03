package handlers

import (
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

type trade struct {
	epoch int64
	nanos int32
	px    float32
	sz    int32
}

type quote struct {
	epoch int64   // 8
	nanos int32   // 4
	bidPx float32 // 4
	askPx float32 // 4
	bidSz int32   // 4
	askSz int32   // 4
}

func Write(writeMap map[io.TimeBucketKey]interface{}) {
	// preallocate the data structures for re-use
	var (
		csm   io.ColumnSeriesMap
		epoch []int64
		nanos []int32
		bidPx []float32
		askPx []float32
		px    []float32
		bidSz []int32
		askSz []int32
		sz    []int32
	)
	csm = io.NewColumnSeriesMap()
	for tbk, bucket := range writeMap {
		switch bucket.(type) {
		case []*quote:
			b := bucket.([]*quote)
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
			b := bucket.([]*trade)
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
