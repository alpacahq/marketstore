package handlers

import (
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
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

// Write writes data (a trade or a quote) with
// the given key
func Write(key string, data interface{}) {
	k := *io.NewTimeBucketKey(key)
	csm := io.NewColumnSeriesMap()
	switch data.(type) {
	case *quote:
		q := data.(*quote)
		csm.AddColumn(k, "Epoch", []int64{q.epoch})
		csm.AddColumn(k, "Nanoseconds", []int32{q.nanos})
		csm.AddColumn(k, "BidPrice", []float32{q.bidPx})
		csm.AddColumn(k, "AskPrice", []float32{q.askPx})
		csm.AddColumn(k, "BidSize", []int32{q.bidSz})
		csm.AddColumn(k, "AskSize", []int32{q.askSz})
	case *trade:
		t := data.(*trade)
		csm.AddColumn(k, "Epoch", []int64{t.epoch})
		csm.AddColumn(k, "Nanoseconds", []int32{t.nanos})
		csm.AddColumn(k, "Price", []float32{t.px})
		csm.AddColumn(k, "Size", []int32{t.sz})

	}

	if err := executor.WriteCSM(csm, true); err != nil {
		log.Error("[alpaca] failed to write csm (%v)", err)
	}
}
