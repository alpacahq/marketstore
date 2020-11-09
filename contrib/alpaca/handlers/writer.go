package handlers

import (
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/api"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var condDefault = int32(-1)

// writeTrade writes a Trade
func writeTrade(t *api.Trade) {
	cs := io.NewColumnSeries()

	timestamp := time.Unix(0, t.Timestamp)
	cs.AddColumn("Epoch", []int64{timestamp.Unix()})
	cs.AddColumn("Nanoseconds", []int32{int32(timestamp.Nanosecond())})
	cs.AddColumn("Price", []float32{t.Price})
	cs.AddColumn("Size", []uint32{t.Size})
	cs.AddColumn("Exchange", []uint32{t.Exchange})
	cs.AddColumn("TapeID", []uint32{t.TapeID})
	c1, c2, c3, c4 := condDefault, condDefault, condDefault, condDefault
	switch len(t.Conditions) {
	case 4:
		c4 = t.Conditions[3]
		fallthrough
	case 3:
		c3 = t.Conditions[2]
		fallthrough
	case 2:
		c2 = t.Conditions[1]
		fallthrough
	case 1:
		c1 = t.Conditions[0]
	}
	cs.AddColumn("Cond1", []int32{c1})
	cs.AddColumn("Cond2", []int32{c2})
	cs.AddColumn("Cond3", []int32{c3})
	cs.AddColumn("Cond4", []int32{c4})

	csm := io.NewColumnSeriesMap()
	key := io.NewTimeBucketKey(fmt.Sprintf("%s/1Sec/TRADE", strings.Replace(t.Symbol, "/", ".", 1)))
	csm.AddColumnSeries(*key, cs)

	writeCSM(&csm, key)
}

// writeQuote writes a Quote
func writeQuote(q *api.Quote) {
	cs := io.NewColumnSeries()

	timestamp := time.Unix(0, q.Timestamp)
	cs.AddColumn("Epoch", []int64{timestamp.Unix()})
	cs.AddColumn("Nanoseconds", []int32{int32(timestamp.Nanosecond())})
	cs.AddColumn("BidPrice", []float32{q.BidPrice})
	cs.AddColumn("AskPrice", []float32{q.AskPrice})
	cs.AddColumn("BidSize", []uint32{q.BidSize})
	cs.AddColumn("AskSize", []uint32{q.AskSize})
	cs.AddColumn("BidExchange", []uint32{q.BidExchange})
	cs.AddColumn("AskExchange", []uint32{q.AskExchange})
	cs.AddColumn("Cond", []int32{q.Conditions[0]})

	csm := io.NewColumnSeriesMap()
	key := io.NewTimeBucketKey(fmt.Sprintf("%s/1Sec/QUOTE", strings.Replace(q.Symbol, "/", ".", 1)))
	csm.AddColumnSeries(*key, cs)

	writeCSM(&csm, key)
}

// writeAggregateToMinute writes an AggregateToMinute
func writeAggregateToMinute(agg *api.AggregateToMinute) {
	cs := io.NewColumnSeries()

	cs.AddColumn("Epoch", []int64{agg.EpochMillis / 1e3})
	cs.AddColumn("Open", []float32{agg.Open})
	cs.AddColumn("High", []float32{agg.High})
	cs.AddColumn("Low", []float32{agg.Low})
	cs.AddColumn("Close", []float32{agg.Close})
	cs.AddColumn("Volume", []uint32{agg.Volume})
	cs.AddColumn("VWAP", []float32{agg.VWAP})
	cs.AddColumn("Average", []float32{agg.Average})
	// NOTE: TickCnt is not set!

	csm := io.NewColumnSeriesMap()
	key := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", agg.Symbol))
	csm.AddColumnSeries(*key, cs)

	writeCSM(&csm, key)
}

func writeCSM(csm *io.ColumnSeriesMap, key *io.TimeBucketKey) {
	if err := executor.WriteCSM(*csm, false); err != nil {
		log.Error("[alpaca] csm write failure for key: [%v] (%v)", key.String(), err)
	}
}
