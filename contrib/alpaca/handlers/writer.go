package handlers

import (
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/api"
	polygon "github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var condDefault = byte(255)

// writeTrade writes a Trade
func writeTrade(t *api.Trade) {
	cs := io.NewColumnSeries()

	timestamp := time.Unix(0, t.Timestamp)
	cs.AddColumn("Epoch", []int64{timestamp.Unix()})
	cs.AddColumn("Nanoseconds", []int32{int32(timestamp.Nanosecond())})
	cs.AddColumn("Price", []float64{t.Price})
	cs.AddColumn("Size", []int64{t.Size})
	cs.AddColumn("Exchange", []byte{polygon.ExchangeCode(t.Exchange)})
	cs.AddColumn("TapeID", []byte{polygon.TapeCode(t.TapeID)})
	c1, c2, c3, c4 := condDefault, condDefault, condDefault, condDefault
	switch len(t.Conditions) {
	case 4:
		c4 = polygon.TradeConditionCode(t.Conditions[3])
		fallthrough
	case 3:
		c3 = polygon.TradeConditionCode(t.Conditions[2])
		fallthrough
	case 2:
		c2 = polygon.TradeConditionCode(t.Conditions[1])
		fallthrough
	case 1:
		c1 = polygon.TradeConditionCode(t.Conditions[0])
	}
	cs.AddColumn("Cond1", []byte{c1})
	cs.AddColumn("Cond2", []byte{c2})
	cs.AddColumn("Cond3", []byte{c3})
	cs.AddColumn("Cond4", []byte{c4})

	csm := io.NewColumnSeriesMap()
	key := io.NewTimeBucketKey(fmt.Sprintf("%s/1Sec/TRADE", strings.Replace(t.Symbol, "/", ".", 1)))
	csm.AddColumnSeries(*key, cs)

	writeCSM(&csm, key, true)
}

// writeQuote writes a Quote
func writeQuote(q *api.Quote) {
	cs := io.NewColumnSeries()

	timestamp := time.Unix(0, q.Timestamp)
	cs.AddColumn("Epoch", []int64{timestamp.Unix()})
	cs.AddColumn("Nanoseconds", []int32{int32(timestamp.Nanosecond())})
	cs.AddColumn("BidPrice", []float64{q.BidPrice})
	cs.AddColumn("AskPrice", []float64{q.AskPrice})
	cs.AddColumn("BidSize", []int64{q.BidSize})
	cs.AddColumn("AskSize", []int64{q.AskSize})
	cs.AddColumn("BidExchange", []byte{q.BidExchange})
	cs.AddColumn("AskExchange", []byte{q.AskExchange})
	cs.AddColumn("Cond", []int8{q.Conditions[0]})

	csm := io.NewColumnSeriesMap()
	key := io.NewTimeBucketKey(fmt.Sprintf("%s/1Sec/QUOTE", strings.Replace(q.Symbol, "/", ".", 1)))
	csm.AddColumnSeries(*key, cs)

	writeCSM(&csm, key, true)
}

// writeAggregateToMinute writes an AggregateToMinute
func writeAggregateToMinute(agg *api.AggregateToMinute) {
	cs := io.NewColumnSeries()

	cs.AddColumn("Epoch", []int64{agg.EpochMillis / 1e3})
	cs.AddColumn("Open", []float64{agg.Open})
	cs.AddColumn("High", []float64{agg.High})
	cs.AddColumn("Low", []float64{agg.Low})
	cs.AddColumn("Close", []float64{agg.Close})
	cs.AddColumn("Volume", []int64{agg.Volume})
	// NOTE: TickCnt is not set!

	csm := io.NewColumnSeriesMap()
	key := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", agg.Symbol))
	csm.AddColumnSeries(*key, cs)

	writeCSM(&csm, key, false)
}

func writeCSM(csm *io.ColumnSeriesMap, key *io.TimeBucketKey, isVariableLength bool) {
	if err := executor.WriteCSM(*csm, isVariableLength); err != nil {
		log.Error("[alpaca] csm write failure for key: [%v] (%v)", key.String(), err)
	}
}
