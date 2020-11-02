package handlers

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/api"
	"github.com/alpacahq/marketstore/v4/contrib/alpaca/enums"
	"github.com/alpacahq/marketstore/v4/contrib/alpaca/metrics"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	ConditionExchangeSummary = 51
	OfficialConditionClosing = 15
	OfficialConditionOpening = 16
	ConditionClosing         = 17
	ConditionReOpening       = 18
	ConditionOpening         = 19
)

func conditionsPresent(conditions []int) (skip bool) {
	for _, c := range conditions {
		switch c {
		case ConditionExchangeSummary, ConditionReOpening, ConditionOpening, ConditionClosing,
			OfficialConditionOpening, OfficialConditionClosing:
			return true
		}
	}
	return
}

// MessageHandler handles incoming messages
// from the websocket
func MessageHandler(msg []byte) {
	if msg == nil {
		return
	}
	message := api.AlpacaMessage{}
	err := json.Unmarshal(msg, &message)
	if err != nil {
		log.Warn("error processing message {%s:%s,%s:%s}",
			"message", string(msg),
			"error", err)
		return
	}

	switch enums.Prefix(message.Data.EventType) {
	case enums.TradeEvent:
		t := api.AlpacaTrade{}
		err := json.Unmarshal(msg, &t)
		if err != nil {
			log.Warn("error processing trade message {%s:%s,%s:%s}",
				"message", string(msg),
				"error", err)
			return
		}
		tradeHandler(&t.Data)
	case enums.QuoteEvent:
		q := api.AlpacaQuote{}
		err := json.Unmarshal(msg, &q)
		if err != nil {
			log.Warn("error processing quote message {%s:%s,%s:%s}",
				"message", string(msg),
				"error", err)
			return
		}
		quoteHandler(&q.Data)
	case enums.AggEvent:
		agg := api.AlpacaAggregate{}
		err := json.Unmarshal(msg, &agg)
		if err != nil {
			log.Warn("error processing aggregate message {%s:%s,%s:%s}",
				"message", string(msg),
				"error", err)
			return
		}
		aggregateHandler(&agg.Data)
	default:
		log.Warn("unexpected non-event message {%s:%s,%s:%s}",
			"event_type", message.Data.EventType,
			"message", string(msg))
	}
}

// tradeHandler handles a Trade
// and stores it to the cache
func tradeHandler(t *api.Trade) {
	switch {
	case conditionsPresent(t.Conditions), t.Size <= 0, t.Price <= 0:
		return
	}
	timestamp := time.Unix(0, t.Timestamp)

	tr := trade{
		epoch: timestamp.Unix(),
		nanos: int32(timestamp.Nanosecond()),
		sz:    int32(t.Size),
		px:    float32(t.Price),
	}
	key := fmt.Sprintf("%s/1Sec/TRADE", strings.Replace(t.Symbol, "/", ".", 1))

	Write(key, &tr)

	lagOnReceipt := time.Now().Sub(timestamp).Seconds()
	metrics.AlpacaStreamUpdateLag.WithLabelValues("trade").Set(lagOnReceipt)
	metrics.AlpacaStreamLastUpdate.WithLabelValues("trade").SetToCurrentTime()
}

// quoteHandler handles a Quote
// and stores it to the cache
func quoteHandler(q *api.Quote) {
	timestamp := time.Unix(0, q.Timestamp)

	qu := quote{
		epoch: timestamp.Unix(),
		nanos: int32(timestamp.Nanosecond()),
		bidPx: float32(q.BidPrice),
		bidSz: int32(q.BidSize),
		askPx: float32(q.AskPrice),
		askSz: int32(q.AskSize),
	}
	key := fmt.Sprintf("%s/1Sec/QUOTE", strings.Replace(q.Symbol, "/", ".", 1))
	Write(key, &qu)

	lagOnReceipt := time.Now().Sub(timestamp).Seconds()
	metrics.AlpacaStreamUpdateLag.WithLabelValues("quote").Set(lagOnReceipt)
	metrics.AlpacaStreamLastUpdate.WithLabelValues("quote").SetToCurrentTime()
}

// aggregateHandler handles an Aggregate
// and stores it to the cache
func aggregateHandler(agg *api.Aggregate) {
	timestamp := time.Unix(0, int64(1e6*agg.EpochMillis))

	epoch := agg.EpochMillis / 1e3

	tbk := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", agg.Symbol))
	csm := io.NewColumnSeriesMap()

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Open", []float32{float32(agg.Open)})
	cs.AddColumn("High", []float32{float32(agg.High)})
	cs.AddColumn("Low", []float32{float32(agg.Low)})
	cs.AddColumn("Close", []float32{float32(agg.Close)})
	cs.AddColumn("Volume", []int32{int32(agg.Volume)})
	csm.AddColumnSeries(*tbk, cs)

	if err := executor.WriteCSM(csm, false); err != nil {
		log.Error("[alpaca] csm write failure for key: [%v] (%v)", tbk.String(), err)
	}

	lagOnReceipt := time.Now().Sub(timestamp).Seconds()
	metrics.AlpacaStreamUpdateLag.WithLabelValues("bar").Set(lagOnReceipt)
	metrics.AlpacaStreamLastUpdate.WithLabelValues("bar").SetToCurrentTime()
}
