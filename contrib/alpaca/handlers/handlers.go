package handlers

import (
	"encoding/json"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/api"
	"github.com/alpacahq/marketstore/v4/contrib/alpaca/enums"
	"github.com/alpacahq/marketstore/v4/contrib/alpaca/metrics"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// MessageHandler handles incoming messages
// from the websocket.
func MessageHandler(msg []byte) {
	if msg == nil {
		return
	}
	message := api.AlpacaMessage{}
	err := json.Unmarshal(msg, &message)
	if err != nil {
		log.Error("[alpaca] error processing message {%s:%s,%s:%s}",
			"message", string(msg),
			"error", err)
		return
	}

	switch message.Data.EventType {
	case enums.TradeEvent:
		t := api.AlpacaTrade{}
		err := json.Unmarshal(msg, &t)
		if err != nil {
			log.Error("[alpaca] error processing trade message {%s:%s,%s:%s}",
				"message", string(msg),
				"error", err)
			return
		}
		tradeHandler(&t.Data)
	case enums.QuoteEvent:
		q := api.AlpacaQuote{}
		err := json.Unmarshal(msg, &q)
		if err != nil {
			log.Error("[alpaca] error processing quote message {%s:%s,%s:%s}",
				"message", string(msg),
				"error", err)
			return
		}
		quoteHandler(&q.Data)
	case enums.AggToMinuteEvent:
		agg := api.AlpacaAggregateToMinute{}
		err := json.Unmarshal(msg, &agg)
		if err != nil {
			log.Error("[alpaca] error processing minute aggregate message {%s:%s,%s:%s}",
				"message", string(msg),
				"error", err)
			return
		}
		aggregateToMinuteHandler(&agg.Data)
	case enums.AggToMinute, enums.Quote, enums.Trade:
		fallthrough
	default:
		log.Warn("[alpaca] unexpected non-event message {%s:%s,%s:%s}",
			"event_type", message.Data.EventType,
			"message", string(msg))
	}
}

// tradeHandler handles a Trade
// and stores it to the cache.
func tradeHandler(t *api.Trade) {
	writeTrade(t)
	updateMetrics("trade", time.Unix(0, t.Timestamp))
}

// quoteHandler handles a Quote
// and stores it to the cache.
func quoteHandler(q *api.Quote) {
	writeQuote(q)
	updateMetrics("quote", time.Unix(0, q.Timestamp))
}

// aggregateToMinuteHandler handles an AggregateToMinute
// and stores it to the cache.
func aggregateToMinuteHandler(agg *api.AggregateToMinute) {
	writeAggregateToMinute(agg)
	updateMetrics("minute_bar", time.Unix(0, int64(1e6*agg.EndTime)))
}

func updateMetrics(msgType string, msgTimestamp time.Time) {
	lagOnReceipt := time.Since(msgTimestamp).Seconds()
	metrics.AlpacaStreamMessagesHandled.WithLabelValues(msgType).Inc()
	metrics.AlpacaStreamUpdateLag.WithLabelValues(msgType).Set(lagOnReceipt)
	metrics.AlpacaStreamLastUpdate.WithLabelValues(msgType).SetToCurrentTime()
}
