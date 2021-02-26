package handlers

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpacav2/api"
	"github.com/alpacahq/marketstore/v4/contrib/alpacav2/enums"
	"github.com/alpacahq/marketstore/v4/contrib/alpacav2/metrics"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/mitchellh/mapstructure"
)

// MessageHandler handles incoming messages
// from the websocket
func MessageHandler(msg []byte) {
	if msg == nil {
		return
	}

	var messages []map[string]interface{}
	err := json.Unmarshal(msg, &messages)
	if err != nil {
		log.Error("[alpacav2] error processing messages {%s:%s,%s:%s}",
			"message", string(msg),
			"error", err)
		return
	}

	for _, message := range messages {
		if err := handleMessage(message); err != nil {
			log.Error("[alpacav2] error processing message {%s:%s,%s:%s}",
				"message", fmt.Sprintf("%+v", message),
				"error", err)
		}
	}
}

func decode(input map[string]interface{}, result interface{}) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata:   nil,
		DecodeHook: mapstructure.StringToTimeHookFunc(time.RFC3339Nano),
		Result:     result,
	})
	if err != nil {
		return err
	}

	if err := decoder.Decode(input); err != nil {
		return err
	}
	return err
}

func handleMessage(m map[string]interface{}) error {
	t, ok := m["T"].(string)
	if !ok {
		return fmt.Errorf("message does not contain T: %+v", m)
	}

	switch t {
	case enums.TradeEvent:
		var trade api.Trade
		if err := decode(m, &trade); err != nil {
			return fmt.Errorf("failed to unmarshal trade, error: %w", err)
		}
		tradeHandler(&trade)
	case enums.QuoteEvent:
		quote := api.Quote{}
		if err := decode(m, &quote); err != nil {
			return fmt.Errorf("failed to unmarshal quote, error: %w", err)
		}
		quoteHandler(&quote)
	case enums.MinuteAggregateEvent:
		bar := api.MinuteAggregate{}
		if err := decode(m, &bar); err != nil {
			return fmt.Errorf("failed to unmarshal bar, error: %w", err)
		}
		barHandler(&bar)
	case enums.ErrorEvent:
		return fmt.Errorf("received error from the server: %+v", m)
	default:
		return fmt.Errorf("received unexpected message: %+v", m)
	}

	return nil
}

// tradeHandler handles a Trade
// and stores it to the cache
func tradeHandler(t *api.Trade) {
	writeTrade(t)
	updateMetrics("trade", t.Timestamp)
}

// quoteHandler handles a Quote
// and stores it to the cache
func quoteHandler(q *api.Quote) {
	writeQuote(q)
	updateMetrics("quote", q.Timestamp)
}

// barHandler handles a MinuteAggregate
// and stores it to the cache
func barHandler(agg *api.MinuteAggregate) {
	writeAggregateToMinute(agg)
	// agg.Timestamp is the beginning of the bar's window
	updateMetrics("minute_bar", agg.Timestamp.Add(time.Minute))
}

func updateMetrics(msgType string, msgTimestamp time.Time) {
	lagOnReceipt := time.Since(msgTimestamp).Seconds()
	metrics.AlpacaV2StreamMessagesHandled.WithLabelValues(msgType).Inc()
	metrics.AlpacaV2StreamUpdateLag.WithLabelValues(msgType).Set(lagOnReceipt)
	metrics.AlpacaV2StreamLastUpdate.WithLabelValues(msgType).SetToCurrentTime()
}
