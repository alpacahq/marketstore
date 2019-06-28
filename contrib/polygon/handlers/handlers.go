package handlers

import (
	"encoding/json"
	"fmt"
	"github.com/alpacahq/marketstore/contrib/polygon/api"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/buger/jsonparser"
	"github.com/nats-io/go-nats"
	"strings"
	"sync"
	"time"
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

// TradeHandler handles a Polygon WS trade
// message and stores it to the cache
func TradeHandler(msg []byte) {
	if msg == nil {
		return
	}
	tt := make([]api.PolyTrade, 0)
	err := json.Unmarshal(msg, &tt)
	if err != nil {
		log.Warn("error processing upstream message",
			"message", string(msg),
			"error", err.Error())
		return
	}

	for _, rt := range tt {
		if conditionsPresent(rt.Conditions) {
			continue
		}

		// Polygon time is in milliseconds since the Unix epoch
		timestamp := time.Unix(0, int64(1000*1000*float64(rt.Timestamp)))
		lagOnReceipt := time.Now().Sub(timestamp).Seconds()
		t := trade{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			sz: int32(rt.Size),
			px: float32(rt.Price),
		}
		// skip empty trades
		if t.sz <= 0 || t.px <= 0 {
			return
		}
		symbol := fmt.Sprintf("%s", strings.Replace(rt.Symbol, "/", ".", 1))
		pkt := &writePacket{
			io.NewTimeBucketKey(symbol + "/1Min/TRADE"),
			&t,
			}
		Write(pkt)
		_ = lagOnReceipt
	}
}

// QuoteHandler handles a Polygon WS quote
// message and stores it to the cache
func QuoteHandler(msg []byte) {
	if msg == nil {
		return
	}
	qq := make([]api.PolyQuote, 0)
	err := json.Unmarshal(msg, &qq)
	if err != nil {
		log.Warn("error processing upstream message",
			"message", string(msg),
			"error", err.Error())
		return
	}
	for _, rq := range qq {
		timestamp := time.Unix(0, int64(1000*1000*float64(rq.Timestamp)))
		lagOnReceipt := time.Now().Sub(timestamp).Seconds()
		q := quote{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			bidPx:    float32(rq.BidPrice),
			bidSz:    int32(rq.BidSize),
			askPx:    float32(rq.AskPrice),
			askSz:    int32(rq.AskSize),
		}
		symbol := fmt.Sprintf("%s", strings.Replace(rq.Symbol, "/", ".", 1))
		pkt := &writePacket{
			io.NewTimeBucketKey(symbol+"/1Min/QUOTE"),
			&q,
		}
		Write(pkt)

		_ = lagOnReceipt
	}

}

func Bar(msg *nats.Msg, backfillM *sync.Map) {
	// quickly parse the json
	symbol, _ := jsonparser.GetString(msg.Data, "sym")

	if strings.Contains(symbol, "/") {
		return
	}

	open, _ := jsonparser.GetFloat(msg.Data, "o")
	high, _ := jsonparser.GetFloat(msg.Data, "h")
	low, _ := jsonparser.GetFloat(msg.Data, "l")
	close, _ := jsonparser.GetFloat(msg.Data, "c")
	volume, _ := jsonparser.GetInt(msg.Data, "v")
	epochMillis, _ := jsonparser.GetInt(msg.Data, "s")

	epoch := epochMillis / 1000

	backfillM.LoadOrStore(symbol, &epoch)

	tbk := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", symbol))
	csm := io.NewColumnSeriesMap()

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Open", []float32{float32(open)})
	cs.AddColumn("High", []float32{float32(high)})
	cs.AddColumn("Low", []float32{float32(low)})
	cs.AddColumn("Close", []float32{float32(close)})
	cs.AddColumn("Volume", []int32{int32(volume)})
	csm.AddColumnSeries(*tbk, cs)

	if err := executor.WriteCSM(csm, false); err != nil {
		log.Error("[polygon] csm write failure for key: [%v] (%v)", tbk.String(), err)
	}
}

