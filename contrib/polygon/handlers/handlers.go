package handlers

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/contrib/polygon/backfill"

	"github.com/alpacahq/marketstore/contrib/polygon/api"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
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
			sz:    int32(rt.Size),
			px:    float32(rt.Price),
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
			bidPx: float32(rq.BidPrice),
			bidSz: int32(rq.BidSize),
			askPx: float32(rq.AskPrice),
			askSz: int32(rq.AskSize),
		}
		symbol := fmt.Sprintf("%s", strings.Replace(rq.Symbol, "/", ".", 1))
		pkt := &writePacket{
			io.NewTimeBucketKey(symbol + "/1Min/QUOTE"),
			&q,
		}
		Write(pkt)

		_ = lagOnReceipt
	}

}

func BarsHandler(msg []byte) {
	if msg == nil {
		return
	}
	am := make([]api.PolyAggregate, 0)
	err := json.Unmarshal(msg, &am)
	if err != nil {
		log.Warn("error processing upstream message",
			"message", string(msg),
			"error", err.Error())
		return
	}
	for _, bar := range am {
		timestamp := time.Unix(0, int64(1000*1000*float64(bar.EpochMillis)))
		lagOnReceipt := time.Now().Sub(timestamp).Seconds()

		epoch := bar.EpochMillis / 1000

		backfill.BackfillM.LoadOrStore(bar.Symbol, &epoch)

		tbk := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", bar.Symbol))
		csm := io.NewColumnSeriesMap()

		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", []int64{epoch})
		cs.AddColumn("Open", []float32{float32(bar.Open)})
		cs.AddColumn("High", []float32{float32(bar.High)})
		cs.AddColumn("Low", []float32{float32(bar.Low)})
		cs.AddColumn("Close", []float32{float32(bar.Close)})
		cs.AddColumn("Volume", []int32{int32(bar.Volume)})
		csm.AddColumnSeries(*tbk, cs)

		if err := executor.WriteCSM(csm, false); err != nil {
			log.Error("[polygon] csm write failure for key: [%v] (%v)", tbk.String(), err)
		}

		_ = lagOnReceipt
	}
}
