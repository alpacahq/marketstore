package handlers

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/metrics"
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
	return skip
}

// TradeHandler handles a Polygon WS trade
// message and stores it to the cache.
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
	writeMap := make(map[io.TimeBucketKey][]*trade)
	for _, rt := range tt {
		switch {
		case conditionsPresent(rt.Conditions), rt.Size <= 0, rt.Price <= 0:
			continue
		}
		// Polygon time is in milliseconds since the Unix epoch
		timestamp := time.Unix(0, int64(1000*1000*float64(rt.Timestamp)))
		lagOnReceipt := time.Since(timestamp).Seconds()
		t := trade{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			sz:    uint64(rt.Size),
			px:    rt.Price,
		}
		key := fmt.Sprintf("%s/1Sec/TRADE", strings.Replace(rt.Symbol, "/", ".", 1))
		appendTrade(writeMap, io.NewTimeBucketKey(key), &t)
		_ = lagOnReceipt
	}
	writeTrades(writeMap)

	metrics.PolygonStreamLastUpdate.WithLabelValues("trade").SetToCurrentTime()
}

// QuoteHandler handles a Polygon WS quote
// message and stores it to the cache.
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
	writeMap := make(map[io.TimeBucketKey][]*quote)
	for _, rq := range qq {
		timestamp := time.Unix(0, int64(1000*1000*float64(rq.Timestamp)))
		lagOnReceipt := time.Since(timestamp).Seconds()
		q := quote{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			bidPx: rq.BidPrice,
			bidSz: uint64(rq.BidSize),
			askPx: rq.AskPrice,
			askSz: uint64(rq.AskSize),
		}
		key := fmt.Sprintf("%s/1Min/QUOTE", strings.Replace(rq.Symbol, "/", ".", 1))
		appendQuote(writeMap, io.NewTimeBucketKey(key), &q)
		_ = lagOnReceipt
	}
	writeQuotes(writeMap)

	metrics.PolygonStreamLastUpdate.WithLabelValues("quote").SetToCurrentTime()
}

func BarsHandler(msg []byte) {
	const millisecToSec = 1000
	const nanosecToMillisec = 1000 * 1000
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
		timestamp := time.Unix(0, int64(nanosecToMillisec*float64(bar.EpochMillis)))
		lagOnReceipt := time.Since(timestamp).Seconds()

		epoch := bar.EpochMillis / millisecToSec

		backfill.BackfillM.LoadOrStore(bar.Symbol, &epoch)

		tbk := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", bar.Symbol))
		csm := io.NewColumnSeriesMap()

		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", []int64{epoch})
		cs.AddColumn("Open", []float64{bar.Open})
		cs.AddColumn("High", []float64{bar.High})
		cs.AddColumn("Low", []float64{bar.Low})
		cs.AddColumn("Close", []float64{bar.Close})
		cs.AddColumn("Volume", []uint64{uint64(bar.Volume)})
		csm.AddColumnSeries(*tbk, cs)

		if err := executor.WriteCSM(csm, false); err != nil {
			log.Error("[polygon] csm write failure for key: [%v] (%v)", tbk.String(), err)
		}

		_ = lagOnReceipt
	}

	metrics.PolygonStreamLastUpdate.WithLabelValues("bar").SetToCurrentTime()
}

func appendTrade(writeMap map[io.TimeBucketKey][]*trade, tbkp *io.TimeBucketKey, tr *trade) {
	tbk := *tbkp
	if bucket, ok := writeMap[tbk]; ok {
		bucket = append(bucket, tr)
		writeMap[tbk] = bucket

	} else {
		writeMap[tbk] = []*trade{tr}
	}
}

func appendQuote(writeMap map[io.TimeBucketKey][]*quote, tbkp *io.TimeBucketKey, q *quote) {
	tbk := *tbkp
	if bucket, ok := writeMap[tbk]; ok {
		bucket = append(bucket, q)
		writeMap[tbk] = bucket
	} else {
		writeMap[tbk] = []*quote{q}
	}
}
