package handlers

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/metrics"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/streaming"
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

func conditionsPresent(conditions []int32) (skip bool) {
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

	rt := streaming.Trade{}
	err := json.Unmarshal(msg, &rt)
	if err != nil {
		log.Warn("error processing upstream message",
			"message", string(msg),
			"error", err.Error())
		return
	}

	if conditionsPresent(rt.Conditions) || rt.Size <= 0 || rt.Price <= 0 {
		return
	}

	writeMap := make(map[io.TimeBucketKey]interface{})
	// Polygon time is in milliseconds since the Unix epoch
	timestamp := time.Unix(0, int64(1000*1000*float64(rt.Timestamp)))
	lagOnReceipt := time.Now().Sub(timestamp).Seconds()
	t := trade{
		epoch: timestamp.Unix(),
		nanos: int32(timestamp.Nanosecond()),
		sz:    int32(rt.Size),
		px:    float32(rt.Price),
	}
	key := fmt.Sprintf("%s/1Min/TRADE", strings.Replace(rt.Symbol, "/", ".", 1))
	appendItem(writeMap, io.NewTimeBucketKey(key), &t)
	_ = lagOnReceipt
	Write(writeMap)

	metrics.PolygonStreamLastUpdate.WithLabelValues("trade").SetToCurrentTime()
}

// QuoteHandler handles a Polygon WS quote
// message and stores it to the cache
func QuoteHandler(msg []byte) {
	if msg == nil {
		return
	}

	rq := streaming.Quote{}
	err := json.Unmarshal(msg, &rq)
	if err != nil {
		log.Warn("error processing upstream message",
			"message", string(msg),
			"error", err.Error())
		return
	}
	writeMap := make(map[io.TimeBucketKey]interface{})
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
	key := fmt.Sprintf("%s/1Min/QUOTE", strings.Replace(rq.Symbol, "/", ".", 1))
	appendItem(writeMap, io.NewTimeBucketKey(key), &q)
	_ = lagOnReceipt
	Write(writeMap)

	metrics.PolygonStreamLastUpdate.WithLabelValues("quote").SetToCurrentTime()
}

func BarsHandlerWrapper(addTickCount bool) func([]byte) {
	return func(msg []byte) {
		BarsHandler(msg, addTickCount)
	}
}

func BarsHandler(msg []byte, addTickCount bool) {
	if msg == nil {
		return
	}
	bar := streaming.Aggregate{}
	err := json.Unmarshal(msg, &bar)
	if err != nil {
		log.Warn("error processing upstream message",
			"message", string(msg),
			"error", err.Error())
		return
	}

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
	if addTickCount {
		cs.AddColumn("TickCnt", []int32{int32(0)})
	}
	csm.AddColumnSeries(*tbk, cs)

	if err := executor.WriteCSM(csm, false); err != nil {
		log.Error("[polygon] csm write failure for key: [%v] (%v)", tbk.String(), err)
	}

	_ = lagOnReceipt

	metrics.PolygonStreamLastUpdate.WithLabelValues("bar").SetToCurrentTime()
}

func appendItem(writeMap map[io.TimeBucketKey]interface{}, tbkp *io.TimeBucketKey, item interface{}) {
	tbk := *tbkp
	if bucketI, ok := writeMap[tbk]; ok {
		switch bucket := bucketI.(type) {
		case []*trade:
			bucket = append(bucket, item.(*trade))
			writeMap[tbk] = bucket
		case []*quote:
			bucket = append(bucket, item.(*quote))
			writeMap[tbk] = bucket
		}
	} else {
		switch val := item.(type) {
		case *trade:
			writeMap[tbk] = []*trade{val}
		case *quote:
			writeMap[tbk] = []*quote{val}
		}
	}
}
