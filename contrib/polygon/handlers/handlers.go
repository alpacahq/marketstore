package handlers

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/buger/jsonparser"
	"github.com/eapache/channels"
	nats "github.com/nats-io/go-nats"
)

const ConditionExchangeSummary = 51

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

func Trade(msg *nats.Msg) {
	var skip = false

	// get the condition in case we should ignore this quote
	jsonparser.ArrayEach(msg.Data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		if c, _ := strconv.Atoi(string(value)); c == ConditionExchangeSummary {
			skip = true
		}
	}, "c")

	if skip {
		return
	}

	// parse symbol and swap / for .
	symbol, _ := jsonparser.GetString(msg.Data, "sym")
	symbol = strings.Replace(symbol, "/", ".", 1)
	size, _ := jsonparser.GetInt(msg.Data, "s")
	px, _ := jsonparser.GetFloat(msg.Data, "p")
	epochMillis, _ := jsonparser.GetInt(msg.Data, "t")

	timestamp := time.Unix(0, 1000*1000*epochMillis)

	pkt := &writePacket{
		io.NewTimeBucketKey(symbol + "/1Min/TRADE"),
		&trade{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			px:    float32(px),
			sz:    int32(size),
		}}

	Write(pkt)
}

type trade struct {
	epoch int64
	nanos int32
	px    float32
	sz    int32
}

func Quote(msg *nats.Msg) {
	// parse symbol and swap / for .
	symbol, _ := jsonparser.GetString(msg.Data, "sym")
	symbol = strings.Replace(symbol, "/", ".", 1)
	bidPx, _ := jsonparser.GetFloat(msg.Data, "bp")
	askPx, _ := jsonparser.GetFloat(msg.Data, "ap")
	bidSize, _ := jsonparser.GetInt(msg.Data, "bs")
	askSize, _ := jsonparser.GetInt(msg.Data, "as")
	epochMillis, _ := jsonparser.GetInt(msg.Data, "t")

	timestamp := time.Unix(0, 1000*1000*epochMillis)

	pkt := &writePacket{
		io.NewTimeBucketKey(symbol + "/1Min/QUOTE"),
		&quote{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			bidPx: float32(bidPx),
			askPx: float32(askPx),
			bidSz: int32(bidSize),
			askSz: int32(askSize),
		}}

	Write(pkt)
}

type quote struct {
	epoch int64   // 8
	nanos int32   // 4
	bidPx float32 // 4
	askPx float32 // 4
	bidSz int32   // 4
	askSz int32   // 4
}

var (
	w = &writer{
		dataBuckets: map[io.TimeBucketKey]interface{}{},
		interval:    100 * time.Millisecond,
		c:           channels.NewInfiniteChannel(),
	}
	once sync.Once
)

type writePacket struct {
	tbk  *io.TimeBucketKey
	data interface{}
}

type writer struct {
	sync.Mutex
	dataBuckets map[io.TimeBucketKey]interface{}
	interval    time.Duration
	c           *channels.InfiniteChannel
}

func (w *writer) write() {
	// preallocate the data structures for re-use
	var (
		csm io.ColumnSeriesMap

		epoch []int64
		nanos []int32
		bidPx []float32
		askPx []float32
		px    []float32
		bidSz []int32
		askSz []int32
		sz    []int32
	)

	for {
		select {
		case m := <-w.c.Out():
			w.Lock()
			packet := m.(*writePacket)

			if bucket, ok := w.dataBuckets[*packet.tbk]; ok {
				switch packet.data.(type) {
				case *quote:
					w.dataBuckets[*packet.tbk] = append(bucket.([]*quote), packet.data.(*quote))
				case *trade:
					w.dataBuckets[*packet.tbk] = append(bucket.([]*trade), packet.data.(*trade))
				}
			} else {
				switch packet.data.(type) {
				case *quote:
					w.dataBuckets[*packet.tbk] = []*quote{packet.data.(*quote)}
				case *trade:
					w.dataBuckets[*packet.tbk] = []*trade{packet.data.(*trade)}
				}
			}

			w.Unlock()

		case <-time.After(w.interval):
			w.Lock()
			csm = io.NewColumnSeriesMap()

			for tbk, bucket := range w.dataBuckets {
				switch bucket.(type) {
				case []*quote:
					b := bucket.([]*quote)

					for _, q := range b {
						epoch = append(epoch, q.epoch)
						nanos = append(nanos, q.nanos)
						bidPx = append(bidPx, q.bidPx)
						askPx = append(askPx, q.askPx)
						bidSz = append(bidSz, q.bidSz)
						askSz = append(askSz, q.askSz)
					}

					if len(epoch) > 0 {
						csm.AddColumn(tbk, "Epoch", epoch)
						csm.AddColumn(tbk, "Nanoseconds", nanos)
						csm.AddColumn(tbk, "BidPrice", bidPx)
						csm.AddColumn(tbk, "AskPrice", askPx)
						csm.AddColumn(tbk, "BidSize", bidSz)
						csm.AddColumn(tbk, "AskSize", askSz)

						// trim the slices
						epoch = epoch[:0]
						nanos = nanos[:0]
						bidPx = bidPx[:0]
						bidSz = bidSz[:0]
						askPx = bidPx[:0]
						askSz = askSz[:0]
						w.dataBuckets[tbk] = b[:0]
					}
				case []*trade:
					b := bucket.([]*trade)

					for _, t := range b {
						epoch = append(epoch, t.epoch)
						nanos = append(nanos, t.nanos)
						px = append(px, t.px)
						sz = append(sz, t.sz)
					}

					if len(epoch) > 0 {
						csm.AddColumn(tbk, "Epoch", epoch)
						csm.AddColumn(tbk, "Nanoseconds", nanos)
						csm.AddColumn(tbk, "Price", px)
						csm.AddColumn(tbk, "Size", sz)

						// trim the slices
						epoch = epoch[:0]
						nanos = nanos[:0]
						px = px[:0]
						sz = sz[:0]
						w.dataBuckets[tbk] = b[:0]
					}
				}
			}

			w.Unlock()

			if err := executor.WriteCSM(csm, true); err != nil {
				log.Error("[polygon] failed to write csm (%v)", err)
			}
		}
	}
}

func Write(pkt *writePacket) {
	once.Do(func() {
		go w.write()
	})

	w.c.In() <- pkt
}
