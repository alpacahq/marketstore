package handlers

import (
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/eapache/channels"

	"github.com/alpacahq/marketstore/v4/contrib/polyiex/orderbook"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func handleTrade(raw []byte) {
	symbol, _ := jsonparser.GetString(raw, "S")

	price, _ := jsonparser.GetFloat(raw, "p")
	size, _ := jsonparser.GetInt(raw, "s")
	millisec, _ := jsonparser.GetInt(raw, "t")
	nanosec, _ := jsonparser.GetInt(raw, "T")

	if price <= 0.0 || size <= 0 {
		// ignore
		return
	}

	timestamp := time.Unix(0, 1000*1000*millisec+nanosec)

	pkt := &writePacket{
		io.NewTimeBucketKey(symbol + "/1Min/TRADE"),
		&trade{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			px:    float32(price),
			sz:    int32(size),
		},
	}
	Write(pkt)
}

/*
Sample Data:
	{
		"ev": "ID",
		"S": "AAPL",
		"b": [
		  [ 154.19, 100 ],
		  [ 154.09, 100 ],
		  [ 154.05, 100 ],
		  [ 154.04, 100 ]
		],
		"a": [
		  [ 153.51, 200 ],
		  [ 153.66, 100 ],
		  [ 153.67, 100 ],
		  [ 153.71, 100 ],
		  [ 153.72, 100 ]
		],
		"x": 15,
		"t": 1541077118809,
		"T": 809950610
	}
*/
func handleBook(raw []byte) {
	symbol, _ := jsonparser.GetString(raw, "S")
	millisec, _ := jsonparser.GetInt(raw, "t")
	nanosec, _ := jsonparser.GetInt(raw, "T")

	book := getOrderBook(symbol)
	jsonparser.ArrayEach(raw, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		px, _ := jsonparser.GetFloat(value, "[0]")
		sz, _ := jsonparser.GetInt(value, "[1]")
		book.Bid(orderbook.Entry{Price: float32(px), Size: int32(sz)})
	}, "b")
	jsonparser.ArrayEach(raw, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		px, _ := jsonparser.GetFloat(value, "[0]")
		sz, _ := jsonparser.GetInt(value, "[1]")
		book.Ask(orderbook.Entry{Price: float32(px), Size: int32(sz)})
	}, "a")

	b, a := book.BBO()

	log.Debug("[polyiex] %v BBO[%s]=(%v)/(%v)\n", string(raw), symbol, b, a)

	// maybe we should skip to write if BBO isn't changed
	timestamp := time.Unix(0, 1000*1000*millisec+nanosec)
	pkt := &writePacket{
		io.NewTimeBucketKey(symbol + "/1Min/QUOTE"),
		&quote{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			bidPx: b.Price,
			askPx: a.Price,
			bidSz: b.Size,
			askSz: a.Size,
		},
	}

	Write(pkt)
}

func handleStatus(raw []byte) {
	status, _ := jsonparser.GetString(raw, "status")
	message, _ := jsonparser.GetString(raw, "message")
	log.Info("[polyiex] status = '%s', message = '%s'", status, message)
}

func handleUnknown(raw []byte) {
	var msg string
	if len(raw) < 100 {
		msg = string(raw)
	} else {
		msg = string(raw[:100]) + "..."
	}
	log.Error("[polyiex] unknown message: %s", msg)
}

// Tick is a callback handler to receive upstream data. It expects a JSON
// array with "ev" key in each object in it, and write trades and quotes
// on disk.
func Tick(raw []byte) {
	jsonparser.ArrayEach(raw, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		ev, _ := jsonparser.GetString(value, "ev")
		switch ev {
		case "IT":
			handleTrade(value)
		case "ID":
			handleBook(value)
		case "status":
			handleStatus(value)
		default:
			handleUnknown(value)
		}
	})
}

// orderBooks is a map of OrderBook with symbol key.
var (
	orderBooks = map[string]*orderbook.OrderBook{}
	obMutex    sync.Mutex
)

func getOrderBook(symbol string) *orderbook.OrderBook {
	obMutex.Lock()
	defer obMutex.Unlock()
	book, ok := orderBooks[symbol]
	if !ok {
		book = orderbook.NewOrderBook()
		orderBooks[symbol] = book
	}
	return book
}

type trade struct {
	epoch int64
	nanos int32
	px    float32
	sz    int32
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
	skipWrite   bool
}

func SkipWrite(value bool) {
	w.skipWrite = value
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
				switch b := bucket.(type) {
				case []*quote:
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

			if !w.skipWrite {
				if err := executor.WriteCSM(csm, true); err != nil {
					log.Error("[polygon] failed to write csm (%v)", err)
				}
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
