package orderbook

import (
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/ryszard/goskiplist/skiplist"
)

type Entry struct {
	Price float32
	Size  int32
}

type OrderBook struct {
	bids, asks *skiplist.SkipList
}

func NewOrderBook() *OrderBook {
	return &OrderBook{
		skiplist.NewCustomMap(func(l, r interface{}) bool {
			return l.(float32) > r.(float32)
		}),
		skiplist.NewCustomMap(func(l, r interface{}) bool {
			return l.(float32) < r.(float32)
		}),
	}
}

func (ob *OrderBook) Bid(entry Entry) {
	add(entry, ob.bids)
}

func (ob *OrderBook) Ask(entry Entry) {
	add(entry, ob.asks)
}

func add(entry Entry, sklist *skiplist.SkipList) {
	priceKey := entry.Price
	if _, ok := sklist.Get(priceKey); ok { // Existing price level, append order
		if entry.Size == 0 {
			sklist.Delete(priceKey)
			return
		}
		sklist.Set(priceKey, entry)
	} else if entry.Size != 0 {
		sklist.Set(priceKey, entry) // New price level
	}
}

func (ob *OrderBook) BBO() (bid, ask Entry) {
	var (
		be, ae Entry
		ok     bool
	)
	if ob.bids.Len() == 0 {
		be = Entry{0.0, 0}
	} else {
		be, ok = ob.bids.SeekToFirst().Value().(Entry)
		if !ok {
			log.Error("[bug]failed to cast a bid value to an Entry")
		}
	}
	if ob.asks.Len() == 0 {
		ae = Entry{0.0, 0}
	} else {
		ae, ok = ob.asks.SeekToFirst().Value().(Entry)
		if !ok {
			log.Error("[bug]failed to cast a bid value to an Entry")
		}
	}
	return be, ae
}
