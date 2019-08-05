package stream

import (
	"sync"

	"github.com/alpacahq/alpaca-trade-api-go/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/polygon"
)

var (
	once sync.Once
	u    *Unified
)

// Register a handler for a given stream, Alpaca or Polygon.
func Register(stream string, handler func(msg interface{})) (err error) {
	once.Do(func() {
		if u == nil {
			u = &Unified{
				alpaca:  alpaca.GetStream(),
				polygon: polygon.GetStream(),
			}
		}
	})

	switch stream {
	case alpaca.TradeUpdates:
		fallthrough
	case alpaca.AccountUpdates:
		err = u.alpaca.Subscribe(stream, handler)
	default:
		// polygon
		err = u.polygon.Subscribe(stream, handler)
	}

	return
}

// Close gracefully closes all streams
func Close() error {
	// close alpaca connection
	err1 := u.alpaca.Close()
	// close polygon connection
	err2 := u.polygon.Close()

	if err1 != nil {
		return err1
	}
	return err2
}

// Unified is the unified streaming structure combining the
// interfaces from polygon and alpaca.
type Unified struct {
	alpaca, polygon Stream
}

// Stream is the generic streaming interface implemented by
// both alpaca and polygon.
type Stream interface {
	Subscribe(key string, handler func(msg interface{})) error
	Close() error
}
