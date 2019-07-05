package api

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/utils/log"
	"github.com/alpacahq/marketstore/utils/pool"
)

type Subscription struct {
	Incoming chan interface{}
	pConn    *PolygonWebSocket
	running  bool
	handled  int64
	sync.Mutex
}

// servers := utils.Settings["WS_SERVERS"]
func NewSubscription(t Prefix, symbols []string) (s *Subscription) {
	incoming := make(chan interface{}, 10000)
	return &Subscription{
		Incoming: incoming,
		pConn:    NewPolygonWebSocket(servers, apiKey, t, symbols, incoming),
		running:  false,
	}
}

func (s *Subscription) getRunning() (state bool) {
	s.Lock()
	defer s.Unlock()
	return s.running
}

func (s *Subscription) setRunning(state bool) {
	s.Lock()
	defer s.Unlock()
	s.running = state
}

func (s *Subscription) Hangup() {
	s.Lock()
	defer s.Unlock()
	if s.pConn.doneChan != nil {
		s.pConn.doneChan <- struct{}{}
	}
	s.running = false
}

func (s *Subscription) IsActive() bool {
	s.Lock()
	defer s.Unlock()
	return s.pConn.conn != nil
}
func (s *Subscription) ResetHandled() {
	atomic.StoreInt64(&s.handled, 0)
}
func (s *Subscription) IncrementHandled() {
	atomic.AddInt64(&s.handled, 1)
}
func (s *Subscription) GetHandled() int {
	return int(atomic.LoadInt64(&s.handled))
}

// Subscribe to a websocket connection for a given data type
// by providing a channel that the messages will be
// written to
func (s *Subscription) Subscribe(handler func(msg []byte)) {
	if s.getRunning() {
		return
	}
	s.setRunning(true)

	log.Info("subscribing to upstream Polygon")
	log.Info("enabling ... {%s:%v}", "scope", s.pConn.scope.GetSubScope())

	// initialize & start the async worker pool

	s.ResetHandled()
	workerPool := pool.NewPool(10, func(msg interface{}) {
		handler(msg.([]byte))
		s.IncrementHandled()
	})

	go workerPool.Work(s.Incoming)

	// monitoring goroutines
	go func() {
		tickDebug := time.NewTicker(time.Second)
		for range tickDebug.C {
			log.Debug(
				"{%s:%v,%s:%v,%s:%v,%s:%v}",
				"subscription", s.pConn.scope.GetSubScope(),
				"goroutines", runtime.NumGoroutine(),
				"channel_depth", len(s.Incoming),
				"handled_messages", s.GetHandled())
		}
	}()
	go func() {
		tickInfo := time.NewTicker(10 * time.Second)
		for range tickInfo.C {
			log.Info("{%s:%v,%s:%v,%s:%v}",
				"subscription", s.pConn.scope.GetSubScope(),
				"channel_depth", len(s.Incoming),
				"handled_messages", s.GetHandled())
			s.ResetHandled()
		}
	}()

	go s.pConn.listen()
}
