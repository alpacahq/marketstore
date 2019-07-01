package api

import (
	"runtime"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/utils/log"
	"github.com/alpacahq/marketstore/utils/pool"
)

type Subscription struct {
	Incoming chan interface{}
	pConn    *PolygonWebSocket
	running  bool
	sync.Mutex
}

// servers := utils.Settings["WS_SERVERS"]
func NewSubscription(t Prefix, symbols []string) (s *Subscription) {
	incoming := make(chan interface{}, 100) //sized to 10x the worker pool
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
	workerPool := pool.NewPool(10, func(msg interface{}) {
		handler(msg.([]byte))
	})

	go workerPool.Work(s.Incoming)

	// monitoring goroutine
	go func() {
		tick := time.NewTicker(time.Second)
		for range tick.C {
			log.Debug(
				"channel status {%s:%v,%s:%v,%s:%v}",
				"channel", s.pConn.scope.GetSubScope(),
				"goroutines", runtime.NumGoroutine(),
				"depth", len(s.Incoming))
		}
	}()

	go s.pConn.listen()
}
