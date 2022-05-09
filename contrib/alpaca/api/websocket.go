package api

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eapache/channels"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/config"
	"github.com/alpacahq/marketstore/v4/contrib/alpaca/metrics"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/utils/pool"
)

const (
	sleepStart    = 1 * time.Second
	sleepLimit    = 5 * time.Minute
	connLiveAfter = 5 * time.Second
)

type Subscription struct {
	channel     *channels.InfiniteChannel
	incoming    <-chan interface{}
	ws          *AlpacaWebSocket
	workerCount int
	handled     int64
	once        sync.Once
}

// NewSubscription creates and initializes a Subscription
// that is ready to use.
func NewSubscription(cfg *config.Config) (s *Subscription) {
	c := channels.NewInfiniteChannel()
	return &Subscription{
		channel:     c,
		incoming:    c.Out(),
		ws:          NewAlpacaWebSocket(cfg, c.In()),
		workerCount: cfg.WSWorkerCount,
	}
}

func (s *Subscription) resetHandled() {
	atomic.StoreInt64(&s.handled, 0)
}

func (s *Subscription) incrementHandled() {
	atomic.AddInt64(&s.handled, 1)
}

func (s *Subscription) getHandled() int {
	return int(atomic.LoadInt64(&s.handled))
}

// Start establishes a websocket connection and
// starts processing messages using handler.
// Subsequent calls are no-ops.
func (s *Subscription) Start(handler func(msg []byte)) {
	s.once.Do(func() {
		s.start(handler)
	})
}

func (s *Subscription) start(handler func(msg []byte)) {
	subscriptions := s.ws.subscriptions

	log.Info("[alpaca] subscribing to Alpaca data websocket: %v", s.ws.server)
	log.Info("[alpaca] enabling ... {%s:%v}", "streams", subscriptions)

	// initialize & start the async worker pool
	s.resetHandled()
	workerPool := pool.NewPool(s.workerCount, func(msg []byte) {
		handler(msg)
		s.incrementHandled()
	})
	log.Info("[alpaca] using %d workers", s.workerCount)

	go workerPool.Work(s.incoming)

	// monitoring goroutine
	go func() {
		tickInfo := time.NewTicker(1 * time.Minute)
		defer tickInfo.Stop()
		for range tickInfo.C {
			d := s.channel.Len()
			metrics.AlpacaStreamQueueLength.Set(float64(d))
			log.Info("[alpaca] {%s:%v,%s:%v,%s:%v}",
				"subscription", subscriptions,
				"channel_depth", d,
				"handled_messages", s.getHandled())
			s.resetHandled()
		}
	}()

	// Automatically reconnecting w/ exp. backoff + jitter
	go func() {
		backoff := sleepStart
		random := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			start := time.Now()
			err := s.ws.listen()
			log.Error("[alpaca] error during ws listening {%s:%s}",
				"error", err)
			if time.Since(start) > connLiveAfter {
				backoff = sleepStart
			} else {
				backoff *= 2
				if backoff > sleepLimit {
					backoff = sleepLimit
				}
			}
			jitter := time.Duration(random.Intn(1000)) * time.Millisecond
			log.Info("[alpaca] backing off for %s", backoff)
			time.Sleep(backoff + jitter)
		}
	}()
}
