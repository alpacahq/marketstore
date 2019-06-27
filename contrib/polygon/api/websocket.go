package api

import (
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/alpacahq/marketstore/utils/log"
	"github.com/alpacahq/marketstore/utils/pool"
)

var (
	AllTrades = NewSubscriptionScope(Trade, nil)
	AllQuotes = NewSubscriptionScope(Quote, nil)
	AllBars   = NewSubscriptionScope(Agg, nil)
)

type Subscription struct {
	Servers  []*url.URL
	Incoming chan interface{}
	conn     *websocket.Conn
	doneChan chan struct{} // send to this to stop listener
	running  bool
	scope    *SubscriptionScope
	sync.Mutex
}

// servers := utils.Settings["WS_SERVERS"]
func NewSubscription(servers string, t Prefix, symbols ...string) (s *Subscription, err error) {
	s = &Subscription{}
	if err = s.setURLs(servers); err != nil {
		return
	}
	s.scope = NewSubscriptionScope(t, symbols)
	return
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
	if s.doneChan != nil {
		s.doneChan <- struct{}{}
	}
	s.running = false
}

func (s *Subscription) IsActive() bool {
	s.Lock()
	defer s.Unlock()
	return s.conn != nil
}

func (s *Subscription) connect() (err error) {
	s.disconnect()
	s.Lock()
	defer s.Unlock()
	var hresp *http.Response
	s.conn, hresp, err = websocket.DefaultDialer.Dial(s.Servers[0].String(), nil)
	if err != nil {
		return
	}
	if hresp.StatusCode != 101 { // 101 means "changing protocol", meaning we're upgrading to a websocket
		return fmt.Errorf("upstream connection failure, status_code: %d", hresp.StatusCode)
	}
	// Check to see we have a response from the connection
	err = s.conn.SetReadDeadline(time.Now().Add(time.Second))
	if err != nil {
		return
	}
	resp := s.readMsg()
	if !strings.Contains(resp, "connected") {
		return fmt.Errorf("unable to verify good connection")
	}
	return
}
func (s *Subscription) disconnect() {
	s.Lock()
	defer s.Unlock()
	if s.conn == nil {
		log.Warn("connection was already nil")
		return
	}
	err := s.conn.Close()
	if err != nil {
		log.Warn("error closing connection")
		goto FINALIZE
	}

FINALIZE:
	s.conn = nil
	return
}

func (s *Subscription) readMsg() string {
	_, p, err := s.conn.ReadMessage()
	if err != nil {
		log.Warn("error reading authentication response")
	}
	return string(p)
}

func (s *Subscription) subscribe() (connected bool) {
	var (
		err  error
		resp string
	)
	/*
		ws.send('{"action":"auth","params":"YOUR_API_KEY"}')
		ws.send('{"action":"subscribe","params":"C.AUD/USD,C.USD/EUR,C.USD/JPY"}')
	*/
	authMsg := fmt.Sprintf("{\"action\":\"auth\",\"params\":\"%s\"}", apiKey)
	subMsg := fmt.Sprintf("{\"action\":\"subscribe\", \"params\":\"%s\"}", s.scope.getSubScope())

	// send the subscription message to the upstream
	_ = s.conn.WriteMessage(websocket.TextMessage, []byte(authMsg))
	resp = s.readMsg()
	if strings.Contains(resp, "authenticated") {
		log.Info("authenticated successfully",
			"feed", s.scope)
	} else {
		log.Info("unable to authenticate")
		return false
	}
	err = s.conn.WriteMessage(websocket.TextMessage, []byte(subMsg))
	resp = s.readMsg()
	if strings.Contains(resp, "success") {
		log.Info("subscribed", "feed", s.scope)
	} else {
		log.Warn("upstream subscription failure",
			"data_type", s.scope.getSubScope(),
			"response", resp,
			"error", err)
		return false
	}
	return true
}

func (s *Subscription) listen() {
	var (
		maxMessageSize = int64(2048000)
		// Time allowed to write a message to the peer.
		readWait = 10 * time.Second
		// Time allowed to write a message to the peer.
		writeWait = readWait
		// Time allowed to read the next pong message from the upstream.
		pongWait = readWait
		// Send pings to peer with this period. Must be less than pongWait.
		pingPeriod = time.Second
		pingTicker *time.Ticker
	)
	s.doneChan = make(chan struct{})
	for {
	restartConnection:
		// start the upstream websocket connection
		err := s.connect()
		if err != nil {
			log.Warn("error connecting to upstream",
				"server", s.Servers[0].String(),
				"subscription", s.scope.getSubScope(),
				"error", err.Error())
			time.Sleep(time.Second)
			goto restartConnection // try again
		}

		if !s.subscribe() {
			time.Sleep(time.Second)
			goto restartConnection // try again
		}

		if pingTicker != nil {
			pingTicker.Stop()
		}
		pingTicker = time.NewTicker(pingPeriod)

		s.conn.SetReadLimit(maxMessageSize)
		err = s.conn.SetReadDeadline(time.Time{})
		if err != nil {
			log.Warn("error initializing read loop for upstream, restarting...",
				"subscription", s.scope.getSubScope(),
				"error", err.Error())
			goto restartConnection
		}
		s.conn.SetPongHandler(func(string) (err error) {
			err = s.conn.SetReadDeadline(time.Now().Add(pongWait))
			if err != nil {
				log.Warn("error in pong handler",
					"subscription", s.scope.getSubScope(),
					"error", err.Error())
			}
			return nil
		})
		for {
			select {
			case <-s.doneChan:
				goto cleanup
			case <-pingTicker.C:
				if err := s.conn.WriteControl(websocket.PingMessage, []byte{},
					time.Now().Add(writeWait)); err != nil {
					log.Warn("upstream websocket connection failure",
						"subscription", s.scope.getSubScope(),
						"error", err.Error())
					goto restartConnection
				}
			default:
				err = s.conn.SetReadDeadline(time.Now().Add(readWait))
				tt, p, err := s.conn.ReadMessage()
				switch tt {
				case -1: // "NoFrame" error from the websocket library
					log.Warn("failed websocket connection, restarting...",
						"subscription", s.scope)
					goto restartConnection
				case websocket.BinaryMessage, websocket.PingMessage, websocket.PongMessage:
					log.Warn("ignoring non text msg from upstream",
						"message len", len(p),
						"message type", tt)
					continue // ignore
				case websocket.CloseMessage:
					log.Warn("received websocket close message, restarting...",
						"subscription", s.scope)
					goto restartConnection
				}
				if err != nil {
					time.Sleep(time.Second)
					if strings.Contains(err.Error(), "timeout") {
						log.Info("timeout handling incoming message, restarting connection...",
							"subscription", s.scope.getSubScope(),
							"error", err)
						time.Sleep(time.Second)
						goto restartConnection
					}
					log.Warn("error handling incoming message",
						"subscription", s.scope.getSubScope(),
						"error", err)
					continue
				}
				s.Incoming <- p
			}
		}
	}

cleanup:
	s.disconnect()
	pingTicker.Stop()
	return
}

func (s *Subscription) setURLs(servers string) (err error) {
	//"nats://nats1.polygon.io:31111, nats://nats2.polygon.io:31112, nats://nats3.polygon.io:31113"
	urls := strings.Split(servers, ",")
	if len(urls) < 1 {
		return fmt.Errorf("empty servers string")
	}

	parameters := url.Values{}
	parameters.Add("apiKey", apiKey)

	u := make([]*url.URL, len(urls))
	for i := range urls {
		urls[i] = strings.Trim(urls[i], " ") + "/stocks"
		u[i], err = url.Parse(urls[i])
		u[i].RawQuery = parameters.Encode()
	}
	s.Servers = u
	return
}

// Subscribe to a websocket connection for a given data type
// by providing a channel that the messages will be
// written to
func (s *Subscription) Subscribe(handler func(msg []byte), servers string) {
	if s.getRunning() {
		return
	}
	s.setRunning(true)

	log.Info("subscribing to upstream Polygon")
	log.Info("enabling ...", "scope", s.scope.getSubScope())

	s.Incoming = make(chan interface{}, 100) //sized to 10x the worker pool

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
				"channel status",
				"channel", s.scope.getSubScope(),
				"goroutines", runtime.NumGoroutine(),
				"depth", len(s.Incoming))
		}
	}()

	go s.listen()
}
