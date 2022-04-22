package api

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

type PolygonWebSocket struct {
	maxMessageSize int64
	pingPeriod     time.Duration
	doneChan       chan struct{}
	Servers        []*url.URL
	apiKey         string
	scope          *SubscriptionScope
	conn           *websocket.Conn
	outputChan     chan interface{}
}

func NewPolygonWebSocket(servers, apiKey string, pref Prefix, symbols []string, oChan chan interface{},
) *PolygonWebSocket {
	const (
		// maximum size in bytes for a message read from the peer.
		defaultMaxMessageRecvBytes = 2048000
		defaultOutputChanLength    = 100
	)

	if oChan == nil {
		oChan = make(chan interface{}, defaultOutputChanLength)
	}
	return &PolygonWebSocket{
		maxMessageSize: defaultMaxMessageRecvBytes,
		pingPeriod:     10 * time.Second,
		doneChan:       make(chan struct{}),
		Servers:        setURLs(servers, apiKey),
		apiKey:         apiKey,
		scope:          NewSubscriptionScope(pref, symbols),
		conn:           nil,
		outputChan:     oChan,
	}
}

func (p *PolygonWebSocket) ping() {
	_ = p.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second))
}

func (p *PolygonWebSocket) pongHandler(_ string) (err error) {
	// nolint:gomnd // Slightly longer time than the ping period
	_ = p.conn.SetReadDeadline(time.Now().Add(6 * p.pingPeriod / 5))
	pong := func() {
		time.Sleep(p.pingPeriod)
		log.Debug("ponging...")
		if p.conn == nil || p.conn.UnderlyingConn() == nil {
			return
		}
		_ = p.conn.SetReadDeadline(time.Now().Add(p.pingPeriod))
		_ = p.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second))
	}
	go pong()
	return nil
}

func (p *PolygonWebSocket) listen() {
restartConnection:
	// start the upstream websocket connection
	err := p.connect()
	if err != nil {
		log.Warn("error connecting to upstream {%s:%v,%s:%v,%s:%v}",
			"server", p.Servers[0].String(),
			"subscription", p.scope.GetSubScope(),
			"error", err.Error())
		time.Sleep(time.Second)
		goto restartConnection // try again
	}

	p.conn.SetPongHandler(p.pongHandler)
	p.ping()

	if !p.subscribe() {
		time.Sleep(time.Second)
		goto restartConnection // try again
	}

	p.conn.SetReadLimit(p.maxMessageSize)
	err = p.conn.SetReadDeadline(time.Now().Add(p.pingPeriod))
	if err != nil {
		log.Warn("error initializing read loop for upstream, restarting... {%s:%v,%s:%v}",
			"subscription", p.scope.GetSubScope(),
			"error", err.Error())
		goto restartConnection
	}
	out := make(chan []byte)
	go p.receiveMessages(out)
	for {
		select {
		case <-p.doneChan:
			p.disconnect()
			return
		case msg := <-out:
			switch msg {
			case nil:
				goto restartConnection
			default:
				p.outputChan <- msg
			}
		}
	}
}

func (p *PolygonWebSocket) receiveMessages(out chan []byte) {
	for {
		tt, pp, err := p.conn.ReadMessage()
		switch tt {
		case -1: // "NoFrame" error from the websocket library
			log.Warn("failed websocket connection, restarting... {%s:%v}",
				"subscription", p.scope.GetSubScope())
			goto ErrorOut
		case websocket.BinaryMessage, websocket.PingMessage, websocket.PongMessage:
			log.Warn("ignoring non text msg from upstream {%s:%v,%s:%v}",
				"message len", len(pp),
				"message type", tt)
			continue // ignore
		case websocket.CloseMessage:
			log.Warn("received websocket close message, restarting... {%s:%v,%s:%v}",
				"subscription", p.scope.GetSubScope())
			goto ErrorOut
		}
		if err != nil {
			if strings.Contains(err.Error(), "timeout") {
				log.Info("timeout handling incoming message, restarting connection... {%s:%v,%s:%v}",
					"subscription", p.scope.GetSubScope(),
					"error", err)
				goto ErrorOut
			}
			log.Warn("error handling incoming message {%s:%v,%s:%v}",
				"subscription", p.scope.GetSubScope(),
				"error", err)
			continue
		}
		out <- pp
	}
ErrorOut:
	out <- nil
}

func (p *PolygonWebSocket) connect() (err error) {
	// 101 means "changing protocol", meaning we're upgrading to a websocket
	const statusCodeChangingProtocol = 101

	p.disconnect()
	var hresp *http.Response
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 2 * time.Second
	p.conn, hresp, err = dialer.Dial(p.Servers[0].String(), nil)
	if err != nil {
		return
	}
	if hresp.StatusCode != statusCodeChangingProtocol {
		return fmt.Errorf("upstream connection failure, status_code: %d", hresp.StatusCode)
	}
	// Check to see we have a response from the connection
	err = p.conn.SetReadDeadline(time.Now().Add(time.Second))
	if err != nil {
		return
	}
	resp := p.readMsg()
	if !strings.Contains(resp, "connected") {
		return fmt.Errorf("unable to verify good connection")
	}
	return
}

func (p *PolygonWebSocket) disconnect() {
	if p.conn == nil {
		log.Warn("connection was already nil")
		return
	}
	if err := p.conn.Close(); err != nil {
		log.Warn("error closing connection")
	}

	p.conn = nil
}

func (p *PolygonWebSocket) readMsg() string {
	_, pp, err := p.conn.ReadMessage()
	if err != nil {
		log.Warn("error reading authentication response")
	}
	return string(pp)
}

func (p *PolygonWebSocket) subscribe() (connected bool) {
	var (
		err  error
		resp string
	)
	/*
		ws.send('{"action":"auth","params":"YOUR_API_KEY"}')
		ws.send('{"action":"subscribe","params":"C.AUD/USD,C.USD/EUR,C.USD/JPY"}')
	*/
	authMsg := fmt.Sprintf("{\"action\":\"auth\",\"params\":\"%s\"}", p.apiKey)
	subMsg := fmt.Sprintf("{\"action\":\"subscribe\", \"params\":\"%s\"}", p.scope.GetSubScope())

	// send the subscription message to the upstream
	_ = p.conn.WriteMessage(websocket.TextMessage, []byte(authMsg))
	resp = p.readMsg()
	if strings.Contains(resp, "authenticated") {
		log.Info("authenticated successfully {%s:%v}",
			"feed", p.scope.GetSubScope())
	} else {
		log.Info("unable to authenticate")
		return false
	}
	err = p.conn.WriteMessage(websocket.TextMessage, []byte(subMsg))
	resp = p.readMsg()
	if strings.Contains(resp, "success") {
		log.Info("subscribed {%s:%v}", "feed", p.scope.GetSubScope())
	} else {
		log.Warn("upstream subscription failure {%s:%v,%s:%v,%s:%v}",
			"data_type", p.scope.GetSubScope(),
			"response", resp,
			"error", err)
		return false
	}
	return true
}

func setURLs(servers, apiKey string) (svrs []*url.URL) {
	urls := strings.Split(servers, ",")
	if len(urls) < 1 {
		return
	}
	parameters := url.Values{}
	parameters.Add("apiKey", apiKey)
	var err error
	u := make([]*url.URL, len(urls))
	for i := range urls {
		urls[i] = strings.Trim(urls[i], " ") + "/stocks"
		u[i], err = url.Parse(urls[i])
		if err != nil {
			return
		}
		u[i].RawQuery = parameters.Encode()
	}
	svrs = u
	return
}
