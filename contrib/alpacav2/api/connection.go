package api

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpacav2/config"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/gorilla/websocket"
)

var errExchangeMessage = errors.New("didn't receive expected message")

type AlpacaWebSocket struct {
	maxMessageSize int64
	pingPeriod     time.Duration
	server         string
	apiKey         string
	apiSecret      string
	subscriptions  string
	conn           *websocket.Conn
	outputChan     chan<- interface{}
}

func NewAlpacaWebSocket(config config.Config, oChan chan<- interface{}) *AlpacaWebSocket {
	return &AlpacaWebSocket{
		maxMessageSize: 2048000,
		pingPeriod:     10 * time.Second,
		server:         config.WSServer + "/" + config.Source,
		apiKey:         config.APIKey,
		apiSecret:      config.APISecret,
		subscriptions:  config.Subscription.String(),
		conn:           nil,
		outputChan:     oChan,
	}
}

// listen sets up a websocket connection, authenticates
// and sets up listening. It returns with the error that
// resulted in the connection getting closed.
func (p *AlpacaWebSocket) listen() error {
	// start the websocket connection
	if err := p.connect(); err != nil {
		log.Error("[alpacav2] error connecting to server {%s:%v,%s:%v,%s:%s}",
			"server", p.server,
			"subscription", p.subscriptions,
			"error", err)
		return err
	}
	defer p.conn.Close()

	p.conn.SetReadLimit(p.maxMessageSize)
	p.conn.SetPongHandler(func(string) error {
		// The ping we have sent has received a reply
		// so we extend the deadline beyond our next ping
		return p.setReadDeadline()
	})

	// Subscribe to streams
	if err := p.subscribe(); err != nil {
		return err
	}

	// errorChan is buffered to ensure receiveMessages
	// can always finish
	out, errorChan := make(chan []byte), make(chan error, 1)
	go p.receiveMessages(out, errorChan)
	ticker := time.NewTicker(p.pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case err := <-errorChan:
			return err
		case <-ticker.C:
			err := p.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second))
			if err != nil {
				log.Error("[alpacav2] stream write ping error %s", err)
				return err
			}
		case msg := <-out:
			p.outputChan <- msg
		}
	}
}

func (p *AlpacaWebSocket) setReadDeadline() error {
	return p.conn.SetReadDeadline(time.Now().Add((p.pingPeriod * 6) / 5))
}

func (p *AlpacaWebSocket) receiveMessages(out chan<- []byte, errorChan chan<- error) {
	for {
		tt, pp, err := p.conn.ReadMessage()
		if err != nil {
			log.Error("[alpacav2] error during reading {%s:%s}",
				"error", err)
			errorChan <- err
			return
		}

		if tt == websocket.BinaryMessage {
			log.Warn("[alpacav2] received binary message from server")
			continue
		}

		out <- pp
	}
}

func (p *AlpacaWebSocket) connect() (err error) {
	var hresp *http.Response
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 2 * time.Second
	p.conn, hresp, err = dialer.Dial(p.server, nil)
	if err != nil {
		if hresp != nil {
			body, _ := ioutil.ReadAll(hresp.Body)
			return fmt.Errorf(
				"[alpacav2] connection failure, err: %w, status_code: %d, body: %s",
				err,
				hresp.StatusCode,
				body,
			)
		}
		return fmt.Errorf(
			"[alpacav2] connection failure, err: %w",
			err,
		)
	}

	// Waiting for the welcome message
	_, _, err = p.conn.ReadMessage()
	if err != nil {
		return err
	}

	return nil
}

// Subscribe sends the necessary messages through p.conn
// to authorize the user and subscribe to streams.
func (p *AlpacaWebSocket) subscribe() error {
	var (
		err  error
		resp string
	)
	/*
		ws.send(`{"action":"auth","key":"YOU_API_KEY","secret":"YOUR_API_SECRET"}`)
		ws.send(`{"action":"subscribe","trades":["AAPL"],"quotes":["VOO"],"bars":["AAPL"]}`)
	*/
	authMsg := fmt.Sprintf(
		`{"action":"auth","key":"%s","secret":"%s"}`,
		p.apiKey,
		p.apiSecret,
	)
	subMsg := fmt.Sprintf(
		`{"action": "subscribe", %s}`,
		p.subscriptions,
	)

	// Authenticate
	resp, err = p.exchangeMessage(authMsg, `"authenticated"`)
	if err != nil {
		log.Error("[alpacav2] unable to authenticate {%s:%v,%s:%v}",
			"response", resp,
			"error", err)
		return err
	}
	log.Info("[alpacav2] authenticated successfully")

	// Subscribe
	resp, err = p.exchangeMessage(subMsg, `"subscription"`)
	if err != nil {
		log.Error("[alpacav2] subscription failure {%s:%v,%s:%v,%s:%v}",
			"subscriptions", p.subscriptions,
			"response", resp,
			"error", err)
		return err
	}
	log.Info("[alpacav2] subscribed {%s:%v}", "subscriptions", p.subscriptions)

	// Setting the read deadline to avoid situations where a timeout
	// is not set before our first ping is sent out
	return p.setReadDeadline()
}

func (p *AlpacaWebSocket) exchangeMessage(send, expect string) (response string, err error) {
	err = p.conn.WriteMessage(websocket.TextMessage, []byte(send))
	if err != nil {
		return "", err
	}

	_, pp, err := p.conn.ReadMessage()
	if err != nil {
		return "", err
	}

	response = string(pp)
	if !strings.Contains(response, expect) {
		return response, errExchangeMessage
	}

	return response, nil
}
