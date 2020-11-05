package api

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/config"
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
	subscriptions  []string
	conn           *websocket.Conn
	outputChan     chan<- interface{}
}

func NewAlpacaWebSocket(config config.Config, oChan chan<- interface{}) *AlpacaWebSocket {
	return &AlpacaWebSocket{
		maxMessageSize: 2048000,
		pingPeriod:     10 * time.Second,
		server:         config.WSServer,
		apiKey:         config.APIKey,
		apiSecret:      config.APISecret,
		subscriptions:  config.Subscription.AsCanonical(),
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
		log.Error("[alpaca] error connecting to server {%s:%v,%s:%v,%s:%s}",
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

	// eChan is buffered to ensure receiveMessages
	// can always finish
	out, eChan := make(chan []byte), make(chan error, 1)
	go p.receiveMessages(out, eChan)
	ticker := time.NewTicker(p.pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case err := <-eChan:
			return err
		case <-ticker.C:
			err := p.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second))
			if err != nil {
				log.Error("[alpaca] stream write ping error %s", err)
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
			log.Error("[alpaca] error during reading {%s:%s}",
				"error", err)
			errorChan <- err
			return
		}

		if tt == websocket.BinaryMessage {
			log.Warn("[alpaca] received binary message from server")
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
				"[alpaca] connection failure, err: %w, status_code: %d, body: %s",
				err,
				hresp.StatusCode,
				body,
			)
		}
		return fmt.Errorf(
			"[alpaca] connection failure, err: %w",
			err,
		)
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
		ws.send('{"action": "authenticate", "data": {"key_id": "YOU_API_KEY", "secret_key": "YOUR_API_SECRET"}}')
		ws.send('{"action": "listen","data": {"streams": ["Q.VOO", "T.AAPL"]}}')
	*/
	authMsg := fmt.Sprintf(
		`{"action":"authenticate","data":{"key_id":"%s","secret_key":"%s"}}`,
		p.apiKey,
		p.apiSecret,
	)
	subMsg := fmt.Sprintf(
		`{"action": "listen","data": {"streams": %s}}`,
		strings.ReplaceAll(fmt.Sprintf("%q", p.subscriptions), " ", ","),
	)

	// Authenticate
	resp, err = p.exchangeMessage(authMsg, `"authorized"`)
	if err != nil {
		log.Error("[alpaca] unable to authenticate {%s:%v,%s:%v}",
			"response", resp,
			"error", err)
		return err
	}
	log.Info("[alpaca] authenticated successfully")

	// Subscribe
	resp, err = p.exchangeMessage(subMsg, "streams")
	if err != nil {
		log.Error("[alpaca] subscription failure {%s:%v,%s:%v,%s:%v}",
			"streams", p.subscriptions,
			"response", resp,
			"error", err)
		return err
	}
	log.Info("[alpaca] subscribed {%s:%v}", "streams", p.subscriptions)

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
