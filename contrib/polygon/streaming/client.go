package streaming

/*
Websocket communication sequence:
 > [{"ev":"status","status":"connected","message":"Connected Successfully"}]
 < {"action":"auth","params":"*****"}
 > [{"ev":"status","status":"auth_success","message":"authenticated"}]
 < {"action":"subscribe","params":"T.*,Q.*,AM.*"}
 > [{"ev":"status","status":"success","message":"subscribed to: T.*"}]
 > [{"ev":"status","status":"success","message":"subscribed to: Q.*"}]
 > [{"ev":"status","status":"success","message":"subscribed to: AM.*"}]

Error messages:
 > [{"ev":"status","status":"auth_failed","message":"authentication failed"}]
 > [{"ev":"status","status":"auth_timeout","message":"No authentication request received."}]
 > [{"ev":"status","status":"max_connections","message":"Maximum number of connections exceeded."}]

Example:
```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/marketstore/v4/contrib/polygon/streaming"
	"github.com/mailru/easyjson"
	"github.com/sirupsen/logrus"
)

const (
	endpoint     = "wss://polyfeed.polygon.io/stocks"
	apiKey       = "*****"
	subscription = "T.*,Q.*,AM.*"
)

func infoHandler(msg []byte) {
	logrus.Info(string(msg))
}

func tradeHandler(msg []byte) {
	trade := streaming.Trade{}
	if err := easyjson.Unmarshal(msg, &trade); err != nil {
		panic("unmarshal trade error")
	}
	fmt.Println(trade)
}

func main() {
	ws := streaming.NewClient(endpoint, apiKey, subscription)
	ws.TradeHandler = tradeHandler
	ws.QuoteHandler = infoHandler
	ws.AggregateHandler = infoHandler

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	ws.Listen(ctx)
}
```
*/

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/websocket"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	authTemplate      = `{"action":"auth","params":"%s"}`
	subscribeTemplate = `{"action":"subscribe","params":"%s"}`
)

const (
	ioTimeout    = 10 * time.Second
	pingPeriod   = (ioTimeout * 9) / 10
	restartDelay = 3 * time.Second
)

// Client represents a Polygon streaming websocket client
type Client struct {
	url          string
	apiKey       string
	subscription string

	AggregateHandler func(msg []byte)
	QuoteHandler     func(msg []byte)
	TradeHandler     func(msg []byte)

	once    sync.Once
	restart chan struct{}

	in  chan []byte
	out chan []byte
}

// NewClient creates a new Polygon streaming websocket instance
func NewClient(url, apiKey, subscription string) Client {
	return Client{
		url:          url,
		apiKey:       apiKey,
		subscription: subscription,

		AggregateHandler: defaultHandler,
		QuoteHandler:     defaultHandler,
		TradeHandler:     defaultHandler,

		in: make(chan []byte, 10000),
	}
}

func defaultHandler(msg []byte) {
	log.Debug("polygon websocket debug", "msg", string(msg))
}

// Restart closes the restart channel to signal parserLoop() to stop running.
// It is protected by a sync.Once as it can be closed in writerLoop() and
// readerLoop() goroutines.
func (c *Client) Restart() {
	c.once.Do(func() {
		close(c.restart)
	})
}

// Listen creates a connection to the websocket and reconnects if any error arises.
// This call is blocking so the goroutine will never return. You have to call
// Close() to exit from the infinite loop.
func (c *Client) Listen(ctx context.Context) {
	for {
		wg := sync.WaitGroup{}
		c.out = make(chan []byte)

		c.once = sync.Once{}
		c.restart = make(chan struct{})

		conn, resp, err := websocket.DefaultDialer.Dial(c.url, nil)
		if err != nil {
			log.Warn("polygon websocket connection error", "ws", c.url, "error", err)
			evError.WithLabelValues("connect").Inc()
			goto failed
		}

		if resp.StatusCode != http.StatusSwitchingProtocols {
			log.Warn("polygon websocket upgrade error", "ws", c.url, "error", err)
			evError.WithLabelValues("upgrade").Inc()
			conn.Close()
			goto failed
		}

		wg.Add(3)
		go c.parserLoop(ctx, &wg)
		go c.writerLoop(conn, &wg)
		go c.readerLoop(conn, &wg)
		wg.Wait()

	failed:
		select {
		case <-ctx.Done():
			log.Info("polygon websocket received halt request")
			return

		default:
			log.Warn("polygon websocket connection restart")
			time.Sleep(restartDelay)
		}
	}
}

func (c *Client) statusHandler(msg []byte) bool {
	restart := false

	status, dataType, _, err := jsonparser.Get(msg, "status")
	if err != nil || dataType != jsonparser.String {
		log.Warn("polygon websocket no status", "msg", string(msg))
		evError.WithLabelValues("no_status").Inc()
		return false
	}

	switch string(status) {
	case "connected":
		c.out <- []byte(fmt.Sprintf(authTemplate, c.apiKey))

	case "auth_success":
		c.out <- []byte(fmt.Sprintf(subscribeTemplate, c.subscription))

	case "success":
		log.Info("polygon websocket subscription message", "msg", string(msg))

	case "auth_failed":
		log.Warn("polygon websocket authentication failed")
		evError.WithLabelValues("auth_failed").Inc()
		restart = true

	case "auth_timeout":
		log.Warn("polygon websocket authentication timeout")
		evError.WithLabelValues("auth_timeout").Inc()
		restart = true

	case "max_connections":
		log.Warn("polygon websocket max connection limit reached")
		evError.WithLabelValues("max_connections").Inc()
		restart = true

	default:
		log.Warn("polygon websocket unknown status", "msg", string(msg))
		evError.WithLabelValues("unknown_status").Inc()
	}

	return restart
}

func (c *Client) messageHandler(msg []byte, dataType jsonparser.ValueType, offset int, err error) {
	ev, dataType, _, err := jsonparser.Get(msg, "ev")
	if err != nil || dataType != jsonparser.String {
		log.Warn("polygon websocket no event", "msg", string(msg))
		evError.WithLabelValues("no_event").Inc()
		return
	}

	switch string(ev) {
	// quote message
	case "Q":
		evUpdate.WithLabelValues("quote").Inc()
		evUpdateTime.WithLabelValues("quote").SetToCurrentTime()
		c.QuoteHandler(msg)

	// trade message
	case "T":
		evUpdate.WithLabelValues("trade").Inc()
		evUpdateTime.WithLabelValues("trade").SetToCurrentTime()
		c.TradeHandler(msg)

	// aggregate message
	case "AM":
		evUpdate.WithLabelValues("aggregate").Inc()
		evUpdateTime.WithLabelValues("aggregate").SetToCurrentTime()
		c.AggregateHandler(msg)

	// status message
	case "status":
		if restart := c.statusHandler(msg); restart {
			c.Restart()
		}

	default:
		log.Warn("polygon websocket unknown event", "msg", string(msg))
		evError.WithLabelValues("unknown_event").Inc()
	}
}

func (c *Client) parserLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		close(c.out)
		wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case <-c.restart:
			return

		case msg := <-c.in:
			// We have to iterate over the array as we can get T and Q in the same message
			// Example: `[{"ev":"T",...},{"ev":"Q",...}]`
			if _, err := jsonparser.ArrayEach(msg, c.messageHandler); err != nil {
				log.Warn("polygon websocket unknown message", "msg", string(msg))
				evError.WithLabelValues("unknown_msg").Inc()
			}
		}
	}
}

func (c *Client) readerLoop(conn *websocket.Conn, wg *sync.WaitGroup) {
	defer func() {
		c.Restart()
		wg.Done()
	}()

	//conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(ioTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(ioTimeout))
		return nil
	})

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseNoStatusReceived) {
				log.Warn("polygon websocket read", "error", err)
				evError.WithLabelValues("read").Inc()
			}
			return
		}

		c.in <- msg
	}
}

func (c *Client) writerLoop(conn *websocket.Conn, wg *sync.WaitGroup) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Restart()
		conn.Close()
		for range c.out {
		}
		wg.Done()
	}()

	for {
		select {
		case msg, ok := <-c.out:
			if !ok {
				return
			}

			err := conn.WriteMessage(websocket.TextMessage, []byte(msg))
			if err != nil {
				log.Warn("polygon websocket write", "error", err)
				evError.WithLabelValues("write").Inc()
				return
			}

		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(ioTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Warn("polygon websocket ping", "error", err)
				evError.WithLabelValues("ping").Inc()
				return
			}
		}
	}
}
