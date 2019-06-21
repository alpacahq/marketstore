package binance

import (
	"time"

	"github.com/gorilla/websocket"
)

// WsHandler handle raw websocket message
type WsHandler func(message []byte)

// ErrHandler handles errors
type ErrHandler func(err error)

type wsConfig struct {
	endpoint string
}

func newWsConfig(endpoint string) *wsConfig {
	return &wsConfig{
		endpoint: endpoint,
	}
}

var wsServe = func(cfg *wsConfig, handler WsHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	c, _, err := websocket.DefaultDialer.Dial(cfg.endpoint, nil)
	if err != nil {
		return nil, nil, err
	}
	doneC = make(chan struct{})
	stopC = make(chan struct{})
	go func() {
		defer func() {
			cerr := c.Close()
			if cerr != nil {
				errHandler(cerr)
			}
		}()
		defer close(doneC)
		if WebsocketKeepalive {
			keepAlive(c, WebsocketTimeout)
		}

		for {
			select {
			case <-stopC:
				return
			default:
				_, message, err := c.ReadMessage()
				if err != nil {
					errHandler(err)
					return
				}
				handler(message)
			}
		}
	}()
	return
}

func keepAlive(c *websocket.Conn, timeout time.Duration) {
	ticker := time.NewTicker(timeout)

	lastResponse := time.Now()
	c.SetPongHandler(func(msg string) error {
		lastResponse = time.Now()
		return nil
	})

	go func() {
		defer ticker.Stop()
		for {
			deadline := time.Now().Add(10 * time.Second)
			err := c.WriteControl(websocket.PingMessage, []byte{}, deadline)
			if err != nil {
				return
			}
			<-ticker.C
			if time.Now().Sub(lastResponse) > timeout {
				c.Close()
				return
			}
		}
	}()
}
