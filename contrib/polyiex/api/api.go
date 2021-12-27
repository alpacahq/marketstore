package api

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/buger/jsonparser"
	"github.com/eapache/channels"
	"github.com/gorilla/websocket"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

var (
	baseURL string
	apiKey  string
	NY, _   = time.LoadLocation("America/New_York")
)

func SetAPIKey(key string) {
	apiKey = key
}

func SetBaseURL(url string) {
	baseURL = url
}

type IEXTrade struct {
	Event       string  `json:"ev"`
	Symbol      string  `json:"S"`
	ID          int64   `json:"i"`
	Exchange    uint8   `json:"x"`
	Size        uint32  `json:"s"`
	Conditions  []int   `json:"c"`
	Price       float64 `json:"p"`
	Timestamp   int64   `json:"t"`
	Nanoseconds int64   `json:"T"`
}

type IEXL2 struct {
	Event       string      `json:"ev"`
	Symbol      string      `json:"S"`
	Bids        [][]float64 `json:"b"`
	Asks        [][]float64 `json:"a"`
	Exchange    uint8       `json:"x"`
	Timestamp   int64       `json:"t"`
	Nanoseconds int64       `json:"T"`
}

const (
	TradePrefix = "IT."
	BookPrefix  = "ID."
)

func makeAction(action, params string) []byte {
	msg, _ := json.Marshal(map[string]string{
		"action": action,
		"params": params,
	})
	return msg
}

func expectStatusEvent(conn *websocket.Conn, expected, name string) error {
	_, reply, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	ev, _ := jsonparser.GetString(reply, "[0]", "ev")
	status, _ := jsonparser.GetString(reply, "[0]", "status")
	if ev != "status" || status != expected {
		err := fmt.Errorf("[polyiex] unexpected %s reply: %v", name, string(reply))
		return err
	}
	return nil
}

// Stream from the polygon websocket server.
func Stream(handler func(m []byte), prefix string, symbols []string) (err error) {
	c := channels.NewInfiniteChannel()

	url := baseURL
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Error("[polyiex] failed to connect %s: %v", url, err)
		return
	}
	log.Info("connected %v", prefix)

	if err = expectStatusEvent(conn, "connected", "connection"); err != nil {
		log.Error("%v", err)
		return
	}
	log.Info("authenticated %v", prefix)

	// initial auth handshake
	authMsg := makeAction("auth", apiKey)
	err = conn.WriteMessage(websocket.TextMessage, authMsg)
	if err != nil {
		log.Error("%v", err)
		return
	}

	if err = expectStatusEvent(conn, "success", "authentication"); err != nil {
		log.Error("%v", err)
		return
	}

	go func() {
		defer conn.Close()
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Error("[polyiex] read: %v", err)
				return
			}
			c.In() <- message
		}
	}()

	go func() {
		for msg := range c.Out() {
			// orderbook is not concurrent at the moment, so
			// do not parallelize this until it is protected.
			// the actual write is in a separate goroutine,
			// and I'm not sure if we ever need to parallelize here.
			handler(msg.([]byte))
		}
	}()

	go func() {
		for {
			<-time.After(10 * time.Second)
			if c.Len() > 0 {
				switch prefix {
				case TradePrefix:
					log.Info("[polyiex] trade stream channel depth: %v", c.Len())
				case BookPrefix:
					log.Info("[polyiex] book stream channel depth: %v", c.Len())
				}
			}
		}
	}()

	// subscribe
	subscribe := func(target string) error {
		msg := makeAction("subscribe", target)
		return conn.WriteMessage(websocket.TextMessage, msg)
	}
	if len(symbols) > 0 {
		for _, symbol := range symbols {
			if err = subscribe(prefix + symbol); err != nil {
				return
			}
		}
	} else {
		err = subscribe(prefix + "*")
	}

	return
}
