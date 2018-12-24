package api

import (
	"encoding/json"
	"time"

	"github.com/alpacahq/marketstore/utils/log"
	"github.com/eapache/channels"
	"github.com/gorilla/websocket"
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

func getStatus(raw []byte) string {
	packet := map[string]string{}
	json.Unmarshal(raw, &packet)
	if ev, ok := packet["ev"]; ok && ev == "status" {
		status, ok := packet["status"]
		if ok {
			return status
		}
	}
	return "unrecognized message:" + string(raw)
}

// Stream from the polygon websocket server
func Stream(handler func(m []byte), prefix string, symbols []string) (err error) {
	c := channels.NewInfiniteChannel()

	url := baseURL
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Error("[polyiex]: failed to connect %s: %v", url, err)
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

	//sem := make(chan struct{}, runtime.NumCPU())
	go func() {
		for msg := range c.Out() {
			handler(msg.([]byte))
			// sem <- struct{}{}
			// go func(m interface{}) {
			// 	defer func() { <-sem }()
			// 	handler(m.([]byte))
			// }(msg)
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

	// initial auth handshake
	authMsg := makeAction("auth", apiKey)
	conn.WriteMessage(websocket.TextMessage, authMsg)

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
