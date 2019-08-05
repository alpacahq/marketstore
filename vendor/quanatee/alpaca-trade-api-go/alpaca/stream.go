package alpaca

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/common"
	"github.com/gorilla/websocket"
)

const (
	TradeUpdates   = "trade_updates"
	AccountUpdates = "account_updates"
)

var (
	once sync.Once
	str  *Stream
)

type Stream struct {
	sync.Mutex
	sync.Once
	conn                  *websocket.Conn
	authenticated, closed atomic.Value
	handlers              sync.Map
}

// Subscribe to the specified Alpaca stream channel.
func (s *Stream) Subscribe(channel string, handler func(msg interface{})) (err error) {
	if s.conn == nil {
		s.conn = openSocket()
	}

	switch channel {
	case TradeUpdates:
		fallthrough
	case AccountUpdates:
		if err = s.auth(); err != nil {
			return
		}

		s.Do(func() {
			go s.start()
		})

		s.handlers.Store(channel, handler)

		if err = s.sub(channel); err != nil {
			return
		}
	default:
		err = fmt.Errorf("invalid stream (%s)", channel)
	}
	return
}

// Close gracefully closes the Alpaca stream.
func (s *Stream) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.conn == nil {
		return nil
	}

	if err := s.conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
	); err != nil {
		return err
	}

	// so we know it was gracefully closed
	s.closed.Store(true)

	return s.conn.Close()
}

func (s *Stream) start() {
	for {
		msg := ServerMsg{}

		if err := s.conn.ReadJSON(&msg); err == nil {
			if v, ok := s.handlers.Load(msg.Stream); ok {
				switch msg.Stream {
				case TradeUpdates:
					bytes, _ := json.Marshal(msg.Data)
					var tradeupdate TradeUpdate
					json.Unmarshal(bytes, &tradeupdate)
					h := v.(func(msg interface{}))
					h(tradeupdate)
				default:
					h := v.(func(msg interface{}))
					h(msg.Data)
				}
			}
		} else {
			if websocket.IsCloseError(err) {
				// if this was a graceful closure, don't reconnect
				if s.closed.Load().(bool) {
					return
				}
			} else {
				log.Printf("alpaca stream read error (%v)", err)
			}

			s.conn = openSocket()
		}
	}
}

func (s *Stream) sub(channel string) (err error) {
	s.Lock()
	defer s.Unlock()

	subReq := ClientMsg{
		Action: "listen",
		Data: map[string]interface{}{
			"streams": []interface{}{
				channel,
			},
		},
	}

	if err = s.conn.WriteJSON(subReq); err != nil {
		return
	}

	return
}

func (s *Stream) isAuthenticated() bool {
	return s.authenticated.Load().(bool)
}

func (s *Stream) auth() (err error) {
	s.Lock()
	defer s.Unlock()

	if s.isAuthenticated() {
		return
	}

	authRequest := ClientMsg{
		Action: "authenticate",
		Data: map[string]interface{}{
			"key_id":     common.Credentials().ID,
			"secret_key": common.Credentials().Secret,
		},
	}

	if err = s.conn.WriteJSON(authRequest); err != nil {
		return
	}

	msg := ServerMsg{}

	// ensure the auth response comes in a timely manner
	s.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer s.conn.SetReadDeadline(time.Time{})

	if err = s.conn.ReadJSON(&msg); err != nil {
		return
	}

	m := msg.Data.(map[string]interface{})

	if !strings.EqualFold(m["status"].(string), "authorized") {
		return fmt.Errorf("failed to authorize alpaca stream")
	}

	return
}

// GetStream returns the singleton Alpaca stream structure.
func GetStream() *Stream {
	once.Do(func() {
		str = &Stream{
			authenticated: atomic.Value{},
			handlers:      sync.Map{},
		}

		str.authenticated.Store(false)
		str.closed.Store(false)
	})

	return str
}

func openSocket() *websocket.Conn {
	scheme := "wss"
	ub, _ := url.Parse(base)
	if ub.Scheme == "http" {
		scheme = "ws"
	}
	u := url.URL{Scheme: scheme, Host: ub.Host, Path: "/stream"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		panic(err)
	}
	return c
}
