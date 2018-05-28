package stream

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/utils/io"
	"github.com/eapache/channels"
	"github.com/gobwas/glob"
	"github.com/golang/glog"
	"github.com/gorilla/websocket"
	msgpack "github.com/vmihailenco/msgpack"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

var catalog *Catalog
var send *channels.InfiniteChannel
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Catalog maintains the set of active subscribers
type Catalog struct {
	sync.RWMutex
	subs map[*Subscriber]struct{}
}

// Add a new subscriber to the catalog
func (sc *Catalog) Add(sub *Subscriber) {
	sc.Lock()
	defer sc.Unlock()

	sc.subs[sub] = struct{}{}
}

// Remove a subscriber from the catalog
func (sc *Catalog) Remove(sub *Subscriber) {
	sc.Lock()
	defer sc.Unlock()

	delete(sc.subs, sub)
}

// NewCatalog initializes the stream catalog
func NewCatalog() *Catalog {
	return &Catalog{
		subs: map[*Subscriber]struct{}{},
	}
}

// Subscriber includes the connection, and streams to
// manage a given stream client
type Subscriber struct {
	sync.RWMutex
	c       *websocket.Conn
	done    chan struct{}
	streams map[string]struct{}
}

// Subscribed matches the subscriber's subscribed streams
// with the supplied timebucket key string.
func (s *Subscriber) Subscribed(itemKey string) bool {
	s.RLock()
	defer s.RUnlock()
	for stream := range s.streams {
		if g, err := glob.Compile(stream, '/'); err == nil {
			if g.Match(itemKey) {
				return true
			}
		}
	}
	return false
}

// SubscribeMessage is an inbound message for the client
// to subscribe to streams
type SubscribeMessage struct {
	Streams []string `msgpack:"streams"`
}

// ErrorMessage is used to report errors when a client
// subscribes to invalid streams
type ErrorMessage struct {
	Error string `msgpack:"error"`
}

func (s *Subscriber) handleOutbound(buf []byte) error {
	// prevents concurrent write to the websocket connection
	s.Lock()
	defer s.Unlock()
	return s.c.WriteMessage(websocket.BinaryMessage, buf)
}

func (s *Subscriber) handleInbound(msg SubscribeMessage) error {
	if len(msg.Streams) > 0 {
		// prevents concurrent read/write of stream map
		s.Lock()
		defer s.Unlock()

		// validate each stream before modifying the subscriber's stream map
		m := map[string]struct{}{}
		for _, stream := range msg.Streams {
			if !validStream(stream) {
				return fmt.Errorf("%s is an invalid stream", stream)
			}
			m[stream] = struct{}{}
		}
		s.streams = m
	}
	return nil
}

func validStream(stream string) bool {
	g, err := glob.Compile("*/*/*", '/')
	if err != nil {
		return false
	}
	return g.Match(stream)
}

func (s *Subscriber) consume() {
	defer func() {
		catalog.Remove(s)
		s.done <- struct{}{}
	}()

	s.c.SetPongHandler(func(string) error {
		return s.c.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		msgType, buf, err := s.c.ReadMessage()

		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				glog.Errorf("unexpected websocket closure (%v)", err)
			}
			return
		}

		switch msgType {
		case websocket.TextMessage:
			fallthrough
		case websocket.BinaryMessage:
			m := SubscribeMessage{}

			if err = msgpack.Unmarshal(buf, &m); err != nil {
				glog.Errorf("failed to unmarshal inbound stream message (%v)", err)
				continue
			}
			if err := s.handleInbound(m); err != nil {
				buf, _ = msgpack.Marshal(ErrorMessage{Error: err.Error()})
			}
			if err := s.handleOutbound(buf); err != nil {
				glog.Errorf("failed to send stream message (%v)", err)
			}
		case websocket.CloseMessage:
			return
		}
	}
}

func (s *Subscriber) produce() {
	ticker := time.NewTicker(pingPeriod)
	for {
		select {
		case <-ticker.C:
			s.Lock()
			s.c.WriteMessage(websocket.PingMessage, []byte{})
			s.Unlock()
		case <-s.done:
			return
		}
	}
}

func stream() {
	for v := range send.Out() {
		if v == nil {
			continue
		}
		payload := v.(Payload)

		buf, err := msgpack.Marshal(payload)
		if err != nil {
			glog.Errorf("failed to marshal outbound stream payload (%v)", err)
			continue
		}

		catalog.RLock()

		for s := range catalog.subs {
			if s.Subscribed(payload.Key) {
				if err := s.handleOutbound(buf); err != nil {
					glog.Errorf("failed to stream outbound (%s)", err)
				}
			}
		}

		catalog.RUnlock()
	}
}

// Payload is used to send data over the websocket
type Payload struct {
	Key  string      `msgpack:"key"`
	Data interface{} `msgpack:"data"`
}

// Push sends data over the stream interface
func Push(tbk io.TimeBucketKey, data interface{}) error {
	send.In() <- Payload{Key: tbk.GetItemKey(), Data: data}
	return nil
}

// Initialize builds the send channel as well as the cache, and
// must be called before any data flows over the stream interface
func Initialize() {
	send = channels.NewInfiniteChannel()
	catalog = NewCatalog()

	go stream()
}

// Handler hooks into the HTTP interface and handles the incoming
// streaming requests, and upgrades the connection
func Handler(w http.ResponseWriter, r *http.Request) {
	// upgrade the socket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		glog.Errorf("failed to upgrade stream socket (%s)", err)
		return
	}

	// build the subscriber
	s := &Subscriber{
		c:    ws,
		done: make(chan struct{}),
	}

	if s.c != nil {
		glog.Infof("new stream listener: %v", ws.RemoteAddr().String())
	}

	catalog.Add(s)

	// begin streaming
	go s.consume()
	go s.produce()
}
