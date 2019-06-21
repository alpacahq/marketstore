package socket

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpacahq/slait/cache"
	. "github.com/alpacahq/slait/utils/log"
	"github.com/eapache/channels"

	"github.com/gorilla/websocket"
)

// WebSocket server

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

var upgrader = websocket.Upgrader{}

type connection struct {
	sync.Mutex
	ws     *websocket.Conn
	send   *channels.InfiniteChannel
	done   uint32
	closed uint32
}

func (c *connection) WriteMessage(messageType int, data []byte) error {
	c.Lock()
	c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	defer c.Unlock()
	return c.ws.WriteMessage(messageType, data)
}

func (c *connection) WriteJSON(v interface{}) error {
	c.Lock()
	c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	defer c.Unlock()
	return c.ws.WriteJSON(v)
}

func (c *connection) ReadJSON(v interface{}) error {
	c.ws.SetReadDeadline(time.Now().Add(pongWait))
	return c.ws.ReadJSON(v)
}

func (c *connection) GetAddress() string {
	if c.ws != nil {
		return c.ws.RemoteAddr().String()
	} else {
		return "Unknown address"
	}
}

func (c *connection) Send(p *cache.Publication) {
	if atomic.LoadUint32(&c.done) > 0 {
		if atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
			c.send.Close()
		}
	} else {
		if atomic.LoadUint32(&c.closed) < 1 {
			c.send.In() <- p
		}
	}
}

type SocketMessage struct {
	Action     string
	Topic      string
	Partitions []string
	From       time.Time
}

type subscription struct {
	conn *connection
	m    *sync.Map
	from time.Time
	done chan struct{}
}

func (s *subscription) shouldReceive(topic, partition string) (should bool) {
	s.m.Range(func(key interface{}, value interface{}) bool {
		t := key.(string)
		if topic == t {
			partitions := value.([]string)
			if len(partitions) == 0 {
				should = true
				return false
			}
			for _, p := range partitions {
				if p == partition {
					should = true
					return false
				}
			}
		}
		return true
	})
	return should
}

func (s *subscription) cleanup() {
	if atomic.CompareAndSwapUint32(&s.conn.done, 0, 1) {
		s.conn.WriteMessage(websocket.CloseMessage, []byte{})
		defer Log(INFO, "Unsubscribed %v", s.conn.GetAddress())
		hub.unsubscribe(s)
		s.done <- struct{}{}
		if s.conn.ws != nil {
			err := s.conn.ws.Close()
			if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
				Log(WARNING, "Error occurred closing websocket connection - Error: %v", err.Error())
			}
		}
	}
}

func (s *subscription) consume() {
	defer s.cleanup()
	s.conn.ws.SetPongHandler(func(string) error {
		s.conn.ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		m := SocketMessage{}
		err := s.conn.ReadJSON(&m)
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseGoingAway) {
				Log(WARNING, "Unexpected WS closure - Error: %v", err)
			}
			return
		}
		switch strings.ToLower(m.Action) {
		case "unsubscribe":
			return
		default:
			// update the subscription
			val, loaded := s.m.LoadOrStore(m.Topic, m.Partitions)
			if loaded {
				partitions := val.([]string)
				s.m.Store(m.Topic, append(partitions, m.Partitions...))
			}
			s.from = m.From
			hub.subscribe(s)
		}
	}
}

func (s *subscription) produce() {
	defer s.cleanup()
	ticker := time.NewTicker(pingPeriod)
	for {
		select {
		case data := <-s.conn.send.Out():
			if err := s.conn.WriteJSON(data); err != nil {
				Log(ERROR, "Failed to write JSON to WS - Error: %v", err)
				return
			}
		case <-ticker.C:
			if err := s.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				if !strings.Contains(err.Error(), "websocket: close sent") {
					Log(ERROR, "Failed to write ping message to WS - Error: %v", err)
				}
				return
			}
		case <-s.done:
			return
		}
	}
}

type Hub struct {
	sync.RWMutex
	subscriptions sync.Map
}

func (h *Hub) unsubscribe(s *subscription) {
	h.subscriptions.Delete(s)
}

func (h *Hub) subscribe(s *subscription) {
	h.subscriptions.Store(s, true)
	h.dump(s)
}

func (h *Hub) dump(s *subscription) {
	Log(INFO, "Dumping data to %v", s.conn.GetAddress())
	s.m.Range(func(key interface{}, value interface{}) bool {
		topic := key.(string)
		partitions := value.([]string)
		if len(partitions) == 0 {
			data := cache.GetAll(topic, &s.from, nil, 0)
			for partition, entries := range data {
				s.conn.Send(
					&cache.Publication{
						Topic:     topic,
						Partition: partition,
						Entries:   entries,
					})
			}
		} else {
			for _, partition := range partitions {
				entries := cache.Get(topic, partition, &s.from, nil, 0)
				s.conn.Send(
					&cache.Publication{
						Topic:     topic,
						Partition: partition,
						Entries:   entries,
					})
			}
		}
		return true
	})
	Log(INFO, "Finished data dump to %v", s.conn.GetAddress())
}

func (h *Hub) run() {
	for {
		select {
		case p := <-cache.Pull():
			pub := p.(*cache.Publication)
			h.subscriptions.Range(func(key interface{}, value interface{}) bool {
				sub := key.(*subscription)
				if sub.shouldReceive(pub.Topic, pub.Partition) {
					sub.conn.Send(pub)
				}
				return true
			})
		case a := <-cache.PullAdditions():
			h.subscriptions.Range(func(key interface{}, value interface{}) bool {
				sub := key.(*subscription)
				sub.conn.WriteJSON(SocketMessage{
					Topic:      a.Topic,
					Partitions: []string{a.Partition},
					Action:     "add",
				})
				return true
			})
		case r := <-cache.PullRemovals():
			h.subscriptions.Range(func(key interface{}, value interface{}) bool {
				sub := key.(*subscription)
				err := sub.conn.WriteJSON(SocketMessage{
					Topic:      r.Topic,
					Partitions: []string{r.Partition},
					Action:     "remove",
				})
				if err != nil {
					Log(ERROR, "Writing socket message failed! %v", err)
					return false
				}
				sub.m.Range(func(key interface{}, value interface{}) bool {
					t := key.(string)
					partitions := value.([]string)
					if len(partitions) == 0 {
						return false
					} else {
						for i, p := range partitions {
							if p == r.Partition {
								partitions = append(partitions[:i], partitions[i+1:]...)
								sub.m.Store(t, partitions)
								return false
							}
						}
					}
					return true
				})
				return true
			})
		}
	}
}

var hub = Hub{
	subscriptions: sync.Map{},
}

type SocketHandler struct{}

func (sh *SocketHandler) Serve(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		Log(ERROR, "Failed to upgrade websocket - Error: %v", err)
		return
	}
	c := &connection{
		send: channels.NewInfiniteChannel(),
		ws:   ws,
	}

	s := subscription{
		conn: c,
		m:    &sync.Map{},
		done: make(chan struct{}),
	}

	if s.conn.ws != nil {
		Log(INFO, "New subscriber: %v", s.conn.GetAddress())
	}
	go s.consume()
	go s.produce()
}

// call only once
func GetHandler() *SocketHandler {
	go hub.run()
	sh := &SocketHandler{}
	return sh
}
