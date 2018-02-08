package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"time"

	"github.com/golang/glog"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/slait/cache"
	"github.com/alpacahq/slait/rest/client"
	"github.com/alpacahq/slait/socket"
	"github.com/gorilla/websocket"
)

type SlaitSubscriberConfig struct {
	Endpoint       string     `json:"endpoint"`
	Topic          string     `json:"topic"`
	AttributeGroup string     `json:"attribute_group"`
	Shape          [][]string `json:"shape"`
}

type keyColumnPair struct {
	key     *io.TimeBucketKey
	columns *io.ColumnSeries
}

type SlaitSubscriber struct {
	config         map[string]interface{}
	endpoint       string
	topic          string
	attributeGroup string
	shape          []io.DataShape
	cli            client.SlaitClient
	conn           *websocket.Conn
	done           chan struct{}
	pairC          chan *keyColumnPair
}

func recast(config map[string]interface{}) *SlaitSubscriberConfig {
	data, _ := json.Marshal(config)
	ret := &SlaitSubscriberConfig{}
	json.Unmarshal(data, ret)
	return ret
}

func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config := recast(conf)
	if config.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is empty")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("topic is empty")
	}
	if config.AttributeGroup == "" {
		return nil, fmt.Errorf("attribute group is empty")
	}
	if config.Shape == nil {
		return nil, fmt.Errorf("shape is empty")
	}
	names := make([]string, len(config.Shape))
	types := make([]io.EnumElementType, len(config.Shape))
	for i, shape := range config.Shape {
		if len(shape) != 2 {
			return nil, fmt.Errorf("shape is invalid: %v", shape)
		}
		names[i] = shape[0]
		types[i] = io.EnumElementTypeFromName(shape[1])
	}
	return &SlaitSubscriber{
		config:         conf,
		endpoint:       config.Endpoint,
		topic:          config.Topic,
		attributeGroup: config.AttributeGroup,
		shape:          io.NewDataShapeVector(names, types),
	}, nil
}

func (ss *SlaitSubscriber) Run() {
	for {
		if err := ss.subscribe(); err != nil {
			glog.Warningln(err)
		}
	}
}

// subscription routine to stay connected to Slait websocket
func (ss *SlaitSubscriber) subscribe() (err error) {
	defer func() {
		if ss.conn != nil {
			ss.conn.Close()
		}
	}()

	u := url.URL{Scheme: "ws", Host: ss.endpoint, Path: "/ws"}
	ss.conn, _, err = websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		glog.Errorln("Failed to establish Slait connection.")
		return ss.reconnect(5 * time.Second)
	}

	ss.done = make(chan struct{}, 2)
	ss.pairC = make(chan *keyColumnPair, 10000)

	// websocket read routine
	go ss.read()
	// write routine
	go ss.write()

	// subscribe to all symbols on the partition
	subMsg := socket.SocketMessage{
		Action: "subscribe",
		Topic:  ss.topic,
	}
	err = ss.conn.WriteJSON(subMsg)
	if err != nil {
		return ss.reconnect(5 * time.Second)
	}

	for {
		select {
		case <-ss.done:
			glog.Warning("SlaitSubscriber received signal on done channel")
			return err
		case <-time.After(time.Second):
		}
	}
}

func (ss *SlaitSubscriber) reconnect(sleep time.Duration) error {
	glog.Errorf("Reconnecting in %v...", sleep)
	time.Sleep(sleep)
	return ss.subscribe()
}

func (ss *SlaitSubscriber) handleMessage(msg []byte, msgType int) (err error) {
	switch msgType {
	case websocket.CloseMessage:
		err = errors.New("Received close message")
	case websocket.PingMessage:
		err = ss.conn.WriteMessage(websocket.PongMessage, []byte{})
	case websocket.PongMessage:
		err = ss.conn.WriteMessage(websocket.PingMessage, []byte{})
	default:
		p := cache.Publication{}
		err = json.Unmarshal(msg, &p)
		if err != nil {
			glog.Errorf("Failed to unmarshal JSON from Slait - Msg: %v - Error: %v", string(msg), err)
		} else {
			if p.Entries.Len() > 0 {
				pair, err := ss.handlePublication(p)
				if err != nil {
					return err
				}
				ss.pairC <- pair
			}
		}
	}
	return err
}

func (ss *SlaitSubscriber) write() {
	start := time.Now()
	csm := io.NewColumnSeriesMap()
	buffered := 0
	flush := func() {
		if err := executor.WriteCSM(csm, false); err != nil {
			glog.Errorf("Failed to write CSM for - Error: %v", err)
		}
		buffered = 0
		start = time.Now()
		csm = io.NewColumnSeriesMap()
	}
	for {
		select {
		case pair := <-ss.pairC:
			csm.AddColumnSeries(*pair.key, pair.columns)
			buffered++
			if buffered > 100 {
				flush()
			}
		case <-time.After(500 * time.Millisecond):
			flush()
		case <-ss.done:
			return
		}
	}
}

func (ss *SlaitSubscriber) handlePublication(p cache.Publication) (*keyColumnPair, error) {
	columns := make([]interface{}, len(ss.shape))
	names := make([]string, len(ss.shape))
	length := p.Entries.Len()
	for i, shape := range ss.shape {
		names[i] = shape.Name
		switch shape.Type {
		case io.INT32:
			columns[i] = make([]int32, length)
		case io.INT64:
			columns[i] = make([]int64, length)
		case io.FLOAT32:
			columns[i] = make([]float32, length)
		case io.FLOAT64:
			columns[i] = make([]float64, length)
		default:
			panic(fmt.Sprintf("unsupported shape: %v", shape.Type))
		}
	}
	for i, entry := range p.Entries {
		row := map[string]interface{}{}
		if err := json.Unmarshal(entry.Data, &row); err != nil {
			return nil, fmt.Errorf("Failed to unmarshal slait publication entry to bar - Error: %v", err)
		}
		for name, data := range row {
			var v reflect.Value
			if str, ok := data.(string); ok {
				name = "Epoch"
				t, err := time.Parse(time.RFC3339, str)
				if err != nil {
					return nil, err
				}
				v = reflect.ValueOf(t.Unix())
			} else {
				v = reflect.ValueOf(data)
			}
			for j, colName := range names {
				if name == colName {
					value := reflect.ValueOf(columns[j])
					e := value.Index(i)
					e.Set(reflect.ValueOf(v.Convert(reflect.TypeOf(e.Interface())).Interface()))
				}
			}
		}
	}
	cs := io.NewColumnSeries()
	for i, col := range columns {
		cs.AddColumn(names[i], col)
	}
	tbk := io.NewTimeBucketKey(fmt.Sprintf("%v/1Min/%v", p.Partition, ss.attributeGroup))
	return &keyColumnPair{key: tbk, columns: cs}, nil
}

func (ss *SlaitSubscriber) read() (err error) {
	defer func() {
		// for main routine
		ss.done <- struct{}{}
		// for write routine
		ss.done <- struct{}{}
		close(ss.done)
		close(ss.pairC)
	}()
	for {
		msgType, msg, err := ss.conn.ReadMessage()
		if err != nil {
			glog.Errorf("Failed to read message from Slait - Error: %v", err)
			return err
		}
		if err = ss.handleMessage(msg, msgType); err != nil {
			glog.Errorf("Failed to handle websocket message - Error: %v", err)
			return err
		}
	}
}

func main() {
	fmt.Println("MarketStore <-> Slait plugin")
}
