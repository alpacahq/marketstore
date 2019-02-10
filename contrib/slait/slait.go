package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/alpacahq/slait/cache"
	"github.com/alpacahq/slait/rest/client"
	"github.com/alpacahq/slait/socket"
	"github.com/gorilla/websocket"
)

type SlaitSubscriberConfig struct {
	Endpoint         string     `json:"endpoint"`
	Topic            string     `json:"topic"`
	PartitionsFilter [][]string `json:"partitions"`
	AttributeGroup   string     `json:"attribute_group"`
	Shape            [][]string `json:"shape"`
	Debug            bool     	`json:"debug"`
	PingInterval     string     `json:"ping_interval"`
}

type SlaitSubscriber struct {
	config         map[string]interface{}
	endpoint       string
	topic          string
	partitions     map[string]string
	attributeGroup string
	shape          []io.DataShape
	cli            client.SlaitClient
	conn           *websocket.Conn
	done           chan struct{}
	debug          bool
	ping_interval  time.Duration
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

	if config.PartitionsFilter == nil {
		return nil, fmt.Errorf("partitions is empty")
	}

	// if config.Debug == "" {
	// 	config.Debug = "false"
	// }

	if config.PingInterval == "" {
		config.PingInterval = "5"
	}

	interval, err := strconv.Atoi(config.PingInterval)
	if err != nil {
		return nil, fmt.Errorf("ping_interval must be a integer")
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

	partitions := make(map[string]string, len(config.PartitionsFilter))
	for _, p := range config.PartitionsFilter {
		if len(p) != 2 {
			return nil, fmt.Errorf("partition is invalid: %v", p)
		}
		partitions[p[0]] = p[1]
	}

	log.Info("Slait for debuging: %v", config.Debug)

	return &SlaitSubscriber{
		config:         conf,
		endpoint:       config.Endpoint,
		topic:          config.Topic,
		partitions:     partitions,
		attributeGroup: config.AttributeGroup,
		shape:          io.NewDataShapeVector(names, types),
		debug:          config.Debug,
		ping_interval:  time.Duration(interval),
	}, nil
}

func (ss *SlaitSubscriber) Run() {
	for {
		if err := ss.subscribe(); err != nil {
			log.Error(err.Error())
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
		log.Error("Failed to establish Slait connection.\n")
		return ss.reconnect(ss.ping_interval * time.Second)
	}

	ss.done = make(chan struct{})

	// websocket read routine
	go ss.read()
	go ss.ping()

	// subscribe to all symbols on the partition
	var ps []string
	for p := range ss.partitions {
		if len(p) > 0 {
			ps = append(ps, p)
		}
	}

	subMsg := socket.SocketMessage{
		Action:     "subscribe",
		Topic:      ss.topic,
		Partitions: ps,
	}
	err = ss.conn.WriteJSON(subMsg)
	if err != nil {
		return ss.reconnect(ss.ping_interval * time.Second)
	}

	for {
		select {
		case <-ss.done:
			return err
		case <-time.After(time.Second):
		}
	}
}

func (ss *SlaitSubscriber) reconnect(sleep time.Duration) error {
	log.Info("Reconnecting in %v..\n", sleep)
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
			log.Error("Failed to unmarshal JSON from Slait - Msg: %v - Error: %v\n", string(msg), err)
		} else {
			if val, ok := ss.partitions[p.Partition]; ok {
				if p.Entries.Len() > 0 {
					isVariableLength := val == "D"

					gap := "1Min"
					if !isVariableLength {
						gap = val
					}

					csm, err := ss.publicationToCSM(p, gap)
					if err != nil {
						return err
					}

					if ss.debug {
						log.Info("Got CSM:\n%v\n", csm)
						return err
					}

					if err := executor.WriteCSM(csm, isVariableLength); err != nil {
						return fmt.Errorf("Failed to write CSM for %v - Error: %v", p.Partition, err)
					}
				}
			}
		}
	}
	return err
}

func (ss *SlaitSubscriber) publicationToCSM(p cache.Publication, gap string) (io.ColumnSeriesMap, error) {
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
	csm := io.NewColumnSeriesMap()
	tbk := io.NewTimeBucketKey(fmt.Sprintf("%v/%v/%v", p.Partition, gap, ss.attributeGroup))
	csm.AddColumnSeries(*tbk, cs)
	return csm, nil
}

func (ss *SlaitSubscriber) read() (err error) {
	defer func() {
		ss.done <- struct{}{}
		close(ss.done)
	}()
	for {
		msgType, msg, err := ss.conn.ReadMessage()
		if err != nil {
			log.Error("Failed to read message from Slait - Error: %v\n", err)
			return err
		}

		err = ss.handleMessage(msg, msgType)
		if err != nil {
			log.Error("Failed to handle websocket message - Error: %v\n", err)
			return err
		}

	}
}

func (ss *SlaitSubscriber) ping() {
	ticker := time.NewTicker(time.Second * ss.ping_interval)
	for {
		select {
		case <-ticker.C:

			if ss.conn == nil {
				return
			}

			if err := ss.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				if !strings.Contains(err.Error(), "websocket: close sent") {
					log.Error("Failed to write ping message to WS - Error: %v", err)
				}
				return
			}
		}
	}
}

func getConfig(data string) (ret map[string]interface{}) {
	json.Unmarshal([]byte(data), &ret)
	return
}

func integrateTest() {
	testConfig := getConfig(`{
		"endpoint": "192.168.22.1:5994",
		"topic": "lobs",
		"partitions": [
			["BTC", "*"]
		],
		"attribute_group": "LOB2",
		"shape": [
			["Epoch", "int64"],
			["AP0", "float32"],
			["AV0", "float32"],
			["AP1", "float32"],
			["AV1", "float32"],
			["BP0", "float32"],
			["BV0", "float32"],
			["BP1", "float32"],
			["BV1", "float32"]
		],
		"debug": true
	}`)

	w, _ := NewBgWorker(testConfig)
	worker := w.(*SlaitSubscriber)
	worker.Run()
}

func main() {
	fmt.Println("MarketStore <-> Slait plugin")
	integrateTest()
}
