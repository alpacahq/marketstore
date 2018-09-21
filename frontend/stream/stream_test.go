package stream

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/log"
	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type StreamTestSuite struct{}

var _ = Suite(&StreamTestSuite{})

func (s *StreamTestSuite) SetUpSuite(c *C) {
	root := c.MkDir()
	executor.NewInstanceSetup(root, true, true, false, false)

	Initialize()
}

func (s *StreamTestSuite) TestStream(c *C) {
	srv := httptest.NewServer(http.HandlerFunc(Handler))

	u, _ := url.Parse(srv.URL + "/ws")
	u.Scheme = "ws"

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	c.Assert(err, IsNil)

	// AAPL 5Min bars & all daily bars
	streamKeys := []string{"AAPL/5Min/OHLCV", "*/1D/OHLCV"}

	streamCount := map[string]int{}
	for _, key := range streamKeys {
		streamCount[key] = 0
	}

	handler := func(buf []byte) error {
		var payload *Payload
		c.Assert(msgpack.Unmarshal(buf, &payload), IsNil)

		payload.Key = strings.Replace(payload.Key, "NVDA", "*", 1)
		if count, ok := streamCount[payload.Key]; !ok {
			c.Fatalf("invalid stream key in payload: %v", *payload)
		} else {
			streamCount[payload.Key] = count + 1
		}

		if count1, ok := streamCount[streamKeys[0]]; ok {
			if count2, ok := streamCount[streamKeys[1]]; ok {
				if count1 == 2 && count2 == 1 {
					conn.Close()
				}
			}
		}

		return nil
	}

	buf, err := msgpack.Marshal(SubscribeMessage{Streams: streamKeys})
	c.Assert(err, IsNil)

	c.Assert(conn.WriteMessage(websocket.BinaryMessage, buf), IsNil)

	_, buf, err = conn.ReadMessage()

	subRespMsg := &SubscribeMessage{}
	c.Assert(msgpack.Unmarshal(buf, subRespMsg), IsNil)

	c.Assert(len(subRespMsg.Streams), Equals, len(streamKeys))

	bufC := make(chan []byte, 1)

	// read routine (handled in client code normally)
	go func() {
		for {
			msgType, buf, err := conn.ReadMessage()

			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					Log(ERROR, "unexpected websocket closure (%v)", err)
				}
				close(bufC)
				return
			}

			switch msgType {
			case websocket.TextMessage:
				fallthrough
			case websocket.BinaryMessage:
				bufC <- buf
			case websocket.CloseMessage:
				return
			}
		}
	}()

	// write data
	for i := 0; i < 2; i++ {
		tbk := io.NewTimeBucketKey("AAPL/5Min/OHLCV")
		Push(*tbk, genColumns())
	}

	tbk := io.NewTimeBucketKey("NVDA/1D/OHLCV")
	Push(*tbk, genColumns())

	timer := time.NewTimer(5 * time.Second)

	for {
		finished := false
		select {
		case buf, ok := <-bufC:
			if ok {
				c.Assert(handler(buf), IsNil)
			} else {
				finished = true
			}
		case <-timer.C:
			c.Fatalf("test timed out [%v]", streamCount)
		}
		if finished {
			break
		}
	}
}

func genColumns() map[string]interface{} {
	return map[string]interface{}{
		"Open":   float32(1.0),
		"High":   float32(2.0),
		"Low":    float32(0.5),
		"Close":  float32(1.5),
		"Volume": int32(10),
		"Epoch":  int64(123456789),
	}
}
