package stream_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

func setup(t *testing.T, testName string,
) (tearDown func()) {
	t.Helper()

	rootDir, _ := os.MkdirTemp("", fmt.Sprintf("stream_test-%s", testName))
	_, _, _, err := executor.NewInstanceSetup(rootDir, nil, nil, 5, executor.BackgroundSync(false))
	assert.Nil(t, err)
	stream.Initialize()

	return func() { test.CleanupDummyDataDir(rootDir) }
}

func TestStream(t *testing.T) {
	tearDown := setup(t, "TestStream")
	defer tearDown()

	srv := httptest.NewServer(http.HandlerFunc(stream.Handler))

	u, _ := url.Parse(srv.URL + "/ws")
	u.Scheme = "ws"

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	defer conn.Close()
	assert.Nil(t, err)

	// AAPL 5Min bars & all daily bars
	streamKeys := []string{"AAPL/5Min/OHLCV", "*/1D/OHLCV"}

	streamCount := map[string]int{}
	for _, key := range streamKeys {
		streamCount[key] = 0
	}

	handler := func(buf []byte) error {
		var payload *stream.Payload
		err2 := msgpack.Unmarshal(buf, &payload)
		assert.Nil(t, err2)

		payload.Key = strings.Replace(payload.Key, "NVDA", "*", 1)
		if count, ok := streamCount[payload.Key]; !ok {
			t.Fatalf("invalid stream key in payload: %v", *payload)
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

	buf, err := msgpack.Marshal(stream.SubscribeMessage{Streams: streamKeys})
	assert.Nil(t, err)

	assert.Nil(t, conn.WriteMessage(websocket.BinaryMessage, buf))

	_, buf, err = conn.ReadMessage()
	assert.Nil(t, err)

	subRespMsg := &stream.SubscribeMessage{}
	err = msgpack.Unmarshal(buf, subRespMsg)
	assert.Nil(t, err)

	assert.Equal(t, len(subRespMsg.Streams), len(streamKeys))

	bufC := make(chan []byte, 1)

	// read routine (handled in client code normally)
	go func() {
		for {
			msgType, buf, err := conn.ReadMessage()
			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					log.Error("unexpected websocket closure (%v)", err)
				}
				close(bufC)
				return
			}

			switch msgType {
			case websocket.TextMessage, websocket.BinaryMessage:
				bufC <- buf
			case websocket.CloseMessage:
				return
			}
		}
	}()

	// write data
	for i := 0; i < 2; i++ {
		tbk := io.NewTimeBucketKey("AAPL/5Min/OHLCV")
		stream.Push(*tbk, genColumns())
	}

	tbk := io.NewTimeBucketKey("NVDA/1D/OHLCV")
	stream.Push(*tbk, genColumns())

	timer := time.NewTimer(5 * time.Second)

	for {
		finished := false
		select {
		case buf, ok := <-bufC:
			if ok {
				assert.Nil(t, handler(buf))
			} else {
				finished = true
			}
		case <-timer.C:
			t.Fatalf("test timed out [%v]", streamCount)
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
