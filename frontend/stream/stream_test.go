package stream_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
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
)

func setup(t *testing.T) {
	t.Helper()

	rootDir := t.TempDir()
	_, _, err := executor.NewInstanceSetup(rootDir, nil, nil, 5, executor.BackgroundSync(false))
	assert.Nil(t, err)
	stream.Initialize()
}

func TestStream(t *testing.T) {
	setup(t)

	srv := httptest.NewServer(http.HandlerFunc(stream.Handler))

	u, _ := url.Parse(srv.URL + "/ws")
	u.Scheme = "ws"

	conn, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
	defer func(conn *websocket.Conn) {
		err1 := resp.Body.Close()
		if err2 := conn.Close(); err1 != nil || err2 != nil {
			log.Error("failed to close websocket connection")
		}
	}(conn)
	assert.Nil(t, err)

	// AAPL 5Min bars & all daily bars
	streamKeys := []string{"AAPL/5Min/OHLCV", "*/1D/OHLCV"}

	streamCount := map[string]int{}
	for _, key := range streamKeys {
		streamCount[key] = 0
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
	go readRoutine(conn, bufC)

	// write data
	for i := 0; i < 2; i++ {
		tbk := io.NewTimeBucketKey("AAPL/5Min/OHLCV")
		err = stream.Push(*tbk, genColumns())
		assert.Nil(t, err)
	}

	tbk := io.NewTimeBucketKey("NVDA/1D/OHLCV")
	err = stream.Push(*tbk, genColumns())
	assert.Nil(t, err)

	total := 3 // "AAPL/5Min/OHLCV"=2, "NVDA/1D/OHLCV"=1
	count := 0

	timer := time.NewTimer(5 * time.Second)

	var receivedBufs [][]byte
	for {
		finished := false
		select {
		case buf, ok := <-bufC:
			if ok {
				receivedBufs = append(receivedBufs, buf)
				count++
				if count == total {
					conn.Close()
				}
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
	handlePayload(t, receivedBufs, map[string]int{"AAPL/5Min/OHLCV": 2, "*/1D/OHLCV": 1})
}

func readRoutine(conn *websocket.Conn, bufC chan []byte) {
	// read routine (handled in client code normally)
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

func handlePayload(t *testing.T, bufs [][]byte, expectedStreamKeyCount map[string]int) {
	t.Helper()

	streamCount := make(map[string]int)
	for streamKey := range expectedStreamKeyCount {
		streamCount[streamKey] = 0
	}

	for _, buf := range bufs {
		var payload *stream.Payload
		err2 := msgpack.Unmarshal(buf, &payload)
		assert.Nil(t, err2)

		payload.Key = strings.Replace(payload.Key, "NVDA", "*", 1)
		if count, ok := streamCount[payload.Key]; !ok {
			t.Fatalf("invalid stream key in payload: %v", *payload)
		} else {
			streamCount[payload.Key] = count + 1
		}
	}

	for streamKey := range expectedStreamKeyCount {
		count, ok := streamCount[streamKey]
		assert.True(t, ok)
		assert.Equal(t, expectedStreamKeyCount[streamKey], count)
	}
}
