package client

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/utils/rpc/msgpack2"
)

type Client struct {
	BaseURL string
}

// NewClient intializes a new MarketStore RPC client.
func NewClient(baseurl string) (cl *Client, err error) {
	cl = new(Client)
	_, err = url.Parse(baseurl)
	if err != nil {
		return nil, err
	}
	cl.BaseURL = baseurl
	return cl, nil
}

// DoRPC makes an RPC request to MarketStore's API.
func (cl *Client) DoRPC(functionName string, args interface{}) (response interface{}, err error) {
	/*
		Does a remote procedure call using the msgpack2 protocol for RPC that return a QueryReply
	*/
	if args == nil {
		return nil, fmt.Errorf("args must be non-nil - have: args: %v\n",
			args)
	}
	message, err := msgpack2.EncodeClientRequest("DataService."+functionName, args)
	if err != nil {
		return nil, err
	}
	reqURL := cl.BaseURL + "/rpc"
	req, err := http.NewRequestWithContext(context.Background(), "POST", reqURL, bytes.NewBuffer(message))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-msgpack")
	client := new(http.Client)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Handle any error in the RPC call
	if resp.StatusCode != 200 {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		var errText string
		if err != nil {
			errText = err.Error()
		} else {
			if bodyBytes != nil {
				errText = string(bodyBytes)
			}
		}
		return nil, fmt.Errorf("response error (%d): %s", resp.StatusCode, errText)
	}

	// Unpack and format the response from the RPC call
	switch functionName {
	case "GetInfo":
		result := &frontend.MultiGetInfoResponse{}
		err = msgpack2.DecodeClientResponse(resp.Body, result)
		if err != nil {
			return nil, err
		}
		return result, nil

	case "Create", "Destroy":
		result := &frontend.MultiServerResponse{}
		err = msgpack2.DecodeClientResponse(resp.Body, result)
		if err != nil {
			return nil, err
		}
		return result, nil

	case "Query", "SQLStatement":
		result := &frontend.MultiQueryResponse{}
		err = msgpack2.DecodeClientResponse(resp.Body, result)
		if err != nil {
			return nil, err
		}

		return result.ToColumnSeriesMap()
	case "ListSymbols":
		result := &frontend.ListSymbolsResponse{}
		err = msgpack2.DecodeClientResponse(resp.Body, result)
		return result.Results, nil
	case "Write":
		result := &frontend.MultiServerResponse{}
		err = msgpack2.DecodeClientResponse(resp.Body, result)

	default:
		return nil, fmt.Errorf("unsupported RPC response")
	}

	return nil, nil
}

func ColumnSeriesFromResult(shapes []io.DataShape, columns map[string]interface{}) (cs *io.ColumnSeries, err error) {
	cs = io.NewColumnSeries()
	for _, shape := range shapes {
		name := shape.Name
		typ := shape.Type
		base := columns[name].([]interface{})

		if base == nil {
			return nil, fmt.Errorf("unable to unpack %s", name)
		}

		iCol, err := io.CreateSliceFromSliceOfInterface(base, typ)
		if err != nil {
			return nil, err
		}
		cs.AddColumn(name, iCol)
	}
	return cs, nil
}

// Subscribe to the marketstore websocket interface with a
// message handler, a set of streams and cancel channel.
func (cl *Client) Subscribe(
	handler func(pl stream.Payload) error,
	cancel <-chan struct{},
	streams ...string) (done <-chan struct{}, err error) {
	u, _ := url.Parse(cl.BaseURL + "/ws")
	u.Scheme = "ws"

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}

	buf, err := msgpack.Marshal(stream.SubscribeMessage{Streams: streams})
	if err != nil {
		return nil, err
	}

	if err := conn.WriteMessage(websocket.BinaryMessage, buf); err != nil {
		return nil, err
	}

	select {
	case buf = <-read(conn, make(chan struct{}), 1):
		// make sure subscription succeeded
		subRespMsg := &stream.SubscribeMessage{}
		if err = msgpack.Unmarshal(buf, subRespMsg); err != nil {
			conn.Close()
			return nil, fmt.Errorf("marketstore stream subscribe failed (%s)", err)
		}
		if !streamsEqual(streams, subRespMsg.Streams) {
			conn.Close()
			return nil, fmt.Errorf("marketstore stream subscribe failed")
		}
	case <-time.After(10 * time.Second):
		// timeout
		conn.Close()
		return nil, fmt.Errorf("marketstore stream subscribe timed out")
	}

	return streamConn(conn, handler, cancel), nil
}

func streamConn(
	c *websocket.Conn,
	handler func(pl stream.Payload) error,
	cancel <-chan struct{}) <-chan struct{} {
	done := make(chan struct{}, 1)

	go func() {
		defer c.Close()
		bufC := read(c, done, -1)

		for {
			finished := false

			select {
			case buf, ok := <-bufC:
				if ok {
					pl := stream.Payload{}

					// convert to payload
					if err := msgpack.Unmarshal(buf, &pl); err != nil {
						log.Error("error unmarshaling stream message (%v)", err)
						continue
					}

					// handle payload
					if err := handler(pl); err != nil {
						log.Error("error handling stream message (%v)", err)
						continue
					}
				} else {
					finished = true
				}
			case <-cancel:
				finished = true
			}
			if finished {
				break
			}
		}
	}()

	return done
}

func read(c *websocket.Conn, done chan struct{}, count int) chan []byte {
	bufC := make(chan []byte, 1)
	msgsRead := 0
	go func() {
		defer close(bufC)
		for {
			msgType, buf, err := c.ReadMessage()
			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					log.Error("unexpected websocket closure (%v)", err)
				}
				done <- struct{}{}
				return
			}

			switch msgType {
			case websocket.PingMessage:
				err = c.WriteMessage(websocket.PongMessage, []byte{})
			case websocket.PongMessage:
				err = c.WriteMessage(websocket.PingMessage, []byte{})
			case websocket.TextMessage:
				fallthrough
			case websocket.BinaryMessage:
				bufC <- buf
			case websocket.CloseMessage:
				return
			}

			msgsRead++
			if count > 0 && msgsRead >= count {
				break
			}
		}
	}()

	return bufC
}

func streamsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if !strings.EqualFold(v, b[i]) {
			return false
		}
	}
	return true
}
