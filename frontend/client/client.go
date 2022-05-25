package client

import (
	"bytes"
	"context"
	"fmt"
	goio "io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/utils/rpc/msgpack2"
)

const (
	streamSubscribeTimeout = 10 * time.Second
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

func decodeMultiGetInfoResponse(resp *http.Response) (response interface{}, err error) {
	result := &frontend.MultiGetInfoResponse{}
	if err = msgpack2.DecodeClientResponse(resp.Body, result); err != nil {
		return nil, err
	}
	return result, nil
}

func decodeMultiServerResponse(resp *http.Response) (response interface{}, err error) {
	result := &frontend.MultiServerResponse{}
	if err = msgpack2.DecodeClientResponse(resp.Body, result); err != nil {
		return nil, err
	}
	return result, nil
}

func decodeMultiQueryResponse(resp *http.Response) (response interface{}, err error) {
	result := &frontend.MultiQueryResponse{}
	err = msgpack2.DecodeClientResponse(resp.Body, result)
	if err != nil {
		return nil, err
	}

	return result.ToColumnSeriesMap()
}

func decodeListSymbols(resp *http.Response) (response interface{}, err error) {
	result := &frontend.ListSymbolsResponse{}
	err = msgpack2.DecodeClientResponse(resp.Body, result)
	if err != nil {
		return nil, fmt.Errorf("decode ListSymbols API client response:%w", err)
	}
	return result.Results, nil
}

var decodeFuncMap = map[string]func(resp *http.Response) (response interface{}, err error){
	"GetInfo":      decodeMultiGetInfoResponse,
	"Create":       decodeMultiServerResponse,
	"Destroy":      decodeMultiServerResponse,
	"Query":        decodeMultiQueryResponse,
	"SQLStatement": decodeMultiQueryResponse,
	"ListSymbols":  decodeListSymbols,
	"Write": func(resp *http.Response) (response interface{}, err error) {
		_, err = decodeMultiServerResponse(resp)
		if err != nil {
			return nil, fmt.Errorf("decode Write API client response:%w", err)
		}
		return nil, nil
	},
}

// DoRPC makes an RPC request to MarketStore's API.
func (cl *Client) DoRPC(functionName string, args interface{}) (response interface{}, err error) {
	/*
		Does a remote procedure call using the msgpack2 protocol for RPC that return a QueryReply
	*/
	if args == nil {
		return nil, fmt.Errorf("args must be non-nil - have: args: %v", args)
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
	defer func(resp *http.Response) {
		if err2 := resp.Body.Close(); err2 != nil {
			log.Error(fmt.Sprintf("failed to close http client for marketstore api. err=%v", err2))
		}
	}(resp)

	// Handle any error in the RPC call
	const statusOK = 200
	if resp.StatusCode != statusOK {
		bodyBytes, err2 := goio.ReadAll(resp.Body)
		var errText string
		if err2 != nil {
			errText = err2.Error()
		} else if bodyBytes != nil {
			errText = string(bodyBytes)
		}
		return nil, fmt.Errorf("response error (%d): %s", resp.StatusCode, errText)
	}

	// Unpack and format the response from the RPC call
	decodeFunc, found := decodeFuncMap[functionName]
	if !found {
		return nil, fmt.Errorf("unsupported RPC response")
	}
	return decodeFunc(resp)
}

// Subscribe to the marketstore websocket interface with a
// message handler, a set of streams and cancel channel.
func (cl *Client) Subscribe(
	handler func(pl stream.Payload) error,
	cancel <-chan struct{},
	streams ...string,
) (done <-chan struct{}, err error) {
	u, _ := url.Parse(cl.BaseURL + "/ws")
	u.Scheme = "ws"

	conn, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
	defer func(Body goio.ReadCloser) {
		if err2 := Body.Close(); err2 != nil {
			log.Error("failed to close websocket response body:" + err2.Error())
		}
	}(resp.Body)
	if err != nil {
		return nil, err
	}

	buf, err := msgpack.Marshal(stream.SubscribeMessage{Streams: streams})
	if err != nil {
		return nil, err
	}

	if err2 := conn.WriteMessage(websocket.BinaryMessage, buf); err2 != nil {
		return nil, err2
	}

	select {
	case buf = <-read(conn, make(chan struct{}), 1):
		// make sure subscription succeeded
		subRespMsg := &stream.SubscribeMessage{}
		if err = msgpack.Unmarshal(buf, subRespMsg); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("marketstore stream subscribe failed:%w", err)
		}
		if !streamsEqual(streams, subRespMsg.Streams) {
			_ = conn.Close()
			return nil, fmt.Errorf("marketstore stream subscribe failed")
		}
	case <-time.After(streamSubscribeTimeout):
		// timeout
		_ = conn.Close()
		return nil, fmt.Errorf("marketstore stream subscribe timed out")
	}

	return streamConn(conn, handler, cancel), nil
}

func streamConn(
	c *websocket.Conn,
	handler func(pl stream.Payload) error,
	cancel <-chan struct{},
) <-chan struct{} {
	done := make(chan struct{}, 1)

	go func() {
		defer func(c *websocket.Conn) {
			if err := c.Close(); err != nil {
				log.Error(fmt.Sprintf("failed to close websocket connection. err=%v", err))
			}
		}(c)
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
				_ = c.WriteMessage(websocket.PongMessage, []byte{})
			case websocket.PongMessage:
				_ = c.WriteMessage(websocket.PingMessage, []byte{})
			case websocket.TextMessage, websocket.BinaryMessage:
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
