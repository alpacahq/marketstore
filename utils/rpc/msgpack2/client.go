// This is a copy from gorilla's jsonrpc2 using msgpack
//
// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package msgpack2

import (
	"io"
	"math/rand"

	msgpack "github.com/vmihailenco/msgpack"
)

// ----------------------------------------------------------------------------
// Request and Response
// ----------------------------------------------------------------------------

// clientRequest represents a JSON-RPC request sent by a client.
type clientRequest struct {
	// JSON-RPC protocol.
	Version string `msgpack:"jsonrpc"`

	// A String containing the name of the method to be invoked.
	Method string `msgpack:"method"`

	// Object to pass as request parameter to the method.
	Params interface{} `msgpack:"params"`

	// The request id. This can be of any type. It is used to match the
	// response with the request that it is replying to.
	ID uint64 `msgpack:"id"`
}

// clientResponse represents a JSON-RPC response returned to a client.
type clientResponse struct {
	Version string      `msgpack:"jsonrpc"`
	Result  interface{} `msgpack:"result"`
	Error   interface{} `msgpack:"error"`
}

// EncodeClientRequest encodes parameters for a JSON-RPC client request.
func EncodeClientRequest(method string, args interface{}) ([]byte, error) {
	c := &clientRequest{
		Version: "2.0",
		Method:  method,
		Params:  args,
		ID:      uint64(rand.Int63()),
	}
	return msgpack.Marshal(c)
}

// DecodeClientResponse decodes the response body of a client request into
// the interface reply.
func DecodeClientResponse(r io.Reader, reply interface{}) error {
	var c clientResponse
	if err := msgpack.NewDecoder(r).Decode(&c); err != nil {
		return err
	}
	if c.Error != nil {
		msgErr := &Error{}
		encoded, err := msgpack.Marshal(c.Error)
		if err != nil {
			return err
		}
		if err = msgpack.Unmarshal(encoded, msgErr); err != nil {
			return &Error{
				Code:    ErrServer,
				Message: string(encoded),
			}
		}
		return msgErr
	}

	if c.Result == nil {
		return ErrNullResult
	}

	encoded, err := msgpack.Marshal(c.Result)
	if err != nil {
		return err
	}
	return msgpack.Unmarshal(encoded, reply)
}
