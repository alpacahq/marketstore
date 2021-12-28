// This is a copy from gorilla's jsonrpc2 using msgpack
//
// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package msgpack2_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/alpacahq/marketstore/v4/utils/rpc/msgpack2"

	rpc "github.com/alpacahq/rpc/rpc2"
	msgpack "github.com/vmihailenco/msgpack"
)

// ResponseRecorder is an implementation of http.ResponseWriter that
// records its mutations for later inspection in tests.
type ResponseRecorder struct {
	Code      int           // the HTTP response code from WriteHeader
	HeaderMap http.Header   // the HTTP response headers
	Body      *bytes.Buffer // if non-nil, the bytes.Buffer to append written data to
	Flushed   bool
}

// NewRecorder returns an initialized ResponseRecorder.
func NewRecorder() *ResponseRecorder {
	return &ResponseRecorder{
		HeaderMap: make(http.Header),
		Body:      new(bytes.Buffer),
	}
}

// DefaultRemoteAddr is the default remote address to return in RemoteAddr if
// an explicit DefaultRemoteAddr isn't set on ResponseRecorder.
const DefaultRemoteAddr = "1.2.3.4"

// Header returns the response headers.
func (rw *ResponseRecorder) Header() http.Header {
	return rw.HeaderMap
}

// Write always succeeds and writes to rw.Body, if not nil.
func (rw *ResponseRecorder) Write(buf []byte) (int, error) {
	if rw.Body != nil {
		rw.Body.Write(buf)
	}
	if rw.Code == 0 {
		rw.Code = http.StatusOK
	}
	return len(buf), nil
}

// WriteHeader sets rw.Code.
func (rw *ResponseRecorder) WriteHeader(code int) {
	rw.Code = code
}

// Flush sets rw.Flushed to true.
func (rw *ResponseRecorder) Flush() {
	rw.Flushed = true
}

// ----------------------------------------------------------------------------

var ErrResponseError = errors.New("response error")

type Service1Request struct {
	A int
	B int
}

type Service1NoParamsRequest struct {
	V  string `msgpack:"jsonrpc"`
	M  string `msgpack:"method"`
	ID uint64 `msgpack:"id"`
}

type Service1ParamsArrayRequest struct {
	V string `msgpack:"jsonrpc"`
	P []struct {
		T string
	} `msgpack:"params"`
	M  string `msgpack:"method"`
	ID uint64 `msgpack:"id"`
}

type Service1Response struct {
	Result int
}

type Service1 struct{}

const Service1DefaultResponse = 9999

func (t *Service1) Multiply(r *http.Request, req *Service1Request, res *Service1Response) error {
	if req.A == 0 && req.B == 0 {
		// Sentinel value for test with no params.
		res.Result = Service1DefaultResponse
	} else {
		res.Result = req.A * req.B
	}
	return nil
}

func (t *Service1) ResponseError(r *http.Request, req *Service1Request, res *Service1Response) error {
	return ErrResponseError
}

func execute(t *testing.T, s *rpc.Server, method string, req, res interface{}) error {
	t.Helper()

	if !s.HasMethod(method) {
		t.Fatal("Expected to be registered:", method)
	}

	buf, _ := msgpack2.EncodeClientRequest(method, req)
	body := bytes.NewBuffer(buf)
	r, _ := http.NewRequest("POST", "http://localhost:8080/", body)
	r.Header.Set("Content-Type", "application/x-msgpack")

	w := NewRecorder()
	s.ServeHTTP(w, r)

	return msgpack2.DecodeClientResponse(w.Body, res)
}

func executeRaw(t *testing.T, s *rpc.Server, req, res interface{}) error {
	j, _ := msgpack.Marshal(req)
	r, _ := http.NewRequest("POST", "http://localhost:8080/", bytes.NewBuffer(j))
	r.Header.Set("Content-Type", "application/x-msgpack")

	w := NewRecorder()
	s.ServeHTTP(w, r)

	return msgpack2.DecodeClientResponse(w.Body, res)
}

func TestService(t *testing.T) {
	t.Parallel()
	s := rpc.NewServer()
	s.RegisterCodec(msgpack2.NewCodec(), "application/x-msgpack")
	s.RegisterService(new(Service1), "")

	var res Service1Response
	if err := execute(t, s, "Service1.Multiply", &Service1Request{4, 2}, &res); err != nil {
		t.Error("Expected err to be nil, but got:", err)
	}
	if res.Result != 8 {
		t.Errorf("Wrong response: %v.", res.Result)
	}

	if err := execute(t, s, "Service1.ResponseError", &Service1Request{4, 2}, &res); err == nil {
		t.Errorf("Expected to get %q, but got nil", ErrResponseError)
	} else if err.Error() != ErrResponseError.Error() {
		t.Errorf("Expected to get %q, but got %q", ErrResponseError, err)
	}

	// No parameters.
	res = Service1Response{}
	if err := executeRaw(t, s, &Service1NoParamsRequest{"2.0", "Service1.Multiply", 1}, &res); err != nil {
		t.Error(err)
	}
	if res.Result != Service1DefaultResponse {
		t.Errorf("Wrong response: got %v, want %v", res.Result, Service1DefaultResponse)
	}

	// Parameters as by-position.
	res = Service1Response{}
	req := Service1ParamsArrayRequest{
		V: "2.0",
		P: []struct {
			T string
		}{{
			T: "test",
		}},
		M:  "Service1.Multiply",
		ID: 1,
	}
	if err := executeRaw(t, s, &req, &res); err != nil {
		t.Error(err)
	}
	if res.Result != Service1DefaultResponse {
		t.Errorf("Wrong response: got %v, want %v", res.Result, Service1DefaultResponse)
	}
}

func TestDecodeNullResult(t *testing.T) {
	t.Parallel()
	data := []byte(`{"jsonrpc": "2.0", "id": 12345, "result": null}`)
	var obj interface{}
	json.Unmarshal(data, &obj)
	data, _ = msgpack.Marshal(obj)
	reader := bytes.NewReader(data)
	var result interface{}

	err := msgpack2.DecodeClientResponse(reader, &result)

	if err != msgpack2.ErrNullResult {
		t.Error("Expected err no be ErrNullResult, but got:", err)
	}

	if result != nil {
		t.Error("Expected result to be nil, but got:", result)
	}
}
