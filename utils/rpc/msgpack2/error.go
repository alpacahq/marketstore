// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package msgpack2

import (
	"errors"
)

type ErrorCode int

const (
	ErrParse      ErrorCode = -32700
	ErrInvalidReq ErrorCode = -32600
	ErrNoMethod   ErrorCode = -32601
	ErrBadParams  ErrorCode = -32602
	ErrInternal   ErrorCode = -32603
	ErrServer     ErrorCode = -32000
)

var ErrNullResult = errors.New("result is null")

type Error struct {
	// A Number that indicates the error type that occurred.
	Code ErrorCode `msgpack:"code"` /* required */

	// A String providing a short description of the error.
	// The message SHOULD be limited to a concise single sentence.
	Message string `msgpack:"message"` /* required */

	// A Primitive or Structured value that contains additional information about the error.
	Data interface{} `msgpack:"data"` /* optional */
}

func (e *Error) Error() string {
	return e.Message
}
