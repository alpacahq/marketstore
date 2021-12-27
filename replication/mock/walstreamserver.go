package mock

import (
	"context"
	"errors"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/alpacahq/marketstore/v4/proto"
)

type WALStreamServer struct {
	SendFunc func(resp *proto.GetWALStreamResponse) error
}

type Addr struct{}

func (Addr) Network() string {
	return "tcp"
}

func (Addr) String() string {
	return "192.0.2.1:25"
}

func (m *WALStreamServer) Send(resp *proto.GetWALStreamResponse) error {
	return m.SendFunc(resp)
}

func (m *WALStreamServer) Context() context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{Addr: Addr{}})
}

// ------------.
func (m *WALStreamServer) SetHeader(metadata.MD) error {
	return errors.New("not implemented")
}

func (m *WALStreamServer) SendHeader(metadata.MD) error {
	return errors.New("not implemented")
}
func (m *WALStreamServer) SetTrailer(metadata.MD) {}
func (m *WALStreamServer) SendMsg(msg interface{}) error {
	return errors.New("not implemented")
}

func (m *WALStreamServer) RecvMsg(msg interface{}) error {
	return errors.New("not implemented")
}

// -------------.
type ErrorWALStreamServer struct {
	WALStreamServer
}

func (m *ErrorWALStreamServer) Send(*proto.GetWALStreamResponse) error {
	return errors.New("some error")
}

// --------------.
type GetClientAddrErrorWALStreamServer struct {
	WALStreamServer
}

func (m *GetClientAddrErrorWALStreamServer) Context() context.Context {
	// clientAddr is not set in context
	return context.Background()
}
