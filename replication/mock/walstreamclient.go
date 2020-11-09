package mock

import (
	"context"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"google.golang.org/grpc/metadata"
)

type WALStreamClient struct {
	Response *pb.GetWALStreamResponse
	Error    error
}

func (wsc *WALStreamClient) Recv() (*pb.GetWALStreamResponse, error) { return wsc.Response, wsc.Error }
func (wsc *WALStreamClient) Header() (metadata.MD, error)            { return nil, nil }
func (wsc *WALStreamClient) Trailer() metadata.MD                    { return nil }
func (wsc *WALStreamClient) CloseSend() error                        { return nil }
func (wsc *WALStreamClient) Context() context.Context                { return nil }
func (wsc *WALStreamClient) SendMsg(m interface{}) error             { return nil }
func (wsc *WALStreamClient) RecvMsg(m interface{}) error             { return nil }
