package mock

import (
	"context"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"google.golang.org/grpc"
)

type ReplicationClient struct {
	StreamClient pb.Replication_GetWALStreamClient
	Error error
}

func (rc ReplicationClient) GetWALStream(_ context.Context, in *pb.GetWALStreamRequest, opts ...grpc.CallOption) (pb.Replication_GetWALStreamClient, error) {
	return rc.StreamClient, rc.Error
}
