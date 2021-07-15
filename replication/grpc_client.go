package replication

import (
	"context"
	"io"

	"github.com/pkg/errors"

	pb "github.com/alpacahq/marketstore/v4/proto"
)

type GRPCReplicationClient struct {
	Client       pb.ReplicationClient
	streamClient pb.Replication_GetWALStreamClient
}

func NewGRPCReplicationClient(client pb.ReplicationClient) *GRPCReplicationClient {
	return &GRPCReplicationClient{
		Client: client,
	}
}

func (rc *GRPCReplicationClient) Connect(ctx context.Context) error {
	stream, err := rc.Client.GetWALStream(ctx, &pb.GetWALStreamRequest{})
	if err != nil {
		return errors.Wrap(err, "failed to get wal message stream")
	}

	rc.streamClient = stream

	return nil
}

// Recv blocks until it receives a response from gRPC stream connection.
func (rc *GRPCReplicationClient) Recv() ([]byte, error) {
	if rc.streamClient == nil {
		return nil, errors.New("no stream connection to master")
	}

	// streamClient.Recv() blocks the thread until receive a new message
	resp, err := rc.streamClient.Recv()
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to get a message from gRPC stream")
	}
	if resp == nil {
		return nil, errors.New("nil message received from gRPC stream")
	}
	return resp.TransactionGroup, nil
}
