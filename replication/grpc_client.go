package replication

import (
	"context"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"io"
)

type GRPCReplicationClient struct {
	Client       pb.ReplicationClient
	clientConn   *grpc.ClientConn
	streamClient pb.Replication_GetWALStreamClient
}

func NewGRPCReplicationClient(conn *grpc.ClientConn) (*GRPCReplicationClient, error) {
	c := pb.NewReplicationClient(conn)

	return &GRPCReplicationClient{
		Client:     c,
		clientConn: conn,
	}, nil
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

	// the following line blocks until receive a new message
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

// Close closes gRPC stream client and its connection.
func (rc *GRPCReplicationClient) Close() error {
	err := rc.streamClient.CloseSend()
	if err != nil {
		return errors.Wrap(err, "failed to close gRPC stream connection")
	}

	err = rc.clientConn.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close gRPC connection")
	}

	return nil
}
