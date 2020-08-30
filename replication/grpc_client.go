package replication

import (
	"context"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"io"
)

type GRPCReplicationClient struct {
	EnableSSL    bool
	Client       pb.ReplicationClient
	clientConn   *grpc.ClientConn
	streamClient pb.Replication_GetWALStreamClient
}

func NewGRPCReplicationClient(masterHost string, enableSSL bool) (*GRPCReplicationClient, error) {
	// TODO: implement SSL option
	conn, err := grpc.Dial(masterHost, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect Master server")
	}

	c := pb.NewReplicationClient(conn)

	return &GRPCReplicationClient{
		EnableSSL:  enableSSL,
		Client:     c,
		clientConn: conn,
	}, nil
}

func (rc GRPCReplicationClient) Connect(ctx context.Context) (pb.Replication_GetWALStreamClient, error) {
	stream, err := rc.Client.GetWALStream(ctx, &pb.GetWALStreamRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get wal message stream")
	}

	return stream, nil
}

func (rc GRPCReplicationClient) Recv() ([]byte, error) {
	wal, err := rc.streamClient.Recv()
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to get wal message from gRPC stream")
	}
	if wal == nil {
		return nil, errors.New("nil message received from gRPC stream")
	}
	return wal.Message, nil
}

func (rc GRPCReplicationClient) CloseSend() {
	rc.streamClient.CloseSend()
}

func (rc GRPCReplicationClient) Close() error {
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

//// Server-side streamingを用いてメッセージを交換する
//func (rc *GRPCReplicationClient) GetMessages(ctx context.Context, in *pb.MessagesRequest, opts ...grpc.CallOption) error {
//	stream, err := rc.Client.GetMessages(context.Background(), &pb.MessagesRequest{Id: "123",}, )
//	if err != nil {
//		return errors.Wrap(err, "failed to get wal message stream")
//	}
//
//	for {
//		// サーバからメッセージ受信
//		m, err := stream.Recv()
//		//stream.CloseSend()
//		if m != nil {
//			log.Debug("Receive message>> [%s] %s", m.Name, m.Content)
//		}
//		// EOF、エラーなら終了
//		if err == io.EOF {
//			// EOFなら終了
//			log.Info(fmt.Sprintf("stream received:%v", err))
//			break
//		}
//		if err != nil {
//			log.Error(err.Error())
//			break
//		}
//		time.Sleep(1 * time.Second)
//	}
//
//	return nil
//}

//func (rc *GRPCReplicationClient) Close() error {
//	return rc.clientConn.Close()
//}
