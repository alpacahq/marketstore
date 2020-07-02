package replication

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/utils/log"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"io"
	"net"
	"time"
)

type GRPCReplicationServer struct {
	CertFile    string
	CertKeyFile string
	grpcServer  *grpc.Server
	// Key: IPAddr (e.g. "192.125.18.1:25"), Value: channel for messages sent to each gRPC stream
	StreamChannels map[string]chan []byte
}

func NewGRPCReplicationService(port int) (*GRPCReplicationServer, error) {
	grpcServer := grpc.NewServer()
	r := GRPCReplicationServer{
		grpcServer:     grpcServer,
		StreamChannels: map[string]chan []byte{},
	}

	pb.RegisterReplicationServer(grpcServer, &r)

	// start gRPC connection
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen a port for replication")
	}
	go func() {
		log.Info("starting GRPC server for replication...")
		if err := grpcServer.Serve(lis); err != nil {
			log.Error(fmt.Sprintf("failed to serve replication service:%v", err))
		}
	}()

	return &r, nil
}

// チャットルームの新着メッセージをstreamを使い配信する
func (rs *GRPCReplicationServer) GetMessages(p *pb.MessagesRequest, stream pb.Replication_GetMessagesServer) error {
	// prepare a channel to send messages
	ctx := stream.Context()
	var clientAddr string
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return errors.New()
	}

	clientAddr = pr.Addr.String()
	rs.StreamChannels[clientAddr] = make(chan []byte)

	// 無限ループ
	for {
		// クライアントへメッセージ送信
		if err := stream.Send(&pb.Message{Id: "fff", Name: "a", Content: "sss"}); err != nil {
			return err
		}
		println("fffff")
		time.Sleep(3 * time.Second)
	}
}

// チャットルームへstreamを使いメッセージを送信する
func (rs *GRPCReplicationServer) Send(*pb.WALMessage) error {

}

func (rs *GRPCReplicationServer) GetWALStream(req *pb.GetWALStreamRequest, stream pb.Replication_GetWALStreamServer) error {
	// 無限ループ
	for {
		// クライアントへメッセージ送信
		//if err := stream.Send(&pb.Message{Id: "fff", Name: "a", Content: "sss"}); err != nil {
		if err := stream.Send(&pb.WALMessage{Message: []byte{123}}); err != nil {
			return err
		}
		println("fffff")
		time.Sleep(3 * time.Second)
	}
}

func (rs *GRPCReplicationServer) SendMessage(stream pb.Replication_SendMessageServer) error {
	// 無限ループ
	for {
		// クライアントからメッセージ受信
		m, err := stream.Recv()
		log.Debug("Receive message>> [%s] %s", m.Name, m.Content)
		// EOF、エラーなら終了
		if err == io.EOF {
			// EOFなら接続終了処理
			return stream.SendAndClose(&pb.SendResult{
				Result: true,
			})
		}
		if err != nil {
			return err
		}
		// 終了コマンド
		if m.Content == "/exit" {
			return stream.SendAndClose(&pb.SendResult{
				Result: true,
			})
		}
		time.Sleep(5 * time.Second)
	}
}
