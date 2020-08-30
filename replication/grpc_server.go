package replication

import (
	"fmt"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"net"
)

const (
	defaultReplicationStreamChannelSize = 500
)

type GRPCReplicationServer struct {
	CertFile    string
	CertKeyFile string
	grpcServer  *grpc.Server
	// Key: IPAddr (e.g. "192.125.18.1:25"), Value: channel for messages sent to each gRPC stream
	StreamChannels map[string]chan []byte
}

func NewGRPCReplicationService(grpcServer *grpc.Server, port int) (*GRPCReplicationServer, error) {
	r := GRPCReplicationServer{
		grpcServer:     grpcServer,
		StreamChannels: map[string]chan []byte{},
	}

	pb.RegisterReplicationServer(grpcServer, &r)

	// start gRPC server
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

func getClientAddr(stream grpc.ServerStream) (string, error) {
	ctx := stream.Context()

	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("failed to get client IP address.")
	}
	return pr.Addr.String(), nil
}

func (rs *GRPCReplicationServer) GetWALStream(req *pb.GetWALStreamRequest, stream pb.Replication_GetWALStreamServer) error {
	// prepare a channel to send messages
	clientAddr, err := getClientAddr(stream)
	if err != nil {
		return errors.New("failed to get client IP address.")
	}
	log.Info(fmt.Sprintf("new replica connection from:%s", clientAddr))

	streamChannel := make(chan []byte, defaultReplicationStreamChannelSize)
	rs.StreamChannels[clientAddr] = streamChannel

	// infinite loop
	for {
		log.Debug("[master] waiting for write requests...")
		serializedTransactionGroup := <-streamChannel
		if serializedTransactionGroup == nil {
			log.Info("streamChannel for replication is closed.")
			break
		}

		log.Debug("sending a replication message...")
		err := stream.Send(&pb.WALMessage{Message: serializedTransactionGroup})
		if err != nil {
			log.Error(fmt.Sprintf("an error occurred while sending replication message:%s", err))
		}
		log.Debug("successfully sent a replication message")
	}

	return nil
}

func (rs *GRPCReplicationServer) SendTG(transactionGroup []byte) {
	// send a replication message to each replica
	for ip, channel := range rs.StreamChannels {
		log.Info("sending a replication message to %s", ip)
		channel <- transactionGroup
	}
}

func (rs *GRPCReplicationServer) Shutdown() {
	for _, channel := range rs.StreamChannels {
		close(channel)
	}
}
