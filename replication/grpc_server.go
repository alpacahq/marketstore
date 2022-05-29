package replication

import (
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	defaultReplicationStreamChannelSize = 500
)

type GRPCReplicationServer struct {
	pb.UnimplementedReplicationServer
	CertFile    string
	CertKeyFile string
	// Key: IPAddr (e.g. "192.125.18.1:25"), Value: channel for messages sent to each gRPC stream
	StreamChannels map[string]chan []byte
}

func NewGRPCReplicationService() *GRPCReplicationServer {
	return &GRPCReplicationServer{
		StreamChannels: map[string]chan []byte{},
	}
}

func getClientAddr(stream grpc.ServerStream) (string, error) {
	ctx := stream.Context()

	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("failed to get client IP address")
	}
	return pr.Addr.String(), nil
}

func (rs *GRPCReplicationServer) GetWALStream(_ *pb.GetWALStreamRequest, stream pb.Replication_GetWALStreamServer,
) error {
	// prepare a channel to send messages
	clientAddr, err := getClientAddr(stream)
	if err != nil {
		return errors.Wrap(err, "failed to get client IP address")
	}
	log.Info(fmt.Sprintf("new replica connection from:%s", clientAddr))

	streamChannel := make(chan []byte, defaultReplicationStreamChannelSize)
	rs.StreamChannels[clientAddr] = streamChannel

	// infinite loop
	for {
		log.Debug("[master] waiting for write requests...")
		transactionGroup := <-streamChannel
		if transactionGroup == nil {
			log.Info("streamChannel for replication is closed.")
			break
		}

		err := stream.Send(&pb.GetWALStreamResponse{TransactionGroup: transactionGroup})
		if err != nil {
			log.Error(fmt.Sprintf("an error occurred while sending replication message:%s", err))
			break
		}
		log.Debug("successfully sent a replication message")
	}

	// when an error occurred / client connection is closed, close the channel
	delete(rs.StreamChannels, clientAddr)
	close(streamChannel)
	log.Info(fmt.Sprintf("[master] closed replication connection: %v", clientAddr))

	return nil
}

func (rs *GRPCReplicationServer) SendReplicationMessage(transactionGroup []byte) {
	// send a replication message to each replica
	for ip, channel := range rs.StreamChannels {
		log.Debug("sending a replication message to %s", ip)
		channel <- transactionGroup
	}
}
