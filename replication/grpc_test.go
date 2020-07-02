package replication

import (
	"context"
	pb "github.com/alpacahq/marketstore/proto"
	"testing"
	"time"
)

func TestGRPC(t *testing.T){
	_,err := NewGRPCReplicationService(10000)
	if err != nil {
		t.Fatalf("failed to initialize GRPC service")
	}

	time.Sleep(1 * time.Second)
	c,err := NewGRPCReplicationClient("127.0.0.1:10000", false)
	time.Sleep(1 * time.Second)
	err = c.GetMessages(context.Background(),&pb.MessagesRequest{Id: "123",})
	if err != nil {
		t.Error(err)
	}
}