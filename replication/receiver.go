package replication

import (
	"context"
	"fmt"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/pkg/errors"
)

type Receiver struct {
	GRPCClient GRPCReplicationClient
	MasterHost string
}

func NewReceiver(masterHost string, grpcClient GRPCReplicationClient) *Receiver {
	return &Receiver{
		GRPCClient: grpcClient,
		MasterHost: masterHost,
	}
}

func (r *Receiver) Run(ctx context.Context) error {

	err := r.GRPCClient.Connect(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to connect to master instance")
	}

	go func(ctx context.Context) {
		for {
			// block until receive a new wal message
			serializedTransactionGroup, err := r.GRPCClient.Recv()
			if err != nil {
				log.Error("an error occurred while receiving a wal message from master instance")
				break
			}

			replay(serializedTransactionGroup)
		}
	}(ctx)

	return nil
}

//func (r *Receiver) GetMessage() error {
//	conn, err := grpc.Dial(r.MasterHost, grpc.WithInsecure())
//	if err != nil {
//		return errors.Wrap(err, "failed to connect Master server")
//	}
//	defer conn.Close()
//
//	//c := pb.NewReplicationClient(conn)
//	//c.GetMessages()
//	return nil
//}

func replay(serializedTransactionGroup []byte) {
	fmt.Println("受信しました！")
	println(serializedTransactionGroup)
}
