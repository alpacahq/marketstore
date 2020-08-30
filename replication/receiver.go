package replication

import (
	"context"
	"fmt"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/pkg/errors"
	"io"
)

type Receiver struct {
	GRPCClient *GRPCReplicationClient
}

func NewReceiver(grpcClient *GRPCReplicationClient) *Receiver {
	return &Receiver{
		GRPCClient: grpcClient,
	}
}

func (r *Receiver) Run(ctx context.Context) error {

	stream, err := r.GRPCClient.Connect(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to connect to master instance")
	}

	r.GRPCClient.streamClient = stream

	go func(ctx context.Context) {
		for {
			log.Debug("waiting for replication messages from master...")
			// block until receive a new replication message
			writeCommands, err := r.GRPCClient.Recv()
			if err == io.EOF {
				log.Info("received EOF from master server")
				break
			}
			if err != nil {
				log.Error(fmt.Sprintf("an error occurred while receiving a wal message from master instance."+
					"There will be data inconsistency between master and replica:%s", err.Error()))
				break
			}

			err = replay(wal.WriteCommandsFromProto(writeCommands))
			if err != nil {
				log.Error(fmt.Sprintf("an error occurred while replaying. "+
					"There will be data inconsistency between master and replica:%s", err.Error()))
				break
			}
		}

	}(ctx)

	return nil
}
