package replication

import (
	"context"
	"fmt"
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
	err := r.GRPCClient.Connect(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to connect to master instance")
	}

	go func() {
		for {
			log.Debug("waiting for replication messages from master...")
			// block until receive a new replication message
			transactionGroup, err := r.GRPCClient.Recv()
			if err == io.EOF {
				log.Info("received EOF from master server")
				return
			}
			if err != nil {
				log.Error(fmt.Sprintf("an error occurred while receiving a replication message from master instance."+
					"There will be data inconsistency between master and replica:%s", err.Error()))
				return
			}

			err = replay(transactionGroup)
			if err != nil {
				log.Error(fmt.Sprintf("an error occurred while replaying. "+
					"There will be data inconsistency between master and replica:%s", err.Error()))
				break
			}
		}
	}()

	return nil
}
