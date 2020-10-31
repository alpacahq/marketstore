package replication

import (
	"context"
	"fmt"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/pkg/errors"
	"io"
)

type Receiver struct {
	gRPCClient GRPCClient
	replayer   Replayer
}

// GRPCClient is an interface to abstract GRPCReplicationClient
type GRPCClient interface {
	Connect(ctx context.Context) error
	Recv() ([]byte, error)
}

type Replayer interface {
	Replay(transactionGroup []byte) error
}

func NewReceiver(grpcClient GRPCClient, replayer Replayer) *Receiver {
	return &Receiver{
		gRPCClient: grpcClient,
		replayer:   replayer,
	}
}

func (r *Receiver) Run(ctx context.Context) error {
	err := r.gRPCClient.Connect(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to connect to master instance")
	}

	go func() {
		for {
			log.Debug("waiting for replication messages from master...")
			// block until receive a new replication message
			transactionGroup, err := r.gRPCClient.Recv()
			if err == io.EOF {
				log.Info("received EOF from master server")
				return
			}
			if err != nil {
				log.Error(fmt.Sprintf("an error occurred while receiving a replication message from master instance."+
					"There will be data inconsistency between master and replica:%s", err.Error()))
				return
			}

			err = r.replayer.Replay(transactionGroup)
			if err != nil {
				log.Error(fmt.Sprintf("an error occurred while replaying. "+
					"There will be data inconsistency between master and replica:%s", err.Error()))
				return
			}
		}
	}()

	return err
}
