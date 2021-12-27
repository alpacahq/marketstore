package replication

import (
	"context"
	"fmt"
	"io"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

type Receiver struct {
	gRPCClient GRPCClient
	replayer   Replayer
}

// GRPCClient is an interface to abstract GRPCReplicationClient.
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
		return RetryableError("failed to connect to master instance:" + err.Error())
	}
	log.Info("connected to the master instance.")

	for {
		log.Debug("waiting for replication messages from master...")
		// block until receive a new replication message
		transactionGroup, err := r.gRPCClient.Recv()
		if err == io.EOF {
			return fmt.Errorf("received EOF from master server")
		}
		if err != nil {
			log.Error(fmt.Sprintf("an error occurred while receiving a replication message"+
				" from master instance. Will be retried soon...:%s", err.Error()))
			// This might be a temporary network issue. We retry it.
			return RetryableError(err.Error())
		}

		err = r.replayer.Replay(transactionGroup)
		if err != nil {
			// this might be a bug in the replay logic. We won't retry it.
			return fmt.Errorf("an error occurred while replaying. "+
				"There will be data inconsistency between master and replica:%w", err)
		}
	}
}
