package replication

import (
	"context"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	defaultSenderChannelSize = 500
)

type ReplicationService interface {
}

type Sender struct {
	ReplService *GRPCReplicationServer
	//ReplicaHosts []string
	channel chan []*wal.WriteCommand
}

func NewSender(service *GRPCReplicationServer) *Sender {
	c := make(chan []*wal.WriteCommand, defaultSenderChannelSize)

	return &Sender{
		ReplService: service,
		channel:     c,
	}
}


func (s *Sender) Run(ctx context.Context) {
	go func(ctx context.Context, resc chan []*wal.WriteCommand) {
		for {
			select {
			case <-ctx.Done():
				log.Info("shutdown replication sender...")
				return
			case writeCommands := <-resc:
				log.Debug("send a replication message to replicas")
				s.ReplService.SendWriteCommands(writeCommands)
			}
		}
	}(ctx, s.channel)
}

func (s *Sender) Send(writeCommands []*wal.WriteCommand) {
	s.channel <- writeCommands
}


