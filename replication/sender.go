package replication

import (
	"context"
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
	channel chan []byte
}

func NewSender(service *GRPCReplicationServer) *Sender {
	c := make(chan []byte, defaultSenderChannelSize)

	return &Sender{
		ReplService: service,
		channel:     c,
	}
}

func (s *Sender) Run(ctx context.Context) {
	go func(ctx context.Context, resc chan []byte) {
		for {
			select {
			case <-ctx.Done():
				log.Info("shutdown replication sender...")
				return
			case transactionGroup := <-resc:
				log.Debug("send a replication message to replicas")
				s.ReplService.SendReplicationMessage(transactionGroup)
			}
		}
	}(ctx, s.channel)
}

func (s *Sender) Send(transactionGroup []byte) {
	s.channel <- transactionGroup
}
