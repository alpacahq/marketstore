package replication

import (
	"context"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	// the maximum number of transaction messages that a replicator can hold.
	defaultSenderChannelSize = 500
)

type Service interface {
	SendReplicationMessage(transactionGroup []byte)
}

type Sender struct {
	replService Service
	// ReplicaHosts []string
	channel chan []byte
}

func NewSender(service Service) *Sender {
	c := make(chan []byte, defaultSenderChannelSize)

	return &Sender{
		replService: service,
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
				s.replService.SendReplicationMessage(transactionGroup)
			}
		}
	}(ctx, s.channel)
}

func (s *Sender) Send(transactionGroup []byte) {
	s.channel <- transactionGroup
}
