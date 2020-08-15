package replication

import (
	"log"
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

//func (s *Sender) initialize() error {
//
//	return nil
//}

func (s *Sender) Run() error {
	//if err := s.initialize(); err != nil {
	//	return errors.Wrap(err, "an error occurred while serving replication service")
	//}

	go func(resc chan []byte) {
		for {
			select {
			case transactionGroup := <-resc:
				log.Println("another goroutine receives a request")
				println(transactionGroup)
			}
		}
	}(s.channel)

	return nil
}

func (s *Sender) replicate() {

}

func (s *Sender) Send(serializedTransactionGroup []byte) {
	s.channel <- serializedTransactionGroup
}
