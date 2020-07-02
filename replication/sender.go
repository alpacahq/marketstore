package replication

import (
	"github.com/pkg/errors"
	"log"
)

type ReplicationService interface {
}

type Sender struct {
	ReplService  *GRPCReplicationServer
	//ReplicaHosts []string
	Port         int
	Channel      chan []byte
}

func NewSender(service *GRPCReplicationServer, port int) *Sender {
	c := make(chan []byte)

	return &Sender{
		ReplService:  service,
		Port:         port,
		Channel: c,
	}
}

func (s *Sender) initialize() error {

	return nil
}

func (s *Sender) Run(resc chan []byte) error {
	if err := s.initialize(); err != nil {
		return errors.Wrap(err, "an error occurred while serving replication service")
	}

	go func(resc chan []byte) {
		for {
			select {
			case transactionGroup := <-resc:
				log.Println("another goroutine receives a request")
				println(transactionGroup)
			}
		}
	}(resc)

	return nil
}

func (s *Sender) replicate() {

}

func (s *Sender) Send(walMessage []byte) {
	s.ReplService.SendMessage()
}
