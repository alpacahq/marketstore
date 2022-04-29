package pool

import (
	"fmt"
	"sync"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Pool is a basic work pool - I didn't like any
// of the ones I found, so I made one.
type Pool struct {
	workerQ chan struct{}
	f       func(input []byte)
	wg      sync.WaitGroup
}

// NewPool creates a new worker pool with a goroutine limit
// and a job function to execute on the incoming data.
func NewPool(routines int, job func(input []byte)) *Pool {
	q := make(chan struct{}, routines)
	for i := 0; i < routines; i++ {
		q <- struct{}{}
	}
	return &Pool{
		workerQ: q,
		f:       job,
		wg:      sync.WaitGroup{},
	}
}

// Work is a blocking call that starts the
// pool working on a data input channel.
func (p *Pool) Work(c <-chan interface{}) {
	for v := range c {
		vBytes, ok := v.([]byte)
		if !ok {
			log.Error(fmt.Sprintf("failed to cast a work message to bytes: %v", v))
			return
		}
		<-p.workerQ
		p.wg.Add(1)
		go func(input []byte) {
			defer p.wg.Done()
			p.f(input)
			p.workerQ <- struct{}{}
		}(vBytes)
	}
}

// Wait waits until the pool is finished.
func (p *Pool) Wait() {
	p.wg.Wait()
}
