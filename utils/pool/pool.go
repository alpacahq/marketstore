package pool

import (
	"sync"
)

// Pool is a basic work pool - I didn't like any
// of the ones I found, so I made one
type Pool struct {
	workerQ chan struct{}
	f       func(input interface{})
	wg      sync.WaitGroup
}

// NewPool creates a new worker pool with a goroutine limit
// and a job function to execute on the incoming data
func NewPool(routines int, job func(input interface{})) *Pool {
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
// pool working on a data input channel
func (p *Pool) Work(c <-chan interface{}) {
	for v := range c {
		<-p.workerQ
		p.wg.Add(1)
		go func(input interface{}) {
			defer p.wg.Done()
			p.f(input)
			p.workerQ <- struct{}{}
		}(v)
	}
}

// Wait waits until the pool is finished
func (p *Pool) Wait() {
	p.wg.Wait()
}
