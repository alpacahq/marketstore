package worker

import (
	"sync"
)

type Pool struct {
	input     chan func()
	waitGroup sync.WaitGroup
}

func NewWorkerPool(poolSize int) *Pool {
	wp := Pool{
		input: make(chan func()),
	}

	for i := 0; i < poolSize; i++ {
		wp.waitGroup.Add(1)
		go work(wp.input, &wp.waitGroup)
	}

	return &wp
}

func (p *Pool) Do(fn func()) {
	p.input <- fn
}

func (p *Pool) CloseAndWait() {
	close(p.input)
	p.waitGroup.Wait()
}

func work(input chan func(), waitGroup *sync.WaitGroup) {
	for fn := range input {
		fn()
	}

	waitGroup.Done()
}
