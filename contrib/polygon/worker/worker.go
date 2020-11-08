package worker

import (
	"sync"
)

type WorkerPool struct {
	input     chan func()
	waitGroup sync.WaitGroup
}

func NewWorkerPool(poolSize int) *WorkerPool {
	wp := WorkerPool{
		input: make(chan func()),
	}

	for i := 0; i < poolSize; i++ {
		wp.waitGroup.Add(1)
		go work(wp.input, &wp.waitGroup)
	}

	return &wp
}

func (p *WorkerPool) Do(fn func()) {
	p.input <- fn
}

func (p *WorkerPool) CloseAndWait() {
	close(p.input)
	p.waitGroup.Wait()
}

func work(input chan func(), waitGroup *sync.WaitGroup) {
	for fn := range input {
		fn()
	}

	waitGroup.Done()
}
