package pool_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/utils/pool"
)

func TestPool(t *testing.T) {
	t.Parallel()
	jobCount := 0

	job := func(input []byte) {
		jobCount++
	}
	p := pool.NewPool(10, job)

	cc := make(chan interface{})
	go p.Work(cc)

	for i := 0; i < 10; i++ {
		cc <- []byte{}
	}

	close(cc)
	<-time.After(time.Second)

	assert.Equal(t, 10, jobCount)
}
