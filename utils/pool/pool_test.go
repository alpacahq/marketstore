package pool_test

import (
	"github.com/alpacahq/marketstore/v4/utils/pool"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	t.Parallel()
	jobCount := 0

	job := func(input interface{}) {
		jobCount++
	}
	p := pool.NewPool(10, job)

	cc := make(chan interface{})
	go p.Work(cc)

	for i := 0; i < 10; i++ {
		cc <- struct{}{}
	}

	close(cc)
	<-time.After(time.Second)

	assert.Equal(t, jobCount, 10)
}
