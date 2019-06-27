package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type PoolTestSuite struct {
	suite.Suite
}

func TestPoolTestSuite(t *testing.T) {
	suite.Run(t, new(PoolTestSuite))
}

func (s *PoolTestSuite) TestPool() {
	jobCount := 0

	job := func(input interface{}) {
		jobCount++
	}
	p := NewPool(10, job)

	c := make(chan interface{})
	go p.Work(c)

	for i := 0; i < 10; i++ {
		c <- struct{}{}
	}

	close(c)
	<-time.After(time.Second)

	assert.Equal(s.T(), jobCount, 10)
}
