package pool

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&PoolTestSuite{})

type PoolTestSuite struct{}

func (s *PoolTestSuite) TestPool(c *C) {
	jobCount := 0

	job := func(input interface{}) {
		jobCount++
	}
	p := NewPool(10, job)

	cc := make(chan interface{})
	go p.Work(cc)

	for i := 0; i < 10; i++ {
		cc <- struct{}{}
	}

	close(cc)
	<-time.After(time.Second)

	c.Assert(jobCount, Equals, 10)
}
