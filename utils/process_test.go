package utils

import (
	"fmt"
	"time"

	. "gopkg.in/check.v1"
)

/*
Already done in other testing files in this package

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }
*/

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *C)    {}
func (s *TestSuite) TearDownSuite(c *C) {}

func (s *TestSuite) TestProcess(c *C) {
	pid := NewProcess(nil, nil)
	proc := GetProcFromPID(pid)
	c.Assert(pid == 0, Equals, true)
	c.Assert(proc == nil, Equals, true)

	myfunc := func(input chan interface{}, output chan interface{}) {
		for {
			select {
			case msg := <-input:
				fmt.Printf("Msg: %v\n", msg)
				output <- "Pong!"
			}
		}
	}

	pid = NewProcess(myfunc, nil)
	proc = GetProcFromPID(pid)
	proc.PutInput("Ping!")
	time.Sleep(time.Millisecond)
	ts, msgs, err := proc.GetOutput()
	c.Assert(len(ts), Equals, 1)
	c.Assert(len(msgs), Equals, 1)
	for i, msg := range msgs {
		fmt.Printf("ts, msg: %v:%v\n", ts[i], msg)
	}
	pong := msgs[0].(string)
	c.Assert(pong, Equals, "Pong!")

	proc.PutInput("Ping2!")
	time.Sleep(time.Millisecond)
	ts, msgs, err = proc.GetOutput()
	c.Assert(len(ts), Equals, 2)
	c.Assert(len(msgs), Equals, 2)
	pong = msgs[1].(string)
	c.Assert(pong, Equals, "Pong!")
	proc.Kill()
	ts, msgs, err = proc.GetOutput()
	if err != nil {
		fmt.Println("Error: ", err)
	}
	c.Assert(err != nil, Equals, true)
}
