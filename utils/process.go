package utils

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

/*
This is the authoritative root context for all processes managed by this package
In other words, if you cancel/kill this parent, all processes managed here will be canceled/killed
*/
var (
	runningProcesses struct {
		sync.Mutex
		procs       map[uint32]*Process
		rootContext context.Context
		kill        context.CancelFunc
	}

	lastPID uint32
)

func init() {
	/*
		This sets up a root context for running processes, so that we can kill all procs by canceling root
	*/
	runningProcesses.rootContext,
		runningProcesses.kill = context.WithCancel(context.Background())
	runningProcesses.procs = make(map[uint32]*Process)
}

func GetProcFromPID(pid uint32) *Process {
	runningProcesses.Lock()
	defer runningProcesses.Unlock()
	if proc, ok := runningProcesses.procs[pid]; ok {
		return proc
	}
	return nil
}

func IsRunning(pid uint32) bool {
	proc := GetProcFromPID(pid)
	return proc.running()
}

type Process struct {
	PID           uint32
	Context       context.Context
	kill          context.CancelFunc
	input, output chan interface{}
	runfunc       func(chan interface{}, chan interface{})
	Messages      *MessageQueue
}

func (pr *Process) Kill() {
	pr.input, pr.output = nil, nil
	pr.kill()
	runningProcesses.Lock()
	delete(runningProcesses.procs, pr.PID)
	runningProcesses.Unlock()
}

func (pr *Process) running() bool {
	switch {
	case pr == nil:
		fallthrough
	case pr.input == nil || pr.output == nil:
		return false
	}
	return true
}

func (pr *Process) PutInput(msg interface{}) (err error) {
	if pr.running() {
		pr.input <- msg
		return nil
	}
	return fmt.Errorf("process is not running")
}

func (pr *Process) GetOutput() (timestamps []time.Time, messages []interface{}, err error) {
	if pr.running() {
		timestamps, messages := pr.Messages.GetMessages()
		return timestamps, messages, nil
	}
	return nil, nil, fmt.Errorf("process is not running")
}

func NewPID() uint32 {
	return atomic.AddUint32(&lastPID, 1)
}

func NewProcess(run func(chan interface{}, chan interface{}), parentContext context.Context) (pid uint32) {
	if parentContext == nil {
		/*
			Parent context allows for process grouping, so that all under it can be managed together
		*/
		parentContext = runningProcesses.rootContext
	}

	ctx, kill := context.WithCancel(parentContext)
	input, output := make(chan interface{}), make(chan interface{})
	mq := NewMessageQueue(50) // Buffer output from process in a message queue
	proc := &Process{
		0,
		ctx,
		kill,
		input,
		output,
		run,
		mq,
	}
	if run != nil {
		proc.PID = NewPID()
		go run(input, output)
		go BufferProcessOutput(output, mq)
		runningProcesses.Lock()
		runningProcesses.procs[proc.PID] = proc
		runningProcesses.Unlock()
	}
	return proc.PID
}

type MessageQueue struct {
	sync.Mutex
	length, cursor int
	timeStamp      []time.Time
	messages       []interface{}
}

func NewMessageQueue(length int) *MessageQueue {
	mq := new(MessageQueue)
	mq.length = length
	mq.timeStamp = make([]time.Time, length)
	mq.messages = make([]interface{}, length)
	return mq
}
func (mq *MessageQueue) Len() int { return mq.length }
func (mq *MessageQueue) Swap(i, j int) {
	mq.Lock()
	defer mq.Unlock()
	mq.timeStamp[j], mq.timeStamp[i] = mq.timeStamp[i], mq.timeStamp[j]
	mq.messages[j], mq.messages[i] = mq.messages[i], mq.messages[j]
}
func (mq *MessageQueue) Less(i, j int) bool {
	mq.Lock()
	defer mq.Unlock()
	return mq.timeStamp[i].After(mq.timeStamp[j])
}
func (mq *MessageQueue) AddMessage(msg interface{}) {
	mq.Lock()
	defer mq.Unlock()
	mq.messages[mq.cursor] = msg
	mq.timeStamp[mq.cursor] = time.Now()
	mq.cursor = (mq.cursor + 1) % mq.length
}
func (mq *MessageQueue) GetMessages() (times []time.Time, messages []interface{}) {
	sort.Sort(mq)
	mq.Lock()
	defer mq.Unlock()
	for i, ts := range mq.timeStamp {
		if ts.IsZero() == false {
			times = append(times, ts)
			messages = append(messages, mq.messages[i])
		}
	}
	return times, messages
}

func BufferProcessOutput(output chan interface{}, mq *MessageQueue) {
	for {
		msg := <-output
		mq.AddMessage(msg)
	}
}
