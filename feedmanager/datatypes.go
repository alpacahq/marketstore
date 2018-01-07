package feedmanager

import (
	"github.com/alpacahq/marketstore/utils"
	"sync"
)

var (
	initializedFeeds struct {
		sync.Mutex
		feeds map[FeedKeyType]*Feed
	}
)

func init() {
	initializedFeeds.feeds = make(map[FeedKeyType]*Feed)
}

type FeedRunner struct {
	ParentPID        uint32 // Root of process group, allows kill of whole group
	RunningProcesses map[uint32]*utils.Process
}

func NewFeedRunner() *FeedRunner {
	fr := new(FeedRunner)
	/*
		We create a parent context so that we can stop all running feed processes by calling the parent's cancel()
	*/
	fr.ParentPID = utils.NewProcess(nil, nil)
	fr.RunningProcesses = make(map[uint32]*utils.Process)
	return fr
}
