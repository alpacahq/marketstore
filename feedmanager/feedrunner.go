package feedmanager

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

type FeedRunState struct {
	pollFrequency time.Duration
	feed          *Feed
}

var RunningFeeds struct {
	sync.Mutex
	Descriptions map[uint32]string
}

func init() {
	RunningFeeds.Descriptions = make(map[uint32]string)
}

func validateFeed(pid uint32, frs *FeedRunState) error {
	proc := utils.GetProcFromPID(pid)
	proc.PutInput(frs)
	time.Sleep(1 * time.Millisecond)
	_, msgs, err := proc.GetOutput()
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return fmt.Errorf("unable to get message from feed process")
	} else {
		val, ok := msgs[0].(string)
		if !ok || val != "OK" {
			return fmt.Errorf("feed runner did not initialize properly")
		}
	}
	RunningFeeds.Lock()
	RunningFeeds.Descriptions[pid] = frs.feed.Description(frs.pollFrequency)
	RunningFeeds.Unlock()
	return nil
}

func PollFeed(pollFrequency time.Duration, fd *Feed) (pid uint32, err error) {
	pid = utils.NewProcess(Runner, nil)
	frs := &FeedRunState{
		pollFrequency,
		fd,
	}
	err = validateFeed(pid, frs)
	return pid, err
}

func SubscribeFeed(fd *Feed) (pid uint32, err error) {
	pid = utils.NewProcess(Runner, nil)
	frs := &FeedRunState{
		time.Duration(math.MaxInt64),
		fd,
	}
	err = validateFeed(pid, frs)
	return pid, err
}

func write(output chan interface{}, frs *FeedRunState, csm io.ColumnSeriesMap) {
	writers := frs.feed.Writers
	destinations := frs.feed.Destinations
	for i, bucket := range destinations {
		writer := writers[i]
		if cs, ok := csm[*bucket]; ok {
			rs := cs.ToRowSeries(*bucket)
			rowdata := rs.GetData()
			times := rs.GetTime()
			writer.WriteRecords(times, rowdata)
		} else {
			err := fmt.Errorf("poll result not in requested bucket list, key: %s", bucket.String())
			output <- err // Send this error out, will be gathered in the message queue
		}
	}
}

func KillAllFeeds() {
	var pidlist []uint32
	RunningFeeds.Lock()
	for pid := range RunningFeeds.Descriptions {
		pidlist = append(pidlist, pid)
	}
	RunningFeeds.Unlock()
	for _, pid := range pidlist {
		KillFeed(pid)
	}
}

func KillFeed(pid uint32) {
	if utils.IsRunning(pid) {
		proc := utils.GetProcFromPID(pid)
		proc.Kill()
	}
	RunningFeeds.Lock()
	delete(RunningFeeds.Descriptions, pid)
	RunningFeeds.Unlock()
}

func Runner(input, output chan interface{}) {
	/*
		When initially run, we will expect an input message containing the feed
		We finish by sending an "OK" back or an error message
	*/
	var frs *FeedRunState
	var ok bool
	select {
	case msg := <-input:
		if frs, ok = msg.(*FeedRunState); !ok {
			err := fmt.Errorf("unable to extract feed from input")
			output <- err
		} else {
			output <- "OK"
		}
	}

	fs := frs.feed.FeedState
	ticker := time.NewTicker(frs.pollFrequency)
	var err error
	var csm io.ColumnSeriesMap
	for {
		select {
		case <-ticker.C:
			csm, err = frs.feed.Datafeed.Poll(fs, nil)
			if err != nil {
				output <- err
				log.Log(log.ERROR, err.Error())
				continue
			}
		case data := <-frs.feed.Datafeed.Recv():
			switch value := data.(type) {
			case io.ColumnSeriesMap:
				csm = value
			case error:
				output <- value
				log.Log(log.ERROR, value.Error())
				continue
			}
		}
		write(output, frs, csm)
	}
}
