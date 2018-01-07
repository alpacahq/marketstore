package frontend

import (
	"net/http"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/feedmanager"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/log"
)

type FeedKillReply struct{}
type FeedKillArgs struct {
	PID int `msgpack:"pid"`
}

type FeedListReply struct {
	Descriptions map[string]string `msgpack:"descriptions"`
}
type FeedListArgs struct{}

type FeedStartReply struct {
	PID       int    `msgpack:"pid"`
	ErrorText string `msgpack:"errortext"`
}

type FeedStartArgs struct {
	PluginName       string        `msgpack:"pluginName"`
	FormatName       string        `msgpack:"formatName"`
	SymbolList       []string      `msgpack:"symbolList"`
	Timeframe        string        `msgpack:"timeframe"`
	PollingFrequency time.Duration `msgpack:"pollingfrequency"`
	IsVariable       bool          `msgpack:"isvariable"`
}

func (s *DataService) FeedStart(r *http.Request, args *FeedStartArgs, response *FeedStartReply) (err error) {
	pluginName := args.PluginName
	d := executor.ThisInstance.CatalogDir

	var destinations []*io.TimeBucketKey
	for _, sym := range args.SymbolList {
		itemKey := sym + "/" + args.Timeframe + "/" + args.FormatName
		tbk := io.NewTimeBucketKey(itemKey)
		destinations = append(destinations, tbk)
	}

	fd, err := feedmanager.NewFeed(pluginName, "", d, destinations, args.IsVariable)
	if err != nil {
		response.ErrorText = err.Error()
		Log(ERROR, err.Error())
		return nil
	}

	pid, err := feedmanager.PollFeed(args.PollingFrequency, fd)
	if err != nil {
		response.ErrorText = err.Error()
		Log(ERROR, err.Error())
		return err
	}
	response.PID = int(pid)
	return nil
}

func (s *DataService) FeedList(r *http.Request, args *FeedListArgs, response *FeedListReply) (err error) {
	response.Descriptions = make(map[string]string)
	feedmanager.RunningFeeds.Lock()
	for pid, desc := range feedmanager.RunningFeeds.Descriptions {
		if utils.IsRunning(pid) {
			response.Descriptions[strconv.Itoa(int(pid))] = desc
		}
	}
	feedmanager.RunningFeeds.Unlock()
	return nil
}

func (s *DataService) FeedKill(r *http.Request, args *FeedKillArgs, response *FeedKillReply) (err error) {
	if args.PID == -1 {
		feedmanager.KillAllFeeds()
	} else {
		feedmanager.KillFeed(uint32(args.PID))
	}
	return nil
}
