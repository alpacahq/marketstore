package feedmanager

import (
	"bytes"
	"encoding/base64"
	"hash/fnv"
	"io"
	"time"

	"github.com/alpacahq/marketstore/catalog"
	"github.com/alpacahq/marketstore/cmd/plugins/datafeeds"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	plugins "github.com/alpacahq/marketstore/plugins"
	io2 "github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

type FeedKeyType string

func NewFeedKey(pluginName, baseURL string, destinations []*io2.TimeBucketKey) FeedKeyType {
	hasher := fnv.New64()
	io.WriteString(hasher, pluginName)
	io.WriteString(hasher, baseURL)
	for _, tbk := range destinations {
		io.WriteString(hasher, tbk.GetCatKey())
		io.WriteString(hasher, tbk.GetItemKey())
	}
	return FeedKeyType(base64.URLEncoding.EncodeToString(hasher.Sum(nil)))
}

type Feed struct {
	Datafeed         *datafeeds.DatafeedType
	PluginName       string //like "myplugin.so", will be used with plugin.Open()
	BaseURL          string //used to Init() the feed, stored in case feed needs to re-initialize
	Destinations     []*io2.TimeBucketKey
	FeedState        interface{}
	Writers          []*executor.Writer
	IsVariableLength bool
}

func NewFeed(pluginName, baseURL string, d *catalog.Directory, destinations []*io2.TimeBucketKey, isVariableLength bool) (fd *Feed, err error) {
	key := NewFeedKey(pluginName, baseURL, destinations)

	if _, ok := initializedFeeds.feeds[key]; !ok {
		/*
			Load the plugin
		*/
		pi, err := plugins.LoadFromGOPATH(pluginName)
		if err != nil {
			log.Log(log.ERROR, "Unable to open plugin: "+err.Error())
			return nil, err
		}
		sym, err := pi.Lookup("Datafeed")
		if err != nil {
			log.Log(log.ERROR, "Unable to lookup plugin symbol: "+err.Error())
			return nil, err
		}

		fd = &Feed{
			sym.(*datafeeds.DatafeedType),
			pluginName,
			baseURL,
			destinations,
			nil,
			nil,
			isVariableLength,
		}
		/*
			Initialize the feed
		*/
		if err = fd.Init(d, isVariableLength); err != nil {
			log.Log(log.ERROR, "Unable to Init feed: "+err.Error())
			return nil, err
		}

		initializedFeeds.feeds[key] = fd
	}
	return initializedFeeds.feeds[key], nil
}

func (fd *Feed) Description(freq time.Duration) (desc string) {
	/*
		Descriptions for each feed, used for display of running processes
	*/
	if len(fd.Destinations) == 0 {
		return "No Destinations"
	}
	var buffer bytes.Buffer
	if fd.IsVariableLength {
		buffer.WriteString("VariableLen ")
	} else {
		buffer.WriteString("FixedLength ")
	}

	if freq != 0 {
		buffer.WriteString(freq.String() + ":Polled ")
	} else {
		buffer.WriteString("Subbed ")
	}

	buffer.WriteString(fd.PluginName + " ")

	tf := fd.Destinations[0].GetItemInCategory("Timeframe")
	buffer.WriteString(tf + "/")

	for i, p_key := range fd.Destinations {
		if i == 0 {
			buffer.WriteString(p_key.GetItemInCategory("AttributeGroup") + " [")
		}
		buffer.WriteString(p_key.GetItemInCategory("Symbol"))
		if i+1 < len(fd.Destinations) {
			buffer.WriteString(",")
		} else {
			buffer.WriteString("] ")
		}
	}

	return buffer.String()
}

func (fd *Feed) Init(d *catalog.Directory, isVariableLength bool) (err error) {
	/*
		We initialize the feed and create a bucket to write results into using
		the returned sample columnseries dataset as the template for the bucket
	*/
	var csmExample io2.ColumnSeriesMap
	fd.FeedState, csmExample, err = fd.Datafeed.Init(fd.BaseURL, fd.Destinations)

	var cs *io2.ColumnSeries
	for _, val := range csmExample {
		cs = val
	}

	for _, tbk := range fd.Destinations {
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			return err
		}

		var recordType io2.EnumRecordType
		if isVariableLength {
			recordType = io2.VARIABLE
		} else {
			recordType = io2.FIXED
		}

		tbi := io2.NewTimeBucketInfo(
			*tf,
			tbk.GetPathToYearFiles(d.GetPath()),
			"Created By Feeder", 2017,
			cs.GetDataShapes(), recordType)

		/*
			Verify there is an available TimeBucket for the destination
		*/
		err = d.AddTimeBucket(tbk, tbi)
		if err != nil {
			// If File Exists error, ignore it, otherwise return the error
			if _, ok := err.(catalog.FileAlreadyExists); !ok {
				return err
			}
		}

		/*
			Create a writer for this TimeBucket
		*/
		q := planner.NewQuery(d)
		q.AddTargetKey(tbk)
		pr, err := q.Parse()
		if err != nil {
			return err
		}
		wr, err := executor.NewWriter(pr, executor.ThisInstance.TXNPipe, d)
		if err != nil {
			return err
		}
		fd.Writers = append(fd.Writers, wr)
	}
	return nil
}

func (fd *Feed) GetFeedKey() FeedKeyType {
	/*
		Returns the unique identifier for this feed
	*/
	return NewFeedKey(fd.PluginName, fd.BaseURL, fd.Destinations)
}
