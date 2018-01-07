package datafeeds

import (
	"sync"

	"github.com/alpacahq/marketstore/utils/io"
)

type DatafeedType struct {
	sync.Mutex
	Init func(BaseURL string, Destinations []*io.TimeBucketKey) (feedState interface{}, exampleData io.ColumnSeriesMap, err error)
	Get  func(feedState interface{}, Input interface{}) (results interface{}, err error)
	Poll func(feedState interface{}, Input interface{}) (results io.ColumnSeriesMap, err error)
	Recv func() <-chan interface{}
}
