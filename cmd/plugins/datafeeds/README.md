## Data Adapter Plugins: Description

These Go plugins contain adapters that acquire data and format it for insertion into a Marketstore database.

An adapter contains the following callable functions - these will be used by the system to configure and
run the data fetch process:

#### Init(BaseURL, Targets []*catalog.TimeBucketKey) (feedState interface{}, exampleData io.ColumnSeriesMap, err error)

Arguments:
1) BaseURL: used to communicate with the source - a string conforming to URL syntax
2) Targets: a slice of TimeBucketKey, each specifies target bucket, e.g.:
        "AAPL/1Min/OHLCV:Symbol/Timeframe/AttributeGroup"
list of symbols with candle timeframes like: "AAPL/1Min TSLA/1Min"


Responses:
1) feedState: any state associated with the feed is contained here
2) exampleData: a ColumnSeriesMap used to inform the feed runner about the shape of the data produced by this feed
2) err: an error message if something goes wrong - there should be a good description of the guessed problem here

#### Get(feedState interface{}, Input interface{}) (results interface{}, err error)

Arguments:
1) feedState: any state associated with the feed is contained here
2) Input: used to inform the source about conditions for this call, e.g. "time at which data is needed"

Responses:
1) results: formatted in the sender's format
2) err: an error message if something goes wrong - there should be a good description of the guessed problem here

This will connect with the upstream source and return a result or err if there is some problem.

This method can be used to validate that the source is still working correctly independently of data
formatting.

#### Poll(feedState interface{}, Input interface{}) (results ColumnSeriesMap, err error)

Arguments:
1) feedState: the parsed URL handle from url.Parse
2) Input: used to inform the source about conditions for this call, e.g. "time at which data is needed"

Responses:
1) results: formatted as a ColumnSeriesMap
2) err: an error message if something goes wrong - there should be a good description of the guessed problem here

Acquires data from the source for this polling interval. It is expected that Poll() will be called on a
repeating interval controlled by the user of this adapter. For instance, we might call it every 30 seconds
to acquire the next set of 1Min candles from an upstream data provider.

Data formatting is the main job of the adapter - converting the upstream source's data items to a []ColumnSeries
structure using the ColumnSeries API, suitable for insertion into a Marketstore database instance.

#### Recv(Response interface{}) (results ColumnSeriesMap, err error)

Used as a callback for a pub/sub interface

Arguments:
1) Response: incoming structure used by the upstream, passed as an empty interface{} that can be unpacked
 inside the implemented Recv().

Responses:
1) results: the results of the receive, formatted as a ColumnSeriesMap
2) err: an error message if something goes wrong - there should be a good description of the guessed problem here
