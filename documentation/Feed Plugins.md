# Plugin Feed Documentation

Marketstore provides a feed plugin interface for downloading third party data sources and storing their values into Marketstore. An example feed plugin can be found at feedmanager/testplugin/testplugin.go.

## API

**Init()**

Initalizes the feed.

* Input: baseURl string, destinations []\*TimeBucketKey

	* baseURL: Contains the address for accessing the desired data.
	* destinations: A slice of TimeBucketKeys for downloaded data to be indexed as in a ColumnSeriesMaps  

* Output: feedState interface{}, exampleData io.ColumnSeriesMap, err error

	* feedState: Contains information on the given state of a feed during execution
	* exampleData: Contains test data from Poll() and verifies connectivity to third party service
	* err: Will be nil should an error occur

**Get()**

A single query out to the third party service for requested data.

* Input: feedState interface{}, Input interface{}

	* feedState: Needed for the pointer to the Feed data structure location to query
	* Input: Data used to query

* Output: quotes interface{}, err error

	* quotes: Data returned by the request
	* err: Will be nil should an error occur

**Poll()**

A single test query to check if connectivity to the service can be established.

* Input: feedState interface{}, Input interface{}

	* feedState: Needed for the pointer to the Feed data structure location to query
	* Input: Data used to query

* Output: results io.ColumnSeriesMap, err error

	* results: Data returned from the test query
	* err: Will be nil should an error occur

**Recv()**

Runs continuously in the background downloading and writtng data to Marketstore.

* Input: None

* Output: chan interface{}
