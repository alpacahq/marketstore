# MarketStore frontend API

## Transport Methods

MarketStore communicates with its clients through standard HTTP in
Messagepack RPC (Messagepack version of JSON-RPC 2.0).

## API Calls

### DataService.ListSymbols()

#### Input
no parameters

#### Output
list of string for each unique symbol stored in the server.

### DataService.Query()

#### Input
Query() interface accepts a list of "requests", each of which is a map with the following fields.

* destination (`string`)

	A string path of the query target. A TimeBucketKey contains a Symbol, Timeframe, and an AttributeGroup. For example, "TSLA/1Min/OHLCV" is an example TimeBucketKey. In this example, TSLA is the Symbol, 1Min is the TimeFrame, and OHLCV is the AttributeGroup. Moreover, a single destination can include multiple symbols split by commas for a multi-symbol query. For example, "TSLA,F,NVDA/1Min/OHLCV" will query data for Symbols TSLA, F, and NVDA all across the same TimeFrame, AttributeGroup.

* epoch_start (`int64`)

	An integer epoch seconds from Unix epoch time.  Rows timestamped equal to or after this time will be returned.

* epoch_end (`int64`)

	An integer epoch seconds from Unix epoch time.  Rows timestamped equal to or before this time will be returned.

* limit_record_count (`int`)

	An integer to limit the number of rows to be returned from the query.

* limit_from_start (`bool`)

	A boolean value to indicate if limit_recourd_count should be counted from the lower side of result set or upper.  Default to false, meaning from the upper.

Note: It is also possible to query multiple TimeBucketKeys at once. The requests parameter is passed a list of query structures (See examples).

#### Output
The output returns the same number of "responses" as the requests, each of which has the following fields.

* result

	A MultiDataset type.  See below for this type.


### DataService.Write()

#### Input

* dataset

	A MultiDataset type.  See below for this type.

* is_variable_length (`bool`)

	A boolean value for telling MarketStore if the write procedure will be dynamic in length.

#### Output
The API will return an empty response on success. Should the write call fail, the response will include the original input as well as an error returned by the server.


## MultiDataset type
This is the common wire format to represent a series of columns containing
multiple slices (horizontal partitions).  It is a map with the following
fields that represents set of column-oriented data

* types (`[]string`)

	a list of strings for the column types compatible with numpy dtypes (e.g., 'i4', 'f8')

* names (`[]string`)

	a list of strings for the column names

* data (`[][]byte`)

	a list of byte arrays with each being the binary column data

* startindex (`[]int`)

	a list of integer to indicate which element each slice starts at

* lengths (`[]int`)

	a list of integer to indicate how many elements each slice has
