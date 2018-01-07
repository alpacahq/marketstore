# MarketStore API Documentation

MarketStore is Alpaca’s market securities server and database system. The program was built in Google’s Go language, and is designed to handle high levels of concurrent market data processing with little overhead. It is also designed to be scalable so that MarketStore can grow with its client base.

## Key Features

MarketStore was designed with two primary functions in mind with respect to market data: data mastery, and data delivery. MarketStore polls data from various market apis, writes them to its own custom database, and responds to queries as well as subscribers. The system writes data durably to maintain data integrity, and simultaneous delivers that data to subscribers and client queries.

## Data Coverage

MarketStore supports any type of numerical data and it is designed to be flexible.

## Transport Methods

Marketstore supports HTTP query for both historical and subscription data. The HTTP interface allows Marketstore to perform Remote Procedural Calls (JSON-RPC) such as DataService.Query and DataService.Write.

## API Calls

**DataService.ListSymbols()**

* Input: Parameters

	* Parameters: Empty dictionary to list all of the symbols in MarketStore   

* Output: Provides a complete list of symbols supported by MarketStore that are available, in a JSON serialized array.

* Example:

```curl -s --data-binary '{"jsonrpc":"2.0", "method":"DataService.ListSymbols", "id":1, "params": {"parameters": {}}}' -H 'Content-Type: application/json' http://127.0.0.1:5992/rpc```
```
{
  "jsonrpc": "2.0",
  "result": {
    "Results": [
      "TSLA",
      "NVDA",
      "AAPL"
    ]
  },
  "id": 1
}
```
Note: the above ListSymbols() example was done with a small subset of the actual data stored in MarketStore.

**DataService.Query()**

* Input: Destination, TimeStart, TimeEnd

	* Destination: A dictionary containing "key" and a TimeBucketKey. A TimeBucketKey contains a Symbol, Timeframe, and an AttributeGroup. For example, "TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup" is an example TimeBucketKey. In this example, TSLA is the Symbol, 1Min is the TimeFrame, and OHLCV is the AttributeGroup. Moreover, a single destination can include multiple symbols for a multi-query. For example, "TSLA,FORD,NVDA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup" will query data for Symbols TSLA, FORD, and NVDA all across the same TimeFrame, AttributeGroup, start time, and end time.

	* TimeStart: A timestamp in seconds.

	* TimeEnd: A timestamp in seconds.

Note: It is also possible to query multiple TimeBucketKeys at once. The requests parameter is passed a list of query structures (See examples).

* Output: Returns a JSON serialized array of records containing the data within the given time boundaries for the specified TimeBucketKey.

    * All results are guaranteed to in ascending order by timestamp.

* Example:

```curl -H 'Content-Type: application/json' http://127.0.0.1:5992/rpc --data-binary '{"jsonrpc":"2.0", "method":"DataService.Query", "id":1,"params":{"requests":[{"destination":{"key":"TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup"}, "timestart":1497484800, "timeend":4294967296}]}}'```
```
{
	"jsonrpc": "2.0",
	"result": {
		"Responses": [
			{
				"Result": {
					"Header": "?NUMPY?{'descr': [('Epoch', '<i8', (1533,)), ('Open', '<f4', (1533,)), ('High', '<f4', (1533,)), ('Low', '<f4', (1533,)), ('Close', '<f4', (1533,)), ('Volume', '<i4', (1533,)), ], 'fortran_order': False, 'shape': (1,),}",
					"ColumnNames": [
						"Epoch",
						"Open",
						"High",
						"Low",
						"Close",
						"Volume"
					],
					"ColumnData": ["nItCWQAAAAD...."],
					"StartIndex": {
						"TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup":0
					},
					"Lengths": {
						"TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup":1533
					}
				},
				"PreviousTime": {
					"TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup":1497470401
				},
				"Version":"dev"
			}
		]
	},
	"id":1
}
```

```curl -H 'Content-Type: application/json' http://127.0.0.1:5992/rpc --data-binary '{"jsonrpc":"2.0", "method":"DataService.Query", "id":1,"params":{"requests":[{"destination":{"key":"TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup"}, "timestart":1497484800, "timeend":4294967296}, {"destination":{"key":"NVDA/5Min/OHLCV:Symbol/Timeframe/AttributeGroup"}, "timestart":1500000000, "timeend":4294967296}]}}'```
```
{
	"jsonrpc": "2.0",
	"result": {
		"Responses": [
			{
				"Result": {
					"Header": "?NUMPY?{'descr': [('Epoch', '<i8', (1533,)), ('Open', '<f4', (1533,)), ('High', '<f4', (1533,)), ('Low', '<f4', (1533,)), ('Close', '<f4', (1533,)), ('Volume', '<i4', (1533,)), ], 'fortran_order': False, 'shape': (1,),}",
					"ColumnNames": [
						"Epoch",
						"Open",
						"High",
						"Low",
						"Close",
						"Volume"
					],
					"ColumnData": ["nItCWQAAAAD...."],
					"StartIndex": {
						"TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup":0
					},
					"Lengths": {
						"TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup":1533
					}
				},
				"PreviousTime": {
					"TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup":1497470401
				},
				"Version":"dev"
			},
			{
				"Result": null,
				"PreviousTime": {
					"NVDA/5Min/OHLCV:Symbol/Timeframe/AttributeGroup":1497556800
				},
				"Version":"dev"
			}
		]
	},
	"id":1
}
```
Note: The above query for multiple TimeBucketKeys also showcases what an empty reply looks like with a return value of null.

**DataService.Write()**

* Input: Data, IsVariableLength

	* Data: A dictionary containing Header, ColumnNames, ColumnData, Length, StartIndex, and Lengths.

		* Header: A Numpy ndarray header.

		* ColumnNames: A list of column names in the Numpy ndarray.

		* ColumnData: Serialized byte format of a Numpy ndarray. The ndarray is flattened into a single list, then the underlying byte format of the list is generated.

		* Length: The length of a row in a Numpy ndarray.

		* StartIndex: A dictionary containing a TimeBucketKey and the starting index of where the TimeBucketKey's data begins. For example, {"TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup":0}. This is useful for packing multiple Numpy ndarray's into one write call.

		* Lengths: A dictionary containing a TimeBucketKey and the length of a row in the TimeBucketKey's Numpy ndarray. For example, {"TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup":1399}. This is useful for packing multiple Numpy ndarray's into one write call.

	* IsVariableLength: A boolean value for telling Marketstore if the write procedure will be dynamic in length.
* Output: The API will return an empty response on success. Should the write call fail, the response will include the original input as well as an error returned by the server.
