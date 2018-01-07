package main

import (
	"fmt"
	"strings"
)

func processHelp(line string) {
	args := strings.Split(line, " ")
	args = args[1:] // chop off the first word which should be "help"
	var helpKey string
	if len(args) == 0 {
		helpKey = "help"
	} else {
		helpKey = args[0]
	}
	switch helpKey {
	case "feed":
		fmt.Println(`
			>> \feed [start,list,kill] options

		Manages data feed processes with options on a running Marketstore server
		The data feeds are plugins built to process upstream data provider sources
		Each feed is started as an asynchronous Go process and can be listed and
		killed using the PID of the process

		===== Starting a feed
			>> \feed start pluginName symbolList timeframe formatName pollingFrequency [variable]

		- Example 1: Three symbols polled every 30s and stored in a 1Min Timeframe bucket with record name SNAPQUOTE:
			>> \feed start yahoo-free-csv-poller AAPL,TSLA,MSFT 1Min SNAPQUOTE 30s

		- Example 2: One symbol (AAPL) polled every 10s and stored in a 1H Timeframe bucket with record name TICKS with
		variable length records:
			>> \feed start yahoo-free-csv-poller AAPL 1H TICKS 10s variable

		===== Listing the running feeds
			>> \feed list

		===== Kill running feeds
			>> \feed kill [pid,"all"]

		- Example 1: Kill a specific feed
			>> \feed list
			PID                     Description
			--------                --------------------------------------
       			2                VariableLen 1m0s:Polled yahoo-free-csv-poller.so 1H/TICKS [TSLA,MSFT,CG,JPM]
       			1                FixedLength 5s:Polled yahoo-free-csv-poller.so 1Min/SNAP1MIN [AAPL]
			>> \feed kill 2
			>> \feed list
			PID                     Description
			--------                --------------------------------------
       			1                FixedLength 5s:Polled yahoo-free-csv-poller.so 1Min/SNAP1MIN [AAPL]

		- Example 2: Kill all running feeds
			>> \feed list
			PID                     Description
			--------                --------------------------------------
       			1                FixedLength 5s:Polled yahoo-free-csv-poller.so 1Min/SNAP1MIN [AAPL]
       			3                FixedLength 5s:Polled yahoo-free-csv-poller.so 1Min/SNAP1MIN [AAPL]
       			4                FixedLength 5s:Polled yahoo-free-csv-poller.so 1Min/SNAP1MIN [JPM,MSFT,CG]
			>> \feed kill all
			>> \feed list
			PID                     Description
			--------                --------------------------------------

`)

	case "show", "trim", "gaps":
		fmt.Println(`
		Syntax: (same for show/trim/gaps):

			>> \show <Symbol/Timeframe/RecordFormat> <start time> [<end time>]

		- Example: start time only:

			>> \show TSLA/1Min/OHLCV 2016-09-15T13:30

		- Example: start and finish times:

			>> \show TSLA/1Min/OHLCV 2016-09-15 2016-09-16

	trim: removes the data in the date range from the DB
	show: displays data in the date range
	gaps: finds gaps in data in the date range`)

	case "load":
		fmt.Println(`
		The load command loads data into the DB from csv files.

		Syntax:

			>> \load <Symbol/Timeframe/RecordFormat> <csv input file> [<loader control file>]

		- Example:

			>> \load TSLA/1Min/RecordFormat test.csv test.yaml

		(optional) Loader control file format (YAML):
		- Example:
			firstRowHasColumnNames: false
			timeFormat: "20060102 150405"
			timeZone: "US/Eastern" # If not blank, the time format must not feature a timezone
			columnNameMap: [Epoch, Open, High, Low, Close, Volume]
			timeZone: if specified, this will override the timezone of the epoch found in the input file
			columnNameMap: optional mapping of column position to name

		Note: "Epoch" is a special name, as is "Epoch-date" and "Epoch-time"
		If the input file has the time index epoch in separate date and time columns, you will
		specify the epoch-date and epoch-time columns in the columnNameMap
	`)
	case "help":
		fmt.Println(`
		usage: help command_name

		Available commands: show, trim, gaps, load, create, feed`)

	case "create":
		fmt.Println(`
		Syntax:

			>> \create <full key spec> <row data shape spec> <row type>

		- Example: We create a new DB entry to store 1 minute candles for TSLA:

			>> \create TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup Open,High,Low,Close/float32:Volume/int32 fixed

		where:

		<full key spec>: The metadata key to be created. A combination of item name key and
		 	category key: Name1/Name2/Name3:Cat1/Cat2/Cat3

		- Example: If we have Symbol/Timeframe/AttributeGroup as categories, we might have
		TSLA/1Min/OHLCV as item names and the full key would be:

			<full key spec> = TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup

		<row data shape spec>: The data types for each element
			in a row: name1,name2,name3/type:name4,name5/type:name6/type

		- Example: We have OHLCV data where prices are 32-bit floats and volume is 32-bit int:

			<row data shape spec> = Open,High,Low,Close/float32:Volume/int32

		<row type>: The type of rows to be stored, one of "fixed" or "variable":

		- Example: We are storing tick data, where each time interval can contain a variable
		number of rows:

			<row type> = variable`)

	default:
		fmt.Printf("	No help available for %s...\n", helpKey)
	}
}
