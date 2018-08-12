package session

import (
	"fmt"
	"strings"
)

// functionHelp prints helpful information about specific commands.
func (c *Client) functionHelp(line string) {
	args := strings.Split(line, " ")
	args = args[1:] // chop off the first word which should be "help"
	var helpKey string
	if len(args) == 0 {
		helpKey = "help"
	} else {
		helpKey = args[0]
	}
	switch helpKey {
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
		Usage: \help command_name

		Available commands: show, trim, gaps, load, create, feed`)

	case "create":
		fmt.Println(`
		The create command generates new subdirectories and buckets for a database, and requires specially formatted schema keys as arguments.

		Syntax:

			>> \create <full key schema> <row data shape schema> <row type>

		- Example: We create a new DB entry to store 1 minute candles for TSLA:

			>> \create TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup Open,High,Low,Close/float32:Volume/int32 fixed

		where:

		<full key schema>: The metadata key to be created. A combination of item name key and
		 	category key: Name1/Name2/Name3:Cat1/Cat2/Cat3

		- Example: If we have Symbol/Timeframe/AttributeGroup as categories, we might have
		TSLA/1Min/OHLCV as item names and the full key would be:

			<full key schema> = TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup

		<row data shape schema>: The data types for each element
			in a row: name1,name2,name3/type:name4,name5/type:name6/type

		- Example: We have OHLCV data where prices are 32-bit floats and volume is 32-bit int:

			<row data shape schema> = Open,High,Low,Close/float32:Volume/int32

		<row type>: The type of rows to be stored, one of "fixed" or "variable":

		- Example: We are storing tick data, where each time interval can contain a variable
		number of rows:

			<row type> = variable`)

	default:
		fmt.Printf("No help available for %s\n", helpKey)
	}
}
