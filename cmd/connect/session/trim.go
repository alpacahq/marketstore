package session

import (
	"fmt"
	"os"
	"strings"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/log"
)

// trim removes the data in the date range from the db.
func (c *Client) trim(line string) {
	Log(INFO, "Trimming...")
	args := strings.Split(line, " ")
	if len(args) < 3 {
		fmt.Println("Not enough arguments - need \"trim key date\"")
		return
	}
	trimDate, err := parseTime(args[len(args)-1])
	if err != nil {
		Log(ERROR, "Failed to parse trim date - Error: %v", trimDate)
	}
	fInfos := executor.ThisInstance.CatalogDir.GatherTimeBucketInfo()
	for _, info := range fInfos {
		if info.Year == int16(trimDate.Year()) {
			offset := io.TimeToOffset(trimDate, info.GetTimeframe(), info.GetRecordLength())
			fp, err := os.OpenFile(info.Path, os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				Log(ERROR, "Failed to open file %v - Error: %v", info.Path, err)
				continue
			}
			fp.Seek(offset, os.SEEK_SET)
			zeroes := make([]byte, io.FileSize(info.GetTimeframe(), int(info.Year), int(info.GetRecordLength()))-offset)
			fp.Write(zeroes)
			fp.Close()
		}
	}
}
