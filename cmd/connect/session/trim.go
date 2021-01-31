package session

import (
	"fmt"
	"os"
	"strings"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// trim removes the data in the date range from the db.
// Note that this is implemented only for LocalClient.
func (c *Client) trim(line string) {
	cli, ok := c.apiClient.(*LocalAPIClient)
	if !ok {
		fmt.Printf("Trim command can be used only when '--dir' is specified to connect marketstore.")
		return
	}

	log.Info("Trimming...")
	args := strings.Split(line, " ")
	if len(args) < 3 {
		fmt.Println("Not enough arguments - need \"trim key date\"")
		return
	}
	trimDate, err := parseTime(args[len(args)-1])
	if err != nil {
		log.Error("Failed to parse trim date - Error: %v", trimDate)
	}
	fInfos := cli.catalogDir.GatherTimeBucketInfo()
	for _, info := range fInfos {
		if info.Year == int16(trimDate.Year()) {
			offset := io.TimeToOffset(trimDate, info.GetTimeframe(), info.GetRecordLength())
			fp, err := os.OpenFile(info.Path, os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				log.Error("Failed to open file %v - Error: %v", info.Path, err)
				continue
			}
			fp.Seek(offset, os.SEEK_SET)
			zeroes := make([]byte, io.FileSize(info.GetTimeframe(), int(info.Year), int(info.GetRecordLength()))-offset)
			fp.Write(zeroes)
			fp.Close()
		}
	}
}
