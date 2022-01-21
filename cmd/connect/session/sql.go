package session

import (
	"fmt"
	"time"
)

// sql executes a sql statement against the current db.
func (c *Client) sql(line string) {
	timeStart := time.Now()

	cs, err := c.apiClient.SQL(line)
	if err != nil {
		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Println(err)
		return
	}

	runTime := time.Since(timeStart)

	err = printResult(line, cs, c.target)
	if err != nil {
		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Println(err.Error())
	}

	if c.timing {
		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Printf("Elapsed query time: %5.3f ms\n", 1000*runTime.Seconds())
	}
}
