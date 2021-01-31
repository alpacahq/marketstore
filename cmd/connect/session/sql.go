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
		fmt.Println(err)
		return
	}

	runTime := time.Since(timeStart)

	err = printResult(line, cs, c.target)
	if err != nil {
		fmt.Println(err.Error())
	}

	if c.timing {
		fmt.Printf("Elapsed query time: %5.3f ms\n", 1000*runTime.Seconds())
	}
}
