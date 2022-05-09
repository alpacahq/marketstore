package session

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// sql executes a sql statement against the current db.
func (c *Client) sql(line string) {
	timeStart := time.Now()

	cs, err := c.apiClient.SQL(line)
	if err != nil {
		log.Error(err.Error())
		return
	}

	runTime := time.Since(timeStart)

	err = printResult(line, cs, c.target)
	if err != nil {
		log.Error(err.Error())
	}

	if c.printExecutionTime {
		log.Info(fmt.Sprintf("Elapsed query time: %5.3f ms\n", 1000*runTime.Seconds()))
	}
}
