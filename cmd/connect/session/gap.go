package session

import (
	"fmt"
	"os"
	"strings"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	. "github.com/alpacahq/marketstore/utils/log"
)

// findGaps finds gaps in data in the date range.
func (c *Client) findGaps(line string) {
	args := strings.Split(line, " ")
	args = args[1:]
	c.target = terminal
	tbk, start, end := c.parseQueryArgs(args)

	query := planner.NewQuery(executor.ThisInstance.CatalogDir)
	query.AddTargetKey(tbk)

	if start != nil && end != nil {
		query.SetRange(start.Unix(), end.Unix())
	} else if end == nil {
		query.SetRange(start.Unix(), planner.MaxEpoch)
	}

	pr, err := query.Parse()
	if err != nil {
		Log(ERROR, "Parsing query: %v", err)
		os.Exit(1)
	}

	scanner, err := executor.NewReader(pr)
	if err != nil {
		Log(ERROR, "Error return from query scanner: %v", err)
		return
	}
	csm, _, err := scanner.Read()
	if err != nil {
		Log(ERROR, "Error return from query scanner: %v", err)
		return
	}

	/*
		For each of the symbols in the returned set, count the number of samples
	*/
	dataCountResults := make(map[string]int, len(csm))
	averageResult := float64(0)
	for key, cs := range csm {
		sym := key.GetItemInCategory("Symbol")
		epochs := cs.GetEpoch()
		dataCountResults[sym] = len(epochs)
		averageResult += float64(dataCountResults[sym])
	}
	averageResult /= float64(len(dataCountResults))

	fmt.Printf("The average number of records: %6.3f\n", averageResult)
	fmt.Printf("Following are the symbols that deviate from the average by more than 10 percent:\n")
	numZeros := 0
	for sym, count := range dataCountResults {
		if float64(count) <= 0.9*averageResult {
			if count == 0 {
				fmt.Printf("Sym: %s  Zero Count\n", sym)
				numZeros++
			} else {
				fmt.Printf("Sym: %s  Count: %d\n", sym, count)
			}
		}
	}

	fmt.Printf("Number of Zero data: %d\n", numZeros)
}
