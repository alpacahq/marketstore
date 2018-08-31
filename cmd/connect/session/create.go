package session

import (
	"fmt"
	"strings"

	"github.com/alpacahq/marketstore/frontend"
)

// create generates new subdirectories and buckets for a database.
func (c *Client) create(line string) {
	if c.mode != local {
		fmt.Println("Create currently only functions in local mode")
		return
	}
	args := strings.Split(line, " ")
	args = args[1:] // chop off the first word which should be "create"

	reqs := frontend.MultiCreateRequest{
		Requests: []frontend.CreateRequest{
			{Key: args[0], DataShapes: args[1], RowType: args[2]},
		}}
	var responses frontend.MultiServerResponse
	ds := frontend.DataService{}
	err := ds.Create(nil, &reqs, &responses)
	if err != nil {
		fmt.Printf("Failed with error: %s\n", err.Error())
		return
	}

	for _, resp := range responses.Responses {
		if len(resp.Error) != 0 {
			fmt.Printf("Failed with error: %s\n", resp.Error)
			return
		}
	}
	fmt.Printf("Successfully created a new catalog entry\n")
}
