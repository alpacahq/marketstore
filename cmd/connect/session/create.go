package session

import (
	"fmt"
	"strings"

	"github.com/alpacahq/marketstore/frontend"
)

// create generates new subdirectories and buckets for a database.
func (c *Client) create(line string) {
	args := strings.Split(line, " ")
	args = args[1:] // chop off the first word which should be "create"

	req := frontend.CreateRequest{Key: args[0], DataShapes: args[1], RowType: args[2]}
	reqs := &frontend.MultiCreateRequest{
		Requests: []frontend.CreateRequest{req},
	}
	responses := &frontend.MultiServerResponse{}
	var err error
	if c.mode == local {
		ds := frontend.DataService{}
		err = ds.Create(nil, reqs, responses)
	} else {
		var respI interface{}
		respI, err = c.rc.DoRPC("Create", reqs)
		if respI != nil {
			responses = respI.(*frontend.MultiServerResponse)
		}
	}
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

// destroy generates new subdirectories and buckets for a database.
func (c *Client) destroy(line string) {
	args := strings.Split(line, " ")
	args = args[1:] // chop off the first word which should be "destroy"

	req := frontend.DestroyRequest{Key: args[0]}
	reqs := &frontend.MultiDestroyRequest{
		Requests: []frontend.DestroyRequest{req},
	}
	responses := &frontend.MultiServerResponse{}
	var err error
	if c.mode == local {
		ds := frontend.DataService{}
		err = ds.Destroy(nil, reqs, responses)
	} else {
		var respI interface{}
		respI, err = c.rc.DoRPC("Destroy", reqs)
		if respI != nil {
			responses = respI.(*frontend.MultiServerResponse)
		}
	}
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
	fmt.Printf("Successfully removed a catalog entry: %s\n", args[0])
}
