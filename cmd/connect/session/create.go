package session

import (
	"fmt"
	"strings"

	"github.com/alpacahq/marketstore/v4/utils/io"

	"github.com/alpacahq/marketstore/v4/frontend"
)

// getinfo gets information about a bucket in the database.
func (c *Client) getinfo(line string) {
	args := strings.Split(line, " ")
	args = args[1:] // chop off the first word which should be "getinfo"

	/*
		req := frontend.KeyRequest{Key: args[0]}
		reqs := &frontend.MultiKeyRequest{
			Requests: []frontend.KeyRequest{req},
		}
		responses := &frontend.MultiGetInfoResponse{}
		var err error
		if c.mode == local {
			ds := frontend.DataService{}
			err = ds.GetInfo(nil, reqs, responses)
		} else {
			var respI interface{}
			respI, err = c.rc.DoRPC("GetInfo", reqs)
			if respI != nil {
				responses = respI.(*frontend.MultiGetInfoResponse)
			}
		}
		if err != nil {
			fmt.Printf("Failed with error: %s\n", err.Error())
			return
		}
	*/

	tbk_p := io.NewTimeBucketKey(args[0])
	if tbk_p == nil {
		fmt.Printf("Failed to convert argument to key: %s\n", args[0])
		return
	}
	tbk := *tbk_p
	resp, err := c.GetBucketInfo(tbk)
	if err != nil {
		fmt.Printf("Failed with error: %s\n", err.Error())
		return
	}
	/*
		Process the single response
	*/
	// Print out the bucket information we obtained
	fmt.Printf("Bucket: %s\n", args[0])
	fmt.Printf("Latest Year: %v, RecordType: %v, TF: %v\n",
		resp.LatestYear, resp.RecordType.String(), resp.TimeFrame)
	fmt.Printf("Data Types: {")
	for i, shape := range resp.DSV {
		fmt.Printf("%s", shape.String())
		if i < len(resp.DSV)-1 {
			fmt.Printf(" ")
		}
	}
	fmt.Printf("}\n")
}

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
	fmt.Printf("Successfully created a new catalog entry for bucket %s\n", args[0])
}

// destroy removes the subdirectories and buckets for a provided key
func (c *Client) destroy(line string) {
	args := strings.Split(line, " ")
	args = args[1:] // chop off the first word which should be "destroy"

	req := frontend.KeyRequest{Key: args[0]}
	reqs := &frontend.MultiKeyRequest{
		Requests: []frontend.KeyRequest{req},
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
	fmt.Printf("Successfully removed catalog entry for key: %s\n", args[0])
}

func (c *Client) GetBucketInfo(key io.TimeBucketKey) (resp *frontend.GetInfoResponse, err error) {
	req := frontend.KeyRequest{Key: key.String()}
	reqs := &frontend.MultiKeyRequest{
		Requests: []frontend.KeyRequest{req},
	}
	responses := &frontend.MultiGetInfoResponse{}

	if c.mode == local {
		ds := frontend.DataService{}
		err = ds.GetInfo(nil, reqs, responses)
	} else {
		var respI interface{}
		respI, err = c.rc.DoRPC("GetInfo", reqs)
		if respI != nil {
			responses = respI.(*frontend.MultiGetInfoResponse)
		}
	}
	if err != nil {
		return nil, err
	}

	/*
		Process the single response
	*/
	resp = &responses.Responses[0]
	if len(resp.ServerResp.Error) != 0 {
		return nil, fmt.Errorf("%s", resp.ServerResp.Error)
	}

	return resp, nil
}
