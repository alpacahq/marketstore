package session

import (
	"fmt"
	"strings"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// getinfo gets information about a bucket in the database.
func (c *Client) getinfo(line string) error {
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

	tbkP := io.NewTimeBucketKey(args[0])
	resp, err := c.GetBucketInfo(tbkP)
	if err != nil {
		log.Error("Failed with error: %s\n", err.Error())
		return fmt.Errorf("failed with error: %w", err)
	}
	/*
		Process the single response
	*/
	// Print out the bucket information we obtained
	// nolint:forbidigo // CLI output needs fmt.Println
	fmt.Printf("Bucket: %s\n", args[0])
	// nolint:forbidigo // CLI output needs fmt.Println
	fmt.Printf("Latest Year: %v, RecordType: %v, TF: %v\n",
		resp.LatestYear, resp.RecordType.String(), resp.TimeFrame)
	// nolint:forbidigo // CLI output needs fmt.Println
	fmt.Printf("Data Types: {")
	for i, shape := range resp.DSV {
		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Printf("%s", shape.String())
		if i < len(resp.DSV)-1 {
			// nolint:forbidigo // CLI output needs fmt.Println
			fmt.Printf(" ")
		}
	}
	// nolint:forbidigo // CLI output needs fmt.Println
	fmt.Printf("}\n")
	return nil
}

// create generates new subdirectories and buckets for a database.
// It returns true if the bucket is successfully created.
func (c *Client) create(line string) error {
	args := strings.Split(line, " ")
	args = args[1:] // chop off the first word which should be "create"
	// args[0]:tbk, args[1]:dataTypeStr, args[2]:"fixed" or "variable"
	const argLen = 3
	if len(args) < argLen {
		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Println(`Not enough arguments - need "\create [tbk] [dataTypeStr] [recordType('fixed' or 'variable')]"`)
		// nolint:forbidigo // CLI output needs fmt.Println
		fmt.Println(`example usage: "\create TEST/1Min/OHLCV:Symbol/Timeframe/AttributeGroup ` +
			`Open,High,Low,Close/float32:Volume/int64 variable"`)
		return fmt.Errorf(`not enough arguments - need "\create [tbk] [dataTypeStr] [recordType('fixed' or 'variable')]"`)
	}

	columnNames, columnTypes, err := toColumns(args[1])
	if err != nil {
		log.Error("Failed with error: %s", err.Error())
		return fmt.Errorf("create command failed with error: %w", err)
	}

	var isVariableLength bool
	switch args[2] {
	case "fixed":
		isVariableLength = false
	case "variable":
		isVariableLength = true
	default:
		log.Error("record type \"%s\" is not one of fixed or variable\n", args[2])
		return fmt.Errorf("record type \"%s\" is not one of fixed or variable", args[2])
	}

	req := frontend.CreateRequest{
		Key:              args[0],
		ColumnNames:      columnNames,
		ColumnTypes:      columnTypes,
		IsVariableLength: isVariableLength,
	}
	reqs := &frontend.MultiCreateRequest{
		Requests: []frontend.CreateRequest{req},
	}
	responses := &frontend.MultiServerResponse{}

	err = c.apiClient.Create(reqs, responses)
	if err != nil {
		log.Error("Failed with error: %s", err.Error())
		return fmt.Errorf("create command failed with error: %w", err)
	}

	for _, resp := range responses.Responses {
		if resp.Error != "" {
			log.Error("Failed with error: %v", resp.Error)
			return fmt.Errorf("create command failed with error: %w", err)
		}
	}
	log.Info("Successfully created a new catalog entry for bucket %s\n", args[0])
	return nil
}

func toColumns(dataShapeStr string) (columnNames, columnTypeStrs []string, err error) {
	// e.g. dataShapeStr = "Epoch,Open,High,Low,Close/float32,Volume/int32"
	dsv, err := io.DataShapesFromInputString(dataShapeStr)
	if err != nil {
		return nil, nil, err
	}

	columnNames = make([]string, len(dsv))
	columnTypeStrs = make([]string, len(dsv))
	for i, ds := range dsv {
		columnNames[i] = ds.Name
		typeStr, ok := io.ToTypeStr(ds.Type) // e.g. i8, f4
		if !ok {
			return nil, nil,
				fmt.Errorf("type:%v is not supported", ds.Type)
		}

		columnTypeStrs[i] = typeStr
	}

	return columnNames, columnTypeStrs, nil
}

// destroy removes the subdirectories and buckets for a provided key.
func (c *Client) destroy(line string) error {
	args := strings.Split(line, " ")
	args = args[1:]  // chop off the first word which should be "destroy"
	const argLen = 1 // arg[0] should be a bucket name
	if len(args) == 0 {
		log.Error("Please specify a bucket to destroy. (e.g. \\destroy TEST/1D/TICK)\n")
		return nil
	}
	if len(args) > argLen {
		log.Error("Only one bucket can be deleted at a time.\n")
		return nil
	}

	req := frontend.KeyRequest{Key: args[0]}
	reqs := &frontend.MultiKeyRequest{
		Requests: []frontend.KeyRequest{req},
	}
	responses := &frontend.MultiServerResponse{}

	err := c.apiClient.Destroy(reqs, responses)
	if err != nil {
		log.Error("Failed with error: %s\n", err.Error())
		return fmt.Errorf("failed with error: %w", err)
	}

	for _, resp := range responses.Responses {
		if resp.Error != "" {
			log.Error("Failed with error: %s\n", resp.Error)
			return fmt.Errorf("failed with error: %w", err)
		}
	}
	log.Info("Successfully removed catalog entry for key: %s\n", args[0])
	return nil
}

func (c *Client) GetBucketInfo(key *io.TimeBucketKey) (resp *frontend.GetInfoResponse, err error) {
	req := frontend.KeyRequest{Key: key.String()}
	reqs := &frontend.MultiKeyRequest{
		Requests: []frontend.KeyRequest{req},
	}
	responses := &frontend.MultiGetInfoResponse{}

	err = c.apiClient.GetBucketInfo(reqs, responses)
	if err != nil {
		return nil, err
	}

	/*
		Process the single response
	*/
	if len(responses.Responses) == 0 {
		return nil, fmt.Errorf("no BucketInfo is returned for %v", key)
	}
	resp = &responses.Responses[0]
	if resp.ServerResp.Error != "" {
		return nil, fmt.Errorf("%s", resp.ServerResp.Error)
	}

	return resp, nil
}
