package session

import (
	"fmt"
	"os"
	"time"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// NewRemoteAPIClient generates a new client struct.
func NewRemoteAPIClient(url string, client RPCClient) *RemoteAPIClient {
	return &RemoteAPIClient{url: url, rpcClient: client}
}

// RemoteAPIClient represents an agent that manages a database
// connection and parses/executes the statements specified by a
// user in a command-line buffer.
type RemoteAPIClient struct {
	// url is the optional address of a db instance on a different machine.
	url string
	// rpcClient is the optional remote client.
	rpcClient RPCClient
}

func (rc *RemoteAPIClient) PrintConnectInfo() {
	fmt.Fprintf(os.Stderr, "Connected to remote instance at: %v\n", rc.url)
}

func (rc *RemoteAPIClient) Write(reqs *frontend.MultiWriteRequest, responses *frontend.MultiServerResponse) error {
	var respI interface{}
	respI, err := rc.rpcClient.DoRPC("Write", reqs)
	if err != nil {
		return fmt.Errorf("DoRPC:Write error:%w", err)
	}

	if respI != nil {
		if val, ok := respI.(*frontend.MultiServerResponse); ok {
			*responses = *val
		} else {
			return fmt.Errorf("[bug] unexpected data type returned from DoRPC:Write func. resp=%v", respI)
		}
	}
	return nil
}

func (rc *RemoteAPIClient) Show(tbk *io.TimeBucketKey, start, end *time.Time) (csm io.ColumnSeriesMap, err error) {
	if end == nil {
		t := planner.MaxTime
		end = &t
	}
	epochStart := start.UTC().Unix()
	epochEnd := end.UTC().Unix()
	req := frontend.QueryRequest{
		IsSQLStatement: false,
		SQLStatement:   "",
		Destination:    tbk.String(),
		EpochStart:     &epochStart,
		EpochEnd:       &epochEnd,
	}
	args := &frontend.MultiQueryRequest{
		Requests: []frontend.QueryRequest{req},
	}

	respI, err := rc.rpcClient.DoRPC("Query", args)
	if err != nil {
		return nil, fmt.Errorf("DoRPC:Query error:%w", err)
	}

	if respI == nil {
		return io.ColumnSeriesMap{}, nil
	}

	val, ok := respI.(*io.ColumnSeriesMap)
	if !ok {
		return nil, fmt.Errorf("[bug] unexpected data type returned from DoRPC:Query func. resp=%v",
			respI)
	}
	return *val, nil
}

func (rc *RemoteAPIClient) Create(reqs *frontend.MultiCreateRequest, responses *frontend.MultiServerResponse) error {
	var respI interface{}
	respI, err := rc.rpcClient.DoRPC("Create", reqs)
	if err != nil {
		return fmt.Errorf("DoRPC:Create error:%w", err)
	}
	if respI != nil {
		if val, ok := respI.(*frontend.MultiServerResponse); ok {
			*responses = *val
		} else {
			return fmt.Errorf("[bug] unexpected data type returned from DoRPC:Create func. resp=%v", respI)
		}
	}
	return nil
}

func (rc *RemoteAPIClient) Destroy(reqs *frontend.MultiKeyRequest, responses *frontend.MultiServerResponse) error {
	var respI interface{}
	respI, err := rc.rpcClient.DoRPC("Destroy", reqs)
	if err != nil {
		return fmt.Errorf("DoRPC:Destroy error:%w", err)
	}
	if respI != nil {
		if val, ok := respI.(*frontend.MultiServerResponse); ok {
			*responses = *val
		} else {
			return fmt.Errorf("[bug] unexpected data type returned from DoRPC:Destroy func. resp=%v", respI)
		}
	}
	return nil
}

func (rc *RemoteAPIClient) GetBucketInfo(reqs *frontend.MultiKeyRequest, responses *frontend.MultiGetInfoResponse,
) error {
	var respI interface{}
	respI, err := rc.rpcClient.DoRPC("GetInfo", reqs)
	if err != nil {
		return fmt.Errorf("DoRPC:GetBucketInfo error:%w", err)
	}

	if respI != nil {
		if val, ok := respI.(*frontend.MultiGetInfoResponse); ok {
			*responses = *val
		} else {
			return fmt.Errorf("[bug] unexpected data type returned from DoRPC:GetBucketInfo func. resp=%v", respI)
		}
	}
	return nil
}

func (rc *RemoteAPIClient) SQL(line string) (cs *io.ColumnSeries, err error) {
	req := frontend.QueryRequest{
		IsSQLStatement: true,
		SQLStatement:   line,
	}
	args := &frontend.MultiQueryRequest{Requests: []frontend.QueryRequest{req}}

	resp, err := rc.rpcClient.DoRPC("Query", args)
	if err != nil {
		return nil, fmt.Errorf("DoRPC:Query SQL error: %w", err)
	}

	if val, ok := resp.(*io.ColumnSeriesMap); ok {
		for _, sub := range *val {
			cs = sub
			break
		}
	} else {
		return nil, fmt.Errorf("[bug] unexpected data type returned from DoRPC:SQL func. resp=%v", resp)
	}
	return cs, err
}
