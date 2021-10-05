package session

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// NewRemoteAPIClient generates a new client struct.
func NewRemoteAPIClient(url string, client RPCClient) (rc *RemoteAPIClient, err error) {
	// TODO: validate url using go core packages.
	splits := strings.Split(url, ":")
	if len(splits) != 2 {
		msg := fmt.Sprintf("incorrect URL, need \"hostname:port\", have: %s\n", url)
		return nil, errors.New(msg)
	}
	// build url.
	url = "http://" + url
	return &RemoteAPIClient{url: url, rpcClient: client}, nil
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
	if respI != nil {
		*responses = *respI.(*frontend.MultiServerResponse)
	}
	return err
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

	resp, err := rc.rpcClient.DoRPC("Query", args)
	if err != nil {
		return nil, err
	}

	return *resp.(*io.ColumnSeriesMap), nil
}

func (rc *RemoteAPIClient) Create(reqs *frontend.MultiCreateRequest, responses *frontend.MultiServerResponse) error {
	var respI interface{}
	respI, err := rc.rpcClient.DoRPC("Create", reqs)
	if respI != nil {
		*responses = *respI.(*frontend.MultiServerResponse)
	}
	return err

}

func (rc *RemoteAPIClient) Destroy(reqs *frontend.MultiKeyRequest, responses *frontend.MultiServerResponse) error {
	var respI interface{}
	respI, err := rc.rpcClient.DoRPC("Destroy", reqs)
	if respI != nil {
		*responses = *respI.(*frontend.MultiServerResponse)
	}
	return err
}

func (rc *RemoteAPIClient) GetBucketInfo(reqs *frontend.MultiKeyRequest, responses *frontend.MultiGetInfoResponse,
) error {
	var (
		respI interface{}
	)
	respI, err := rc.rpcClient.DoRPC("GetInfo", reqs)
	if err != nil {
		return fmt.Errorf("DoRPC:GetInfo error:%w", err)
	}

	if respI != nil {
		if val, ok := respI.(*frontend.MultiGetInfoResponse); ok {
			*responses = *val
		} else {
			return fmt.Errorf("[bug] unexpected data type returned from DoRPC func. resp=%v", respI)
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
		return nil, err
	}

	for _, sub := range *resp.(*io.ColumnSeriesMap) {
		cs = sub
		break
	}
	return cs, err
}
