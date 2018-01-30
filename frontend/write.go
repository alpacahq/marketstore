package frontend

import (
	"net/http"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
)

type WriteRequest struct {
	Data             *io.NumpyMultiDataset `msgpack:"dataset"`
	IsVariableLength bool                  `msgpack:"is_variable_length"`
}

type MultiWriteRequest struct {
	/*
		A multi-request allows for different Timeframes and record formats for each request
	*/
	Requests []WriteRequest `msgpack:"requests"`
}

type WriteResponse struct {
	Error   string `msgpack:"error"`
	Version string `msgpack:"version"` // Server Version
}

type MultiWriteResponse struct {
	Responses []WriteResponse `msgpack:"responses"`
}

func (s *DataService) Write(r *http.Request, reqs *MultiWriteRequest, response *MultiWriteResponse) (err error) {
	for _, req := range reqs.Requests {
		csm, err := req.Data.ToColumnSeriesMap()
		if err != nil {
			appendErrorResponse(err, response)
			continue
		}
		if err = executor.WriteCSM(csm, req.IsVariableLength); err != nil {
			appendErrorResponse(err, response)
			continue
		}
	}
	return nil
}

/*
Utility functions
*/

func appendErrorResponse(err error, response *MultiWriteResponse) {
	response.Responses = append(response.Responses,
		WriteResponse{
			err.Error(),
			utils.GitHash,
		},
	)
}
