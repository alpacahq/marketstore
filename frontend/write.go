package frontend

import (
	"net/http"

	"fmt"
	"strings"
	"time"

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

type ServerResponse struct {
	Error   string `msgpack:"error"`
	Version string `msgpack:"version"` // Server Version
}

type MultiServerResponse struct {
	Responses []ServerResponse `msgpack:"responses"`
}

func (s *DataService) Write(r *http.Request, reqs *MultiWriteRequest, response *MultiServerResponse) (err error) {
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
		//TODO: There should be an error response for every server request, need to add the below commented line
		//appendErrorResponse(err, response)
	}
	return nil
}

/*
	Create: Creates a new time bucket in the DB
*/
type CreateRequest struct {
	Key, DataShapes, RowType string
}
type MultiCreateRequest struct {
	Requests []CreateRequest
}

func (s *DataService) Create(r *http.Request, reqs *MultiCreateRequest, response *MultiServerResponse) (err error) {
	for _, req := range reqs.Requests {
		// Construct a time bucket key from the input string
		parts := strings.Split(req.Key, ":")
		if len(parts) != 2 {
			err = fmt.Errorf("key \"%s\" is not in proper format, should be like: TSLA/1Min/OHLCV:Symbol/TimeFrame/AttributeGroup",
				req.Key)
			appendErrorResponse(err, response)
			continue
		}
		tbk := io.NewTimeBucketKey(parts[0], parts[1])
		if tbk == nil {
			err = fmt.Errorf("key \"%s\" is not in proper format, should be like: TSLA/1Min/OHLCV:Symbol/TimeFrame/AttributeGroup",
				req.Key)
			appendErrorResponse(err, response)
			continue
		}

		dsv, err := io.DataShapesFromInputString(req.DataShapes)
		if err != nil {
			appendErrorResponse(err, response)
			continue
		}

		rowType := req.RowType
		switch rowType {
		case "fixed", "variable":
		default:
			err = fmt.Errorf("record type \"%s\" is not one of fixed or variable\n", rowType)
			appendErrorResponse(err, response)
			continue
		}

		rootDir := executor.ThisInstance.RootDir
		year := int16(time.Now().Year())
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			appendErrorResponse(err, response)
			continue
		}
		rt := io.EnumRecordTypeByName(rowType)
		tbinfo := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Default", year, dsv, rt)

		err = executor.ThisInstance.CatalogDir.AddTimeBucket(tbk, tbinfo)
		if err != nil {
			err = fmt.Errorf("creation of new catalog entry failed: %s", err.Error())
			appendErrorResponse(err, response)
			continue
		}
		appendErrorResponse(err, response)
	}
	return nil
}

/*
Utility functions
*/

func appendErrorResponse(err error, response *MultiServerResponse) {
	var errorText string
	if err == nil {
		errorText = ""
	} else {
		errorText = err.Error()
	}
	response.Responses = append(response.Responses,
		ServerResponse{
			errorText,
			utils.GitHash,
		},
	)
}
