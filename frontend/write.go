package frontend

import (
	"net/http"

	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
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
			response.appendResponse(err)
			continue
		}
		if err = executor.WriteCSM(csm, req.IsVariableLength); err != nil {
			response.appendResponse(err)
			continue
		}
		//TODO: There should be an error response for every server request, need to add the below commented line
		//appendResponse(err, response)
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
			response.appendResponse(err)
			continue
		}
		tbk := io.NewTimeBucketKey(parts[0], parts[1])
		if tbk == nil {
			err = fmt.Errorf("key \"%s\" is not in proper format, should be like: TSLA/1Min/OHLCV:Symbol/TimeFrame/AttributeGroup",
				req.Key)
			response.appendResponse(err)
			continue
		}

		dsv, err := io.DataShapesFromInputString(req.DataShapes)
		if err != nil {
			response.appendResponse(err)
			continue
		}

		rowType := req.RowType
		switch rowType {
		case "fixed", "variable":
		default:
			err = fmt.Errorf("record type \"%s\" is not one of fixed or variable\n", rowType)
			response.appendResponse(err)
			continue
		}

		rootDir := executor.ThisInstance.RootDir
		year := int16(time.Now().Year())
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			response.appendResponse(err)
			continue
		}
		rt := io.EnumRecordTypeByName(rowType)
		tbinfo := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Default", year, dsv, rt)

		err = executor.ThisInstance.CatalogDir.AddTimeBucket(tbk, tbinfo)
		if err != nil {
			err = fmt.Errorf("creation of new catalog entry failed: %s", err.Error())
			response.appendResponse(err)
			continue
		}
		response.appendResponse(err)
	}
	return nil
}

type KeyRequest struct {
	Key string `msgpack:"key"`
}
type MultiKeyRequest struct {
	Requests []KeyRequest `msgpack:"requests"`
}

type GetInfoResponse struct {
	LatestYear int
	TimeFrame  time.Duration
	DSV        []io.DataShape
	RecordType io.EnumRecordType
	ServerResp ServerResponse
}

type MultiGetInfoResponse struct {
	Responses []GetInfoResponse `msgpack:"responses"`
}

func (s *DataService) GetInfo(r *http.Request, reqs *MultiKeyRequest, response *MultiGetInfoResponse) (err error) {
	errorString := "key \"%s\" is not in proper format, should be like: TSLA/1Min/OHLCV"

	for _, req := range reqs.Requests {
		// Construct a time bucket key from the input string
		parts := strings.Split(req.Key, ":")
		if len(parts) < 2 {
			// The schema string is optional for Delete, so we append a blank if none is provided
			parts = append(parts, "")
		}

		tbk := io.NewTimeBucketKey(parts[0], parts[1])
		if tbk == nil {
			err = fmt.Errorf(errorString, req.Key)
			response.appendResponse(nil, err)
			continue
		}

		tbi, err := executor.ThisInstance.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
		if err != nil {
			err = fmt.Errorf("unable to get info about key %s: %s", req.Key, err.Error())
			response.appendResponse(nil, err)
			continue
		}
		response.appendResponse(tbi, err)
	}

	return nil
}

func (s *DataService) Destroy(r *http.Request, reqs *MultiKeyRequest, response *MultiServerResponse) (err error) {
	errorString := "key \"%s\" is not in proper format, should be like: TSLA/1Min/OHLCV"

	for _, req := range reqs.Requests {
		// Construct a time bucket key from the input string
		parts := strings.Split(req.Key, ":")
		if len(parts) < 2 {
			// The schema string is optional for Delete, so we append a blank if none is provided
			parts = append(parts, "")
		}

		tbk := io.NewTimeBucketKey(parts[0], parts[1])
		if tbk == nil {
			err = fmt.Errorf(errorString, req.Key)
			response.appendResponse(err)
			continue
		}

		err = executor.ThisInstance.CatalogDir.RemoveTimeBucket(tbk)
		if err != nil {
			err = fmt.Errorf("removal of catalog entry failed: %s", err.Error())
			response.appendResponse(err)
			continue
		}
		response.appendResponse(err)
	}

	return nil
}

/*
Utility functions
*/

func (mr *MultiServerResponse) appendResponse(err error) {
	var errorText string
	if err == nil {
		errorText = ""
	} else {
		errorText = err.Error()
	}
	mr.Responses = append(mr.Responses,
		ServerResponse{
			errorText,
			utils.GitHash,
		},
	)
}

func (mg *MultiGetInfoResponse) appendResponse(tbi *io.TimeBucketInfo, err error) {
	var errorText string
	if err == nil {
		errorText = ""
	} else {
		errorText = err.Error()
	}
	if tbi != nil {
		mg.Responses = append(mg.Responses,
			GetInfoResponse{
				LatestYear: int(tbi.Year),
				TimeFrame:  tbi.GetTimeframe(),
				DSV:        tbi.GetDataShapesWithEpoch(),
				RecordType: tbi.GetRecordType(),
				ServerResp: ServerResponse{
					errorText,
					utils.GitHash,
				},
			},
		)
	} else {
		mg.Responses = append(mg.Responses,
			GetInfoResponse{
				LatestYear: 0,
				TimeFrame:  time.Duration(0),
				DSV:        nil,
				RecordType: 0,
				ServerResp: ServerResponse{
					errorText,
					utils.GitHash,
				},
			},
		)
	}
}
