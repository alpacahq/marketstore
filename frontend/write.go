package frontend

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

const colonSeparatedPartsLen = 2 // expecting a key string like "TSLA/1Min/OHLCV:Symbol/Timeframe/AttributeGroup"

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

func (s *DataService) Write(_ *http.Request, reqs *MultiWriteRequest, response *MultiServerResponse) (err error) {
	for _, req := range reqs.Requests {
		csm, err := req.Data.ToColumnSeriesMap()
		if err != nil {
			response.appendResponse(err)
			continue
		}
		if err = s.writer.WriteCSM(csm, req.IsVariableLength); err != nil {
			response.appendResponse(err)
			continue
		}
		// TODO: There should be an error response for every server request, need to add the below commented line
		// appendResponse(err, response)
	}
	return nil
}

/*
	Create: Creates a new time bucket in the DB
*/
type CreateRequest struct {
	// bucket key string. e.g. "TSLA/1Min/OHLC"
	Key string `msgpack:"key"`
	// a list of type strings such as i4 and f8
	ColumnTypes []string `msgpack:"column_types"`
	// a list of column names
	ColumnNames      []string `msgpack:"column_names"`
	IsVariableLength bool     `msgpack:"is_variable_length"`
}

type MultiCreateRequest struct {
	Requests []CreateRequest `msgpack:"requests"`
}

func (s *DataService) Create(_ *http.Request, reqs *MultiCreateRequest, response *MultiServerResponse) (err error) {
	for _, req := range reqs.Requests {
		// Construct a time bucket key from the input string
		parts := strings.Split(req.Key, ":")
		if len(parts) != colonSeparatedPartsLen {
			err = fmt.Errorf("key \"%s\" is not in proper format, should be like: " +
				"TSLA/1Min/OHLCV:Symbol/TimeFrame/AttributeGroup",
				req.Key)
			response.appendResponse(err)
			continue
		}

		tbk := io.NewTimeBucketKey(parts[0], parts[1])
		if tbk == nil {
			err = fmt.Errorf("key \"%s\" is not in proper format, should be like: " +
				"TSLA/1Min/OHLCV:Symbol/TimeFrame/AttributeGroup",
				req.Key)
			response.appendResponse(err)
			continue
		}

		// --- Timeframe
		year := int16(time.Now().Year())
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			response.appendResponse(err)
			continue
		}

		// --- Record Type
		var recordType io.EnumRecordType
		if req.IsVariableLength {
			recordType = io.VARIABLE
		} else {
			recordType = io.FIXED
		}

		// --- DataShapes
		dsv := make([]io.DataShape, len(req.ColumnNames))
		for i, name := range req.ColumnNames {
			t, ok := io.TypeStrToElemType(req.ColumnTypes[i])
			if !ok {
				response.appendResponse(fmt.Errorf("unexpected data type:%v", req.ColumnTypes[i]))
				return nil
			}

			dsv[i] = io.DataShape{Name: name, Type: t}
		}

		tbinfo := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(s.rootDir), "Default", year, dsv, recordType)

		err = s.catalogDir.AddTimeBucket(tbk, tbinfo)
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

func (s *DataService) GetInfo(_ *http.Request, reqs *MultiKeyRequest, response *MultiGetInfoResponse) (err error) {
	const errorString = "key \"%s\" is not in proper format, should be like: TSLA/1Min/OHLCV"

	for _, req := range reqs.Requests {
		// Construct a time bucket key from the input string
		parts := strings.Split(req.Key, ":")
		if len(parts) < colonSeparatedPartsLen {
			// The schema string is optional for Delete, so we append a blank if none is provided
			parts = append(parts, "")
		}

		tbk := io.NewTimeBucketKey(parts[0], parts[1])
		if tbk == nil {
			err = fmt.Errorf(errorString, req.Key)
			response.appendResponse(nil, err)
			continue
		}

		tbi, err := s.catalogDir.GetLatestTimeBucketInfoFromKey(tbk)
		if err != nil {
			err = fmt.Errorf("unable to get info about key %s: %s", req.Key, err.Error())
			response.appendResponse(nil, err)
			continue
		}
		response.appendResponse(tbi, err)
	}

	return nil
}

func (s *DataService) Destroy(_ *http.Request, reqs *MultiKeyRequest, response *MultiServerResponse) (err error) {
	errorString := "key \"%s\" is not in proper format, should be like: TSLA/1Min/OHLCV"

	for _, req := range reqs.Requests {
		// Construct a time bucket key from the input string
		parts := strings.Split(req.Key, ":")
		if len(parts) < colonSeparatedPartsLen {
			// The schema string is optional for Delete, so we append a blank if none is provided
			parts = append(parts, "")
		}

		tbk := io.NewTimeBucketKey(parts[0], parts[1])
		if tbk == nil {
			err = fmt.Errorf(errorString, req.Key)
			response.appendResponse(err)
			continue
		}

		err = s.catalogDir.RemoveTimeBucket(tbk)
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
