package frontend

import (
	"context"
	"errors"
	"fmt"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/proto"
	"github.com/alpacahq/marketstore/sqlparser"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
	"math"
	"strings"
	"sync/atomic"
	"time"
)

var dataTypeMap = map[proto.DataType]io.EnumElementType{
	proto.DataType_FLOAT32: io.FLOAT32,
	proto.DataType_INT32:   io.INT32,
	proto.DataType_FLOAT64: io.FLOAT64,
	proto.DataType_INT64:   io.INT64,
	proto.DataType_EPOCH:   io.EPOCH,
	proto.DataType_BYTE:    io.BYTE,
	proto.DataType_BOOL:    io.BOOL,
	proto.DataType_NONE:    io.NONE,
	proto.DataType_STRING:  io.STRING,
	proto.DataType_INT16:   io.INT16,
	proto.DataType_UINT8:   io.UINT8,
	proto.DataType_UINT16:  io.UINT16,
	proto.DataType_UINT32:  io.UINT32,
	proto.DataType_UINT64:  io.UINT64,
}

var reverseMap = reverseDataTypeMap(dataTypeMap)

func reverseDataTypeMap(m map[proto.DataType]io.EnumElementType) map[io.EnumElementType]proto.DataType {
	n := map[io.EnumElementType]proto.DataType{}
	for k, v := range m {
		n[v] = k
	}
	return n
}

func ToProtoDataType(elemType io.EnumElementType) proto.DataType {
	return reverseMap[elemType]
}

// GRPCService is the implementation of GRPC API for Marketstore.
// All grpc/protobuf-related logics and models are defined in this file.
type GRPCService struct{}

func (s GRPCService) Query(ctx context.Context, reqs *proto.MultiQueryRequest) (*proto.MultiQueryResponse, error) {
	response := proto.MultiQueryResponse{}
	response.Version = utils.GitHash
	response.Timezone = utils.InstanceConfig.Timezone.String()
	for _, req := range reqs.Requests {
		switch req.IsSqlStatement {
		case true:
			ast, err := sqlparser.NewAstBuilder(req.SqlStatement)
			if err != nil {
				return nil, err
			}
			es, err := sqlparser.NewExecutableStatement(ast.Mtree)
			if err != nil {
				return nil, err
			}
			cs, err := es.Materialize()
			if err != nil {
				return nil, err
			}
			nds, err := NewNumpyDataset(cs)
			if err != nil {
				return nil, err
			}
			tbk := io.NewTimeBucketKeyFromString(req.SqlStatement + ":SQL")
			nmds, err := NewNumpyMultiDataset(nds, *tbk)
			if err != nil {
				return nil, err
			}
			response.Responses = append(response.Responses,
				&proto.QueryResponse{
					Result: nmds,
				})

		case false:
			/*
				Assumption: Within each TimeBucketKey, we have one or more of each category, with the exception of
				the AttributeGroup (aka Record Format) and Timeframe
				Within each TimeBucketKey in the request, we allow for a comma separated list of items, e.g.:
					destination1.items := "TSLA,AAPL,CG/1Min/OHLCV"
				Constraints:
				- If there is more than one record format in a single destination, we return an error
				- If there is more than one Timeframe in a single destination, we return an error
			*/
			dest := io.NewTimeBucketKey(req.Destination, req.KeyCategory)
			/*
				All destinations in a request must share the same record format (AttributeGroup) and Timeframe
			*/
			RecordFormat := dest.GetItemInCategory("AttributeGroup")
			Timeframe := dest.GetItemInCategory("Timeframe")
			Symbols := dest.GetMultiItemInCategory("Symbol")

			if len(Timeframe) == 0 || len(RecordFormat) == 0 || len(Symbols) == 0 {
				return nil, fmt.Errorf("destinations must have a Symbol, Timeframe and AttributeGroup, have: %s",
					dest.String())
			} else if len(Symbols) == 1 && Symbols[0] == "*" {
				// replace the * "symbol" with a list all known actual symbols
				allSymbols := executor.ThisInstance.CatalogDir.GatherCategoriesAndItems()["Symbol"]
				symbols := make([]string, 0, len(allSymbols))
				for symbol := range allSymbols {
					symbols = append(symbols, symbol)
				}
				keyParts := []string{strings.Join(symbols, ","), Timeframe, RecordFormat}
				itemKey := strings.Join(keyParts, "/")
				dest = io.NewTimeBucketKey(itemKey, req.KeyCategory)
			}

			epochStart := req.EpochStart
			epochEnd := req.EpochEnd
			if req.EpochEnd == 0 {
				epochEnd = int64(math.MaxInt64)
			}
			limitRecordCount := int(req.LimitRecordCount)
			limitFromStart := req.LimitFromStart

			columns := make([]string, 0)
			if req.Columns != nil {
				columns = req.Columns
			}

			start := io.ToSystemTimezone(time.Unix(epochStart, 0))
			stop := io.ToSystemTimezone(time.Unix(epochEnd, 0))
			csm, err := executeQuery(
				dest,
				start, stop,
				limitRecordCount, limitFromStart,
				columns,
			)
			if err != nil {
				return nil, err
			}

			/*
				Execute function pipeline, if requested
			*/
			if len(req.Functions) != 0 {
				for tbkStr, cs := range csm {
					csOut, err := runAggFunctions(req.Functions, cs)
					if err != nil {
						return nil, err
					}
					csm[tbkStr] = csOut
				}
			}

			/*
				Separate each TimeBucket from the result and compose a NumpyMultiDataset
			*/
			var nmds *proto.NumpyMultiDataset
			for tbk, cs := range csm {
				nds, err := NewNumpyDataset(cs)
				if err != nil {
					return nil, err
				}
				if nmds == nil {
					nmds, err = NewNumpyMultiDataset(nds, tbk)
					if err != nil {
						return nil, err
					}
				} else {
					Append(nmds, cs, tbk)
				}
			}

			/*
				Append the NumpyMultiDataset to the MultiResponse
			*/

			response.Responses = append(response.Responses,
				&proto.QueryResponse{
					Result: nmds,
				})

		}
	}
	return &response, nil
}

func NewNumpyDataset(cs *io.ColumnSeries) (nds *proto.NumpyDataset, err error) {
	nds = new(proto.NumpyDataset)
	nds.Length = int32(cs.Len())
	nds.DataShapes = GetProtoDataShapes(cs)
	for i, name := range cs.GetColumnNames() {
		nds.ColumnNames = append(nds.ColumnNames, name)
		colBytes := io.CastToByteSlice(cs.GetColumn(name))
		nds.ColumnData = append(nds.ColumnData, colBytes)
		if typeStr, ok := io.ToTypeStr(dataTypeMap[nds.DataShapes[i].Type]); !ok {
			log.Error("unsupported type %v", nds.DataShapes[i].String())
			return nil, fmt.Errorf("unsupported type")
		} else {
			nds.ColumnTypes = append(nds.ColumnTypes, typeStr)
		}
	}
	return nds, nil
}

func NewNumpyMultiDataset(nds *proto.NumpyDataset, tbk io.TimeBucketKey) (nmds *proto.NumpyMultiDataset, err error) {
	nmds = &proto.NumpyMultiDataset{
		Data: &proto.NumpyDataset{
			ColumnTypes: nds.ColumnTypes,
			ColumnNames: nds.ColumnNames,
			ColumnData:  nds.ColumnData,
			Length:      int32(nds.Length),
			DataShapes:  nds.DataShapes,
		},
	}
	nmds.StartIndex = make(map[string]int32)
	nmds.Lengths = make(map[string]int32)
	nmds.StartIndex[tbk.String()] = 0
	nmds.Lengths[tbk.String()] = int32(nds.Length)
	return nmds, nil
}

func Append(nmds *proto.NumpyMultiDataset, cs *io.ColumnSeries, tbk io.TimeBucketKey) (err error) {
	if int(nmds.Data.Length) != cs.GetNumColumns() {
		err = errors.New("Length of columns mismatch with NumpyMultiDataset")
		return
	}
	colSeriesNames := cs.GetColumnNames()
	for idx, name := range nmds.Data.ColumnNames {
		if name != colSeriesNames[idx] {
			err = errors.New("Data shape mismatch of ColumnSeries and NumpyMultiDataset")
			return
		}
	}
	nmds.StartIndex[tbk.String()] = nmds.Data.Length
	nmds.Lengths[tbk.String()] = int32(cs.Len())
	nmds.Data.Length += int32(cs.Len())
	for idx, col := range colSeriesNames {
		newBuffer := io.CastToByteSlice(cs.GetColumn(col))
		nmds.Data.ColumnData[idx] = append(nmds.Data.ColumnData[idx], newBuffer...)
	}
	return nil
}

func (s GRPCService) Write(ctx context.Context, reqs *proto.MultiWriteRequest) (*proto.MultiServerResponse, error) {
	response := proto.MultiServerResponse{}
	for _, req := range reqs.Requests {
		csm, err := ToColumnSeriesMap(req.Data)
		if err != nil {
			appendResponse(&response, err)
			continue
		}
		if err = executor.WriteCSM(csm, req.IsVariableLength); err != nil {
			appendResponse(&response, err)
			continue
		}
		//TODO: There should be an error response for every server request, need to add the below commented line
		//appendResponse(err, response)
	}
	return &response, nil
}

func ToColumnSeriesMap(nmds *proto.NumpyMultiDataset) (csm io.ColumnSeriesMap, err error) {
	csm = io.NewColumnSeriesMap()
	for tbkStr, idx := range nmds.StartIndex {
		length := nmds.Lengths[tbkStr]
		var cs *io.ColumnSeries
		if length > 0 {
			cs, err = ToColumnSeries(nmds.Data, idx, length)
			if err != nil {
				return nil, err
			}
		} else {
			cs = io.NewColumnSeries()
		}
		tbk := io.NewTimeBucketKeyFromString(tbkStr)
		csm.AddColumnSeries(*tbk, cs)
	}
	return csm, nil
}

func ToColumnSeries(nds *proto.NumpyDataset, options ...int32) (cs *io.ColumnSeries, err error) {
	var startIndex, length int32
	if len(options) != 0 {
		if len(options) != 2 {
			return nil, fmt.Errorf("incorrect number of arguments")
		}
		startIndex, length = options[0], options[1]
	} else {
		startIndex, length = 0, nds.Length
	}

	cs = io.NewColumnSeries()
	if len(nds.ColumnData[0]) == 0 {
		return cs, nil
	}
	/*
		Coerce the []byte for each column into it's native pointer type
	*/
	if nds.DataShapes == nil {
		nds.DataShapes, err = buildDataShapes(nds)
		if err != nil {
			return nil, err
		}
	}
	for i, shape := range nds.DataShapes {
		size := dataTypeMap[shape.Type].Size()
		start := int(startIndex) * size
		end := start + int(length)*size
		newColData := dataTypeMap[shape.Type].ConvertByteSliceInto(nds.ColumnData[i][start:end])
		cs.AddColumn(shape.Name, newColData)
	}
	return cs, nil
}

func buildDataShapes(nds *proto.NumpyDataset) ([]*proto.DataShape, error) {
	etypes := []io.EnumElementType{}
	for _, typeStr := range nds.ColumnTypes {
		if typ, ok := io.TypeStrToElemType(typeStr); !ok {
			return nil, fmt.Errorf("unsupported type string %s", typeStr)
		} else {
			etypes = append(etypes, typ)
		}
	}
	return NewDataShapeVector(nds.ColumnNames, etypes), nil
}

func GetProtoDataShapes(cs *io.ColumnSeries) (ds []*proto.DataShape) {
	var et []io.EnumElementType
	for _, name := range cs.GetColumnNames() {
		et = append(et, io.GetElementType(cs.GetColumns()[name]))
	}

	dsv := make([]*proto.DataShape, len(cs.GetColumnNames()))
	for i, name := range cs.GetColumnNames() {
		dsv[i] = &proto.DataShape{Name: name, Type: ToProtoDataType(et[i])}
	}

	return dsv
}

// NewDataShapeVector returns a new array of DataShapes for the given array of
// names and element types
func NewDataShapeVector(names []string, etypes []io.EnumElementType) (dsv []*proto.DataShape) {
	for i, name := range names {
		dsv = append(dsv, &proto.DataShape{Name: name, Type: ToProtoDataType(etypes[i])})
	}
	return dsv
}

func appendResponse(mr *proto.MultiServerResponse, err error) {
	var errorText string
	if err == nil {
		errorText = ""
	} else {
		errorText = err.Error()
	}
	mr.Responses = append(mr.Responses,
		&proto.ServerResponse{
			Error:   errorText,
			Version: utils.GitHash,
		},
	)
}

func (s GRPCService) ListSymbols(ctx context.Context, req *proto.ListSymbolsRequest) (*proto.ListSymbolsResponse, error) {
	response := proto.ListSymbolsResponse{}
	if atomic.LoadUint32(&Queryable) == 0 {
		return nil, queryableError
	}
	for symbol := range executor.ThisInstance.CatalogDir.GatherCategoriesAndItems()["Symbol"] {
		response.Results = append(response.Results, symbol)
	}
	return &response, nil
}

func (s GRPCService) Destroy(ctx context.Context, req *proto.MultiKeyRequest) (*proto.MultiServerResponse, error) {
	errorString := "key \"%s\" is not in proper format, should be like: TSLA/1Min/OHLCV"

	response := proto.MultiServerResponse{}
	for _, req := range req.Requests {
		// Construct a time bucket key from the input string
		parts := strings.Split(req.Key, ":")
		if len(parts) < 2 {
			// The schema string is optional for Delete, so we append a blank if none is provided
			parts = append(parts, "")
		}

		tbk := io.NewTimeBucketKey(parts[0], parts[1])
		if tbk == nil {
			err := fmt.Errorf(errorString, req.Key)
			appendResponse(&response, err)
			continue
		}

		err := executor.ThisInstance.CatalogDir.RemoveTimeBucket(tbk)
		if err != nil {
			err = fmt.Errorf("removal of catalog entry failed: %s", err.Error())
			appendResponse(&response, err)
			continue
		}
		appendResponse(&response, err)
	}

	return &response, nil
}

func (s GRPCService) ServerVersion(ctx context.Context, req *proto.ServerVersionRequest) (*proto.ServerVersionResponse, error) {
	return &proto.ServerVersionResponse{
		Version: utils.GitHash,
	}, nil
}
