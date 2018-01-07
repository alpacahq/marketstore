package frontend

import (
	"net/http"

	"github.com/alpacahq/marketstore/catalog"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
)

type WriteRequest struct {
	Data             *io.NumpyMultiDataset `msgpack:"data"`
	IsVariableLength bool
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
		if err = WriteCSM(csm, req.IsVariableLength); err != nil {
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
			utils.Version,
		},
	)
}

func WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) (err error) {
	d := executor.ThisInstance.CatalogDir
	for tbk, cs := range csm {
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			return err
		}

		tbi, err := d.GetLatestTimeBucketInfoFromKey(&tbk)
		if err != nil {
			var recordType io.EnumRecordType
			if isVariableLength {
				recordType = io.VARIABLE
			} else {
				recordType = io.FIXED
			}

			tbi = io.NewTimeBucketInfo(
				*tf,
				tbk.GetPathToYearFiles(d.GetPath()),
				"Created By Writer", 2017,
				cs.GetDataShapes(), recordType)

			/*
				Verify there is an available TimeBucket for the destination
			*/
			err = d.AddTimeBucket(&tbk, tbi)
			if err != nil {
				// If File Exists error, ignore it, otherwise return the error
				if _, ok := err.(catalog.FileAlreadyExists); !ok {
					return err
				}
			}
		}

		/*
			Create a writer for this TimeBucket
		*/
		q := planner.NewQuery(d)
		q.AddTargetKey(&tbk)
		pr, err := q.Parse()
		if err != nil {
			return err
		}
		wr, err := executor.NewWriter(pr, executor.ThisInstance.TXNPipe, d)
		if err != nil {
			return err
		}
		rs := cs.ToRowSeries(tbk)
		rowdata := rs.GetData()
		times := rs.GetTime()
		wr.WriteRecords(times, rowdata)
	}
	wal := executor.ThisInstance.WALFile
	tgc := executor.ThisInstance.TXNPipe
	wal.FlushToWAL(tgc)
	wal.FlushToPrimary()
	return nil
}
