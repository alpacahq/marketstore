package executor

import (
	"fmt"
	stdio "io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/metrics"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	. "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"

	"github.com/klauspost/compress/snappy"
)


// Writer is produced that complies with the parsed query results, including a possible date
// range restriction.  If there is a date range restriction, the write() routine should produce
// an error when an out-of-bounds write is tried.
type Writer struct {
	root *catalog.Directory
	tgc  *TransactionPipe
	tbi  *io.TimeBucketInfo
}

func NewWriter(tbi *io.TimeBucketInfo, tgc *TransactionPipe, rootCatDir *catalog.Directory, walFile *WALFileType,
) (*Writer, error) {
	// Check to ensure there is a valid WALFile for this instance before writing
	if walFile == nil {
		err := fmt.Errorf("there is not an active WALFile for this instance, so cannot write")
		log.Error("NewWriter: %v", err)
		return nil, err
	}
	return &Writer{
		root: rootCatDir,
		tgc:  tgc,
		tbi:  tbi,
	}, nil
}

func (w *Writer) addNewYearFile(year int16) (err error) {
	newTbi, err := w.root.GetSubDirectoryAndAddFile(w.tbi.Path, year)
	if err != nil {
		return err
	}
	w.tbi = newTbi
	return nil
}

// formatRecord chops off the Epoch column(first 8bytes).
// If the record type is VARIABLE, append IntervalTicks(4byte) after that.
func formatRecord(buf, row []byte, t time.Time, index, intervalsPerDay int64, isVariable bool) []byte {
	/*
		Incoming data records ALWAYS have the 8-byte Epoch column first
	*/
	row = row[8:] // Chop off the Epoch column
	if !isVariable {
		return row
	}
	/*
		[VariableLength record] append IntervalTicks since bucket time instead of Epoch
	*/
	var outBuf []byte
	outBuf = append(buf, row...)
	outBuf = AppendIntervalTicks(outBuf, t, index, intervalsPerDay)
	return outBuf

}

// WriteRecords creates a WriteCommand from the supplied timestamp and data buffer,
// and sends it over the write channel to be flushed to disk in the WAL sync subroutine.
// The caller should assume that by calling WriteRecords directly, the data will be written
// to the file regardless if it satisfies the on-disk data shape, possible corrupting
// the data files. It is recommended to call WriteCSM() for any writes as it is safer.
func (w *Writer) WriteRecords(ts []time.Time, data []byte, dsWithEpoch []DataShape) {
	/*
		[]data contains a number of records, each including the epoch in the first 8 bytes
	*/
	numRows := len(ts)
	if numRows == 0 {
		return
	}

	var (
		prevIndex int64
		prevYear  int16
		cc        *wal.WriteCommand
		outBuf    []byte
		rowLen    = len(data) / numRows
	)

	wkp := FullPathToWALKey(ThisInstance.WALFile.RootPath, w.tbi.Path)
	vrl := w.tbi.GetVariableRecordLength()
	rt := w.tbi.GetRecordType()
	for i := 0; i < numRows; i++ {
		pos := i * rowLen
		record := data[pos : pos+rowLen]
		t := ts[i]
		year := int16(t.Year())
		if year != w.tbi.Year {
			if err := w.addNewYearFile(year); err != nil {
				panic(err)
			}
			wkp = FullPathToWALKey(ThisInstance.WALFile.RootPath, w.tbi.Path)
		}
		index := TimeToIndex(t, w.tbi.GetTimeframe())
		offset := IndexToOffset(index, w.tbi.GetRecordLength())

		// first row
		if i == 0 {
			prevIndex = index
			prevYear = year
			outBuf = formatRecord([]byte{}, record, t, index, w.tbi.GetIntervals(), w.tbi.GetRecordType() == VARIABLE)
			cc = &wal.WriteCommand{
				RecordType: rt,
				WALKeyPath: wkp,
				VarRecLen:  int(vrl),
				Offset:     offset,
				Index:      index,
				Data:       outBuf,
				DataShapes: dsWithEpoch,
			}
			continue
		}
		// Because index is relative time from the beginning of the year,
		// To confirm that the next data is a different data, both index and year should be checked.
		// (ex. when writing "2017-02-03 04:05:06" and "2018-02-03 04:05:06", index (02-03 04:05:06) is the same)
		if index == prevIndex && year == prevYear {
			/*
				This is the interior of a multi-row write buffer
			*/
			outBuf = formatRecord(outBuf, record, t, index, w.tbi.GetIntervals(), w.tbi.GetRecordType() == VARIABLE)
			cc.Data = outBuf
			continue
		}
		if index != prevIndex || year != prevYear {
			/*
				This row is at a new index, output previous output buffer
			*/
			w.tgc.writeChannel <- cc
			// Setup next command
			prevIndex = index
			outBuf = formatRecord([]byte{}, record, t, index, w.tbi.GetIntervals(), w.tbi.GetRecordType() == VARIABLE)
			cc = &wal.WriteCommand{
				RecordType: w.tbi.GetRecordType(),
				WALKeyPath: FullPathToWALKey(ThisInstance.WALFile.RootPath, w.tbi.Path),
				VarRecLen:  int(w.tbi.GetVariableRecordLength()),
				Offset:     offset,
				Index:      index,
				Data:       outBuf,
				DataShapes: dsWithEpoch,
			}
		}
	}

	// output to WAL
	w.tgc.writeChannel <- cc
}

func AppendIntervalTicks(buf []byte, t time.Time, index, intervalsPerDay int64) (outBuf []byte) {
	iticks := GetIntervalTicks32Bit(t, index, intervalsPerDay)
	postdata, _ := Serialize([]byte{}, iticks)
	outBuf = append(buf, postdata...)
	return outBuf
}

func WriteBufferToFile(fp stdio.WriterAt, buffer wal.OffsetIndexBuffer) error {
	offset := buffer.Offset()
	data := buffer.IndexAndPayload()
	_, err := fp.WriteAt(data, offset)
	return err
}

type IndirectRecordInfo struct {
	Index, Offset, Len int64
}

func WriteBufferToFileIndirect(fp *os.File, buffer wal.OffsetIndexBuffer, varRecLen int,
) (err error) {
	/*
		Here we write the data payload of the buffer to the end of the data file
		Prior to writing the new data, we fetch any previously written data and
		prepend it to the current data. This implements "append"
	*/
	primaryOffset := buffer.Offset() // Offset to storage of indirect record info
	index := buffer.Index()
	dataToBeWritten := buffer.Payload()
	dataLen := int64(len(dataToBeWritten))
	/*
		Now we write or update the index record
		First we read the file at the index location to see if this is an incremental write
	*/
	fp.Seek(primaryOffset, stdio.SeekStart)
	idBuf := make([]byte, 24) // {Index, Offset, Len}
	if _, err = fp.Read(idBuf); err != nil {
		return err
	}
	currentRecInfo := SwapSliceByte(idBuf, IndirectRecordInfo{}).([]IndirectRecordInfo)[0]
	/*
		Read the data from the previously written location, if it exists
	*/
	if currentRecInfo.Index != 0 {
		if _, err = fp.Seek(currentRecInfo.Offset, stdio.SeekStart); err != nil {
			return err
		}
		oldData := make([]byte, currentRecInfo.Len)
		if _, err := fp.Read(oldData); err != nil {
			return err
		}
		if !utils.InstanceConfig.DisableVariableCompression {
			oldData, err = snappy.Decode(nil, oldData)
			if err != nil {
				return err
			}
		}
		dataToBeWritten = append(oldData, dataToBeWritten...)
		dataLen = int64(len(dataToBeWritten))
	}

	// Determine if this is a continuation write
	endOfCurrentBucketData := currentRecInfo.Offset + currentRecInfo.Len
	endOfFileOffset, _ := fp.Seek(0, stdio.SeekEnd)
	if endOfCurrentBucketData == endOfFileOffset {
		endOfFileOffset = currentRecInfo.Offset
		fp.Seek(endOfFileOffset, stdio.SeekStart)
	}

	/*
		Sort the data by the timestamp to maintain on-disk sorted order
	*/
	sort.Stable(NewByIntervalTicks(dataToBeWritten, int(dataLen)/varRecLen, varRecLen))

	/*
		Write the data at the end of the file
	*/
	if !utils.InstanceConfig.DisableVariableCompression {
		comp := snappy.Encode(nil, dataToBeWritten)
		if _, err = fp.Write(comp); err != nil {
			return err
		}
		dataLen = int64(len(comp))
	} else {
		if _, err = fp.Write(dataToBeWritten); err != nil {
			return err
		}
	}

	//log.Info("LAL end_off:%d, len:%d, data:%v", endOfFileOffset, dataLen, dataToBeWritten)

	/*
		Write the indirect record info at the primaryOffset
	*/
	targetRecInfo := IndirectRecordInfo{Index: index, Offset: endOfFileOffset, Len: dataLen}
	odata := []int64{targetRecInfo.Index, targetRecInfo.Offset, targetRecInfo.Len}
	obuf := SwapSliceData(odata, byte(0)).([]byte)

	fp.Seek(primaryOffset, stdio.SeekStart)
	_, err = fp.Write(obuf)
	return err
}

// WriteCSM writes ColumnSeriesMap (csm) to each destination file, and flush it to the disk,
// isVariableLength is set to true if the record content is variable-length type. WriteCSM
// also verifies the DataShapeVector of the incoming ColumnSeriesMap matches the on-disk
// DataShapeVector defined by the file header. WriteCSM will create any files if they do
// not already exist for the given ColumnSeriesMap based on its TimeBucketKey.
func WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) (err error) {
	// WRITE is not allowed on a replica
	if utils.InstanceConfig.Replication.MasterHost != "" {
		return errors.New("write is not allowed on replica")
	}

	return WriteCSMInner(csm, isVariableLength)
}

func WriteCSMInner(csm io.ColumnSeriesMap, isVariableLength bool) (err error) {
	start := time.Now()
	cDir := ThisInstance.CatalogDir
	txnPipe := ThisInstance.TXNPipe
	walfile := ThisInstance.WALFile

	for tbk, cs := range csm {
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			return err
		}

		/*
			Prepare data for writing
		*/
		var alignData bool
		times, err := cs.GetTime()
		if err != nil {
			return err
		}
		if isVariableLength {
			cs.Remove("Nanoseconds")
			alignData = false
		}

		tbi, err := cDir.GetLatestTimeBucketInfoFromKey(&tbk)
		if err != nil {
			/*
				If we can't get the info, we try here to add a new one
			*/
			var recordType io.EnumRecordType
			if isVariableLength {
				recordType = io.VARIABLE
			} else {
				recordType = io.FIXED
			}

			t, err := cs.GetTime()
			if err != nil {
				return err
			}
			if len(t) == 0 {
				continue
			}

			year := int16(t[0].Year())
			tbi = io.NewTimeBucketInfo(
				*tf,
				tbk.GetPathToYearFiles(cDir.GetPath()),
				"Created By Writer", year,
				cs.GetDataShapes(), recordType)

			/*
				Verify there is an available TimeBucket for the destination
			*/
			if err := cDir.AddTimeBucket(&tbk, tbi); err != nil {
				// If File Exists error, ignore it, otherwise return the error
				if !strings.Contains(err.Error(), "Can not overwrite file") && !strings.Contains(err.Error(), "file exists") {
					return err
				}
			}
		}
		// Check if the previously-written data schema matches the input
		columnMismatchError := "unable to match data columns (%v) to bucket columns (%v)"
		dbDSV := tbi.GetDataShapesWithEpoch()
		csDSV := cs.GetDataShapes()
		if len(dbDSV) != len(csDSV) {
			return fmt.Errorf(columnMismatchError, csDSV, dbDSV)
		}
		missing, coercion := GetMissingAndTypeCoercionColumns(dbDSV, csDSV)
		if missing != nil {
			return fmt.Errorf(columnMismatchError, csDSV, dbDSV)
		}

		if coercion != nil {
			for _, dbDS := range coercion {
				if err := cs.CoerceColumnType(dbDS.Name, dbDS.Type); err != nil {
					csType := GetElementType(cs.GetColumn(dbDS.Name))
					log.Error("[%s] error coercing %s from %s to %s", tbk.GetItemKey(), dbDS.Name, csType.String(), dbDS.Type.String())
					return err
				}
			}
		}

		/*
			Create a writer for this TimeBucket
		*/
		w, err := NewWriter(tbi, txnPipe, cDir, walfile)
		if err != nil {
			return err
		}

		rowData := cs.ToRowSeries(tbk, alignData).GetData()
		w.WriteRecords(times, rowData, dbDSV)
	}
	walfile.RequestFlush(txnPipe)
	metrics.WriteCSMDuration.Observe(time.Since(start).Seconds())
	return nil
}
