package executor

import (
	"fmt"
	"github.com/klauspost/compress/snappy"
	stdio "io"
	"os"
	"strings"
	"time"
	"unsafe"

	"github.com/alpacahq/marketstore/catalog"
	"github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

//#include "quickSort.h"
//#cgo CFLAGS: -O3 -Wno-ignored-optimization-argument
import "C"

var Compressed = true

type Writer struct {
	root *catalog.Directory
	tgc  *TransactionPipe
	tbi  *io.TimeBucketInfo
}

func NewWriter(tbi *io.TimeBucketInfo, tgc *TransactionPipe, rootCatDir *catalog.Directory) (*Writer, error) {
	/*
		A writer is produced that complies with the parsed query results, including a possible date
		range restriction.  If there is a date range restriction, the write() routine should produce
		an error when an out-of-bounds write is tried.
	*/
	// Check to ensure there is a valid WALFile for this instance before writing
	if ThisInstance.WALFile == nil {
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

func (w *Writer) AddNewYearFile(year int16) (err error) {
	newTbi, err := w.root.GetSubDirectoryAndAddFile(w.tbi.Path, year)
	if err != nil {
		return err
	}
	w.tbi = newTbi
	return nil
}

// WriteRecords creates a WriteCommand from the supplied timestamp and data buffer,
// and sends it over the write channel to be flushed to disk in the WAL sync subroutine.
// The caller should assume that by calling WriteRecords directly, the data will be written
// to the file regardless if it satisfies the on-disk data shape, possible corrupting
// the data files. It is recommended to call WriteCSM() for any writes as it is safer.
func (w *Writer) WriteRecords(ts []time.Time, data []byte) {
	/*
		[]data contains a number of records, each including the epoch in the first 8 bytes
	*/
	numRows := len(ts)
	rowLen := len(data) / numRows
	var prevIndex int64
	var cc *WriteCommand
	var outBuf []byte

	formatRecord := func(buf, record []byte, t time.Time, index, intervalsPerDay int64) (outBuf []byte) {
		/*
			Incoming data records ALWAYS have the 8-byte Epoch column first
		*/
		record = record[8:] // Chop off the Epoch column
		if w.tbi.GetRecordType() == VARIABLE {
			/*
				Trim the Epoch column off and replace it with ticks since bucket time
			*/
			outBuf = append(buf, record...)
			outBuf = AppendIntervalTicks(outBuf, t, index, intervalsPerDay)
			return outBuf
		}
		return record
	}

	for i := 0; i < numRows; i++ {
		pos := i * rowLen
		record := data[pos : pos+rowLen]
		t := ts[i]
		year := int16(t.Year())
		if year != w.tbi.Year {
			if err := w.AddNewYearFile(year); err != nil {
				panic(err)
			}
		}
		index := TimeToIndex(t, w.tbi.GetTimeframe())
		offset := IndexToOffset(index, w.tbi.GetRecordLength())

		if i == 0 {
			prevIndex = index
			cc = &WriteCommand{
				RecordType: w.tbi.GetRecordType(),
				WALKeyPath: ThisInstance.WALFile.FullPathToWALKey(w.tbi.Path),
				Offset:     offset,
				Index:      index,
				Data:       nil}
		}
		if index == prevIndex {
			/*
				This is the interior of a multi-row write buffer
			*/
			outBuf = formatRecord(outBuf, record, t, index, w.tbi.GetIntervals())
			cc.Data = outBuf
		}
		if index != prevIndex {
			/*
				This row is at a new index, output previous output buffer
			*/
			w.tgc.writeChannel <- cc
			// Setup next command
			prevIndex = index
			outBuf = formatRecord([]byte{}, record, t, index, w.tbi.GetIntervals())
			cc = &WriteCommand{
				RecordType: w.tbi.GetRecordType(),
				WALKeyPath: ThisInstance.WALFile.FullPathToWALKey(w.tbi.Path),
				Offset:     offset,
				Index:      index,
				Data:       outBuf}
		}
		if i == (numRows - 1) {
			/*
				The last iteration must output it's command buffer
			*/
			w.tgc.writeChannel <- cc
		}
	}
}

func AppendIntervalTicks(buf []byte, t time.Time, index, intervalsPerDay int64) (outBuf []byte) {
	iticks := GetIntervalTicks32Bit(t, index, intervalsPerDay)
	postdata, _ := Serialize([]byte{}, iticks)
	outBuf = append(buf, postdata...)
	return outBuf
}

func WriteBufferToFile(fp stdio.WriterAt, buffer offsetIndexBuffer) error {
	offset := buffer.Offset()
	data := buffer.IndexAndPayload()
	_, err := fp.WriteAt(data, offset)
	return err
}

type IndirectRecordInfo struct {
	Index, Offset, Len int64
}

func WriteBufferToFileIndirect(fp *os.File, buffer offsetIndexBuffer, varRecLen int32) (err error) {
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
		if Compressed {
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
	TimSortBufferUINT32(
		unsafe.Pointer(&dataToBeWritten[0]),
		uint64(dataLen),
		uint64(varRecLen),
	)
	/*
		Write the data at the end of the file
	*/
	if Compressed {
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

// WriteCSM writs ColumnSeriesMap csm to each destination file, and flush it to the disk,
// isVariableLength is set to true if the record content is variable-length type. WriteCSM
// also verifies the DataShapeVector of the incoming ColumnSeriesMap matches the on-disk
// DataShapeVector defined by the file header. WriteCSM will create any files if they do
// not already exist for the given ColumnSeriesMap based on its TimeBucketKey.
func WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) (err error) {
	cDir := ThisInstance.CatalogDir
	for tbk, cs := range csm {
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			return err
		}

		/*
			Prepare data for writing
		*/
		times := cs.GetTime()
		if isVariableLength {
			cs.Remove("Nanoseconds")
		}
		rs := cs.ToRowSeries(tbk)
		rowdata := rs.GetData()

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

			year := int16(cs.GetTime()[0].Year())
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
		if missing != nil || coercion != nil {
			return fmt.Errorf(columnMismatchError, csDSV, dbDSV)
		}

		/*
			Create a writer for this TimeBucket
		*/
		w, err := NewWriter(tbi, ThisInstance.TXNPipe, cDir)
		if err != nil {
			return err
		}

		w.WriteRecords(times, rowdata)
	}
	wal := ThisInstance.WALFile
	wal.RequestFlush()
	return nil
}
