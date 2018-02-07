package executor

import (
	"fmt"
	stdio "io"
	"os"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/catalog"
	"github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/io"
	"github.com/golang/glog"
)

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
		glog.Errorf("NewWriter: %v", err)
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

func WriteBufferToFileIndirect(fp *os.File, buffer offsetIndexBuffer) (err error) {
	// TODO: Incorporate previously written data into new writes - requires re-read of existing into buffer
	/*
		Here we write the data payload of the buffer to the end of the data file
	*/

	primaryOffset := buffer.Offset() // Offset to storage of indirect record info
	index := buffer.Index()
	dataToBeWritten := buffer.Payload()
	dataLen := int64(len(dataToBeWritten))

	/*
		Write the data at the end of the file
	*/
	endOfFileOffset, _ := fp.Seek(0, os.SEEK_END)
	_, err = fp.Write(dataToBeWritten)
	if err != nil {
		return err
	}

	/*
		Now we write or update the index record
		First we read the file at the index location to see if this is an incremental write
	*/
	fp.Seek(primaryOffset, os.SEEK_SET)
	idBuf := make([]byte, 24) // {Index, Offset, Len}
	_, err = fp.Read(idBuf)
	if err != nil {
		return err
	}

	currentRecInfo := SwapSliceByte(idBuf, IndirectRecordInfo{}).([]IndirectRecordInfo)[0]
	/*
		The default is a new write at the end of the file
	*/
	targetRecInfo := IndirectRecordInfo{Index: index, Offset: endOfFileOffset, Len: dataLen}

	/*
		If this is a continuation write, we adjust the targetRecInfo accordingly
	*/
	if currentRecInfo.Index != 0 { // If the index from the file is 0, this is a new write
		cursor := currentRecInfo.Offset + currentRecInfo.Len
		if endOfFileOffset == cursor {
			// Incremental write
			targetRecInfo.Len += currentRecInfo.Len
			targetRecInfo.Offset = currentRecInfo.Offset
		}
	}

	/*
		Write the indirect record info at the primaryOffset
	*/
	odata := []int64{targetRecInfo.Index, targetRecInfo.Offset, targetRecInfo.Len}
	obuf := SwapSliceData(odata, byte(0)).([]byte)

	fp.Seek(-24, os.SEEK_CUR)
	_, err = fp.Write(obuf)
	if err != nil {
		return err
	}

	return nil
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

		// TODO check if the previsouly-written data schema matches the input
		tbi, err := cDir.GetLatestTimeBucketInfoFromKey(&tbk)
		if err != nil {
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

		/*
			Create a writer for this TimeBucket
		*/
		w, err := NewWriter(tbi, ThisInstance.TXNPipe, cDir)
		if err != nil {
			return err
		}
		for i, ds := range tbi.GetDataShapesWithEpoch() {
			if csDs := cs.GetDataShapes()[i]; !ds.Equal(csDs) {
				return fmt.Errorf(
					"data shape does not match on-disk data shape: %v != %v",
					cs.GetDataShapes(),
					tbi.GetDataShapesWithEpoch(),
				)

			}
		}
		rs := cs.ToRowSeries(tbk)
		rowdata := rs.GetData()
		times := rs.GetTime()
		w.WriteRecords(times, rowdata)
	}
	wal := ThisInstance.WALFile
	wal.RequestFlush()
	return nil
}
