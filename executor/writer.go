package executor

import (
	"errors"
	"fmt"
	stdio "io"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/snappy"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/metrics"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Writer is produced that complies with the parsed query results, including a possible date
// range restriction.  If there is a date range restriction, the write() routine should produce
// an error when an out-of-bounds write is tried.
type Writer struct {
	rootCatDir *catalog.Directory
	walFile    *WALFileType
}

func NewWriter(rootCatDir *catalog.Directory, walFile *WALFileType) (*Writer, error) {
	// Check to ensure there is a valid WALFile for this instance before writing
	if walFile == nil {
		err := fmt.Errorf("there is not an active WALFile for this instance, so cannot write")
		log.Error("NewWriter: %v", err)
		return nil, err
	}
	return &Writer{
		rootCatDir: rootCatDir,
		walFile:    walFile,
	}, nil
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
	buf = append(buf, row...)
	outBuf = buf
	outBuf = appendIntervalTicks(outBuf, t, index, intervalsPerDay)
	return outBuf
}

// WriteRecords creates a WriteCommand from the supplied timestamp and data buffer,
// and sends it over the write channel to be flushed to disk in the WAL sync subroutine.
// The caller should assume that by calling WriteRecords directly, the data will be written
// to the file regardless if it satisfies the on-disk data shape, possible corrupting
// the data files. It is recommended to call WriteCSM() for any writes as it is safer.
func (w *Writer) WriteRecords(ts []time.Time, data []byte, dsWithEpoch []io.DataShape, tbi *io.TimeBucketInfo) error {
	/*
		[]data contains a number of records, each including the epoch in the first 8 bytes
	*/
	numRows := len(ts)
	if numRows == 0 {
		return nil
	}

	var (
		prevIndex int64
		prevYear  int16
		cc        *wal.WriteCommand
		outBuf    []byte
		rowLen    = len(data) / numRows
		err       error
	)

	vrl := tbi.GetVariableRecordLength()
	rt := tbi.GetRecordType()
	for i := 0; i < numRows; i++ {
		pos := i * rowLen
		record := data[pos : pos+rowLen]
		t := ts[i]
		year := int16(t.Year())
		if year != tbi.Year {
			// add a new year's file
			tbi, err = w.rootCatDir.GetSubDirectoryAndAddFile(tbi.Path, year)
			if err != nil {
				return fmt.Errorf("add new year file. tbi=%v, err: %w", tbi, err)
			}
		}
		index := io.TimeToIndex(t, tbi.GetTimeframe())
		offset := io.IndexToOffset(index, tbi.GetRecordLength())

		// first row
		if i == 0 {
			prevIndex = index
			prevYear = year
			outBuf = formatRecord([]byte{}, record, t, index, tbi.GetIntervals(), tbi.GetRecordType() == io.VARIABLE)
			cc = w.walFile.WriteCommand(rt, tbi.Path, int(vrl), offset, index, outBuf, dsWithEpoch)
			continue
		}
		// Because index is relative time from the beginning of the year,
		// To confirm that the next data is a different data, both index and year should be checked.
		// (ex. when writing "2017-02-03 04:05:06" and "2018-02-03 04:05:06", index (02-03 04:05:06) is the same)
		if index == prevIndex && year == prevYear {
			/*
				This is the interior of a multi-row write buffer
			*/
			outBuf = formatRecord(outBuf, record, t, index, tbi.GetIntervals(), tbi.GetRecordType() == io.VARIABLE)
			cc.Data = outBuf
			continue
		}
		if index != prevIndex || year != prevYear {
			/*
				This row is at a new index, output previous output buffer
			*/
			w.walFile.QueueWriteCommand(cc)
			// Setup next command
			prevIndex = index
			outBuf = formatRecord([]byte{}, record, t, index, tbi.GetIntervals(), tbi.GetRecordType() == io.VARIABLE)
			cc = w.walFile.WriteCommand(
				tbi.GetRecordType(), tbi.Path, int(tbi.GetVariableRecordLength()), offset, index,
				outBuf, dsWithEpoch)
		}
	}

	// output to WAL
	w.walFile.QueueWriteCommand(cc)

	return nil
}

func appendIntervalTicks(buf []byte, t time.Time, index, intervalsPerDay int64) (outBuf []byte) {
	iticks := io.GetIntervalTicks32Bit(t, index, intervalsPerDay)
	postdata, _ := io.Serialize([]byte{}, iticks)
	buf = append(buf, postdata...)
	outBuf = buf
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

const indexOffsetLengthBytes = 24

func WriteBufferToFileIndirect(fp stdio.ReadWriteSeeker, buffer wal.OffsetIndexBuffer, varRecLen int,
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
	if _, err = fp.Seek(primaryOffset, stdio.SeekStart); err != nil {
		return fmt.Errorf("failed to seek primaryOffset:%w", err)
	}
	idBuf := make([]byte, indexOffsetLengthBytes) // {Index, Offset, Len}
	if _, err = fp.Read(idBuf); err != nil {
		return err
	}
	currentRecInfoI, err := io.SwapSliceByte(idBuf, IndirectRecordInfo{})
	if err != nil {
		return err
	}
	currentRecInfo, ok := currentRecInfoI.([]IndirectRecordInfo)
	if !ok {
		return errors.New("failed to cast record to IndirectRecordInfo slice")
	}
	/*
		Read the data from the previously written location, if it exists
	*/
	if currentRecInfo[0].Index != 0 {
		if _, err = fp.Seek(currentRecInfo[0].Offset, stdio.SeekStart); err != nil {
			return err
		}
		oldData := make([]byte, currentRecInfo[0].Len)
		if _, err2 := fp.Read(oldData); err2 != nil {
			return err2
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
	endOfCurrentBucketData := currentRecInfo[0].Offset + currentRecInfo[0].Len
	endOfFileOffset, _ := fp.Seek(0, stdio.SeekEnd)
	if endOfCurrentBucketData == endOfFileOffset {
		endOfFileOffset = currentRecInfo[0].Offset
		if _, err = fp.Seek(endOfFileOffset, stdio.SeekStart); err != nil {
			return fmt.Errorf("failed to seek endOfFileOffset:%w", err)
		}
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
	} else if _, err = fp.Write(dataToBeWritten); err != nil {
		return err
	}

	// log.Info("LAL end_off:%d, len:%d, data:%v", endOfFileOffset, dataLen, dataToBeWritten)

	/*
		Write the indirect record info at the primaryOffset
	*/
	targetRecInfo := IndirectRecordInfo{Index: index, Offset: endOfFileOffset, Len: dataLen}
	odata := []int64{targetRecInfo.Index, targetRecInfo.Offset, targetRecInfo.Len}
	obuf, ok := io.SwapSliceData(odata, byte(0)).([]byte)
	if !ok {
		return fmt.Errorf("failed to cast OffsetIndexBuffer of the target record to bytes:%v", targetRecInfo)
	}

	if _, err = fp.Seek(primaryOffset, stdio.SeekStart); err != nil {
		log.Error("failed to seek offset to write primary record", err.Error())
	}
	_, err = fp.Write(obuf)
	return err
}

// WriteCSM has the same logic as the executor.WriteCSM function.
// In order to improve testability, use this function instead of the static WriteCSM function.
func (w *Writer) WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) error {
	start := time.Now()
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
			if err = cs.Remove("Nanoseconds"); err != nil {
				log.Warn(fmt.Sprintf("failed to remove 'Nanoseconds' column. err=%v", err))
			}
			alignData = false
		}

		tbi, err := w.rootCatDir.GetLatestTimeBucketInfoFromKey(&tbk)
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

			t, err2 := cs.GetTime()
			if err2 != nil {
				return err2
			}
			if len(t) == 0 {
				continue
			}

			year := int16(t[0].Year())
			tbi = io.NewTimeBucketInfo(
				*tf,
				tbk.GetPathToYearFiles(w.rootCatDir.GetPath()),
				"Created By Writer", year,
				cs.GetDataShapes(), recordType)

			/*
				Verify there is an available TimeBucket for the destination
			*/
			if err2 := w.rootCatDir.AddTimeBucket(&tbk, tbi); err2 != nil {
				// If File Exists error, ignore it, otherwise return the error
				if !strings.Contains(err2.Error(), "Can not overwrite file") && !strings.Contains(err2.Error(), "file exists") {
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
		missing, coercion, err := io.GetMissingAndTypeCoercionColumns(dbDSV, csDSV)
		if err != nil {
			return fmt.Errorf("find missing and type coercion columns: %w", err)
		}
		if missing != nil {
			return fmt.Errorf(columnMismatchError, csDSV, dbDSV)
		}

		for _, dbDS := range coercion {
			if err2 := cs.CoerceColumnType(dbDS.Name, dbDS.Type); err2 != nil {
				csType := io.GetElementType(cs.GetColumn(dbDS.Name))
				log.Error("[%s] error coercing %s from %s to %s", tbk.GetItemKey(), dbDS.Name, csType.String(), dbDS.Type.String())
				return err2
			}
		}

		rs, err := cs.ToRowSeries(tbk, alignData)
		if err != nil {
			return fmt.Errorf("convert column series to row series. tbk=%s: %w", tbk, err)
		}
		rowData := rs.GetData()
		err = w.WriteRecords(times, rowData, dbDSV, tbi)
		if err != nil {
			return fmt.Errorf("write records to %v: %w", tbi, err)
		}
	}

	w.walFile.RequestFlush()
	metrics.WriteCSMDuration.Observe(time.Since(start).Seconds())
	return nil
}

// WriteCSM writes ColumnSeriesMap (csm) to each destination file, and flush it to the disk,
// isVariableLength is set to true if the record content is variable-length type. WriteCSM
// also verifies the DataShapeVector of the incoming ColumnSeriesMap matches the on-disk
// DataShapeVector defined by the file header. WriteCSM will create any files if they do
// not already exist for the given ColumnSeriesMap based on its TimeBucketKey.
func WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) (err error) {
	writer, err := NewWriter(ThisInstance.CatalogDir, ThisInstance.WALFile)
	if err != nil {
		return err
	}

	return writer.WriteCSM(csm, isVariableLength)
}
