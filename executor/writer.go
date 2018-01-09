package executor

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/alpacahq/marketstore/catalog"
	"github.com/alpacahq/marketstore/planner"
	. "github.com/alpacahq/marketstore/utils/io"
	"github.com/golang/glog"
)

type Writer struct {
	pr             planner.ParseResult
	iop            *ioplan
	BaseDirectory  *catalog.Directory
	tgc            *TransactionPipe
	FileInfoByYear map[int16]*TimeBucketInfo // One-one relationship for a writer between year and target file
	KeyPathByYear  map[int16]string          // This key includes the year filename
}

func NewWriter(pr *planner.ParseResult, tgc *TransactionPipe, rootCatDir *catalog.Directory) (w *Writer, err error) {
	if pr.IntervalsPerDay == 0 {
		return nil, fmt.Errorf("No query results, cannot create writer")
	}
	/*
		A writer is produced that complies with the parsed query results, including a possible date
		range restriction.  If there is a date range restriction, the write() routine should produce
		an error when an out-of-bounds write is tried.
	*/
	// Check to ensure there is a valid WALFile for this instance before writing
	if ThisInstance.WALFile == nil {
		err = fmt.Errorf("There is not an active WALFile for this instance, so cannot write.")
		glog.Errorf("NewWriter: %v", err)
		return nil, err
	}
	w = new(Writer)
	SortedFiles := SortedFileList(pr.QualifiedFiles)
	sort.Sort(SortedFiles)
	w.pr = *pr
	if pr.Range == nil {
		pr.Range = planner.NewDateRange()
	}
	secondsPerInterval := 3600 * 24 / pr.IntervalsPerDay
	if w.iop, err = NewIOPlan(SortedFiles, pr, secondsPerInterval); err != nil {
		return nil, err
	}

	// Process the ioplan to determine if it has a single base directory target, required for a writer
	baseDirectories := make(map[string]int, 0)
	w.FileInfoByYear = make(map[int16]*TimeBucketInfo, 0)
	w.KeyPathByYear = make(map[int16]string, 0)
	for _, fp := range w.iop.FilePlan {
		if w.BaseDirectory == nil {
			if w.BaseDirectory, err = rootCatDir.GetOwningSubDirectory(fp.FullPath); err != nil {
				glog.Errorf("NewWriter: %v", err)
				return nil, err
			}
		}
		baseDirectories[w.BaseDirectory.GetPath()] = 0
		year := fp.GetFileYear()
		if w.FileInfoByYear[year], err = w.BaseDirectory.PathToTimeBucketInfo(fp.FullPath); err != nil {
			glog.Errorf("NewWriter: %v", err)
			return nil, err
		}
		w.KeyPathByYear[year] = ThisInstance.WALFile.FullPathToWALKey(w.FileInfoByYear[year].Path)
	}
	if len(baseDirectories) != 1 {
		return nil, SingleTargetRequiredForWriter("NewWriter")
	}
	w.tgc = tgc // TransactionPipe, will be used to implement all writes

	return w, nil
}
func (w *Writer) AddNewYearFile(year int16) (err error) {
	w.FileInfoByYear[year], err = w.BaseDirectory.AddFile(year)
	if err != nil {
		return err
	}
	w.KeyPathByYear[year] = ThisInstance.WALFile.FullPathToWALKey(w.FileInfoByYear[year].Path)
	return nil
}

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
		if w.iop.RecordType == VARIABLE {
			/*
				Trim the Epoch column off and replace it with ticks since bucket time
			*/
			outBuf = append(buf, record...)
			outBuf = AppendIntervalTicks(outBuf, t, index, intervalsPerDay)
			return outBuf
		} else {
			return record
		}
	}

	for i := 0; i < numRows; i++ {
		pos := i * rowLen
		record := data[pos : pos+rowLen]
		t := ts[i]
		year := int16(t.Year())
		if _, ok := w.FileInfoByYear[year]; !ok {
			w.AddNewYearFile(year)
		}
		intervalsPerDay := w.FileInfoByYear[year].GetIntervals()
		offset := TimeToOffset(t, intervalsPerDay, w.FileInfoByYear[year].GetRecordLength())
		index := TimeToIndex(t, intervalsPerDay)

		if i == 0 {
			prevIndex = index
			cc = &WriteCommand{
				RecordType: w.iop.RecordType,
				WALKeyPath: w.KeyPathByYear[year],
				Offset:     offset,
				Index:      index,
				Data:       nil}
		}
		if index == prevIndex {
			/*
				This is the interior of a multi-row write buffer
			*/
			outBuf = formatRecord(outBuf, record, t, index, intervalsPerDay)
			cc.Data = outBuf
		}
		if index != prevIndex {
			/*
				This row is at a new index, output previous output buffer
			*/
			w.tgc.writeChannel <- cc
			// Setup next command
			prevIndex = index
			outBuf = formatRecord([]byte{}, record, t, index, intervalsPerDay)
			cc = &WriteCommand{
				RecordType: w.iop.RecordType,
				WALKeyPath: w.KeyPathByYear[year],
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

func WriteBufferToFile(fp *os.File, offsetIndexDataBuffer []byte) (err error) {
	offset := ToInt64(offsetIndexDataBuffer[:8])
	_, err = fp.WriteAt(offsetIndexDataBuffer[8:], offset)
	if err != nil {
		return err
	}
	return nil
}

type IndirectRecordInfo struct {
	Index, Offset, Len int64
}

func WriteBufferToFileIndirect(fp *os.File, offsetIndexDataBuffer []byte) (err error) {
	// TODO: Incorporate previously written data into new writes - requires re-read of existing into buffer
	/*
		Here we write the data payload of the buffer to the end of the data file
	*/

	primaryOffset := ToInt64(offsetIndexDataBuffer[:8]) // Offset to storage of indirect record info

	index := ToInt64(offsetIndexDataBuffer[8:])
	dataToBeWritten := offsetIndexDataBuffer[16:] // data payload begins at 8 + 8 = 16
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
