package executor

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"time"
	"unsafe"

	"github.com/alpacahq/marketstore/catalog"
	"github.com/alpacahq/marketstore/executor/readhint"
	"github.com/alpacahq/marketstore/planner"
	. "github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/log"
)

const RecordsPerRead = 2000

type SortedFileList []planner.QualifiedFile

func (fl SortedFileList) Len() int           { return len(fl) }
func (fl SortedFileList) Swap(i, j int)      { fl[i], fl[j] = fl[j], fl[i] }
func (fl SortedFileList) Less(i, j int) bool { return fl[i].File.Year < fl[j].File.Year }

type ioFilePlan struct {
	Offset   int64
	Length   int64
	FullPath string // Full file path, including leaf (Year) file
	// The time that begins each file in seconds since the Unix epoch
	BaseTime    int64
	seekingLast bool
}

func (iofp *ioFilePlan) GetFileYear() int16 {
	return int16(time.Unix(iofp.BaseTime, 0).UTC().Year())
}

type ioplan struct {
	FilePlan          []*ioFilePlan
	PrevFilePlan      []*ioFilePlan
	RecordLen         int32
	RecordType        EnumRecordType
	VariableRecordLen int
	IntervalSeconds   int64 // Interval size in seconds
	Limit             *planner.RowLimit
}

func (iop *ioplan) GetIntervalsPerDay() int64 {
	return (24 * 60 * 60) / iop.IntervalSeconds
}

func NewIOPlan(fl SortedFileList, pr *planner.ParseResult, secsPerInterval int64) (iop *ioplan, err error) {
	iop = new(ioplan)
	iop.FilePlan = make([]*ioFilePlan, 0)
	iop.PrevFilePlan = make([]*ioFilePlan, 0)
	iop.IntervalSeconds = secsPerInterval
	iop.Limit = pr.Limit
	/*
		At this point we have a date unconstrained group of sorted files
		We will do two things here:
		1) create the list of date qualified files to read for the primary data
		2) create a list of files with times prior to the date range in reverse order
	*/
	prevPaths := make([]*ioFilePlan, 0)
	for _, file := range fl {
		fileStartTime := time.Date(int(file.File.Year), time.January, 1, 0, 0, 0, 0, time.UTC)
		startOffset := int64(Headersize)
		endOffset := int64(FileSize(file.File.GetIntervals(), int(file.File.Year), int(file.File.GetRecordLength())))
		length := endOffset - startOffset
		maxLength := length
		if iop.RecordLen == 0 {
			iop.RecordLen = file.File.GetRecordLength()
			iop.RecordType = file.File.GetRecordType()
			iop.VariableRecordLen = int(file.File.GetVariableRecordLength())
		} else {
			// check that we're reading the same recordlength across all files, return err if not
			if file.File.GetRecordLength() != iop.RecordLen {
				return nil, RecordLengthNotConsistent("NewIOPlan")
			}
		}
		if file.File.Year < pr.Range.StartYear {
			// Add the whole file to the previous files list for use in back scanning before the start
			prevPaths = append(prevPaths, &ioFilePlan{startOffset, length, file.File.Path, fileStartTime.Unix(), false})
		} else if file.File.Year <= pr.Range.EndYear {
			/*
			 Calculate the number of bytes to be read for each file and the offset
			*/
			// Set the starting and ending indices based on the range
			if file.File.Year == pr.Range.StartYear {
				startOffset = TimeToOffset(pr.Range.Start, file.File.GetIntervals(), file.File.GetRecordLength())
			}
			if file.File.Year == pr.Range.EndYear {
				endOffset = TimeToOffset(pr.Range.End, file.File.GetIntervals(), file.File.GetRecordLength()) + int64(file.File.GetRecordLength())
			}
			if lastKnownOffset, ok := readhint.GetLastKnown(file.File.Path); ok {
				hinted := lastKnownOffset + int64(file.File.GetRecordLength())
				if hinted < endOffset {
					endOffset = hinted
				}
			}
			length = endOffset - startOffset
			// Limit the scan to the end of the fixed length data
			if length > maxLength {
				length = maxLength
			}
			fp := &ioFilePlan{startOffset, length, file.File.Path, fileStartTime.Unix(), false}
			if iop.Limit.Direction == LAST {
				fp.seekingLast = true
			}
			iop.FilePlan = append(iop.FilePlan, fp)
			// in backward scan, tell the last known index for the later reader
			// Add a previous file if we are at the beginning of the range
			if file.File.Year == pr.Range.StartYear {
				length := startOffset - int64(Headersize)
				prevPaths = append(prevPaths, &ioFilePlan{int64(Headersize), length, file.File.Path, fileStartTime.Unix(), false})
			}
		}
	}
	// Reverse the prevPath filelist order
	for i := len(prevPaths) - 1; i >= 0; i-- {
		iop.PrevFilePlan = append(iop.PrevFilePlan, prevPaths[i])
	}
	return iop, nil
}

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
		return nil, fmt.Errorf("No query results, can not create writer")
	}
	/*
		A writer is produced that complies with the parsed query results, including a possible date
		range restriction.  If there is a date range restriction, the write() routine should produce
		an error when an out-of-bounds write is tried.
	*/
	// Check to ensure there is a valid WALFile for this instance before writing
	if ThisInstance.WALFile == nil {
		err = fmt.Errorf("There is not an active WALFile for this instance, so can not write.")
		Log(ERROR, "NewWriter", err)
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
				Log(ERROR, "NewWriter", err)
				return nil, err
			}
		}
		baseDirectories[w.BaseDirectory.GetPath()] = 0
		year := fp.GetFileYear()
		if w.FileInfoByYear[year], err = w.BaseDirectory.PathToTimeBucketInfo(fp.FullPath); err != nil {
			Log(ERROR, "NewWriter", err)
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

type reader struct {
	pr     planner.ParseResult
	IOPMap map[TimeBucketKey]*ioplan
	// for packingReader to avoid redundant allocation.
	// really ought to be somewhere close to the function...
	readBuffer []byte
	fileBuffer []byte
}

func NewReader(pr *planner.ParseResult) (r *reader, err error) {
	r = new(reader)
	r.pr = *pr
	if pr.Range == nil {
		pr.Range = planner.NewDateRange()
	}
	secondsPerInterval := 3600 * 24 / pr.IntervalsPerDay

	sortedFileMap := make(map[TimeBucketKey]SortedFileList)
	for _, qf := range pr.QualifiedFiles {
		sortedFileMap[qf.Key] = append(sortedFileMap[qf.Key], qf)
	}
	r.IOPMap = make(map[TimeBucketKey]*ioplan)
	maxRecordLen := int32(0)
	for key, sfl := range sortedFileMap {
		sort.Sort(sfl)
		if r.IOPMap[key], err = NewIOPlan(sfl, pr, secondsPerInterval); err != nil {
			return nil, err
		}
		recordLen := r.IOPMap[key].RecordLen
		if maxRecordLen < recordLen {
			maxRecordLen = recordLen
		}
	}
	// Number of bytes to buffer, some multiple of record length
	// This should be at least bigger than 4096 and be better multiple of 4KB,
	// which is the common io size on most of the storage/filesystem.
	readSize := RecordsPerRead * maxRecordLen
	r.readBuffer = make([]byte, readSize)
	r.fileBuffer = make([]byte, readSize)
	return r, nil
}

func (r *reader) Read() (csm ColumnSeriesMap, tPrevMap map[TimeBucketKey]int64, err error) {
	csm = NewColumnSeriesMap()
	tPrevMap = make(map[TimeBucketKey]int64)

	catMap := r.pr.GetCandleAttributes()
	rtMap := r.pr.GetRowType()
	dsMap := r.pr.GetDataShapes()
	rlMap := r.pr.GetRowLen()
	for key, iop := range r.IOPMap {
		cat := catMap[key]
		rt := rtMap[key]
		rlen := rlMap[key]
		buffer, tPrev, err := r.read(iop)
		if err != nil {
			return nil, nil, err
		}
		tPrevMap[key] = tPrev
		if len(buffer) == 0 {
			continue
		}
		rs := NewRowSeries(key, tPrev, buffer, dsMap[key], rlen, cat, rt)
		key, cs := rs.ToColumnSeries()
		csm[key] = cs
	}

	return csm, tPrevMap, err
}

/*
bufferMeta stores an indirect index to variable length data records. It's used to read the actual data in a second pass.
*/
type bufferMeta struct {
	FullPath  string
	Data      []byte
	VarRecLen int
	Intervals int64
}

// Reads the data from files, removing holes. The resulting buffer will be packed
// Uses the index that prepends each row to identify filled rows versus holes
func (r *reader) read(iop *ioplan) (resultBuffer []byte, tPrev int64, err error) {
	const GatherTprev = true
	// Number of bytes to buffer, some multiple of record length
	// This should be at least bigger than 4096 and be better multiple of 4KB,
	// which is the common io size on most of the storage/filesystem.
	maxToBuffer := RecordsPerRead * iop.RecordLen
	readBuffer := r.readBuffer[:maxToBuffer]
	// Scan direction
	direction := iop.Limit.Direction

	// Set the result set size based on defined limits
	var limitBytes int32
	if iop.Limit.Number != math.MaxInt32 {
		limitBytes = iop.RecordLen * iop.Limit.Number
	} else {
		limitBytes = math.MaxInt32
		if direction == LAST {
			return nil, 0, fmt.Errorf("Reverse scan only supported with a limited result set")
		}
	}

	/*
		if direction == FIRST
			Read Forward to fill final buffer
			Read Backward to get previous record (for Tprev overlap)
				Strip Tprev from previous record
		if direction == LAST
			Read Backward to fill final buffer
				Strip Tprev from first record
				Cut first record from final buffer
	*/

	/*
		We save a map of file paths to the buffer data so that we can handle indirect data later
		For indirect data, we read the triplets {index, offset, len} from the primary area, then in a
		second pass, we read the data itself using the offset, len from the triplet.
	*/
	var bufMeta []bufferMeta
	// avoid allocation if not needed
	if iop.RecordType == VARIABLE {
		bufMeta = make([]bufferMeta, 0)
	}
	var finished bool
	if direction == FIRST || direction == 0 {
		for _, fp := range iop.FilePlan {
			dataLen := len(resultBuffer)
			resultBuffer, finished, err = readForward(resultBuffer,
				fp,
				iop.IntervalSeconds,
				iop.RecordLen,
				limitBytes,
				readBuffer)
			if iop.RecordType == VARIABLE {
				// If we've added data to the buffer from this file, record it for possible later use
				if len(resultBuffer) > dataLen {
					bufMeta = append(bufMeta, bufferMeta{
						FullPath:  fp.FullPath,
						Data:      resultBuffer[dataLen:],
						VarRecLen: iop.VariableRecordLen,
						Intervals: iop.GetIntervalsPerDay(),
					})
				}
			}
			if finished {
				break
			}
		}
		if GatherTprev {
			// Set the default tPrev to the base time of the oldest file in the PrevPlan minus one minute
			prevCount := len(iop.PrevFilePlan)
			if prevCount > 0 {
				tPrev = time.Unix(iop.PrevFilePlan[prevCount-1].BaseTime, 0).Add(-time.Duration(time.Minute)).UTC().Unix()
			}
			// Scan backward until we find the first previous time
			// Scan the file at the beginning of the date range unless the range started at the file begin
			finished = false
			for _, fp := range iop.PrevFilePlan {
				var tPrevBuff []byte
				tPrevBuff, finished, bytesRead, err := readBackward(
					tPrevBuff,
					fp,
					iop.IntervalSeconds,
					iop.RecordLen,
					iop.RecordLen,
					readBuffer,
					r.fileBuffer)
				if finished {
					if bytesRead != 0 {
						// We found a record, let's grab the tPrev time from it
						tPrev = *((*int64)(unsafe.Pointer(&tPrevBuff[0])))
					}
					break
				} else if err != nil {
					// We did not finish the scan and have an error, return the error
					return nil, 0, err
				}
			}
		}
	} else if direction == LAST {
		if GatherTprev {
			// Add one more record to the results in order to obtain the previous time
			limitBytes += iop.RecordLen
		}
		// This is safe because we know limitBytes is a sane value for reverse scans
		bytesLeftToFill := limitBytes
		fp := iop.FilePlan
		var bytesRead int32
		for i := len(fp) - 1; i >= 0; i-- {
			// Backward scan - we know that we are going to produce a limited result set here
			resultBuffer, finished, bytesRead, err = readBackward(resultBuffer,
				fp[i],
				iop.IntervalSeconds,
				iop.RecordLen,
				bytesLeftToFill,
				readBuffer,
				r.fileBuffer)
			bytesLeftToFill -= bytesRead
			if iop.RecordType == VARIABLE {
				// If we've added data to the buffer from this file, record it for possible later use
				if bytesRead > 0 {
					if bytesLeftToFill < 0 {
						bytesLeftToFill = 0
					}
					bufMeta = append(bufMeta, bufferMeta{
						FullPath:  fp[i].FullPath,
						Data:      resultBuffer[bytesLeftToFill:],
						VarRecLen: iop.VariableRecordLen,
						Intervals: iop.GetIntervalsPerDay(),
					})
				}
			}
			if finished {
				// We may have hit an error, but we finished the scan
				break
			} else if err != nil {
				// We did not finish the scan and have an error, return the error
				return nil, 0, err
			}
		}

		// We will return only what we've read, note that bytesLeftToFill might be negative because of buffering
		if bytesLeftToFill > 0 && len(resultBuffer) > 0 {
			resultBuffer = resultBuffer[bytesLeftToFill:]
		}

		/*
			Reverse the order of the files because the data was filled in reverse order
		*/
		if iop.RecordType == VARIABLE {
			lenOF := len(bufMeta)
			for i := 0; i < lenOF/2; i++ {
				bufMeta[(lenOF-1)-i] = bufMeta[i]
			}
		}

		if GatherTprev {
			if len(resultBuffer) > 0 {
				tPrev = *((*int64)(unsafe.Pointer(&resultBuffer[0])))
				// Chop off the first record
				resultBuffer = resultBuffer[iop.RecordLen:]
				if iop.RecordType == VARIABLE {
					/*
						Chop the first record off of the buffer map as well
					*/
					bufMeta[0].Data = bufMeta[0].Data[iop.RecordLen:]
				}
			} else {
				tPrev = 0
			}
		}
	}

	/*
		If this is a variable record type, we need a second stage of reading to get the data from the files
	*/
	if iop.RecordType == VARIABLE {
		resultBuffer, err = r.readSecondStage(bufMeta)
		if err != nil {
			return nil, 0, err
		}
	}

	return resultBuffer, tPrev, err
}

func packingReader(packedBuffer *[]byte, f io.ReadSeeker, recordSize int32, buffer []byte,
	maxRead, intervalSecs int64, fp *ioFilePlan) error {
	// Reads data from file f positioned after the header
	// Will read records of size recordsize, decoding the index value to determine if this is a null or valid record
	// The output is a buffer "packedBuffer" that contains only valid records
	// The index value is converted to a UNIX Epoch timestamp based on the basetime and intervalsecs
	// buffer is the temporary buffer to store read content from file, and indicates the maximum size to read
	// maxRead limits the number of bytes to be read from the file
	// Exit conditions:
	// ==> leftbytes <= 0

	baseTime := fp.BaseTime

	var totalRead int64
	for {
		n, _ := f.Read(buffer)

		nn := int64(n)
		totalRead += nn
		if nn == 0 {
			// We are done reading
			return nil
		} else if nn < int64(recordSize) {
			return fmt.Errorf("packingReader: Short read %d bytes, recordsize: %d bytes", n, recordSize)
		}
		// Calculate how many are left to read
		leftBytes := maxRead - totalRead
		if leftBytes < 0 {
			//			fmt.Printf("We are here leftBytes: %d, maxRead: %d, totalRead: %d\n",leftBytes,maxRead, totalRead)
			// Limit how many items we pack to maxread
			nn += leftBytes
		}

		numToRead := nn / int64(recordSize)
		var i int64
		for i = 0; i < numToRead; i++ {
			curpos := i * int64(recordSize)
			index := *(*int64)(unsafe.Pointer(&buffer[curpos]))
			if index != 0 {
				// Convert the index to a UNIX timestamp (seconds from epoch)
				index = baseTime + (index-1)*intervalSecs
				*(*int64)(unsafe.Pointer(&buffer[curpos])) = index
				*packedBuffer = append(*packedBuffer, buffer[curpos:curpos+int64(recordSize)]...)

				// Update lastKnown only once the first time
				if fp.seekingLast {
					if offset, err := f.Seek(0, os.SEEK_CUR); err == nil {
						offset = offset - nn + i*int64(recordSize)
						readhint.SetLastKnown(fp.FullPath, offset)
					}
					fp.seekingLast = false
				}
			}
		}
		if leftBytes <= 0 {
			return nil
		}
	}
}

func readForward(finalBuffer []byte, fp *ioFilePlan, intervalSeconds int64, recordLen, bytesToRead int32, readBuffer []byte) (
	resultBuffer []byte, finished bool, err error) {

	filepath := fp.FullPath
	offset := fp.Offset
	length := fp.Length

	if finalBuffer == nil {
		finalBuffer = make([]byte, 0, len(readBuffer))
	}
	// Forward scan
	f, err := os.OpenFile(filepath, os.O_RDONLY, 0666)
	if err != nil {
		Log(ERROR, "Read: opening %s\n%s", filepath, err)
		return nil, false, err
	}
	defer f.Close()

	if _, err = f.Seek(offset, os.SEEK_SET); err != nil {
		Log(ERROR, "Read: seeking in %s\n%s", filepath, err)
		return finalBuffer, false, err
	}

	if err = packingReader(&finalBuffer, f, recordLen, readBuffer, length, intervalSeconds, fp); err != nil {
		Log(ERROR, "Read: reading data from %s\n%s", filepath, err)
		return finalBuffer, false, err

	}
	//			fmt.Printf("Length of final buffer: %d\n",len(finalBuffer))
	if int32(len(finalBuffer)) >= bytesToRead {
		//				fmt.Printf("Clipping final buffer: %d\n",limitBytes)
		finalBuffer = finalBuffer[:bytesToRead]
		return finalBuffer, true, nil
	}
	return finalBuffer, false, nil
}

func readBackward(finalBuffer []byte, fp *ioFilePlan, intervalSeconds int64,
	recordLen, bytesToRead int32, readBuffer []byte, fileBuffer []byte) (
	result []byte, finished bool, bytesRead int32, err error) {

	filepath := fp.FullPath
	beginPos := fp.Offset
	length := fp.Length

	maxToBuffer := int32(len(readBuffer))
	if finalBuffer == nil {
		finalBuffer = make([]byte, bytesToRead, bytesToRead)
	}

	f, err := os.OpenFile(filepath, os.O_RDONLY, 0666)
	if err != nil {
		Log(ERROR, "Read: opening %s\n%s", filepath, err)
		return nil, false, 0, err
	}
	defer f.Close()

	// Seek to the right end of the search set
	f.Seek(beginPos+length, os.SEEK_SET)
	// Seek backward one buffer size (max)
	maxToRead, curpos, err := seekBackward(f, maxToBuffer, beginPos)
	if err != nil {
		Log(ERROR, "Read: seeking within %s\n%s", filepath, err)
		return nil, false, 0, err
	}

	for {
		fileBuffer = fileBuffer[:0]
		// Read a packed buffer of data max size maxToBuffer
		if err = packingReader(&fileBuffer, f, recordLen, readBuffer,
			maxToRead, intervalSeconds, fp); err != nil {

			Log(ERROR, "Read: reading data from %s\n%s", filepath, err)
			return nil, false, 0, err
		}

		numRead := int32(len(fileBuffer))

		// Copy the found data into the final buffer in reverse order
		if numRead != 0 {
			bytesRead += numRead
			if numRead <= bytesToRead {
				bytesToRead -= numRead
				copy(finalBuffer[bytesToRead:], fileBuffer)
			} else {
				copy(finalBuffer, fileBuffer[numRead-bytesToRead:])
				bytesToRead = 0
				break
			}
		}

		/*
			Check if current cursor has hit the left boundary (offset)
		*/
		if curpos != beginPos {
			// Seek backward two buffers worth - one for the buffer we just read and one
			// more backward to the new data
			maxToRead, curpos, err = seekBackward(f, 2*maxToBuffer, beginPos)
			// Subtract the previous buffer size
			maxToRead -= int64(maxToBuffer)
			// Exit the read operation if we get here with an error
			if err != nil {
				Log(ERROR, "Read: seeking within %s\n%s", filepath, err)
				return nil, false, 0, err
			}
		} else {
			break
		}

	}
	if bytesToRead == 0 {
		return finalBuffer, true, bytesRead, nil
	} else {
		return finalBuffer, false, bytesRead, nil
	}
}

func seekBackward(f io.Seeker, relative_offset int32, lowerBound int64) (seekAmt int64, curpos int64, err error) {
	// Find the current file position
	curpos, err = f.Seek(0, os.SEEK_CUR)
	if err != nil {
		Log(ERROR, "Read: can not find current file position: %s", err)
		return 0, curpos, err
	}
	// If seeking backward would go lower than the lower bound, seek to lower bound
	if (curpos - int64(relative_offset)) <= int64(lowerBound) {
		seekAmt = curpos - lowerBound
	} else {
		seekAmt = int64(relative_offset)
	}
	curpos, err = f.Seek(-seekAmt, os.SEEK_CUR)
	if err != nil {
		err = fmt.Errorf("Error: seeking to rel offset: %d lowerBound: %d | %s",
			relative_offset, lowerBound, err)
		return 0, curpos, err
	}
	return seekAmt, curpos, nil
}

func addUncommittedData(inputData []byte, key TimeBucketKey, pr planner.ParseResult, recordLength,
	nrecords int) (moddedData []byte) {
	inputData = appendUncommitted(inputData, key, pr.Range.End, recordLength)

	rowCount := len(inputData) / recordLength
	if rowCount > nrecords && nrecords != 0 {
		endIdx := recordLength * (rowCount - 1)
		endEpoch := ToInt64(inputData[endIdx : endIdx+8])
		endm1Epoch := ToInt64(inputData[endIdx-recordLength : endIdx-recordLength+8])
		if endEpoch == 0 {
			moddedData = inputData[:recordLength*(rowCount-1)]
		} else {
			if endEpoch == endm1Epoch || pr.Limit.Direction == FIRST {
				moddedData = inputData[:endIdx-recordLength]
			} else {
				moddedData = inputData[recordLength:]
			}
		}
	} else {
		return inputData
	}
	return moddedData
}

func appendUncommitted(buffer []byte, key TimeBucketKey, end time.Time, recordLength int) []byte {
	ThisInstance.AggregateCache.RLock()
	if data, ok := ThisInstance.AggregateCache.DataMap[key]; !ok {
		ThisInstance.AggregateCache.RUnlock()
	} else {
		ThisInstance.AggregateCache.RUnlock()
		if len(buffer) == 0 {
			return data
		}
		uncommittedTs := time.Unix(ToInt64(data[:8]), 0).UTC()
		lastTs := time.Unix(ToInt64(buffer[len(buffer)-recordLength:len(buffer)-recordLength+8]), 0).UTC()
		if (uncommittedTs.Before(end) || uncommittedTs.Equal(end)) && lastTs.Before(uncommittedTs) {
			buffer = append(buffer, data...)
		}
	}
	return buffer
}
