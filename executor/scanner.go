package executor

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"time"

	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	utilsio "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	recordsPerRead        = 8192
	epochLenBytes         = 8
	nanosecLenBytes       = 4
	intervalTicksLenBytes = 4
)

type SortedFileList []planner.QualifiedFile

func (fl SortedFileList) Len() int           { return len(fl) }
func (fl SortedFileList) Swap(i, j int)      { fl[i], fl[j] = fl[j], fl[i] }
func (fl SortedFileList) Less(i, j int) bool { return fl[i].File.Year < fl[j].File.Year }

type ioFilePlan struct {
	tbi      *utilsio.TimeBucketInfo
	Offset   int64
	Length   int64
	FullPath string // Full file path, including leaf (Year) file
	// The time that begins each file in seconds since the Unix epoch
	BaseTime    int64
	seekingLast bool
}

func (iofp *ioFilePlan) GetFileYear() int16 {
	return iofp.tbi.Year
}

type IOPlan struct {
	FilePlan          []*ioFilePlan
	RecordLen         int32
	RecordType        utilsio.EnumRecordType
	VariableRecordLen int
	Limit             *planner.RowLimit
	Range             *planner.DateRange
	TimeQuals         []planner.TimeQualFunc
}

func NewIOPlan(fl SortedFileList, limit *planner.RowLimit, range2 *planner.DateRange, timeQuals []planner.TimeQualFunc,
) (iop *IOPlan, err error) {
	iop = &IOPlan{
		FilePlan: make([]*ioFilePlan, 0),
		Limit:    limit,
		Range:    range2,
	}
	/*
		At this point we have a date unconstrained group of sorted files
		We will do two things here:
		1) create the list of date qualified files to read for the primary data
		2) create a list of files with times prior to the date range in reverse order
	*/
	for _, file := range fl {
		fileStartTime := time.Date(
			int(file.File.Year),
			time.January,
			1, 0, 0, 0, 0,
			utils.InstanceConfig.Timezone)
		startOffset := int64(utilsio.Headersize)
		endOffset := utilsio.FileSize(
			file.File.GetTimeframe(),
			int(file.File.Year),
			int(file.File.GetRecordLength()))
		length := endOffset - startOffset
		maxLength := length + int64(file.File.GetRecordLength())
		if iop.RecordLen == 0 {
			iop.RecordLen = file.File.GetRecordLength()
			iop.RecordType = file.File.GetRecordType()
			iop.VariableRecordLen = int(file.File.GetVariableRecordLength())
		} else if file.File.GetRecordLength() != iop.RecordLen {
			// check that we're reading the same recordlength across all files, return err if not
			return nil, RecordLengthNotConsistent("NewIOPlan")
		}

		if file.File.Year >= int16(range2.Start.Year()) && file.File.Year <= int16(range2.End.Year()) {
			/*
			 Calculate the number of bytes to be read for each file and the offset
			*/
			// Set the starting and ending indices based on the range
			if file.File.Year == int16(range2.Start.Year()) {
				// log.Info("range start: %v", pr.Range.Start)
				startOffset = utilsio.TimeToOffset(
					range2.Start,
					file.File.GetTimeframe(),
					file.File.GetRecordLength(),
				)
				// log.Info("start offset: %v", startOffset)
			}
			if file.File.Year == int16(range2.End.Year()) {
				// log.Info("range end: %v", pr.Range.End)

				endOffset = utilsio.TimeToOffset(
					range2.End,
					file.File.GetTimeframe(),
					file.File.GetRecordLength()) + int64(file.File.GetRecordLength())
			}
			length = endOffset - startOffset
			// Limit the scan to the end of the fixed length data
			if length > maxLength {
				length = maxLength
			}
			fp := &ioFilePlan{
				file.File,
				startOffset,
				length,
				file.File.Path,
				fileStartTime.Unix(),
				false,
			}
			if iop.Limit.Direction == utilsio.LAST {
				fp.seekingLast = true
			}
			iop.FilePlan = append(iop.FilePlan, fp)
		}
	}

	iop.TimeQuals = timeQuals

	return iop, nil
}

type Reader struct {
	pr     planner.ParseResult
	IOPMap map[utilsio.TimeBucketKey]*IOPlan
	// for packingReader to avoid redundant allocation.
	// really ought to be somewhere close to the function...
	readBuffer []byte
	fileBuffer []byte
}

func NewReader(pr *planner.ParseResult) (r *Reader, err error) {
	r = new(Reader)
	r.pr = *pr
	if pr.Range == nil {
		pr.Range = planner.NewDateRange()
	}

	sortedFileMap := make(map[utilsio.TimeBucketKey]SortedFileList)
	for _, qf := range pr.QualifiedFiles {
		sortedFileMap[qf.Key] = append(sortedFileMap[qf.Key], qf)
	}
	r.IOPMap = make(map[utilsio.TimeBucketKey]*IOPlan)
	maxRecordLen := int32(0)
	for key, sfl := range sortedFileMap {
		sort.Sort(sfl)
		if r.IOPMap[key], err = NewIOPlan(sfl, pr.Limit, pr.Range, pr.TimeQuals); err != nil {
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
	readSize := recordsPerRead * maxRecordLen
	r.readBuffer = make([]byte, readSize)
	r.fileBuffer = make([]byte, readSize)

	return r, nil
}

func (r *Reader) Read() (csm utilsio.ColumnSeriesMap, err error) {
	// TODO: Need to consider the huge buffer which use loooong time gap to query.
	// Which probably cause out of memory issue and need new mechanism to handle
	// those data and not just simply return one ColumnSeriesMap.
	// Solution: Hack ColumnSeries add subsection fields to break the one big query
	// down to several parts of small query and each one's Range.Start follow the last's
	// Range.End with same other conditions.
	csm = utilsio.NewColumnSeriesMap()
	rtMap := r.pr.GetRecordType()
	dsMap := r.pr.GetDataShapes()
	rlMap := r.pr.GetRowLen()
	for key, iop := range r.IOPMap {
		rt := rtMap[key]
		rlen := rlMap[key]
		buffer, err2 := r.read(iop)
		if err2 != nil {
			return nil, err2
		}
		if rt == utilsio.VARIABLE {
			buffer = trimResultsToRange(r.pr.Range, rlen, buffer)
			buffer = trimResultsToLimit(r.pr.Limit, rlen, buffer)
		}
		rs := utilsio.NewRowSeries(key, buffer, dsMap[key], rlen, rt)
		key, cs := rs.ToColumnSeries()
		csm[key] = cs
	}
	return csm, err
}

func trimResultsToRange(dr *planner.DateRange, rowlen int, src []byte) (dest []byte) {
	// find the beginning of the range (sorted order)
	rowLength := rowlen + epochLenBytes + nanosecLenBytes - intervalTicksLenBytes
	nrecords := len(src) / rowLength
	if nrecords == 0 {
		return nil
	}
	cursor := 0
	for i := 0; i < nrecords; i++ {
		t := TimeOfVariableRecord(src, cursor, rowLength)
		if t.Equal(dr.Start) || t.After(dr.Start) {
			dest = src[cursor:]
			break
		}
		cursor += rowLength
	}

	nrecords = len(dest) / rowLength
	if nrecords <= 1 {
		return dest
	}
	for i := nrecords; i > 0; i-- {
		cursor = (i - 1) * rowLength
		t := TimeOfVariableRecord(dest, cursor, rowLength)
		if t.Equal(dr.End) || t.Before(dr.End) {
			dest = dest[:cursor+rowLength]
			break
		}
	}

	return dest
}

func TimeOfVariableRecord(buf []byte, cursor, rowLength int) time.Time {
	epoch := utilsio.ToInt64(buf[cursor : cursor+epochLenBytes])
	nanos := utilsio.ToInt32(buf[cursor+rowLength-nanosecLenBytes : cursor+rowLength])
	return utilsio.ToSystemTimezone(time.Unix(epoch, int64(nanos)))
}

func trimResultsToLimit(l *planner.RowLimit, rowLen int, src []byte) []byte {
	rowLength := rowLen + epochLenBytes + nanosecLenBytes - intervalTicksLenBytes

	nrecords := len(src) / rowLength
	limit := int(l.Number)

	if nrecords > limit {
		if l.Direction == utilsio.FIRST {
			return src[:limit*rowLength]
		}
		return src[len(src)-limit*rowLength:]
	}
	return src
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
// Uses the index that prepends each row to identify filled rows versus holes.
func (r *Reader) read(iop *IOPlan) ([]byte, error) {
	var (
		resultBuffer []byte
		err          error
	)
	// Number of bytes to buffer, some multiple of record length
	// This should be at least bigger than 4096 and be better multiple of 4KB,
	// which is the common io size on most of the storage/filesystem.
	maxToBuffer := recordsPerRead * iop.RecordLen
	readBuffer := r.readBuffer[:maxToBuffer]
	// Scan direction
	direction := iop.Limit.Direction

	// Set the result set size based on defined limits
	var limitBytes int32
	if iop.Limit.Number != math.MaxInt32 {
		limitBytes = iop.RecordLen * iop.Limit.Number
	} else {
		limitBytes = math.MaxInt32
		if direction == utilsio.LAST {
			return nil, fmt.Errorf("reverse scan only supported with a limited result set")
		}
	}

	ex := newIoExec(iop)

	/*
		if direction == FIRST
			Read Forward to fill final buffer
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
	if iop.RecordType == utilsio.VARIABLE {
		bufMeta = make([]bufferMeta, 0)
	}
	var finished bool
	if direction == utilsio.FIRST {
		for _, fp := range iop.FilePlan {
			dataLen := len(resultBuffer)
			resultBuffer, finished, err = ex.readForward(resultBuffer,
				fp,
				limitBytes,
				readBuffer)
			if iop.RecordType == utilsio.VARIABLE {
				// If we've added data to the buffer from this file, record it for possible later use
				if len(resultBuffer) > dataLen {
					bufMeta = append(bufMeta, bufferMeta{
						FullPath:  fp.FullPath,
						Data:      resultBuffer[dataLen:],
						VarRecLen: iop.VariableRecordLen,
						Intervals: fp.tbi.GetIntervals(),
					})
				}
			}
			if finished {
				break
			}
		}
	} else if direction == utilsio.LAST {
		// This is safe because we know limitBytes is a sane value for reverse scans
		bytesLeftToFill := limitBytes
		fp := iop.FilePlan
		var bytesRead int32
		for i := len(fp) - 1; i >= 0; i-- {
			// Backward scan - we know that we are going to produce a limited result set here
			resultBuffer, finished, bytesRead, err = ex.readBackward(
				resultBuffer,
				fp[i],
				bytesLeftToFill,
				readBuffer,
				r.fileBuffer)

			bytesLeftToFill -= bytesRead
			if iop.RecordType == utilsio.VARIABLE {
				// If we've added data to the buffer from this file, record it for possible later use
				if bytesRead > 0 {
					bufMetaLen := bytesRead
					// read enough amount of records
					if bytesLeftToFill < 0 {
						bytesLeftToFill = 0
						bufMetaLen = int32(len(resultBuffer))
					}
					bufMeta = append(bufMeta, bufferMeta{
						FullPath:  fp[i].FullPath,
						Data:      resultBuffer[bytesLeftToFill : bytesLeftToFill+bufMetaLen],
						VarRecLen: iop.VariableRecordLen,
						Intervals: fp[i].tbi.GetIntervals(),
					})
				}
			}
			if finished {
				// We may have hit an error, but we finished the scan
				break
			} else if err != nil {
				// We did not finish the scan and have an error, return the error
				return nil, err
			}
		}

		// We will return only what we've read, note that bytesLeftToFill might be negative because of buffering
		if bytesLeftToFill > 0 && len(resultBuffer) > 0 {
			resultBuffer = resultBuffer[bytesLeftToFill:]
		}

		/*
			Reverse the order of the files because the data was filled in reverse order
		*/
		if iop.RecordType == utilsio.VARIABLE {
			lenOF := len(bufMeta)
			for i := 0; i < lenOF/2; i++ {
				bufMeta[(lenOF-1)-i], bufMeta[i] = bufMeta[i], bufMeta[(lenOF-1)-i]
			}
		}
	}

	/*
		If this is a variable record type, we need a second stage of reading to get the data from the files
	*/
	if iop.RecordType == utilsio.VARIABLE {
		resultBuffer, err = r.readSecondStage(bufMeta)
		if err != nil {
			return nil, err
		}
	}
	return resultBuffer, err
}

type ioExec struct {
	plan *IOPlan
}

func (ex *ioExec) packingReader(packedBuffer *[]byte, f io.ReadSeeker, buffer []byte,
	maxRead int64, fp *ioFilePlan,
) error {
	// Reads data from file f positioned after the header
	// Will read records of size recordsize, decoding the index value to determine if this is a null or valid record
	// The output is a buffer "packedBuffer" that contains only valid records
	// The index value is converted to a UNIX Epoch timestamp based on the basetime and intervalsecs
	// buffer is the temporary buffer to store read content from file, and indicates the maximum size to read
	// maxRead limits the number of bytes to be read from the file
	// Exit conditions:
	// ==> leftbytes <= 0

	recordSize := ex.plan.RecordLen
	recordSize64 := int64(recordSize)

	var totalRead int64
	for {
		n, _ := f.Read(buffer)

		nn := int64(n)
		totalRead += nn
		if nn == 0 {
			// We are done reading
			return nil
		} else if nn < recordSize64 {
			return fmt.Errorf("packingReader: Short read %d bytes, recordsize: %d bytes", n, recordSize)
		}
		// Calculate how many are left to read
		leftBytes := maxRead - totalRead
		if leftBytes < 0 {
			//			fmt.Printf("We are here leftBytes: %d, maxRead: %d, totalRead: %d\n",leftBytes,maxRead, totalRead)
			// Limit how many items we pack to maxread
			nn += leftBytes
		}

		numToRead := int32(nn) / recordSize
		var i int32
		var indexuint64 uint64

		buf := buffer
		for i = 0; i < numToRead; i++ {
			indexuint64 = binary.LittleEndian.Uint64(buf)

			if indexuint64 != 0 {
				// Convert the index to a UNIX timestamp (seconds from epoch)
				index := utilsio.IndexToTime(int64(indexuint64), fp.tbi.GetTimeframe(), fp.GetFileYear()).Unix()
				if !ex.checkTimeQuals(index) {
					continue
				}
				idxpos := len(*packedBuffer)
				*packedBuffer = append(*packedBuffer, buf[:int64(recordSize)]...)
				b := *packedBuffer
				binary.LittleEndian.PutUint64(b[idxpos:], uint64(index))

				// Update lastKnown only once the first time
				if fp.seekingLast {
					_, _ = f.Seek(0, io.SeekCurrent)
					fp.seekingLast = false
				}
			}

			buf = buf[recordSize:]
		}
		if leftBytes <= 0 {
			return nil
		}
	}
}

func (ex *ioExec) readForward(finalBuffer []byte, fp *ioFilePlan, bytesToRead int32, readBuffer []byte) (
	resultBuffer []byte, finished bool, err error,
) {
	const readWriteAll = 0o666
	// log.Info("reading forward [recordLen: %v bytesToRead: %v]", recordLen, bytesToRead)
	filePath := fp.FullPath

	if finalBuffer == nil {
		finalBuffer = make([]byte, 0, len(readBuffer))
	}
	// Forward scan
	f, err := os.OpenFile(filePath, os.O_RDONLY, readWriteAll)
	if err != nil {
		log.Error("Read: opening %s\n%s", filePath, err)
		return nil, false, err
	}
	defer f.Close()

	if _, err = f.Seek(fp.Offset, io.SeekStart); err != nil {
		log.Error("Read: seeking in %s\n%s", filePath, err)
		return finalBuffer, false, err
	}

	if err = ex.packingReader(&finalBuffer, f, readBuffer, fp.Length, fp); err != nil {
		log.Error("Read: reading data from %s\n%s", filePath, err)
		return finalBuffer, false, err
	}
	//			fmt.Printf("Length of final buffer: %d\n",len(finalBuffer))
	if int32(len(finalBuffer)) >= bytesToRead {
		//				fmt.Printf("Clipping final buffer: %d\n",limitBytes)
		return finalBuffer[:bytesToRead], true, nil
	}
	return finalBuffer, false, nil
}

func (ex *ioExec) readBackward(finalBuffer []byte, fp *ioFilePlan,
	bytesToRead int32, readBuffer, fileBuffer []byte) (
	result []byte, finished bool, bytesRead int32, err error,
) {
	const readWriteAll = 0o666
	// log.Info("reading backward [recordLen: %v bytesToRead: %v offset: %v]", recordLen, bytesToRead, fp.Offset)

	filePath := fp.FullPath
	beginPos := fp.Offset

	maxToBuffer := int32(len(readBuffer))
	if finalBuffer == nil {
		finalBuffer = make([]byte, bytesToRead)
	}

	f, err := os.OpenFile(filePath, os.O_RDONLY, readWriteAll)
	if err != nil {
		log.Error("Read: opening %s\n%s", filePath, err)
		return nil, false, 0, err
	}
	defer f.Close()

	// Seek to the right end of the search set
	f.Seek(beginPos+fp.Length, io.SeekStart)
	// Seek backward one buffer size (max)
	maxToRead, curpos, err := seekBackward(f, maxToBuffer, beginPos)
	if err != nil {
		log.Error("Read: seeking within %s\n%s", filePath, err)
		return nil, false, 0, err
	}

	for {
		fileBuffer = fileBuffer[:0]
		// Read a packed buffer of data max size maxToBuffer
		if err = ex.packingReader(
			&fileBuffer,
			f, readBuffer,
			maxToRead, fp); err != nil {
			log.Error("Read: reading data from %s\n%s", filePath, err)
			return nil, false, 0, err
		}

		numRead := int32(len(fileBuffer))

		// Copy the found data into the final buffer in reverse order
		if numRead != 0 {
			bytesRead += numRead
			if numRead < bytesToRead {
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
				log.Error("Read: seeking within %s\n%s", filePath, err)
				return nil, false, 0, err
			}
		} else {
			break
		}
	}
	if bytesToRead == 0 {
		return finalBuffer, true, bytesRead, nil
	}
	return finalBuffer, false, bytesRead, nil
}

func seekBackward(f io.Seeker, relativeOffset int32, lowerBound int64) (seekAmt, curpos int64, err error) {
	// Find the current file position
	curpos, err = f.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Error("Read: cannot find current file position: %s", err)
		return 0, curpos, err
	}
	// If seeking backward would go lower than the lower bound, seek to lower bound
	if (curpos - int64(relativeOffset)) <= lowerBound {
		seekAmt = curpos - lowerBound
	} else {
		seekAmt = int64(relativeOffset)
	}
	curpos, err = f.Seek(-seekAmt, io.SeekCurrent)
	if err != nil {
		err = fmt.Errorf("error: seeking to rel offset: %d lowerBound: %d :%w",
			relativeOffset, lowerBound, err)
		return 0, curpos, err
	}
	return seekAmt, curpos, nil
}

func (ex *ioExec) checkTimeQuals(epoch int64) bool {
	if len(ex.plan.TimeQuals) > 0 {
		for _, timeQual := range ex.plan.TimeQuals {
			if !timeQual(epoch) {
				return false
			}
		}
	}
	return true
}

func newIoExec(iop *IOPlan) *ioExec {
	return &ioExec{
		plan: iop,
	}
}
