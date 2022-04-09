package io

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	epochColumnName = "Epoch"
	// see docs/design/file_format_design.txt for the details.
	versionHeaderBytes     = 8
	descriptionHeaderBytes = 256
	yearHeaderBytes        = 8
	intervalsHeaderBytes   = 8
	recordTypeHeaderBytes  = 8 // 0: fixed length records, 1: variable length records
	nFieldsHeaderBytes     = 8 // number of fields per record
	recLenHeaderBytes      = 8 // recordLength
	reservedHeader1Bytes   = 8
	elementNameHeaderBytes = 32   // 32bytes per element
	maxNumElements         = 1024 // max number of elements in a bucket
	reservedHeader2Bytes   = 365
	Headersize             = 37024
	FileinfoVersion        = int64(2.0)
	epochLenBytes          = 8
)

func nanosecondsInYear(year int) int64 {
	start := time.Date(year, time.January, 1, 0, 0, 0, 0, time.Local)
	end := time.Date(year+1, time.January, 1, 0, 0, 0, 0, time.Local)
	return end.Sub(start).Nanoseconds()
}

// FileSize returns the necessary size for a data file.
func FileSize(tf time.Duration, year, recordSize int) int64 {
	return Headersize + (nanosecondsInYear(year)/tf.Nanoseconds())*int64(recordSize)
}

type TimeBucketInfo struct {
	// Year, Path and IsRead are all set on catalog startup
	Year int16
	// Path is the absolute path to the data binary file.
	// (e.g. "/project/marketstore/data/TEST/1Sec/Tick/2021.bin")
	Path   string
	IsRead bool

	version     int64
	description string
	timeframe   time.Duration
	nElements   int32
	recordType  EnumRecordType
	// recordLength:
	// - For fixed recordType, the sum of field lengths in elementTypes.
	//   - e.g. if the columns are "Epoch(INT64), Ask(FLOAT32), Bid(FLOAT32)", recordLength=8+4+4=16.
	// - For variable recordType, this is the length of the indirect data pointer {index, offset, len}
	//   - as of 2022-02-01, always 24(=8+8+8)[byte].
	recordLength int32
	// variableRecordLength:
	// - For fixed recordType, always 0.
	// - In case of variable recordType, the sum of field lengths in elementTypes.
	//   - it doesn't include "Epoch" or "Nanoseconds" column, but include "IntervalTicks" bytes(=4 bytes).
	//   - e.g. if the columns are "Epoch(INT64), Ask(FLOAT32), Bid(FLOAT32), Nanoseconds(INT32)",
	//     variableRecordLength=12(Ask(4byte)+Bid(4byte)+IntervalTicks(4byte))
	variableRecordLength int32
	// e.g. []string{"Bid", "Ask"}.  elementNames doesn't include "Epoch" column or "Nanoseconds" column.
	elementNames []string
	// e.g. []io.EnumElementType{FLOAT32, FLOAT32}. elementTypes doesn't include "Epoch" column or "Nanoseconds" column.
	elementTypes []EnumElementType

	once sync.Once
}

func AlignedSize(unalignedSize int) (alignedSize int) {
	machineWordSize := int(unsafe.Alignof(uintptr(0)))
	remainder := unalignedSize % machineWordSize
	if remainder == 0 {
		return unalignedSize
	}
	return unalignedSize + machineWordSize - remainder
}

func NewTimeBucketInfo(tf utils.Timeframe, path, description string, year int16,
	dsv []DataShape, recordType EnumRecordType,
) (f *TimeBucketInfo) {
	elementTypes, elementNames := CreateShapesForTimeBucketInfo(dsv)
	f = &TimeBucketInfo{
		version:      FileinfoVersion,
		Path:         filepath.Join(path, strconv.Itoa(int(year))+".bin"),
		IsRead:       true,
		timeframe:    tf.Duration,
		description:  description,
		Year:         year,
		nElements:    int32(len(elementTypes)),
		elementTypes: elementTypes,
		elementNames: elementNames,
		recordType:   recordType,
	}
	if f.recordType == FIXED {
		f.recordLength = int32(AlignedSize(f.getFieldRecordLength())) + epochLenBytes // add an 8-byte epoch field
	} else if f.recordType == VARIABLE {
		f.recordLength = 24 // Length of the indirect data pointer {index, offset, len}
		f.variableRecordLength = 0
	}
	return f
}

func CreateShapesForTimeBucketInfo(dsv []DataShape) (elementTypes []EnumElementType, elementNames []string) {
	/*
		Takes a datashape array and returns elementTypes and elementNames
		***NOTE*** Excludes the Epoch column if found from the datashapes used for the file

	*/
	for _, shape := range dsv {
		if shape.Name != epochColumnName {
			elementTypes = append(elementTypes, shape.Type)
			elementNames = append(elementNames, shape.Name)
		}
	}
	return elementTypes, elementNames
}

func (f *TimeBucketInfo) GetDataShapes() []DataShape {
	return NewDataShapeVector(
		f.GetElementNames(),
		f.GetElementTypes())
}

func (f *TimeBucketInfo) GetDataShapesWithEpoch() (out []DataShape) {
	ep := DataShape{Name: epochColumnName, Type: INT64}
	dsv := f.GetDataShapes()
	out = append(out, ep)
	out = append(out, dsv...)
	return out
}

func (f *TimeBucketInfo) getFieldRecordLength() (fieldRecordLength int) {
	for _, elType := range f.GetElementTypes() {
		fieldRecordLength += elType.Size()
	}
	return fieldRecordLength
}

// GetDeepCopy returns a copy of this TimeBucketInfo.
func (f *TimeBucketInfo) GetDeepCopy() *TimeBucketInfo {
	f.once.Do(f.initFromFile)
	fcopy := TimeBucketInfo{
		Year:                 f.Year,
		Path:                 f.Path,
		IsRead:               f.IsRead,
		version:              f.version,
		description:          f.description,
		timeframe:            f.timeframe,
		nElements:            f.nElements,
		recordType:           f.recordType,
		recordLength:         f.recordLength,
		variableRecordLength: f.variableRecordLength,
	}
	fcopy.elementNames = make([]string, len(f.elementNames))
	fcopy.elementTypes = make([]EnumElementType, len(f.elementTypes))
	copy(fcopy.elementNames, f.elementNames)
	copy(fcopy.elementTypes, f.elementTypes)
	return &fcopy
}

func (f *TimeBucketInfo) initFromFile() {
	if f.IsRead {
		// do nothing if we found it already done
		return
	}
	if err := f.readHeader(f.Path); err != nil {
		log.Fatal(err.Error())
	}
	f.IsRead = true
}

// GetVersion returns the version number for the given TimeBucketInfo.
func (f *TimeBucketInfo) GetVersion() int64 {
	f.once.Do(f.initFromFile)
	return f.version
}

// GetDescription returns the description string contained in the
// given TimeBucketInfo.
func (f *TimeBucketInfo) GetDescription() string {
	f.once.Do(f.initFromFile)
	return f.description
}

// GetTimeframe returns the duration for which each record's data is valid.
// This means for 1Min resolution data, GetTimeframe will return time.Minute.
func (f *TimeBucketInfo) GetTimeframe() time.Duration {
	f.once.Do(f.initFromFile)
	return f.timeframe
}

// GetIntervals returns the number of records that can fit in a 24 hour day.
func (f *TimeBucketInfo) GetIntervals() int64 {
	f.once.Do(f.initFromFile)
	return utils.Day.Nanoseconds() / f.timeframe.Nanoseconds()
}

// GetNelements returns the number of elements (data fields) for a given
// TimeBucketInfo.
func (f *TimeBucketInfo) GetNelements() int32 {
	f.once.Do(f.initFromFile)
	return f.nElements
}

// GetRecordLength returns the length of a single record in the file described
// by the given TimeBucketInfo.
func (f *TimeBucketInfo) GetRecordLength() int32 {
	f.once.Do(f.initFromFile)
	return f.recordLength
}

// GetVariableRecordLength returns the length of a single record for a variable
// length TimeBucketInfo file.
func (f *TimeBucketInfo) GetVariableRecordLength() int32 {
	const intervalTicksLenBytes = 4
	f.once.Do(f.initFromFile)

	if f.recordType == VARIABLE && f.variableRecordLength == 0 {
		// Variable records use the raw element sizes plus a 4-byte trailer for interval ticks
		f.variableRecordLength = int32(f.getFieldRecordLength()) + intervalTicksLenBytes
	}
	return f.variableRecordLength
}

// GetRecordType returns the type of the file described by the TimeBucketInfo
// as an EnumRecordType.
func (f *TimeBucketInfo) GetRecordType() EnumRecordType {
	f.once.Do(f.initFromFile)
	return f.recordType
}

// GetElementNames returns the field names contained by the file described by
// the given TimeBucketInfo.
func (f *TimeBucketInfo) GetElementNames() []string {
	f.once.Do(f.initFromFile)
	return f.elementNames
}

// GetElementTypes returns the field types contained by the file described by
// the given TimeBucketInfo.
func (f *TimeBucketInfo) GetElementTypes() []EnumElementType {
	f.once.Do(f.initFromFile)
	return f.elementTypes
}

// SetElementTypes sets the field types contained by the file described by
// the given TimeBucketInfo.
func (f *TimeBucketInfo) SetElementTypes(newTypes []EnumElementType) error {
	if len(newTypes) != len(f.elementTypes) {
		return fmt.Errorf("element count not equal")
	}
	for i, val := range newTypes {
		f.elementTypes[i] = val
	}
	return nil
}

func (f *TimeBucketInfo) readHeader(path string) (err error) {
	const headerPart1Bytes = versionHeaderBytes + descriptionHeaderBytes + yearHeaderBytes + intervalsHeaderBytes +
		recordTypeHeaderBytes + nFieldsHeaderBytes + recLenHeaderBytes + reservedHeader1Bytes
	file, err := os.Open(path)
	if err != nil {
		log.Error("Failed to open file: %v - Error: %v", path, err.Error())
		return err
	}
	defer func(file *os.File) {
		if err2 := file.Close(); err2 != nil {
			log.Error("failed to close file: %v - Error: %v", path, err2.Error())
		}
	}(file)

	var buffer [Headersize]byte
	header := (*Header)(unsafe.Pointer(&buffer))
	// Read the top part of the header, which is not dependent on the number of elements
	n, err := file.Read(buffer[:headerPart1Bytes])
	if err != nil || n != headerPart1Bytes {
		log.Error("Failed to read header part1 from file: %v - Error: %v", path, err)
		return err
	}

	// Second part of read element names
	secondReadSize := header.NElements * elementNameHeaderBytes
	n, err = file.Read(buffer[headerPart1Bytes : headerPart1Bytes+secondReadSize])
	if err != nil || n != int(secondReadSize) {
		log.Error("Failed to read header part2 from file: %v - Error: %v", path, err)
		return err
	}

	// Read past empty element name space
	_, err = file.Seek(maxNumElements*elementNameHeaderBytes-secondReadSize, io.SeekCurrent)
	if err != nil {
		log.Error("Failed to read empty space for element names from file: %v - Error: %v", path, err)
		return err
	}

	// Read element types
	start := headerPart1Bytes + maxNumElements*elementNameHeaderBytes
	n, err = file.Read(buffer[start : start+int(header.NElements)])
	if err != nil || n != int(header.NElements) {
		log.Error("Failed to read header part3 from file: %v - Error: %v", path, err)
		return err
	}
	if EnumRecordType(header.RecordType) == VARIABLE {
		// Read to end of header
		start += int(header.NElements)
		n, err = file.Read(buffer[start:Headersize])
		if err != nil || n != (Headersize-start) {
			log.Error("Failed to read header part4 from file: %v - Error: %v", path, err)
			return err
		}
	}
	f.load(header, path)
	return nil
}

func (f *TimeBucketInfo) load(hp *Header, path string) {
	f.version = hp.Version
	f.description = string(bytes.Trim(hp.Description[:], "\x00"))
	f.Year = int16(hp.Year)
	f.Path = filepath.Clean(path)
	f.IsRead = true
	f.timeframe = time.Duration(hp.Timeframe)
	f.nElements = int32(hp.NElements)
	f.recordLength = int32(hp.RecordLength)
	f.recordType = EnumRecordType(hp.RecordType)
	f.elementNames = nil
	f.elementTypes = nil
	for i := 0; i < int(f.nElements); i++ {
		baseName := string(bytes.Trim(hp.ElementNames[i][:], "\x00"))
		f.elementNames = append(f.elementNames, baseName)
		f.elementTypes = append(f.elementTypes, EnumElementType(hp.ElementTypes[i]))
	}
}

// NewTimeBucketInfoFromHeader creates a TimeBucketInfo from a given Header.
func NewTimeBucketInfoFromHeader(hp *Header, path string) *TimeBucketInfo {
	tbi := new(TimeBucketInfo)
	tbi.load(hp, path)
	return tbi
}

// Header is the on-disk byte representation of the file header.
type Header struct {
	Version      int64
	Description  [descriptionHeaderBytes]byte
	Year         int64
	Timeframe    int64 // Duration in nanoseconds
	RecordType   int64
	NElements    int64
	RecordLength int64
	// nolint:structcheck // reserved
	reserved1 int64
	// Above is the fixed header portion - size is 312 Bytes = (7*8 + 256)
	ElementNames [maxNumElements][elementNameHeaderBytes]byte
	ElementTypes [maxNumElements]byte
	// nolint:structcheck // reserved
	reserved2 [reservedHeader2Bytes]int64
}

// WriteHeader writes the header described by a given TimeBucketInfo to the
// supplied file pointer.
func WriteHeader(file *os.File, f *TimeBucketInfo) error {
	header := Header{}
	header.Load(f)
	bp := (*[Headersize]byte)(unsafe.Pointer(&header))
	_, err := file.Write(bp[:])
	return err
}

// Load loads the header information from a given TimeBucketInfo.
func (hp *Header) Load(f *TimeBucketInfo) {
	if f.GetVersion() != FileinfoVersion {
		log.Warn(
			"FileInfoVersion does not match this version of MarketStore %v != %v",
			f.GetVersion(), FileinfoVersion)
	}
	hp.Version = f.GetVersion()
	copy(hp.Description[:], f.GetDescription())
	hp.Year = int64(f.Year)
	hp.Timeframe = f.GetTimeframe().Nanoseconds()
	hp.NElements = int64(f.GetNelements())
	hp.RecordLength = int64(f.GetRecordLength())
	hp.RecordType = int64(f.GetRecordType())
	for i := 0; i < int(hp.NElements); i++ {
		copy(hp.ElementNames[i][:], f.GetElementNames()[i])
		hp.ElementTypes[i] = byte(f.GetElementTypes()[i])
	}
	hp.RecordType = int64(f.GetRecordType())
}
