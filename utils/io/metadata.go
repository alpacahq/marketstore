package io

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const Headersize = 37024
const FileinfoVersion = int64(2.0)

type IsInitialized = uint32

var (
	TbiInitialized    IsInitialized = 1
	TbiNotInitialized IsInitialized = 0
)

func nanosecondsInYear(year int) int64 {
	start := time.Date(year, time.January, 1, 0, 0, 0, 0, time.Local)
	end := time.Date(year+1, time.January, 1, 0, 0, 0, 0, time.Local)
	return end.Sub(start).Nanoseconds()
}

// FileSize returns the necessary size for a data file
func FileSize(tf time.Duration, year int, recordSize int) int64 {
	return Headersize + (nanosecondsInYear(year)/tf.Nanoseconds())*int64(recordSize)
}

type TimeBucketInfo struct {
	// Year, Path and IsInitialized are all set on catalog startup
	Year int16
	// Path is the absolute path to the data binary file.
	// (e.g. "/project/marketstore/data/TEST/1Sec/Tick/2021.bin")
	Path          string
	IsInitialized IsInitialized

	version     int64
	description string
	timeframe   time.Duration
	nElements   int32
	recordType  EnumRecordType
	/*
	   recordLength:
	   - For fixed recordType, the sum of field lengths in elementTypes
	   - For variable recordType, this is the length of the indirect data pointer {index, offset, len}
	*/
	recordLength         int32
	variableRecordLength int32 // In case of variable recordType, the sum of field lengths in elementTypes
	elementNames         []string
	elementTypes         []EnumElementType

	mu sync.Mutex
}

func AlignedSize(unalignedSize int) (alignedSize int) {
	machineWordSize := int(unsafe.Alignof(uintptr(0)))
	remainder := unalignedSize % machineWordSize
	if remainder == 0 {
		return unalignedSize
	}
	return unalignedSize + machineWordSize - remainder
}

func NewTimeBucketInfo(tf utils.Timeframe, path, description string, year int16, dsv []DataShape, recordType EnumRecordType) (f *TimeBucketInfo) {
	elementTypes, elementNames := CreateShapesForTimeBucketInfo(dsv)
	f = &TimeBucketInfo{
		Year:          year,
		Path:          filepath.Join(path, strconv.Itoa(int(year))+".bin"),
		IsInitialized: 1,
		version:       FileinfoVersion,
		description:   description,
		timeframe:     tf.Duration,

		nElements:  int32(len(elementTypes)),
		recordType: recordType,

		elementTypes: elementTypes,
		elementNames: elementNames,
	}
	if f.recordType == FIXED {
		f.recordLength = int32(AlignedSize(f.getFieldRecordLength())) + 8 // add an 8-byte epoch field
		f.variableRecordLength = 0
	} else if f.recordType == VARIABLE {
		f.recordLength = 24 // Length of the indirect data pointer {index, offset, len}
		f.variableRecordLength = 0
	}
	return f
}

func NewTimeBucketInfoFromFile(path string) (*TimeBucketInfo, error) {
	header, err := readHeader(path)
	if err != nil {
		return nil, fmt.Errorf("read header of TimeBucketInfo path=%s:%w", path, err)
	}

	return NewTimeBucketInfoFromHeader(header, path), nil
}

// NewTimeBucketInfoFromHeader creates a TimeBucketInfo from a given Header
func NewTimeBucketInfoFromHeader(hp *Header, path string) *TimeBucketInfo {
	f := &TimeBucketInfo{
		Year:                 int16(hp.Year),
		Path:                 filepath.Clean(path),
		IsInitialized:        TbiInitialized,
		version:              hp.Version,
		description:          string(bytes.Trim(hp.Description[:], "\x00")),
		timeframe:            time.Duration(hp.Timeframe),
		nElements:            int32(hp.NElements),
		recordType:           EnumRecordType(hp.RecordType),
		recordLength:         int32(hp.RecordLength),
		variableRecordLength: 0,
		elementNames:         nil,
		elementTypes:         nil,
	}
	for i := 0; i < int(f.nElements); i++ {
		baseName := string(bytes.Trim(hp.ElementNames[i][:], "\x00"))
		f.elementNames = append(f.elementNames, baseName)
		f.elementTypes = append(f.elementTypes, EnumElementType(hp.ElementTypes[i]))
	}
	return f
}

func CreateShapesForTimeBucketInfo(dsv []DataShape) (elementTypes []EnumElementType, elementNames []string) {
	/*
		Takes a datashape array and returns elementTypes and elementNames
		***NOTE*** Excludes the Epoch column if found from the datashapes used for the file

	*/
	for _, shape := range dsv {
		if shape.Name != "Epoch" {
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
	ep := DataShape{Name: "Epoch", Type: INT64}
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
	err := f.initFromFile()
	if err != nil {

	}
	fcopy := TimeBucketInfo{
		Year:                 f.Year,
		Path:                 f.Path,
		IsInitialized:        f.IsInitialized,
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

// initFromFile retrieves all TimeBucketInfo parameters from f.filepath
// if the tbi is not initialized.
// Context: When catalog.Directory is loaded, new TimeBucketInfo struct is lazily constructed
// with only f.filepath and f.isInitialized=io.TbiNotInitialized parameters
// so that it doesn't need to actually open files to get the params.
// This function is called when the TimeBucketInfo is actually used.
func (f *TimeBucketInfo) initFromFile() error {
	if atomic.LoadUint32(&f.IsInitialized) == TbiInitialized {
		// do nothing if we found it already done
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// if not initialized yet, read the information from the file
	tbi, err := NewTimeBucketInfoFromFile(f.Path)
	if err != nil {
		return fmt.Errorf("failed to read TimeBucketInfo from file %s:%w", f.Path, err)
	}
	f.Year = tbi.Year
	f.Path = tbi.Path
	f.version = tbi.version
	f.description = tbi.description
	f.timeframe = tbi.timeframe
	f.nElements = tbi.nElements
	f.recordType = tbi.recordType
	f.recordLength = tbi.recordLength
	f.variableRecordLength = tbi.variableRecordLength
	f.elementNames = make([]string, len(tbi.elementNames))
	f.elementTypes = make([]EnumElementType, len(tbi.elementTypes))
	copy(f.elementNames, tbi.elementNames)
	copy(f.elementTypes, tbi.elementTypes)

	atomic.StoreUint32(&f.IsInitialized, TbiInitialized)
	return nil
}

// GetVersion returns the version number for the given TimeBucketInfo.
func (f *TimeBucketInfo) GetVersion() int64 {
	f.initFromFile()
	return f.version
}

// GetDescription returns the description string contained in the
// given TimeBucketInfo.
func (f *TimeBucketInfo) GetDescription() string {
	f.initFromFile()
	return f.description
}

// GetTimeframe returns the duration for which each record's data is valid.
// This means for 1Min resolution data, GetTimeframe will return time.Minute.
func (f *TimeBucketInfo) GetTimeframe() time.Duration {
	f.initFromFile()
	return f.timeframe
}

// GetIntervals returns the number of records that can fit in a 24 hour day.
func (f *TimeBucketInfo) GetIntervals() int64 {
	f.initFromFile()
	return utils.Day.Nanoseconds() / f.timeframe.Nanoseconds()
}

// GetNelements returns the number of elements (data fields) for a given
// TimeBucketInfo.
func (f *TimeBucketInfo) GetNelements() int32 {
	f.initFromFile()
	return f.nElements
}

// GetRecordLength returns the length of a single record in the file described
// by the given TimeBucketInfo
func (f *TimeBucketInfo) GetRecordLength() int32 {
	f.initFromFile()
	return f.recordLength
}

// GetVariableRecordLength returns the length of a single record for a variable
// length TimeBucketInfo file
func (f *TimeBucketInfo) GetVariableRecordLength() int32 {
	f.initFromFile()
	if f.recordType == VARIABLE && f.variableRecordLength == 0 {
		// Variable records use the raw element sizes plus a 4-byte trailer for interval ticks
		f.variableRecordLength = int32(f.getFieldRecordLength()) + 4 // Variable records have a 4-byte trailer
	}
	return f.variableRecordLength
}

// GetRecordType returns the type of the file described by the TimeBucketInfo
// as an EnumRecordType
func (f *TimeBucketInfo) GetRecordType() EnumRecordType {
	f.initFromFile()
	return f.recordType
}

// GetElementNames returns the field names contained by the file described by
// the given TimeBucketInfo
func (f *TimeBucketInfo) GetElementNames() []string {
	f.initFromFile()
	return f.elementNames
}

// GetElementTypes returns the field types contained by the file described by
// the given TimeBucketInfo
func (f *TimeBucketInfo) GetElementTypes() []EnumElementType {
	f.initFromFile()
	return f.elementTypes
}

// SetElementTypes sets the field types contained by the file described by
// the given TimeBucketInfo
func (f *TimeBucketInfo) SetElementTypes(newTypes []EnumElementType) error {
	if len(newTypes) != len(f.elementTypes) {
		return fmt.Errorf("Element count not equal")
	}
	for i, val := range newTypes {
		f.elementTypes[i] = val
	}
	return nil
}

func readHeader(path string) (header *Header, err error) {
	file, err := os.Open(path)
	if err != nil {
		log.Error("Failed to open file: %v - Error: %v", path, err)
		return nil, err
	}
	defer file.Close()
	var buffer [Headersize]byte
	header = (*Header)(unsafe.Pointer(&buffer))
	// Read the top part of the header, which is not dependent on the number of elements
	n, err := file.Read(buffer[:312])
	if err != nil || n != 312 {
		log.Error("Failed to read header part1 from file: %v - Error: %v", path, err)
		return nil, fmt.Errorf("failed to read header part1 from file: %s - Error: %w", path, err)
	}

	// Second part of read element names
	secondReadSize := header.NElements * 32
	n, err = file.Read(buffer[312 : 312+secondReadSize])
	if err != nil || n != int(secondReadSize) {
		log.Error("Failed to read header part2 from file: %v - Error: %v", path, err)
		return nil, fmt.Errorf("failed to read header part2 from file: %s - Error: %w", path, err)
	}
	// Read past empty element name space
	_, err = file.Seek(1024*32-secondReadSize, os.SEEK_CUR)
	if err != nil {
		return nil, fmt.Errorf("failed to seek file %v to read past empty element name space: %w", file, err)
	}

	// Read element types
	start := 312 + 1024*32
	n, err = file.Read(buffer[start : start+int(header.NElements)])
	if err != nil || n != int(header.NElements) {
		log.Error("Failed to read header part3 from file: %v - Error: %v", path, err)
		return nil, fmt.Errorf("failed to read header part3 from file: %s - Error: %w", path, err)
	}
	if EnumRecordType(header.RecordType) == VARIABLE {
		// Read to end of header
		start += int(header.NElements)
		n, err = file.Read(buffer[start:Headersize])
		if err != nil || n != (Headersize-start) {
			log.Error("Failed to read header part4 from file: %v - Error: %v", path, err)
			return nil, fmt.Errorf("failed to read header part4 from file: %s - Error: %w", path, err)
		}
	}
	return header, nil
}

// Header is the on-disk byte representation of the file header
type Header struct {
	Version      int64
	Description  [256]byte
	Year         int64
	Timeframe    int64 // Duration in nanoseconds
	RecordType   int64
	NElements    int64
	RecordLength int64
	reserved1    int64
	// Above is the fixed header portion - size is 312 Bytes = (7*8 + 256)
	ElementNames [1024][32]byte
	ElementTypes [1024]byte
	reserved2    [365]int64
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

// Load loads the header information from a given TimeBucketInfo
func (hp *Header) Load(f *TimeBucketInfo) {
	if f.GetVersion() != FileinfoVersion {
		log.Warn(
			"FileInfoVersion does not match this version of MarketStore %v != %v",
			f.GetVersion(), FileinfoVersion)
	}
	hp.Version = f.GetVersion()
	copy(hp.Description[:], f.GetDescription())
	hp.Year = int64(f.Year)
	hp.Timeframe = int64(f.GetTimeframe().Nanoseconds())
	hp.NElements = int64(f.GetNelements())
	hp.RecordLength = int64(f.GetRecordLength())
	hp.RecordType = int64(f.GetRecordType())
	for i := 0; i < int(hp.NElements); i++ {
		copy(hp.ElementNames[i][:],
			f.GetElementNames()[i])
		hp.ElementTypes[i] = byte(f.GetElementTypes()[i])
	}
	hp.RecordType = int64(f.GetRecordType())
}
