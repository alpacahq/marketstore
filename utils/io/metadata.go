package io

import (
	"bytes"
	"os"
	"unsafe"

	"fmt"
	"github.com/alpacahq/marketstore/utils"
	. "github.com/alpacahq/marketstore/utils/log"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const Headersize = 37024
const FileinfoVersion = int64(1.0)

func daysInYear(year int) int {
	testYear := time.Date(year, time.December, 31, 0, 0, 0, 0, time.Local)
	return testYear.YearDay()
}

func FileSize(intervalsPerDay int64, year int, recordSize int) int64 {
	return Headersize + intervalsPerDay*int64(daysInYear(year)*recordSize)
}

type TimeBucketInfo struct {
	// Year, Path and IsRead are all set on catalog startup
	Year   int16
	Path   string
	IsRead bool

	version     int64
	description string
	intervals   int64 // Number of time intervals per day
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
	f = new(TimeBucketInfo)
	f.Path = filepath.Join(path, strconv.Itoa(int(year))+".bin")
	f.IsRead = true
	f.intervals = int64(tf.PeriodsPerDay())
	f.description = description
	f.Year = year
	f.nElements = int32(len(elementTypes))
	f.elementTypes = elementTypes
	f.elementNames = elementNames
	f.recordType = recordType
	if f.recordType == FIXED {
		f.recordLength = int32(AlignedSize(f.getFieldRecordLength())) + 8 // add an 8-byte epoch field
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
	for _, shape := range dsv {
		out = append(out, shape)
	}
	return out
}

func (f *TimeBucketInfo) getFieldRecordLength() (fieldRecordLength int) {
	for _, elType := range f.GetElementTypes() {
		fieldRecordLength += elType.Size()
	}
	return fieldRecordLength
}
func (f *TimeBucketInfo) GetDeepCopy() *TimeBucketInfo {
	if !f.IsRead {
		f.InitFromFile()
	}
	fcopy := *f
	return &fcopy
}
func (f *TimeBucketInfo) InitFromFile() {
	if err := f.readHeader(f.Path); err != nil {
		Log(FATAL, err.Error())
	}
	f.IsRead = true
}
func (f *TimeBucketInfo) GetVersion() int64 {
	if !f.IsRead {
		f.InitFromFile()
	}
	return f.version
}
func (f *TimeBucketInfo) GetDescription() string {
	if !f.IsRead {
		f.InitFromFile()
	}
	return f.description
}
func (f *TimeBucketInfo) GetIntervals() int64 {
	if !f.IsRead {
		f.InitFromFile()
	}
	return f.intervals
}
func (f *TimeBucketInfo) GetNelements() int32 {
	if !f.IsRead {
		f.InitFromFile()
	}
	return f.nElements
}
func (f *TimeBucketInfo) GetRecordLength() int32 {
	if !f.IsRead {
		f.InitFromFile()
	}
	return f.recordLength
}
func (f *TimeBucketInfo) GetVariableRecordLength() int32 {
	if !f.IsRead {
		f.InitFromFile()
	}
	if f.variableRecordLength == 0 {
		// Variable records use the raw element sizes plus a 4-byte trailer for interval ticks
		f.variableRecordLength = int32(f.getFieldRecordLength()) + 4 // Variable records have a 4-byte trailer
	}
	return f.variableRecordLength
}
func (f *TimeBucketInfo) GetRecordType() EnumRecordType {
	if !f.IsRead {
		f.InitFromFile()
	}
	return f.recordType
}
func (f *TimeBucketInfo) GetElementNames() []string {
	if !f.IsRead {
		f.InitFromFile()
	}
	return f.elementNames
}
func (f *TimeBucketInfo) GetElementTypes() []EnumElementType {
	if !f.IsRead {
		f.InitFromFile()
	}
	return f.elementTypes
}
func (f *TimeBucketInfo) SetElementTypes(newTypes []EnumElementType) error {
	if len(newTypes) != len(f.elementTypes) {
		return fmt.Errorf("Element count not equal")
	}
	for i, val := range newTypes {
		f.elementTypes[i] = val
	}
	return nil
}

func (f *TimeBucketInfo) readHeader(path string) (err error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	defer file.Close()
	if err != nil {
		Log(ERROR, "Failed to open file: %v - Error: %v", path, err)
		return err
	}
	var buffer [Headersize]byte
	header := (*Header)(unsafe.Pointer(&buffer))
	// Read the top part of the header, which is not dependent on the number of elements
	n, err := file.Read(buffer[:312])
	if err != nil || n != 312 {
		Log(ERROR, "Failed to read header part1 from file: %v - Error: %v", path, err)
		return err
	}

	// Second part of read element names
	secondReadSize := header.NElements * 32
	n, err = file.Read(buffer[312 : 312+secondReadSize])
	if err != nil || n != int(secondReadSize) {
		Log(ERROR, "Failed to read header part2 from file: %v - Error: %v", path, err)
		return err
	}
	// Read past empty element name space
	file.Seek(1024*32-secondReadSize, os.SEEK_CUR)

	// Read element types
	start := 312 + 1024*32
	n, err = file.Read(buffer[start : start+int(header.NElements)])
	if err != nil || n != int(header.NElements) {
		Log(ERROR, "Failed to read header part3 from file: %v - Error: %v", path, err)
		return err
	}
	if EnumRecordType(header.RecordType) == VARIABLE {
		// Read to end of header
		start += int(header.NElements)
		n, err = file.Read(buffer[start:Headersize])
		if err != nil || n != (Headersize-start) {
			Log(ERROR, "Failed to read header part4 from file: %v - Error: %v", path, err)
			return err
		}
	}
	f.Load(header, path)
	return nil
}

func (f *TimeBucketInfo) Load(hp *Header, path string) {
	f.version = hp.Version
	f.description = string(bytes.Trim(hp.Description[:], "\x00"))
	f.Year = int16(hp.Year)
	f.intervals = hp.Intervals
	f.Path = filepath.Clean(path)
	f.nElements = int32(hp.NElements)
	f.recordLength = int32(hp.RecordLength)
	f.recordType = EnumRecordType(hp.RecordType)
	f.elementNames = nil
	f.elementTypes = nil
	for i := 0; i < int(f.nElements); i++ {
		baseName := string(bytes.Trim(hp.ElementNames[i][:], "\x00"))
		f.elementNames = append(f.elementNames, strings.Title(baseName)) // Convert to title case
		f.elementTypes = append(f.elementTypes, EnumElementType(hp.ElementTypes[i]))
	}
}

type Header struct {
	Version      int64
	Description  [256]byte
	Year         int64
	Intervals    int64
	RecordType   int64
	NElements    int64
	RecordLength int64
	reserved1    int64
	// Above is the fixed header portion - size is 312 Bytes = (7*8 + 256)
	ElementNames [1024][32]byte
	ElementTypes [1024]byte
	reserved2    [365]int64
}

func WriteHeader(file *os.File, f *TimeBucketInfo) error {
	header := Header{}
	header.Load(f)
	bp := (*[Headersize]byte)(unsafe.Pointer(&header))
	_, err := file.Write(bp[:])
	return err
}
func (hp *Header) Load(f *TimeBucketInfo) {
	hp.Version = f.GetVersion()
	copy(hp.Description[:], f.GetDescription())
	hp.Year = int64(f.Year)
	hp.Intervals = f.GetIntervals()
	hp.NElements = int64(f.GetNelements())
	hp.RecordLength = int64(f.GetRecordLength())
	hp.RecordType = int64(f.GetRecordType())
	for i := 0; i < int(hp.NElements); i++ {
		copy(hp.ElementNames[i][:], f.GetElementNames()[i])
		hp.ElementTypes[i] = byte(f.GetElementTypes()[i])
	}
	hp.RecordType = int64(f.GetRecordType())
}
