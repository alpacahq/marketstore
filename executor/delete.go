package executor

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/alpacahq/marketstore/v4/planner"
	utilsio "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type Deleter struct {
	pr     planner.ParseResult
	IOPMap map[utilsio.TimeBucketKey]*IOPlan
}

func NewDeleter(pr *planner.ParseResult) (de *Deleter, err error) {
	de = new(Deleter)
	de.pr = *pr
	if pr.Range == nil {
		pr.Range = planner.NewDateRange()
	}

	sortedFileMap := make(map[utilsio.TimeBucketKey]SortedFileList)
	for _, qf := range pr.QualifiedFiles {
		sortedFileMap[qf.Key] = append(sortedFileMap[qf.Key], qf)
	}
	de.IOPMap = make(map[utilsio.TimeBucketKey]*IOPlan)
	maxRecordLen := int32(0)
	for key, sfl := range sortedFileMap {
		sort.Sort(sfl)
		if de.IOPMap[key], err = NewIOPlan(sfl, pr.Limit, pr.Range, pr.TimeQuals); err != nil {
			return nil, err
		}
		recordLen := de.IOPMap[key].RecordLen
		if maxRecordLen < recordLen {
			maxRecordLen = recordLen
		}
	}
	return de, nil
}

func (de *Deleter) Delete() (err error) {
	for _, iop := range de.IOPMap {
		err2 := de.delete(iop)
		if err2 != nil {
			return err2
		}
	}
	return err
}

// Deletes the selected time range, preserving the file holes.
func (de *Deleter) delete(iop *IOPlan) error {
	for _, fp := range iop.FilePlan {
		if err := deleteInner(fp, iop.RecordLen); err != nil {
			return err
		}
	}
	return nil
}

func deleteInner(fp *ioFilePlan, recordLen int32) (err error) {
	const allReadWrite = 0o666

	filePath := fp.FullPath
	f, err := os.OpenFile(filePath, os.O_RDWR, allReadWrite)
	if err != nil {
		log.Error("Read: opening %s\n%s", filePath, err)
		return err
	}
	defer func(f *os.File) {
		if err3 := f.Close(); err3 != nil {
			log.Error("close ")
		}
	}(f)

	seekerFunc := func(offset int64) error {
		if _, err = f.Seek(offset, io.SeekStart); err != nil {
			log.Error("Read: seeking in %s\n%s", filePath, err)
			return err
		}
		return nil
	}

	if err2 := seekerFunc(fp.Offset); err2 != nil {
		return err2
	}

	/*
		Read in the whole target data area to find the non-zero index locations
		This will preserve the existing holes in the data area at the expense of
		a potentially large number of file seeks
	*/
	bufferSize := int(fp.Length + int64(recordLen))
	buffer := make([]byte, bufferSize)
	n, err := f.Read(buffer)
	if err != nil || n != bufferSize {
		return fmt.Errorf("delete(): Short read %d bytes", n)
	}
	numRecs := bufferSize / int(recordLen)
	zeroRecord := make([]byte, int(recordLen))
	var isContiguous bool
	for i := 0; i < numRecs; i++ {
		epochLoc := i * int(recordLen)
		index := int64(binary.LittleEndian.Uint64(buffer[epochLoc:]))
		switch {
		case index != 0 && !isContiguous:
			if err := seekerFunc(int64(epochLoc) + fp.Offset); err != nil {
				return err
			}
			isContiguous = true
			fallthrough
		case index != 0 && isContiguous:
			n, err2 := f.Write(zeroRecord)
			if err2 != nil || n != int(recordLen) {
				return fmt.Errorf("delete(): Short write %d bytes, error: %w", n, err2)
			}
		case index == 0:
			isContiguous = false
		}
	}
	buffer = nil
	return nil
}
