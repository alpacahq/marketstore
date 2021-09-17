package executor

import (
	"fmt"
	"os"
)

type CachedFP struct {
	fileName string
	fp       *os.File
}

func NewCachedFP() *CachedFP {
	return new(CachedFP)
}

func (cfp *CachedFP) GetFP(fileName string) (fp *os.File, err error) {
	if fileName == cfp.fileName {
		return cfp.fp, nil
	} else if len(cfp.fileName) != 0 {
		cfp.fp.Close()
	}
	cfp.fp, err = os.OpenFile(fileName, os.O_RDWR, 0700)
	if err != nil {
		return nil, fmt.Errorf("open cached filepath: %w", err)
	}
	cfp.fileName = fileName
	return cfp.fp, nil
}

func (cfp *CachedFP) Close() error {
	if cfp.fp != nil {
		return cfp.fp.Close()
	}
	return nil
}

func (cfp *CachedFP) String() string {
	return fmt.Sprintf("CachedFP(fileName: %s)", cfp.fileName)
}
