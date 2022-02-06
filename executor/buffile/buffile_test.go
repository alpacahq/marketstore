package buffile_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/executor/buffile"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

func TestBufferedFile(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", fmt.Sprintf("plugins_test-%s", "TestBufferedFile"))
	defer test.CleanupDummyDataDir(tempDir)

	filePath := filepath.Join(tempDir, "test.bin")
	fp, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o700)
	assert.Nil(t, err)
	err = fp.Truncate(1024 * 1024)
	assert.Nil(t, err)
	fp.Close()

	bf, err := buffile.New(filePath)
	assert.Nil(t, err)
	dataIn := make([]byte, 64)
	for i := 0; i < len(dataIn); i++ {
		dataIn[i] = 0xaa
	}
	offset := int64(128)
	offset2 := offset * 3
	offset3 := int64(buffile.DefaultBlockSize - 2)
	offset4 := int64(1024*1024 - len(dataIn))
	bf.WriteAt(dataIn, offset)
	bf.WriteAt(dataIn, offset2)
	bf.WriteAt(dataIn, offset3)
	bf.WriteAt(dataIn, offset4)
	bf.Close()

	fp, err = os.Open(filePath)
	assert.Nil(t, err)
	checkFunc := func(offset int64, size int) {
		outData := make([]byte, size+2)
		fp.ReadAt(outData, offset-1)
		assert.Equal(t, outData[0], byte(0x00))
		for i := 0; i < size; i++ {
			assert.Equal(t, outData[i+1], byte(0xaa))
		}
		assert.Equal(t, outData[size+1], byte(0x00))
	}
	checkFunc(offset, len(dataIn))
	checkFunc(offset2, len(dataIn))
	checkFunc(offset3, len(dataIn))
	checkFunc(offset4, len(dataIn))
	fs, _ := fp.Stat()
	// make sure the file hasn't extended
	assert.Equal(t, fs.Size(), int64(1024*1024))
	fp.Close()
}
