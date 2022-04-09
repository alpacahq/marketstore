package buffile_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/executor/buffile"
)

func TestBufferedFile(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "test.bin")
	fp, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o700)
	require.Nil(t, err)
	err = fp.Truncate(1024 * 1024)
	require.Nil(t, err)
	err = fp.Close()
	require.Nil(t, err)

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
	_, err = bf.WriteAt(dataIn, offset)
	require.Nil(t, err)
	_, err = bf.WriteAt(dataIn, offset2)
	require.Nil(t, err)
	_, err = bf.WriteAt(dataIn, offset3)
	require.Nil(t, err)
	_, err = bf.WriteAt(dataIn, offset4)
	require.Nil(t, err)
	err = bf.Close()
	require.Nil(t, err)

	fp, err = os.Open(filePath)
	assert.Nil(t, err)
	checkFunc := func(offset int64, size int) {
		outData := make([]byte, size+2)
		_, _ = fp.ReadAt(outData, offset-1)
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
