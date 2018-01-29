package buffile

import (
	"os"
	"path/filepath"
	"testing"

	. "gopkg.in/check.v1"
)

type TestSuite struct{}

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&TestSuite{})

func (t *TestSuite) TestBufferedFile(c *C) {
	tempDir := c.MkDir()
	filePath := filepath.Join(tempDir, "test.bin")
	fp, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0700)
	c.Check(err, IsNil)
	err = fp.Truncate(1024 * 1024)
	c.Check(err, IsNil)
	fp.Close()

	bf, err := New(filePath)
	c.Check(err, IsNil)
	dataIn := make([]byte, 64)
	for i := 0; i < len(dataIn); i++ {
		dataIn[i] = 0xaa
	}
	offset := int64(128)
	offset2 := offset * 3
	offset3 := int64(defaultBlockSize - 2)
	offset4 := int64(1024*1024 - len(dataIn))
	bf.WriteAt(dataIn, offset)
	bf.WriteAt(dataIn, offset2)
	bf.WriteAt(dataIn, offset3)
	bf.WriteAt(dataIn, offset4)
	bf.Close()

	fp, err = os.Open(filePath)
	checkFunc := func(offset int64, size int) {
		outData := make([]byte, size+2)
		fp.ReadAt(outData, offset-1)
		c.Check(outData[0], Equals, byte(0x00))
		for i := 0; i < size; i++ {
			c.Check(outData[i+1], Equals, byte(0xaa))
		}
		c.Check(outData[size+1], Equals, byte(0x00))
	}
	checkFunc(offset, len(dataIn))
	checkFunc(offset2, len(dataIn))
	checkFunc(offset3, len(dataIn))
	checkFunc(offset4, len(dataIn))
	fs, _ := fp.Stat()
	// make sure the file hasn't extended
	c.Check(fs.Size(), Equals, int64(1024*1024))
	fp.Close()
}
