// Package buffile helps batch write by writes to the temporary in-memory
// buffer under the assumption that many writes come in to the part of
// single file frequently.
package buffile

import (
	"errors"
	"io"
	"os"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

type fileLike interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

// BufferedFile abstracts a file with block-sized buffer to group
// writes that are likely consective.  This object does not provide
// any mean of concurrency guarantee.  Also note that the underlying
// file is assumed to have the content already written on disk, and
// the behavior on writing beyond the file size is not defined.
type BufferedFile struct {
	fp           fileLike
	blockSize    int
	buffer       []byte
	bufferOffset int64
}

const DefaultBlockSize = 32 * 1024

func New(filePath string) (*BufferedFile, error) {
	fp, err := os.OpenFile(filePath, os.O_RDWR, 0o700)
	if err != nil {
		return nil, err
	}
	blockSize := DefaultBlockSize
	return &BufferedFile{
		fp:        fp,
		blockSize: blockSize,
	}, nil
}

func (f *BufferedFile) Close() error {
	if err := f.writeBuffer(); err != nil {
		log.Error("failed to write buffer before closing. err=" + err.Error())
	}
	return f.fp.Close()
}

func (f *BufferedFile) readBuffer(offset int64, size int) error {
	// we always read from block boundary
	readOffset := offset - offset%int64(f.blockSize)

	// read size is block lower + offset residual + actual size
	readSize := int(offset)%f.blockSize + size
	// align to block size
	readSize += f.blockSize
	readSize -= readSize % f.blockSize

	// len(nil slice) is 0
	if len(f.buffer) < readSize {
		f.buffer = make([]byte, readSize)
	}
	if n, err := f.fp.ReadAt(f.buffer, readOffset); err != nil {
		if errors.Is(err, io.EOF) {
			// read short is fine at the end of file
			f.buffer = f.buffer[:n]
		} else {
			return err
		}
	}
	f.bufferOffset = readOffset
	return nil
}

func (f *BufferedFile) writeBuffer() error {
	if f.buffer != nil {
		if _, err := f.fp.WriteAt(f.buffer, f.bufferOffset); err != nil {
			return err
		}
	}
	return nil
}

func (f *BufferedFile) ensureBuffer(data []byte, offset int64) error {
	if f.buffer == nil {
		return f.readBuffer(offset, len(data))
	}
	bufferLower := f.bufferOffset
	bufferUpper := f.bufferOffset + int64(len(f.buffer))
	if offset < bufferLower || offset+int64(len(data)) > bufferUpper {
		if err := f.writeBuffer(); err != nil {
			return err
		}
		if err := f.readBuffer(offset, len(data)); err != nil {
			return err
		}
	}
	return nil
}

// WriteAt writes the data at offset from the beginning of the file.  Upon the
// return from this call, the data does not reach to disk yet.  Make sure to close
// BufferedFile f to write the data on disk.
func (f *BufferedFile) WriteAt(data []byte, offset int64) (int, error) {
	if err := f.ensureBuffer(data, offset); err != nil {
		return 0, err
	}
	writePos := offset - f.bufferOffset
	n := copy(f.buffer[writePos:], data)
	return n, nil
}
