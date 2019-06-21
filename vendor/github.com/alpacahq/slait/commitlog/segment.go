package commitlog

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

const (
	logNameFormat = "%020d.log"
)

type Segment struct {
	file     *os.File
	reader   io.Reader
	filePath string
	BaseNano int64
	Size     int64
	maxBytes int64
	buf      *bytes.Buffer
}

func NewSegment(path string, baseNano int64, maxBytes int64) (*Segment, error) {
	filePath := filepath.Join(path, fmt.Sprintf(logNameFormat, baseNano))

	fi, err := os.Stat(filePath)
	size := int64(0)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrap(err, "file stat failed")
		}
		err = nil
	} else {
		size = fi.Size()
	}
	// TODO sanity check for the first entry to have the consistent baseNano
	s := &Segment{
		file:     nil,
		filePath: filePath,
		maxBytes: maxBytes,
		BaseNano: baseNano,
		Size:     size,
	}

	return s, err
}

func (s *Segment) IsFull() bool {
	return s.Size >= s.maxBytes
}

func (s *Segment) ensureOpen(forWrite bool) error {
	if s.file == nil {
		var flags int
		if forWrite {
			flags = os.O_RDWR | os.O_CREATE | os.O_APPEND
		} else {
			flags = os.O_RDONLY
		}
		file, err := os.OpenFile(s.filePath, flags, 0666)
		if err != nil {
			return errors.Wrap(err, "open file failed")
		}
		s.file = file

		if !forWrite {
			s.reader = bufio.NewReader(s.file)
		}
	}
	return nil
}

func (s *Segment) AppendEntry(entry *Entry) error {
	if err := s.ensureOpen(true); err != nil {
		return err
	}
	rec := NewRecord(entry.Timestamp.UnixNano(), entry.Data)

	if written, err := s.file.Write(rec); err != nil {
		if written != len(rec) {
			// truncate back to revert partial write
			s.file.Truncate(s.Size)
		}
		return errors.Wrap(err, "file write failed")
	} else {
		s.Size += int64(written)
	}
	return nil
}

func (s *Segment) ReadEntry() (*Entry, error) {
	if err := s.ensureOpen(false); err != nil {
		return nil, err
	}

	// re-use the buffer
	if s.buf == nil {
		s.buf = &bytes.Buffer{}
	} else {
		s.buf.Reset()
	}
	if _, err := io.CopyN(s.buf, s.reader, recordHeaderLen); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, errors.Wrap(err, "error reading nanosec")
	}
	nanosec := int64(Encoding.Uint64(s.buf.Bytes()[0:8]))
	size := int32(Encoding.Uint32(s.buf.Bytes()[8:12]))

	if _, err := io.CopyN(s.buf, s.reader, int64(size)); err != nil {
		return nil, errors.Wrap(err, "error reading payload")
	}

	data := make([]byte, len(s.buf.Bytes()[12:]))
	copy(data, s.buf.Bytes()[12:])
	return &Entry{
		Timestamp: time.Unix(0, nanosec).UTC(),
		Data:      data,
	}, nil
}

func (s *Segment) Close() error {
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	if s.buf != nil {
		s.buf = nil
	}
	return nil
}

func (s *Segment) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.filePath); err != nil {
		return err
	}
	return nil
}

func (s *Segment) Truncate(size int64) error {
	if err := s.ensureOpen(true); err != nil {
		return err
	}
	if err := s.file.Truncate(size); err != nil {
		return err
	}
	// according to the doc, "The behavior of Seek on a file opened with O_APPEND is not specified."
	// So, intead of seeking back to the truncate point, simply close it now
	// and open it later again.
	if err := s.Close(); err != nil {
		return err
	}
	return nil
}
