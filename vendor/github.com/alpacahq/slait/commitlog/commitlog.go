package commitlog

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrSegmentNotFound = errors.New("segment not found")
	Encoding           = binary.LittleEndian
)

const (
	LogFileSuffix = ".log"
)

type CommitLog struct {
	Options
	cleaner        Cleaner
	name           string
	segments       []*Segment
	vActiveSegment atomic.Value
}

type Options struct {
	Path            string
	MaxSegmentBytes int64
	CleanerOptions  CleanerOptions
}

type Entry struct {
	Timestamp time.Time
	Data      []byte
}

// position is a file position pointer.  We keep it private for now
// (yet to see if the first element should be segment pointer or baseNano)
type position struct {
	segment *Segment
	offset  int64
}

func New(opts Options) (*CommitLog, error) {
	if opts.Path == "" {
		return nil, errors.New("path is empty")
	}

	if opts.MaxSegmentBytes == 0 {
		opts.MaxSegmentBytes = 32 * 1024
	}

	path, _ := filepath.Abs(opts.Path)
	cleanerOpts := opts.CleanerOptions
	if cleanerOpts == nil {
		cleanerOpts = CleanerOptions{}
	}
	l := &CommitLog{
		Options: opts,
		name:    filepath.Base(path),
		cleaner: NewCleaner(cleanerOpts),
	}

	if err := l.init(); err != nil {
		return nil, err
	}

	if err := l.open(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *CommitLog) init() error {
	err := os.MkdirAll(l.Path, 0755)
	if err != nil {
		return errors.Wrap(err, "mkdir failed")
	}
	return nil
}

func (l *CommitLog) open() error {
	files, err := ioutil.ReadDir(l.Path)
	if err != nil {
		return errors.Wrap(err, "read dir failed")
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), LogFileSuffix) {
			stem := strings.TrimSuffix(file.Name(), LogFileSuffix)
			baseNano, err := strconv.Atoi(stem)
			segment, err := NewSegment(l.Path, int64(baseNano), l.MaxSegmentBytes)
			if err != nil {
				return err
			}
			l.segments = append(l.segments, segment)
		}
	}
	return nil
}

func (l *CommitLog) Append(entry *Entry) error {
	if l.checkSplit() {
		if err := l.split(entry.Timestamp.UnixNano()); err != nil {
			return err
		}
	}
	if err := l.activeSegment().AppendEntry(entry); err != nil {
		return err
	}
	return nil
}

func (l *CommitLog) Truncate(backTo *position) error {
	if backTo.segment == nil {
		return l.DeleteAll()
	}

	lastSegmentIdx := -1
	for i, segment := range l.segments {
		if segment == backTo.segment {
			lastSegmentIdx = i
		}
	}

	if lastSegmentIdx == -1 {
		return errors.New("The segment to truncate back to was not found")
	}

	for i := len(l.segments) - 1; i > lastSegmentIdx; i-- {
		l.segments[i].Delete()
	}
	l.segments[lastSegmentIdx].Truncate(backTo.offset)
	l.segments = l.segments[:lastSegmentIdx+1]

	return nil
}

// Tell tells the current logical position.
func (l *CommitLog) Tell() *position {
	if len(l.segments) == 0 {
		return &position{
			segment: nil, // nil indicates there is no content
			offset:  0,
		}
	}
	segment := l.activeSegment()
	return &position{
		segment: segment,
		offset:  segment.Size,
	}
}

// Trim deletes segment files according to its retention policy.
// Returns the base nanosec time of the first segment if some files have been deleted.
// A zero time is returned if nothing has changed.  Maximum nano sec time
// (= 2262-04-11 23:47:16.854775807 +0000 UTC) is returned if all the segments are
// deleted.
func (l *CommitLog) Trim() (time.Time, error) {
	segments, err := l.cleaner.Clean(l.segments)
	if len(l.segments) == len(segments) {
		return time.Time{}, err
	}
	l.segments = segments

	if len(l.segments) == 0 {
		return time.Unix(0, 0x7fffffffffffffff), nil
	}
	baseNano := l.segments[0].BaseNano
	upto := time.Unix(0, baseNano).UTC()
	return upto, err
}

func (l *CommitLog) activeSegment() *Segment {
	if len(l.segments) > 0 {
		return l.segments[len(l.segments)-1]
	}
	return nil
}

func (l *CommitLog) Close() error {
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *CommitLog) DeleteAll() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Path)
}

func (l *CommitLog) Segments() []*Segment {
	return l.segments
}

func (l *CommitLog) checkSplit() bool {
	if len(l.segments) == 0 {
		return true
	}
	return l.activeSegment().IsFull()
}

func (l *CommitLog) split(baseNanosec int64) error {
	lastActive := l.activeSegment()
	segment, err := NewSegment(l.Path, baseNanosec, l.MaxSegmentBytes)
	if err != nil {
		return err
	}
	segments := append(l.segments, segment)
	if lastActive != nil {
		lastActive.Close()
	}
	// segments, err = l.cleaner.Clean(segments)
	// if err != nil {
	// 	return err
	// }
	l.segments = segments
	return nil
}
