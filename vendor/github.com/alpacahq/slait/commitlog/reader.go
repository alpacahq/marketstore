package commitlog

import ()

type Reader struct {
	filePath       string
	currentSegment int
	clog           *CommitLog
}

func NewReader(path string) (*Reader, error) {
	clog, err := New(Options{
		Path: path,
	})
	if err != nil {
		return nil, err
	}

	return &Reader{
		filePath:       path,
		currentSegment: 0,
		clog:           clog,
	}, nil
}

func (r *Reader) Read() (*Entry, error) {

	for {
		if r.currentSegment >= len(r.clog.segments) {
			return nil, nil
		}
		entry, err := r.clog.segments[r.currentSegment].ReadEntry()
		if entry == nil {
			r.clog.segments[r.currentSegment].Close()
			r.currentSegment++
		} else if err != nil {
			return nil, err
		} else if err == nil {
			return entry, err
		}
	}
}

func (r *Reader) Close() error {
	if r.currentSegment < len(r.clog.segments) {
		return r.clog.segments[r.currentSegment].Close()
	}
	return nil
}
