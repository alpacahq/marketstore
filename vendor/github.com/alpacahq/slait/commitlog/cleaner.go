package commitlog

import (
	"strconv"
	"time"

	"github.com/alpacahq/slait/utils/log"
)

type Cleaner interface {
	Clean([]*Segment) ([]*Segment, error)
}

type CleanerOptions map[string]string

func NewCleaner(options CleanerOptions) Cleaner {
	name, _ := options["Name"]
	switch name {
	case "Duration":
		durationStr, ok := options["Duration"]
		duration := time.Duration(5 * 24 * time.Hour)
		if ok {
			temp, err := time.ParseDuration(durationStr)
			if err != nil {
				log.Warning("Parsing Duration failed: %s, %v", durationStr, err)
			} else {
				duration = temp
			}
		}
		return &DurationCleaner{
			Options:  options,
			Duration: duration,
		}

	case "ByteSize":
		fallthrough
	default:
		maxLogBytesStr, ok := options["MaxLogBytes"]
		maxLogBytes := int64(10 * 32 * 1024 * 1024)
		if ok {
			temp, err := strconv.Atoi(maxLogBytesStr)
			if err != nil {
				log.Warning("Parsing MaxLogBytes failed: %s", maxLogBytesStr)
			} else {
				maxLogBytes = int64(temp)
			}
		}
		return &ByteSizeCleaner{
			Options:     options,
			MaxLogBytes: maxLogBytes,
		}
	}
}

// ByteSizeCleaner deletes leading segment files based on the total byte size of segments
type ByteSizeCleaner struct {
	// Options["MaxLogBytes"] should be number string accepted by strconv.Atoi()
	Options CleanerOptions
	// -1 to avoid any deletes
	MaxLogBytes int64
}

// Clean deletes segment files so that the sum of the segment files in bytes are
// less than maxLogBytes.  It keeps at least one segment file if there are any.
func (cleaner *ByteSizeCleaner) Clean(segments []*Segment) ([]*Segment, error) {
	if len(segments) == 0 || cleaner.MaxLogBytes == -1 {
		return segments, nil
	}
	cleanedSegments := []*Segment{segments[len(segments)-1]}
	totalBytes := cleanedSegments[0].Size
	if len(segments) > 1 {
		var i int
		for i = len(segments) - 2; i > -1; i-- {
			s := segments[i]
			totalBytes += s.Size
			if totalBytes > cleaner.MaxLogBytes {
				break
			}
			// cleanedSegments = append([]*Segment{s}, cleanedSegments...)
		}
		if i > -1 {
			for j := 0; j <= i; j++ {
				s := segments[j]
				if err := s.Delete(); err != nil {
					log.Error("Failed to delete %v: %v", s, err)
					return segments[j:], err
				}
			}
			cleanedSegments = segments[i+1:]
		}
	}
	return cleanedSegments, nil
}

// DurationCleaner deletes leading segments based on -1 * duration from time.Now()
type DurationCleaner struct {
	// Options["Duration"] should be one of the format accepted by time.ParseDuration
	Options  CleanerOptions
	Duration time.Duration
}

func (cleaner *DurationCleaner) Clean(segments []*Segment) ([]*Segment, error) {
	// XXX Since there is no way to know the last timestamp without reading the
	// whole file, it may be better to implement this differently.
	if len(segments) == 0 {
		return segments, nil
	}
	cutoff := time.Now().UTC().Add(-1 * cleaner.Duration)
	cutoffIdx := len(segments) - 1
	// Always leave last segment
	for i, segment := range segments[:len(segments)-1] {
		lastEntry, err := readLastEntry(segment)
		if err != nil {
			log.Error("Failed to read entry from %v: %v", segment, err)
			return segments[i:], err
		}
		if lastEntry.Timestamp.After(cutoff) || lastEntry.Timestamp.Equal(cutoff) {
			cutoffIdx = i
			break
		}
		if err := segment.Delete(); err != nil {
			log.Error("Failed to delete %v: %v", segment, err)
			return segments[i:], err
		}
	}
	return segments[cutoffIdx:], nil
}

func readLastEntry(segment *Segment) (*Entry, error) {
	// close to make sure read it from the beginning
	segment.Close()
	// and close it again on return
	defer segment.Close()

	var lastEntry *Entry
	for {
		entry, err := segment.ReadEntry()
		if err != nil {
			return nil, err
		} else if entry == nil {
			return lastEntry, nil
		}
		lastEntry = entry
	}
}
