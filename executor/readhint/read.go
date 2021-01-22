package readhint

import (
	"fmt"
	"sync"
)

type fileOffsetMap struct {
	sync.RWMutex
	mp map[string]int64
}

var lastKnownMap = &fileOffsetMap{mp: map[string]int64{}}

func GetLastKnown(enableLastknown bool, filePath string) (int64, bool) {
	if !enableLastknown {
		return 0, false
	}
	// avoid "defer" for performance reason
	lastKnownMap.RLock()
	val, ok := lastKnownMap.mp[filePath]
	lastKnownMap.RUnlock()
	return val, ok
}

// Set the byte offset where the last non-NULL record stays in this file.
// Note offset is the beginning of the record.
func SetLastKnown(enableLastKnown bool, filePath string, offset int64) {
	if !enableLastKnown {
		return
	}
	lastKnownMap.Lock()
	// check if it is bigger, since it's ok to read false-NULL records whereas
	// the opposite is not.
	if previous, ok := lastKnownMap.mp[filePath]; !ok || previous < offset {
		lastKnownMap.mp[filePath] = offset
	}
	lastKnownMap.Unlock()
}

func PrintLastKnowns() {
	for key, val := range lastKnownMap.mp {
		fmt.Printf("%s -> %d\n", key, val)
	}
}
