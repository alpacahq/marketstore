package metrics

import (
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Setter is an interface for prometheus metrics to improve unit-testability.
type Setter interface {
	Set(m float64)
}

// StartDiskUsageMonitor retrieves the total disk usage of the provided directory at each provided time interval,
// and set it as a prometheus metric.
func StartDiskUsageMonitor(s Setter, rootDir string, interval time.Duration) {
	s.Set(float64(diskUsage(rootDir)))

	t := time.NewTicker(interval)
	for range t.C {
		s.Set(float64(diskUsage(rootDir)))
	}
}

func diskUsage(path string) int64 {
	var totalSize int64
	err := filepath.Walk(path, func(filepath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			// Since marketstore generates sparse data files by fp.truncate, it does not consume actual disk usage
			// even if the large file size is allocated. Getting stat information here to monitor the disk usage.
			sys := info.Sys()
			if sys != nil {
				stat, ok := sys.(*syscall.Stat_t)
				if !ok {
					log.Error("failed to get Stat_t for the file", filepath)
				}
				du := int64(stat.Blksize>>3) * stat.Blocks // >>3 = convert bits to bytes
				totalSize += du
			}
		}
		return err
	})
	if err != nil {
		log.Error("get the disk usage of the directory for monitoring", path, err)
	}
	return totalSize
}
