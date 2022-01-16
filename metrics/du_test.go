package metrics_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/metrics"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

type mockMetricsSetter struct {
	value float64
}

func (m *mockMetricsSetter) Set(v float64) {
	m.value = v
}

type testCase struct {
	setFilesFunc func(tt testCase, rootDir string) error
	expMetric    float64
}

func TestStartDiskUsageMonitor(t *testing.T) {
	t.Parallel()
	tests := map[string]testCase{
		"ok/ when some small bytes are written, only 1 blocksize is actually used and monitored as a diskUsage metric" +
			" regardless of the allocated filesize": {
			setFilesFunc: func(tt testCase, rootDir string) error {
				fileName := rootDir + "/example"
				fp, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0o600)
				assert.Nil(t, err)

				// truncate => allocate the filesize, writeBuffer => write actual data
				assert.Nil(t, fp.Truncate(1024*256))
				assert.Nil(t, writeBuffer(fp, 300))
				return nil
			},
			expMetric: 4096, // it depends on the block size of the disk that this test runs
		},
	}
	for name := range tests {
		tt := tests[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// --- given ---
			rootDir, _ := ioutil.TempDir("", "diskUsage-monitor-test")
			err := tt.setFilesFunc(tt, rootDir)
			assert.Nil(t, err)
			m := mockMetricsSetter{}

			// --- when ---
			go metrics.StartDiskUsageMonitor(&m, rootDir, 10*time.Millisecond)
			time.Sleep(100 * time.Millisecond)

			// --- then ---
			assert.Equal(t, tt.expMetric, m.value)

			// --- tearDown ---
			test.CleanupDummyDataDir(rootDir)
		})
	}
}

func writeBuffer(fp *os.File, size int) error {
	// fill bytes
	b := make([]byte, size)
	for i := 0; i < size; i++ {
		b[i] = 1
	}

	if _, err := fp.WriteAt(b, 0); err != nil {
		return err
	}

	return nil
}
