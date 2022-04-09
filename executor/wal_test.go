package executor_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func TestWALWrite(t *testing.T) {
	rootDir, _, metadata := setup(t)

	var err error
	mockInstanceID := time.Now().UTC().UnixNano()
	txnPipe := executor.NewTransactionPipe()
	metadata.WALFile, err = executor.NewWALFile(rootDir, mockInstanceID, nil,
		false, &sync.WaitGroup{}, executor.NewTriggerPluginDispatcher(nil),
		txnPipe,
	)
	if err != nil {
		assert.Fail(t, err.Error())
	}

	queryFiles, err := addTGData(t, metadata.CatalogDir, metadata.WALFile, 1000, false)
	if err != nil {
		assert.Fail(t, err.Error())
	}

	// Get the base files associated with this cache so that we can verify they remain correct after flush
	originalFileContents := createBufferFromFiles(t, queryFiles)

	err = metadata.WALFile.FlushToWAL()
	if err != nil {
		t.Log(err)
	}
	// Verify that the file contents have not changed
	assert.True(t, compareFileToBuf(t, originalFileContents, queryFiles))

	err = metadata.WALFile.CreateCheckpoint()
	if err != nil {
		t.Log(err)
	}
	// Verify that the file contents have not changed
	assert.True(t, compareFileToBuf(t, originalFileContents, queryFiles))

	// Add some mixed up data to the cache
	queryFiles, err = addTGData(t, metadata.CatalogDir, metadata.WALFile, 200, true)
	if err != nil {
		assert.Fail(t, err.Error())
	}

	err = metadata.WALFile.FlushToWAL()
	if err != nil {
		t.Log(err)
	}
	assert.Nil(t, err)

	// Old file contents should be different
	assert.False(t, compareFileToBuf(t, originalFileContents, queryFiles))

	assert.True(t, metadata.WALFile.IsOpen())

	canWrite, err := metadata.WALFile.CanWrite("WALTest", mockInstanceID)
	assert.True(t, canWrite)
	assert.Nil(t, err)
	metadata.WALFile.WriteStatus(wal.OPEN, wal.REPLAYED)

	_ = metadata.WALFile.Delete(mockInstanceID)

	assert.False(t, metadata.WALFile.IsOpen())
}

func TestBrokenWAL(t *testing.T) {
	rootDir, _, metadata := setup(t)

	var err error

	// Add some mixed up data to the cache
	_, err = addTGData(t, metadata.CatalogDir, metadata.WALFile, 1000, true)
	if err != nil {
		assert.Fail(t, err.Error())
	}

	// Get the base files associated with this cache so that we can verify later
	// Note that at this point the files are unmodified
	//	originalFileContents := createBufferFromFiles(tgc, c)

	err = metadata.WALFile.FlushToWAL()
	if err != nil {
		t.Log(err)
	}

	// Save the WALFile contents after WAL flush, but before flush to primary
	fstat, _ := metadata.WALFile.FilePtr.Stat()
	fsize := fstat.Size()
	WALFileAfterWALFlush := make([]byte, fsize)
	n, err := metadata.WALFile.FilePtr.ReadAt(WALFileAfterWALFlush, 0)
	assert.Equal(t, int64(n), fsize)
	assert.Nil(t, err)

	err = metadata.WALFile.CreateCheckpoint()
	if err != nil {
		t.Log(err)
	}

	// Now we have a completed WALFile, we can write some degraded files for testing
	fstat, _ = metadata.WALFile.FilePtr.Stat()
	WALLength := fstat.Size()
	WALBuffer := make([]byte, WALLength)
	n, err = metadata.WALFile.FilePtr.ReadAt(WALBuffer, 0)
	assert.Equal(t, int(WALLength), n)
	assert.Nil(t, err)

	// We write a broken WAL File, but we need to replace the Owning PID with a bogus one before we write
	for i, val := range [8]byte{1, 1, 1, 1, 1, 1, 1, 1} {
		WALBuffer[3+i] = val
	}
	BrokenWAL := WALBuffer[:(3 * len(WALBuffer) / 4)]
	// BrokenWAL := WALBuffer[:]
	BrokenWALFileName := "BrokenWAL"
	BrokenWALFilePath := rootDir + "/" + BrokenWALFileName

	_ = os.Remove(BrokenWALFilePath)
	fp, err := os.OpenFile(BrokenWALFilePath, os.O_CREATE|os.O_RDWR, 0o600)
	assert.Nil(t, err)
	_, err = fp.Write(BrokenWAL)
	assert.Nil(t, err)
	io.Syncfs()
	_ = fp.Close()

	// Take over the broken WALFile and replay it
	WALFile, err := executor.TakeOverWALFile(filepath.Join(rootDir, BrokenWALFileName))
	assert.Nil(t, err)
	newTGC := executor.NewTransactionPipe()
	assert.NotNil(t, newTGC)
	err = WALFile.Replay(false)
	assert.Nil(t, err)

	err = WALFile.Delete(WALFile.OwningInstanceID)
	assert.Nil(t, err)
}

func TestWALReplay(t *testing.T) {
	rootDir, _, metadata := setup(t)

	var err error

	allQueryFiles, err := addTGData(t, metadata.CatalogDir, metadata.WALFile, 1000, true)
	if err != nil {
		assert.Fail(t, err.Error())
	}
	// Filter out only year 2003 in the resulting file list
	queryFiles2002 := make([]string, 0)
	for _, filePath := range allQueryFiles {
		if filepath.Base(filePath) == "2002.bin" {
			queryFiles2002 = append(queryFiles2002, filePath)
		}
	}

	// Get the base files associated with this cache so that we can verify later
	// Note that at this point the files are unmodified
	allFileContents := createBufferFromFiles(t, queryFiles2002)
	fileContentsOriginal2002 := make(map[string][]byte)
	for filePath, buffer := range allFileContents {
		if filepath.Base(filePath) == "2002.bin" {
			fileContentsOriginal2002[filePath] = buffer
		}
	}

	err = metadata.WALFile.FlushToWAL()
	if err != nil {
		t.Log(err)
	}

	// Save the WALFile contents after WAL flush, but before checkpoint
	fstat, _ := metadata.WALFile.FilePtr.Stat()
	fsize := fstat.Size()
	WALFileAfterWALFlush := make([]byte, fsize)
	bytesWritten, err := metadata.WALFile.FilePtr.ReadAt(WALFileAfterWALFlush, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(bytesWritten), fsize)

	err = metadata.WALFile.CreateCheckpoint()
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	// Put the modified files into a buffer and then verify the state of the files
	modifiedFileContents := createBufferFromFiles(t, queryFiles2002)
	assert.True(t, compareFileToBuf(t, modifiedFileContents, queryFiles2002))

	// Verify that the file contents have changed for year 2002
	for key, buf := range fileContentsOriginal2002 {
		// t.Log("Key:", key, "Len1: ", len(buf), " Len2: ", len(modifiedFileContents[key]))
		assert.False(t, bytes.Equal(buf, modifiedFileContents[key]))
	}

	// Re-write the original files
	// t.Log("Rewrite")
	rewriteFilesFromBuffer(t, fileContentsOriginal2002)
	// At this point, we should have our original files
	assert.True(t, compareFileToBuf(t, fileContentsOriginal2002, queryFiles2002))

	// Write a WAL file with the pre-flushed state - we will replay this to get the modified files
	newWALFileName := "ReplayWAL"
	newWALFilePath := rootDir + "/" + newWALFileName
	_ = os.Remove(newWALFilePath) // Remove it if it exists
	fp, err := os.OpenFile(newWALFilePath, os.O_CREATE|os.O_RDWR, 0o600)
	assert.Nil(t, err)
	// Replace PID with a bogus PID
	for i, val := range [8]byte{1, 1, 1, 1, 1, 1, 1, 1} {
		WALFileAfterWALFlush[3+i] = val
	}
	bytesWritten, err = fp.WriteAt(WALFileAfterWALFlush, 0)
	assert.Nil(t, err)
	assert.Len(t, WALFileAfterWALFlush, bytesWritten)
	io.Syncfs()

	// Take over the new WALFile and replay it into a new TG cache
	WALFile, err := executor.TakeOverWALFile(filepath.Join(rootDir, newWALFileName))
	assert.Nil(t, err)
	data, _ := os.ReadFile(newWALFilePath)
	_ = os.WriteFile("/tmp/wal", data, 0o600)
	newTGC := executor.NewTransactionPipe()
	assert.NotNil(t, newTGC)
	// Verify that our files are in original state prior to replay
	assert.True(t, compareFileToBuf(t, fileContentsOriginal2002, queryFiles2002))

	// Replay the WALFile into the new cache
	err = WALFile.Replay(false)
	assert.Nil(t, err)

	// Verify that the files are in the correct state after replay
	postReplayFileContents := createBufferFromFiles(t, queryFiles2002)
	for key, buf := range modifiedFileContents {
		if filepath.Base(key) == "2002.bin" {
			buf2 := postReplayFileContents[key]
			// t.Log("Key:", key, "Len1: ", len(buf), " Len2: ", len(buf2))
			if !bytes.Equal(buf, postReplayFileContents[key]) {
				for i, val := range buf {
					if val != buf2[i] {
						t.Log("Diff: pre/post:", buf[i:i+8], buf2[i:i+8])
						t.Log("Diff: pre/post int64:", io.ToInt64(buf[i:i+8]), io.ToInt64(buf2[i:i+8]))
						t.Log("Diff: pre/post float32:", io.ToFloat32(buf[i:i+4]), io.ToFloat32(buf2[i:i+4]))
						assert.Fail(t, "diff")
					}
				}
			}
		}
	}
	// Final verify after replay
	assert.True(t, compareFileToBuf(t, modifiedFileContents, queryFiles2002))

	err = WALFile.Delete(WALFile.OwningInstanceID)
	assert.Nil(t, err)
}

/*
	===================== Helper Functions =================================
*/

func createBufferFromFiles(t *testing.T, queryFiles []string) (originalFileContents map[string][]byte) {
	t.Helper()

	// Get the base files associated with this cache so that we can verify they remain correct after flush
	originalFileContents = make(map[string][]byte)
	for _, filePath := range queryFiles {
		fp, err := os.OpenFile(filePath, os.O_RDONLY, 0o600)
		assert.Nil(t, err)
		fstat, err := fp.Stat()
		assert.Nil(t, err)
		size := fstat.Size()
		originalFileContents[filePath] = make([]byte, size)
		_, err = fp.Read(originalFileContents[filePath])
		assert.Nil(t, err)
		//	t.Log("Read file ", filePath, " Size: ", n)
		_ = fp.Close()
	}
	return originalFileContents
}

func rewriteFilesFromBuffer(t *testing.T, originalFileContents map[string][]byte) {
	t.Helper()

	// Replace the file contents with the contents of the buffer
	for filePath := range originalFileContents {
		fp, err := os.OpenFile(filePath, os.O_RDWR, 0o600)
		assert.Nil(t, err)
		n, err := fp.WriteAt(originalFileContents[filePath], 0)
		assert.Nil(t, err)
		assert.Len(t, originalFileContents[filePath], n)
		//	t.Log("Read file ", filePath, " Size: ", n)
		_ = fp.Close()
	}
}

func compareFileToBuf(t *testing.T, originalFileContents map[string][]byte, queryFiles []string) (isTheSame bool) {
	t.Helper()

	for _, filePath := range queryFiles {
		fp, err := os.OpenFile(filePath, os.O_RDONLY, 0o600)
		assert.Nil(t, err)
		fstat, err := fp.Stat()
		assert.Nil(t, err)
		size := fstat.Size()
		content := make([]byte, size)
		_, err = fp.Read(content)
		assert.Nil(t, err)
		//	t.Log("Read original file ", filePath, " Size: ", n)
		_ = fp.Close()
		if !bytes.Equal(content, originalFileContents[filePath]) {
			return false
		}
	}
	return true
}

func addTGData(t *testing.T, root *catalog.Directory, walFile *executor.WALFileType,
	number int, mixup bool,
) (queryFiles []string, err error) {
	t.Helper()

	// Create some data via a query
	symbols := []string{"NZDUSD", "USDJPY", "EURUSD"}
	tbiByKey := make(map[io.TimeBucketKey]*io.TimeBucketInfo)
	writerByKey := make(map[io.TimeBucketKey]*executor.Writer)
	csm := io.NewColumnSeriesMap()
	queryFiles = make([]string, 0)

	for _, sym := range symbols {
		q := planner.NewQuery(root)
		q.AddRestriction("Symbol", sym)
		q.AddRestriction("AttributeGroup", "OHLC")
		q.AddRestriction("Timeframe", "1Min")
		q.SetRowLimit(io.LAST, number)
		parsed, _ := q.Parse()
		scanner, err := executor.NewReader(parsed)
		if err != nil {
			t.Log("Failed to create a new reader")
			return nil, err
		}

		csmSym, err := scanner.Read()
		if err != nil {
			t.Logf("scanner.Read failed: Err: %s", err)
			return nil, err
		}

		for key, cs := range csmSym {
			// Add this result data to the overall
			csm[key] = cs
			tbi, err := root.GetLatestTimeBucketInfoFromKey(&key)
			if err != nil {
				t.Log("Failed to GetLatestTimeBucketInfoFromKey")
				return nil, err
			}
			tbiByKey[key] = tbi
			writerByKey[key], err = executor.NewWriter(root, walFile)
			if err != nil {
				t.Log("Failed to create a new writer")
				return nil, err
			}
		}
		for _, iop := range scanner.IOPMap {
			for _, iofp := range iop.FilePlan {
				queryFiles = append(queryFiles, iofp.FullPath)
			}
		}
	}

	// Write the data to the TG cache
	for key, cs := range csm {
		epoch := cs.GetEpoch()
		open, ok := cs.GetByName("Open").([]float32)
		assert.True(t, ok)
		high, ok := cs.GetByName("High").([]float32)
		assert.True(t, ok)
		low, ok := cs.GetByName("Low").([]float32)
		assert.True(t, ok)
		clos, ok := cs.GetByName("Close").([]float32)
		assert.True(t, ok)
		// If we have the mixup flag set, change the data
		if mixup {
			asize := len(epoch)
			for i := 0; i < asize/2; i++ {
				ii := (asize - 1) - i
				epoch[i], epoch[ii] = epoch[ii], epoch[i]
				open[i] = float32(-1 * i)
				high[i] = float32(-2 * ii)
				low[i] = -3
				clos[i] = -4
			}
		}
		timestamps := make([]time.Time, len(epoch))
		var buffer []byte
		for i := range epoch {
			timestamps[i] = time.Unix(epoch[i], 0).UTC()
			buffer = append(buffer, io.DataToByteSlice(epoch[i])...)
			buffer = append(buffer, io.DataToByteSlice(open[i])...)
			buffer = append(buffer, io.DataToByteSlice(high[i])...)
			buffer = append(buffer, io.DataToByteSlice(low[i])...)
			buffer = append(buffer, io.DataToByteSlice(clos[i])...)
		}
		_ = writerByKey[key].WriteRecords(timestamps, buffer, tbiByKey[key].GetDataShapesWithEpoch(), tbiByKey[key])
	}

	return queryFiles, nil
}
