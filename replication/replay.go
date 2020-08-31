package replication

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/pkg/errors"
	"regexp"
	"strconv"
	"time"
)

//// Writer is the intereface to decouple WAL Replayer from executor package
//type Writer interface {
//	WriteBufferToFile(fp stdio.WriterAt, offsetIndexBuffer []byte) error
//	// for variable length record
//	WriteBufferToFileIndirect(fp *os.File, offsetIndexBuffer []byte, varRecLen int32) (err error)
//}
//
//type WALReplayer struct {
//	Writer Writer
//}

// e.g. "AMZN/1Min/TICK/2017.bin"
var WkpRegex = regexp.MustCompile(`^(.+)/(.+)/(.+)/([0-9]+)\.bin$`)

func replay(writeCommands []*wal.WriteCommand) error {
	fmt.Println("received!")
	println(writeCommands)

	rootDir := executor.ThisInstance.RootDir
	for _, wc := range writeCommands {
		tbk, year, err := walKeyToTBKInfo(wc.WALKeyPath)
		if err != nil {
			return err
		}

		tf, err := tbk.GetTimeFrame()

		rt := io.EnumRecordTypeByName(rowType)

		fmt.Println(wc)
	}

	io.NewTimeBucketKey()
	if err != nil {
		return err
	}
	rt := io.EnumRecordTypeByName(rowType)
	tbinfo := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Default", year, dsv, rt)

	// レコードの中に新しい年が入っていた場合はその年のフォルダを追加する
	//if err := w.AddNewYearFile(year); err != nil {
	//	panic(err)
	//}

	//// まだバケット用のファイルができていない場合は作る
	//if err := cDir.AddTimeBucket(&tbk, tbi); err != nil {
	//	// If File Exists error, ignore it, otherwise return the error
	//	if !strings.Contains(err.Error(), "Can not overwrite file") && !strings.Contains(err.Error(), "file exists") {
	//		return err
	//	}
	//}

	//err := executor.ThisInstance.WALFile.FlushCommandsToWAL(executor.ThisInstance.TXNPipe, writeCommands, executor.ThisInstance.WALBypass)
	//if err != nil {
	//	return errors.Wrap(err, "[replica] failed to flush WriteCommands to WAL and primary store.")
	//}

	return nil
}

func walKeyToTBKInfo(walKeyPath string) (tbk *io.TimeBucketKey, year int, err error) {
	group := WkpRegex.FindStringSubmatch(walKeyPath)
	if len(group) != 4 {
		return nil, 0, errors.New("failed to extract TBK info from WalKeyPath")
	}

	year, err = strconv.Atoi(group[3])
	if err != nil {
		return nil, 0, errors.New(fmt.Sprintf("failed to extract year from WalKeyPath:%s", group[3]))
	}

	return io.NewTimeBucketKey(fmt.Sprintf("%s/%s/%s", group[0], group[1], group[2])), year, nil
}
