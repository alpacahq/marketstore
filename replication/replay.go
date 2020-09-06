package replication

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/executor"
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

// e.g. "/Users/dakimura/marketstore/data/AMZN/1Min/TICK/2017.bin" -> (AMZN), (1Min), (TICK), (2017)
var WkpRegex = regexp.MustCompile(`([^/]+)/([^/]+)/([^/]+)/([0-9]+)\.bin$`)

func replay(transactionGroup []byte) error {
	// TODO: replay order by transactionGroupID
	fmt.Println("received!")
	println(transactionGroup)

	tgID, wtsets := executor.ParseTGData(transactionGroup, executor.ThisInstance.RootDir)
	fmt.Println(tgID)
	fmt.Println(wtsets)

	csm := WTSetsToCSM(wtSets)

	for _, wtSet := range wtsets {
		isVariableLength := io.EnumRecordType(wtSet.RecordType) == io.VARIABLE
		csm := io.NewColumnSeriesMap()
		csm.AddColumnSeries()
		err := executor.WriteCSM(csm, isVariableLength)
	}
	//for _, wc := range writeCommands {
	//	tbk, year, err := walKeyToTBKInfo(wc.WALKeyPath)
	//	if err != nil {
	//		return err
	//	}
	//
	//	tf, err := tbk.GetTimeFrame()
	//
	//	rt := io.EnumRecordTypeByName(rowType)
	//
	//	fmt.Println(wc)
	//}

	//io.NewTimeBucketKey()
	//if err != nil {
	//	return err
	//}
	//rt := io.EnumRecordTypeByName(rowType)
	//tbinfo := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Default", year, dsv, rt)

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

func WTSetsToCSM(wtSets []executor.WTSet) (io.ColumnSeriesMap, error) {
	csm := io.NewColumnSeriesMap()

	for _,wtSet := range wtSets {
		tbk, year, err := walKeyPathToTBKInfo(wtSet.FilePath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse walKeyPath to bucket info. wkp:"+wtSet.FilePath)
		}
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get TimeFrame from TimeBucketKey. tbk:"+tbk.String())
		}

		cs := io.NewColumnSeries()
		epoch := io.IndexToTime(wtSet.Buffer.Index(), tf.Duration, int16(year))

		ca := io.None
		rs := io.NewRowSeries(*tbk, wtSet.Buffer.Payload(), wtSet.DataShapes, wtSet.DataLen, &ca, io.EnumRecordType(wtSet.RecordType))
		cs.AddColumn("Epoch", epoch)
		cs.AddColumn()
		cs.AddColumn("Open", opens)
		cs.AddColumn("Close", closes)
		cs.AddColumn("High", highs)
		cs.AddColumn("Low", lows)
		cs.AddColumn("Volume", volumes)

		return cs
	}


	csm.AddColumnSeries(tbk, cs)

	for tbkStr, idx := range nmds.StartIndex {
		length := nmds.Lengths[tbkStr]
		var cs *ColumnSeries
		if length > 0 {
			cs, err = nmds.ToColumnSeries(idx, length)
			if err != nil {
				return nil, err
			}
		} else {
			cs = NewColumnSeries()
		}
		tbk := NewTimeBucketKeyFromString(tbkStr)
		csm.AddColumnSeries(*tbk, cs)
	}
}

func walKeyPathToTBKInfo(walKeyPath string) (tbk *io.TimeBucketKey, year int, err error) {
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
