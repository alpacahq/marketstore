package replication

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/pkg/errors"
	"regexp"
	"strconv"
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
	if len(wtsets) == 0 {
		log.Info("[replica] received empty WTset")
		return nil
	}
	fmt.Println(tgID)
	fmt.Println(wtsets)

	csm, err := WTSetsToCSM(wtsets)
	if err != nil {
		return errors.Wrap(err, "failed to convert WTSet to CSM")
	}

	err = executor.WriteCSM(csm, io.EnumRecordType(wtsets[0].RecordType) == io.VARIABLE)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to WriteCSM. csm:%v", csm))
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

	log.Debug("[replica] successfully replayed the WAL message")
	return nil
}

func WTSetsToCSM(wtSets []wal.WTSet) (io.ColumnSeriesMap, error) {
	csm := io.NewColumnSeriesMap()

	for _, wtSet := range wtSets {
		tbk, year, err := walKeyPathToTBKInfo(wtSet.FilePath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse walKeyPath to bucket info. wkp:"+wtSet.FilePath)
		}
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get TimeFrame from TimeBucketKey. tbk:"+tbk.String())
		}

		epoch := io.IndexToTime(wtSet.Buffer.Index(), tf.Duration, int16(year))
		dsv := wtSet.DataShapes

		var buf []byte
		buf, err = io.Serialize(buf, epoch.Unix())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Epoch to buffer:"+epoch.String())
		}

		buf, err = io.Serialize(buf, wtSet.Buffer.Payload())
		if io.EnumRecordType(wtSet.RecordType) == io.VARIABLE {
			intervalTicks := io.ToUInt32(buf[len(buf)-4:])

			// Expand ticks (32-bit) into epoch and nanos
			intervalsPerDay := uint32(utils.Day.Seconds() / tf.Duration.Seconds())
			_, nanosecond := executor.GetTimeFromTicks(uint64(epoch.Unix()), intervalsPerDay, intervalTicks)
			buf, err = io.Serialize(buf, nanosecond)
			dsv = append(dsv, io.DataShape{Name: "Nanoseconds", Type: io.INT32})
		}

		ca := io.None
		rs := io.NewRowSeries(*tbk, buf, wtSet.DataShapes, wtSet.DataLen, &ca, io.EnumRecordType(wtSet.RecordType))
		key, cs := rs.ToColumnSeries()
		csm.AddColumnSeries(key, cs)
	}

	//for tbkStr, idx := range nmds.StartIndex {
	//	length := nmds.Lengths[tbkStr]
	//	var cs *ColumnSeries
	//	if length > 0 {
	//		cs, err = nmds.ToColumnSeries(idx, length)
	//		if err != nil {
	//			return nil, err
	//		}
	//	} else {
	//		cs = NewColumnSeries()
	//	}
	//	tbk := NewTimeBucketKeyFromString(tbkStr)
	//	csm.AddColumnSeries(*tbk, cs)
	//}
	return csm, nil
}

func walKeyPathToTBKInfo(walKeyPath string) (tbk *io.TimeBucketKey, year int, err error) {
	group := WkpRegex.FindStringSubmatch(walKeyPath)
	// group should be like {"AAPL/1Min/Tick/2020.bin","AAPL","1Min","Tick","2017"} (len:5, cap:5)
	if len(group) != 5 {
		return nil, 0, errors.New(fmt.Sprintf("failed to extract TBK info from WalKeyPath:%v", walKeyPath))
	}

	year, err = strconv.Atoi(group[4])
	if err != nil {
		return nil, 0, errors.New(fmt.Sprintf("failed to extract year from WalKeyPath:%s", group[3]))
	}

	return io.NewTimeBucketKey(fmt.Sprintf("%s/%s/%s", group[1], group[2], group[3])), year, nil
}
