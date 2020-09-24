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

// e.g. "/Users/dakimura/marketstore/data/AMZN/1Min/TICK/2017.bin" -> (AMZN), (1Min), (TICK), (2017)
var WkpRegex = regexp.MustCompile(`([^/]+)/([^/]+)/([^/]+)/([0-9]+)\.bin$`)

func replay(transactionGroup []byte) error {
	// TODO: replay ordered by transactionGroupID
	log.Debug(fmt.Sprintf("[replica] received a replication message. size=%v", len(transactionGroup)))

	tgID, wtsets := executor.ParseTGData(transactionGroup, executor.ThisInstance.RootDir)
	if len(wtsets) == 0 {
		log.Info("[replica] received empty WTset")
		return nil
	}
	log.Debug(fmt.Sprintf("[replica] transactionGroupID=%v", tgID))

	csm, err := WTSetsToCSM(wtsets)
	if err != nil {
		return errors.Wrap(err, "failed to convert WTSet to CSM")
	}

	err = executor.WriteCSM(csm, wtsets[0].RecordType == io.VARIABLE)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to WriteCSM. csm:%v", csm))
	}

	log.Debug("[replica] successfully replayed the WAL message")
	return nil
}

func WTSetsToCSM(wtSets []wal.WTSet) (io.ColumnSeriesMap, error) {
	const (
		EpochBytes         = 8
		NanosecBytes       = 4
		IntervalTicksBytes = 4
	)

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

		intervalsPerDay := uint32(utils.Day.Seconds() / tf.Duration.Seconds())
		ca := io.None

		var buf []byte
		switch wtSet.RecordType {
		case io.FIXED: // only 1 record in WTset in case of FIXED
			buf, err = io.Serialize(buf, epoch.Unix())
			if err != nil {
				return nil, errors.Wrap(err, "failed to serialize Epoch to buffer:"+epoch.String())
			}

			buf, err = io.Serialize(buf, wtSet.Buffer.Payload())

		case io.VARIABLE: // can have multiple records that have the same Epoch
			if wtSet.VarRecLen == 0 {
				return nil, errors.New("[bug] variableRecordLength=0")
			}

			payload := wtSet.Buffer.Payload()
			numRows := len(payload) / wtSet.VarRecLen

			// Epoch*numRows + Payload(intervalTicks(4byte) are replaced by Nanoseconds(4byte))
			buf = make([]byte, numRows*EpochBytes+len(wtSet.Buffer.Payload()))
			cursor := 0
			for i := 0; i < numRows; i++ {
				// serialize Epoch (variable length records in a WriteSet have the same Epoch value)
				buf, err = io.Serialize(buf[:cursor], epoch.Unix())
				if err != nil {
					return nil, errors.Wrap(err, "failed to serialize Epoch to buffer:"+epoch.String())
				}
				cursor += EpochBytes

				// append the payload (columns + intervalTicks) for a record
				buf, err = io.Serialize(buf[:cursor], payload[i*wtSet.VarRecLen:(i+1)*wtSet.VarRecLen])
				cursor += wtSet.VarRecLen

				// last 4 byte of each record is an intervalTick
				intervalTicks := io.ToUInt32(buf[len(buf)-IntervalTicksBytes:])
				// expand intervalTicks(32bit) to Epoch and Nanosecond
				_, nanosecond := executor.GetTimeFromTicks(uint64(epoch.Unix()), intervalsPerDay, intervalTicks)
				// chop off the intervalTicks and append nanosec
				buf, err = io.Serialize(buf[:len(buf)-IntervalTicksBytes], int32(nanosecond))
			}

			// add Nanoseconds column to the data shape vector
			dsv = append(dsv, io.DataShape{Name: "Nanoseconds", Type: io.INT32})
		}

		// when rowLen = 0 is specified, the rowLength is calculated automatically from DataShapes
		rs := io.NewRowSeries(*tbk, buf, wtSet.DataShapes, 0, &ca, wtSet.RecordType)
		key, cs := rs.ToColumnSeries()

		csm.AddColumnSeries(key, cs)
	}

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
