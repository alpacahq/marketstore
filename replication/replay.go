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

	err = executor.WriteCSM(csm, io.EnumRecordType(wtsets[0].RecordType) == io.VARIABLE)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to WriteCSM. csm:%v", csm))
	}

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
			buf, err = io.Serialize(buf[:len(buf)-4], int32(nanosecond)) // chop off interval ticks and append nanosec
			dsv = append(dsv, io.DataShape{Name: "Nanoseconds", Type: io.INT32})
		}

		ca := io.None
		rs := io.NewRowSeries(*tbk, buf, wtSet.DataShapes, wtSet.DataLen, &ca, io.EnumRecordType(wtSet.RecordType))
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
