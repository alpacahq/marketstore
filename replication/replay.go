package replication

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type ReplayerImpl struct {
	// parseTGFunc is a function to parse Transaction Group byte array to writeTransactionSet.
	// wal.ParseTGData is always used, but abstracted for testability
	parseTGFunc func(tgSerialized []byte, rootPath string) (tgID int64, wtSets []wal.WTSet)
	// WriteFunc is a function to write CSM to marketstore.
	writeFunc func(csm io.ColumnSeriesMap, isVariableLength bool) (err error)
	// rootDir is the path to the directory in which Marketstore database resides(e.g. "data")
	rootDir string
}

func NewReplayer(
	parseTGFunc func(tgSerialized []byte, rootPath string) (TGID int64, wtSets []wal.WTSet),
	writeFunc func(csm io.ColumnSeriesMap, isVariableLength bool) (err error),
	rootDir string,
) *ReplayerImpl {
	return &ReplayerImpl{
		parseTGFunc: parseTGFunc,
		writeFunc:   writeFunc,
		rootDir:     rootDir,
	}
}

func (r *ReplayerImpl) Replay(transactionGroup []byte) error {
	// TODO: replay ordered by transactionGroupID
	log.Debug(fmt.Sprintf("[replica] received a replication message. size=%v", len(transactionGroup)))

	tgID, wtsets := r.parseTGFunc(transactionGroup, r.rootDir)
	if len(wtsets) == 0 {
		log.Info("[replica] received empty WTset")
		return nil
	}
	log.Debug(fmt.Sprintf("[replica] transactionGroupID=%v", tgID))

	for _, wtSet := range wtsets {
		csm, err := WTSetToCSM(&wtSet)
		if err != nil {
			return errors.Wrap(err, "failed to convert WTSet to CSM")
		}

		err = r.writeFunc(csm, wtsets[0].RecordType == io.VARIABLE)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to WriteCSM. csm:%v", csm))
		}
	}
	log.Debug("[replica] successfully replayed the WAL message")
	return nil
}

// WTSetToCSM converts wal.WTSet to ColumnSeriesMap.
func WTSetToCSM(wtSet *wal.WTSet) (io.ColumnSeriesMap, error) {
	csm := io.NewColumnSeriesMap()
	cs, tbk, err := wtSetToCS(wtSet)
	if err != nil {
		return nil, err
	}
	csm.AddColumnSeries(*tbk, cs)

	return csm, nil
}

func wtSetToCS(wtSet *wal.WTSet) (*io.ColumnSeries, *io.TimeBucketKey, error) {
	// get TimeBucketKey and year from the FilePath in WTSet
	tbk, year, err := io.NewTimeBucketKeyFromWalKeyPath(wtSet.FilePath)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to parse walKeyPath to bucket info. wkp:"+wtSet.FilePath)
	}

	tf, err := tbk.GetTimeFrame()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get TimeFrame from TimeBucketKey. tbk:"+tbk.String())
	}

	// calculate Epoch
	epoch := io.IndexToTime(wtSet.Buffer.Index(), tf.Duration, int16(year))

	var buf []byte
	switch wtSet.RecordType {
	case io.FIXED: // only 1 record in WTset
		buf, err = io.Serialize(buf, epoch.Unix())
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to serialize Epoch to buffer:"+epoch.String())
		}

		buf, err = io.Serialize(buf, wtSet.Buffer.Payload())
		if err != nil {
			return nil, nil, errors.Wrap(err, fmt.Sprintf("failed to serialize Payload to buffer:%v", wtSet.Buffer.Payload()))
		}

	case io.VARIABLE: // WTset can have multiple records that have the same Epoch
		if wtSet.VarRecLen == 0 {
			return nil, nil, errors.New("[bug] variableRecordLength=0")
		}

		intervalsPerDay := uint32(utils.Day.Seconds() / tf.Duration.Seconds())

		// serialize rows in wtSet to []byte
		buf, err = serializeVariableRecords(epoch, intervalsPerDay, wtSet)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to serialize Payload to buffer:"+epoch.String())
		}
	}

	// when rowLen = 0 is specified, the rowLength is calculated automatically from DataShapes
	rs := io.NewRowSeries(*tbk, buf, wtSet.DataShapes, 0, wtSet.RecordType)
	_, cs := rs.ToColumnSeries()

	return cs, tbk, nil
}

// serializeVariableRecord serializes variableLength record(s) data in a WTSet to []byte.
func serializeVariableRecords(epoch time.Time, intervalsPerDay uint32, wtSet *wal.WTSet) ([]byte, error) {
	const (
		EpochBytes         = 8
		IntervalTicksBytes = 4
	)

	payload := wtSet.Buffer.Payload()
	varRecLen := wtSet.VarRecLen

	// the number of records in the wtSet
	numRows := len(payload) / varRecLen

	// allocate bytes for rows in wtSet.
	// Epoch*numRows + Payload(= (columns + 4byte (for Nanoseconds that replaces intervalTicks(4byte))*numRows )
	buf := make([]byte, numRows*EpochBytes+len(payload))
	var err error
	// 1 record size = 8byte(Epoch) + columns + intervalTicks(4byte) = 8byte(Epoch) + VariableLengthRecord
	cursor := 0
	for i := 0; i < numRows; i++ {
		// serialize Epoch (variable length records in a WTSet have the same Epoch value)
		buf, err = io.Serialize(buf[:cursor], epoch.Unix())
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Epoch to buffer:"+epoch.String())
		}
		cursor += EpochBytes

		// append the payload (= columns + intervalTicks) for a record
		buf, err = io.Serialize(buf[:cursor], payload[i*varRecLen:(i+1)*varRecLen])
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Payload to buffer:"+epoch.String())
		}

		// last 4 byte of each record is an intervalTick
		intervalTicks := io.ToUInt32(buf[len(buf)-IntervalTicksBytes:])
		// expand intervalTicks(32bit) to Epoch and Nanosecond
		_, nanosecond := executor.GetTimeFromTicks(uint64(epoch.Unix()), intervalsPerDay, intervalTicks)
		// replace intervalTick with Nanosecond
		buf, err = io.Serialize(buf[:len(buf)-IntervalTicksBytes], int32(nanosecond))
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize Payload to buffer:"+epoch.String())
		}

		cursor += varRecLen
	}

	return buf, nil
}
