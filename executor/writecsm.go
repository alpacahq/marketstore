package executor

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/metrics"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// WriteCSM writes ColumnSeriesMap (csm) to each destination file, and flush it to the disk,
// isVariableLength is set to true if the record content is variable-length type. WriteCSM
// also verifies the DataShapeVector of the incoming ColumnSeriesMap matches the on-disk
// DataShapeVector defined by the file header. WriteCSM will create any files if they do
// not already exist for the given ColumnSeriesMap based on its TimeBucketKey.
func (w *Writer) WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) (err error) {
	// WRITE is not allowed on a replica
	if utils.InstanceConfig.Replication.MasterHost != "" {
		return errors.New("write is not allowed on replica")
	}

	return w.WriteCSMInner(csm, isVariableLength)
}

func (w *Writer) WriteCSMInner(csm io.ColumnSeriesMap, isVariableLength bool) (err error) {
	start := time.Now()
	for tbk, cs := range csm {
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			return err
		}

		/*
			Prepare data for writing
		*/
		var alignData bool
		times, err := cs.GetTime()
		if err != nil {
			return err
		}
		if isVariableLength {
			cs.Remove("Nanoseconds")
			alignData = false
		}

		tbi, err := w.root.GetLatestTimeBucketInfoFromKey(&tbk)
		if err != nil {
			/*
				If we can't get the info, we try here to add a new one
			*/
			var recordType io.EnumRecordType
			if isVariableLength {
				recordType = io.VARIABLE
			} else {
				recordType = io.FIXED
			}

			t, err := cs.GetTime()
			if err != nil {
				return err
			}
			if len(t) == 0 {
				continue
			}

			year := int16(t[0].Year())
			tbi = io.NewTimeBucketInfo(
				*tf,
				tbk.GetPathToYearFiles(w.root.GetPath()),
				"Created By Writer", year,
				cs.GetDataShapes(), recordType)

			/*
				Verify there is an available TimeBucket for the destination
			*/
			if err := w.root.AddTimeBucket(&tbk, tbi); err != nil {
				// If File Exists error, ignore it, otherwise return the error
				if !strings.Contains(err.Error(), "Can not overwrite file") && !strings.Contains(err.Error(), "file exists") {
					return err
				}
			}
		}
		// Check if the previously-written data schema matches the input
		columnMismatchError := "unable to match data columns (%v) to bucket columns (%v)"
		dbDSV := tbi.GetDataShapesWithEpoch()
		csDSV := cs.GetDataShapes()
		if len(dbDSV) != len(csDSV) {
			return fmt.Errorf(columnMismatchError, csDSV, dbDSV)
		}
		missing, coercion := io.GetMissingAndTypeCoercionColumns(dbDSV, csDSV)
		if missing != nil {
			return fmt.Errorf(columnMismatchError, csDSV, dbDSV)
		}

		if coercion != nil {
			for _, dbDS := range coercion {
				if err := cs.CoerceColumnType(dbDS.Name, dbDS.Type); err != nil {
					csType := io.GetElementType(cs.GetColumn(dbDS.Name))
					log.Error("[%s] error coercing %s from %s to %s", tbk.GetItemKey(), dbDS.Name, csType.String(), dbDS.Type.String())
					return err
				}
			}
		}

		/*
			Create a writer for this TimeBucket
		*/
		w, err := NewWriter(tbi, w.tgc, w.root, w.walFile)
		if err != nil {
			return err
		}

		rowData := cs.ToRowSeries(tbk, alignData).GetData()
		w.WriteRecords(times, rowData, dbDSV)
	}
	w.walFile.RequestFlush()
	metrics.WriteCSMDuration.Observe(time.Since(start).Seconds())
	return nil
}
