package reorg

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"
	"github.com/alpacahq/marketstore/v4/contrib/ice/sirs"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func Import(reorgDir string, reimport, storeWithoutSymbol bool) error {
	reorgFiles, err := fileList(reorgDir, enum.ReorgFilePrefix, reimport)
	if err != nil {
		return fmt.Errorf("cannot read reorg files directory - dir=%s: %w", reorgDir, err)
	}
	log.Info("Parsing %d new files", len(reorgFiles))
	for _, reorgFile := range reorgFiles {
		log.Info("======== Processing %s", reorgFile)
		reorgFileDate := datePartOfFilename(reorgFile)
		pathToReorgFile := filepath.Join(reorgDir, reorgFile)

		announcements, err := readAnnouncements(pathToReorgFile)
		if err != nil {
			return fmt.Errorf("error occurred while reading reorg file=%s: %w", reorgFile, err)
		}

		sirsFiles, err := sirs.CollectSirsFiles(reorgDir, reorgFileDate)
		if err != nil {
			return fmt.Errorf("cannot load sirs files: %w", err)
		}
		if !storeWithoutSymbol && len(sirsFiles) == 0 {
			log.Warn("No sirs files loaded, skip to next reorg file")
			continue
		}
		cusipSymbolMap, err := sirs.BuildSecurityMasterMap(sirsFiles)
		if err != nil {
			return fmt.Errorf("cannot read security info data: %w", err)
		}
		err = storeAnnouncements(*announcements, cusipSymbolMap, storeWithoutSymbol)
		if err != nil {
			return fmt.Errorf("error occurred while processing announcements from %s: %w", reorgFile, err)
		}
		if !reimport {
			err = os.Rename(pathToReorgFile, pathToReorgFile+enum.ProcessedFlag)
			if err != nil {
				return fmt.Errorf("failed to rename reorgfile from %s to %s: %w",
					pathToReorgFile, pathToReorgFile+enum.ProcessedFlag, err)
			}
		}
	}
	return nil
}

func datePartOfFilename(filename string) string {
	ext := filepath.Ext(strings.ReplaceAll(filename, enum.ProcessedFlag, ""))
	return ext[1:]
}

func fileList(path, prefix string, reimport bool) (out []string, err error) {
	localDirEntries, err := os.ReadDir(path)
	if err == nil {
		for _, dirEntry := range localDirEntries {
			if !strings.HasPrefix(dirEntry.Name(), prefix) {
				continue
			}
			if reimport || !strings.HasSuffix(dirEntry.Name(), enum.ProcessedFlag) {
				out = append(out, dirEntry.Name())
			}
		}
	}
	return out, err
}

func readAnnouncements(path string) (*[]Announcement, error) {
	buff, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	content := string(buff)
	var announcements []Announcement
	err = readRecords(content, &announcements)
	if err != nil {
		return nil, fmt.Errorf("failed to readRecords: %w", err)
	}
	log.Info(fmt.Sprintf("Read %d records", len(announcements)))
	return &announcements, nil
}

func storeAnnouncement(symbol string, note *Announcement) error {
	tbk := io.NewTimeBucketKeyFromString(symbol + enum.BucketkeySuffix)
	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{note.EntryDate.Unix()})
	cs.AddColumn("TextNumber", []int64{int64(note.TextNumber)})
	cs.AddColumn("UpdateTextNumber", []int64{int64(note.UpdateTextNumber)})
	cs.AddColumn("DeleteTextNumber", []int64{int64(note.DeleteTextNumber)})
	cs.AddColumn("NotificationType", []byte{byte(note.NotificationType)})
	cs.AddColumn("Status", []byte{byte(note.Status)})
	cs.AddColumn("UpdatedNotificationType", []byte{byte(note.UpdatedNotificationType)})
	cs.AddColumn("SecurityType", []byte{byte(note.SecurityType)})
	cs.AddColumn("VoluntaryMandatoryCode", []byte{byte(note.VoluntaryMandatoryCode)})
	cs.AddColumn("RecordDate", []int64{note.RecordDate.Unix()})
	cs.AddColumn("EffectiveDate", []int64{note.EffectiveDate.Unix()})
	cs.AddColumn("ExpirationDate", []int64{note.ExpirationDate.Unix()})
	cs.AddColumn("NewRate", []float64{note.NewRate})
	cs.AddColumn("OldRate", []float64{note.OldRate})
	cs.AddColumn("Rate", []float64{note.Rate})
	csm.AddColumnSeries(*tbk, cs)
	err := executor.WriteCSM(csm, true)
	return err
}

func storeAnnouncements(notes []Announcement, cusipSymbolMap map[string]string, storeWithoutSymbol bool) error {
	for i := range notes {
		if notes[i].TargetCusip == "" {
			continue
		}
		if !(notes[i].Is(enum.StockSplit) || notes[i].Is(enum.ReverseStockSplit) || notes[i].Is(enum.StockDividend)) {
			continue
		}

		symbol := cusipSymbolMap[notes[i].TargetCusip]
		if symbol == "" && storeWithoutSymbol {
			symbol = notes[i].TargetCusip
		}
		if symbol != "" {
			if err := storeAnnouncement(symbol, &notes[i]); err != nil {
				return fmt.Errorf("unable to store Announcement: %w %+v", err, notes[i])
			}
		} else {
			log.Warn("Cannot map CUSIP %s to Symbol", notes[i].TargetCusip)
		}
	}
	return nil
}
