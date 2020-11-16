package reorg

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"
	"github.com/alpacahq/marketstore/v4/contrib/ice/sirs"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func Import(reorgDir string, reimport bool) {
	reorgFiles, err := fileList(reorgDir, enum.ReorgFilePrefix, reimport)
	if err != nil {
		log.Fatal("Cannot read reorg files directory - dir: %s, error: %v", reorgDir, err)
		return
	}
	log.Info("Parsing %d new files", len(reorgFiles))
	for _, reorgFile := range reorgFiles {
		log.Info("======== Processing %s", reorgFile)
		reorgFileDate := datePartOfFilename(reorgFile)
		pathToReorgFile := filepath.Join(reorgDir, reorgFile)

		announcements, err := readAnnouncements(pathToReorgFile)
		if err != nil {
			log.Fatal("Error occured while reading reorg file: %s", reorgFile)
			return
		}

		sirsFiles, err := sirs.CollectSirsFiles(reorgDir, reorgFileDate)
		if err != nil {
			log.Fatal("Cannot loat Sirs files: %+v", err)
			return
		}
		if len(sirsFiles) == 0 {
			log.Warn("No sirs files loaded, skip to next reorg file")
			continue
		}
		cusipSymbolMap, err := sirs.BuildSecurityMasterMap(sirsFiles)
		if err != nil {
			log.Fatal("Cannot read security info data: %v", err)
			return
		}
		err = storeAnnouncements(*announcements, cusipSymbolMap)
		if err != nil {
			log.Fatal("Error occured while processing announcements from %s", reorgFile)
			return
		}
		if !reimport {
			os.Rename(pathToReorgFile, pathToReorgFile+enum.ProcessedFlag)
		}
	}
}

func datePartOfFilename(filename string) string {
	ext := filepath.Ext(strings.ReplaceAll(filename, enum.ProcessedFlag, ""))
	return ext[1:]
}

func fileList(path string, prefix string, reimport bool) (out []string, err error) {
	localfiles, err := ioutil.ReadDir(path)
	if err == nil {
		for _, file := range localfiles {
			if strings.HasPrefix(file.Name(), prefix) && (reimport || (!reimport && !strings.HasSuffix(file.Name(), enum.ProcessedFlag))) {
				out = append(out, file.Name())
			}
		}
	}
	return
}

func readAnnouncements(path string) (*[]Announcement, error) {
	buff, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	content := string(buff)
	var announcements = []Announcement{}
	ReadRecords(content, &announcements)
	log.Info(fmt.Sprintf("Read %d records", len(announcements)))
	return &announcements, nil
}

func storeAnnouncement(symbol string, note *Announcement) error {
	tbk := io.NewTimeBucketKeyFromString(symbol + enum.BucketkeySuffix)
	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{note.EntryDate.Unix()})
	cs.AddColumn("TextNumber", []int64{note.TextNumber})
	cs.AddColumn("UpdateTextNumber", []int64{note.UpdateTextNumber})
	cs.AddColumn("DeleteTextNumber", []int64{note.DeleteTextNumber})
	cs.AddColumn("NotificationType", []byte{note.NotificationType[0]})
	cs.AddColumn("Status", []byte{note.Status[0]})
	cs.AddColumn("SecurityType", []byte{note.SecurityType[0]})
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

func storeAnnouncements(notes []Announcement, cusipSymbolMap map[string]string) error {
	for _, note := range notes {
		if note.TargetCusip == "" {
			continue
		}
		if note.Is(enum.StockSplit) || note.Is(enum.ReverseStockSplit) || note.Is(enum.StockDividend) {
			symbol, present := cusipSymbolMap[note.TargetCusip]
			if present && symbol != "" {
				if err := storeAnnouncement(symbol, &note); err != nil {
					log.Fatal("Unable to store Announcement: %+v %+v", err, note)
					return err
				}
			} else {
				log.Warn("Cannot map CUSIP %s to Symbol", note.TargetCusip)
			}
		}
	}
	return nil
}
