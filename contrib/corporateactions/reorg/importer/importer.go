package main

import (
	"flag"
	"fmt"
	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/reorg"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const (
	NewRecord = iota
	UpdateRecord
	DeleteRecord
)

const (
	processedTag    = ".processed"
	bucketkeySuffix = "/1D/ACTIONS"
	reorgFilePrefix = "reorg"
)

var status_code_map = map[string]int64{
	"N": NewRecord,
	"U": UpdateRecord,
	"D": DeleteRecord,
}

var (
	reorgDir, dataDir string
	reimport          bool
)

func init() {
	flag.StringVar(&reorgDir, "reorg", "", "path to the reorg files")
	flag.StringVar(&dataDir, "data", "", "path to store marketstore files")
	flag.BoolVar(&reimport, "reimport", false, "set to true if you want to process every reorg file again")
	flag.Parse()
	log.Debug("Settings - dataDir: %s reorgDir: %s reimport: %v", dataDir, reorgDir, reimport)
}

func file_list(path string, prefix string) (out []string, err error) {
	localfiles, err := ioutil.ReadDir(path)
	if err == nil {
		for _, file := range localfiles {
			if strings.HasPrefix(file.Name(), prefix) && (reimport || (!reimport && !strings.HasSuffix(file.Name(), processedTag))) {
				out = append(out, file.Name())
			}
		}
	}
	return
}

func readNotifications(path string) (*[]reorg.Notification, error) {
	log.Info("Processing file %s", path)
	buff, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	content := string(buff)
	var notifications = []reorg.Notification{}
	reorg.ReadRecords(content, &notifications)
	log.Info(fmt.Sprintf("Read %d records", len(notifications)))
	return &notifications, nil
}

func storeNotification(note *reorg.Notification) error {
	tbk := io.NewTimeBucketKeyFromString(note.TargetCusip + bucketkeySuffix)
	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{note.EntryDate.Unix()})
	cs.AddColumn("TextNumber", []int64{note.TextNumber})
	cs.AddColumn("UpdateTextNumber", []int64{note.UpdateTextNumber})
	cs.AddColumn("DeleteTextNumber", []int64{note.DeleteTextNumber})
	cs.AddColumn("Status", []int64{status_code_map[note.Status]})
	cs.AddColumn("RecordDate", []int64{note.RecordDate.Unix()})
	cs.AddColumn("EffectiveDate", []int64{note.EffectiveDate.Unix()})
	cs.AddColumn("NewRate", []float64{note.NewRate})
	cs.AddColumn("OldRate", []float64{note.OldRate})
	cs.AddColumn("Rate", []float64{note.Rate})
	csm.AddColumnSeries(*tbk, cs)
	err := executor.WriteCSM(csm, true)
	return err
}

func storeNotifications(notes []reorg.Notification) error {
	for _, note := range notes {
		if note.TargetCusip == "" {
			continue
		}
		if note.IsSplit() || note.IsReverseSplit() {
			msg := fmt.Sprintf("%d %s %s - %s : %.2f, %.2f, %.2f", note.TextNumber, note.Status, note.Remarks, note.TargetCusip, note.OldRate, note.NewRate, note.Rate)
			log.Info(msg)
			if err := storeNotification(&note); err != nil {
				log.Fatal("Unable to store notification: %s", msg)
				return err
			}
		}
	}
	return nil
}

func import_reorg_files() {
	reorg_files, err := file_list(reorgDir, reorgFilePrefix)
	if err != nil {
		log.Fatal("Cannot read reorg files directory - dir: %s, error: %v", reorgDir, err)
		return
	}
	log.Info("Parsing %d new files", len(reorg_files))
	for _, reorg_file := range reorg_files {
		path_to_file := filepath.Join(reorgDir, reorg_file)
		notifications, err := readNotifications(path_to_file)
		if err != nil {
			log.Fatal("Error occured while reading reorg file: %s", reorg_file)
			return
		}
		err = storeNotifications(*notifications)
		if err != nil {
			log.Fatal("Error occured while processing notifications from %s", reorg_file)
			return
		} else {
			if !reimport {
				os.Rename(path_to_file, path_to_file+processedTag)
			}
		}
	}
}

func main() {
	if reorgDir != "" && dataDir != "" {
		executor.NewInstanceSetup(dataDir, true, true, true, true)
		import_reorg_files()
	} else {
		log.Fatal("Please set reorgDir and dataDir parameters!")
	}
}
