package reorg

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/contrib/ice/models"
	"github.com/alpacahq/marketstore/v4/contrib/ice/sirs"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const (
	processedTag    = ".processed"
	bucketkeySuffix = "/1D/ACTIONS"
	reorgFilePrefix = "reorg"
	sirsFilePrefix = "sirs"
)

func Import(reorgDir string, reimport bool) {
	reorg_files, err := fileList(reorgDir, reorgFilePrefix, reimport)
	if err != nil {
		log.Fatal("Cannot read reorg files directory - dir: %s, error: %v", reorgDir, err)
		return
	}
	log.Info("Parsing %d new files", len(reorg_files))
	for _, reorg_file := range reorg_files {
		log.Info("======== Processing %s", reorg_file)
		sirs_file := strings.ReplaceAll(reorg_file, reorgFilePrefix, sirsFilePrefix)
		sirs_file = strings.ReplaceAll(sirs_file, processedTag, "")
		path_to_reorg_file := filepath.Join(reorgDir, reorg_file)
		path_to_sirs_file := filepath.Join(reorgDir, sirs_file)

		notifications, err := readNotifications(path_to_reorg_file)
		if err != nil {
			log.Fatal("Error occured while reading reorg file: %s", reorg_file)
			return
		}

		sirsFiles, err := sirs.CollectSirsFilesFor(path_to_sirs_file)
		if err != nil {
			log.Fatal("Cannot loat Sirs files: %+v", err)
			return 
		}
		// fmt.Printf("SIRS files: %+v\n", sirsFiles)
		if len(sirsFiles) == 0 {
			log.Warn("No sirs files loaded, skip to next reorg file")
			continue
		}
		cusipSymbolMap, err := sirs.BuildSecurityMasterMap(sirsFiles)
		if err != nil {
			log.Fatal("Cannot read security info data from %s", path_to_sirs_file)
			return 
		}
		err = storeNotifications(*notifications, cusipSymbolMap)
		if err != nil {
			log.Fatal("Error occured while processing notifications from %s", reorg_file)
			return
		} else {
			if !reimport {
				os.Rename(path_to_reorg_file, path_to_reorg_file+processedTag)
			}
		}
	}
}


func fileList(path string, prefix string, reimport bool) (out []string, err error) {
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

func readNotifications(path string) (*[]models.Notification, error) {
	buff, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	content := string(buff)
	var notifications = []models.Notification{}
	ReadRecords(content, &notifications)
	log.Info(fmt.Sprintf("Read %d records", len(notifications)))
	return &notifications, nil
}

func storeNotification(symbol string, note *models.Notification) error {
	tbk := io.NewTimeBucketKeyFromString(symbol + bucketkeySuffix)
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

func storeNotifications(notes []models.Notification, cusipSymbolMap map[string]string) error {
	for _, note := range notes {
		if note.TargetCusip == "" {
			continue
		}
		if note.Is(models.Split) || note.Is(models.ReverseSplit) || note.Is(models.Dividend) {
			symbol, present := cusipSymbolMap[note.TargetCusip]
			// msg := fmt.Sprintf("%d %s %s - %s %s : %.2f, %.2f, %.2f", note.TextNumber, note.Status, note.Remarks, note.TargetCusip, symbol, note.OldRate, note.NewRate, note.Rate)
			// log.Info(msg)

			if present && symbol != "" {
				if err := storeNotification(symbol, &note); err != nil {
					log.Fatal("Unable to store notification: %+v %+v", err, note)
					return err
				}
			} else {
				log.Warn("Cannot map CUSIP %s to Symbol!!\n%+v", note.TargetCusip, note)
			}
		}
	}
	return nil
}
