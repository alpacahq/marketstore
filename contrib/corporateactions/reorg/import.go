package reorg

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/models"
	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/sirs"
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
	reorg_files, err := file_list(reorgDir, reorgFilePrefix, reimport)
	if err != nil {
		log.Fatal("Cannot read reorg files directory - dir: %s, error: %v", reorgDir, err)
		return
	}
	log.Info("Parsing %d new files", len(reorg_files))
	for _, reorg_file := range reorg_files {
		sirs_file := strings.ReplaceAll(reorg_file, reorgFilePrefix, sirsFilePrefix)
		sirs_file = strings.ReplaceAll(sirs_file, ".processed", "")
		path_to_reorg_file := filepath.Join(reorgDir, reorg_file)
		path_to_sirs_file := filepath.Join(reorgDir, sirs_file)

		notifications, err := readNotifications(path_to_reorg_file)
		if err != nil {
			log.Fatal("Error occured while reading reorg file: %s", reorg_file)
			return
		}

		sirsFiles, err := sirs.CollectSirsFilesFor(reorg_file)
		if err != nil {
			return 
		}
		cusipSymbolMap, err := sirs.BuildSecurityMasterMap(sirsFiles)
		// cusipSymbolMap, err := readSymbolInfo(path_to_sirs_file)
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


func file_list(path string, prefix string, reimport bool) (out []string, err error) {
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
	log.Info("Processing file %s", path)
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
			msg := fmt.Sprintf("%d %s %s - %s : %.2f, %.2f, %.2f", note.TextNumber, note.Status, note.Remarks, note.TargetCusip, note.OldRate, note.NewRate, note.Rate)
			log.Info(msg)
			symbol, present := cusipSymbolMap[note.TargetCusip]
			if present {
				if err := storeNotification(symbol, &note); err != nil {
					log.Fatal("Unable to store notification: %s", msg)
					return err
				}
			} else {
				log.Error("Cannot map CUSIP %s to Symbol!!", note.TargetCusip)
			}
		}
	}
	return nil
}

// func readSymbolInfo(path string) (map[string]string, error) {
// 	rm := map[string]string{}
// 	file, err := os.Open(path)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()			
// 	records, err := sirs.Load(file) 
// 	if err == nil {
// 		for _, r := range records {
// 			rm[r.Cusip] = r.Symbol
// 		}
// 	}
// 	return rm, err
// }


// func show_records(cusip string) {
// 	ca := uda.NewCorporateActions(cusip)
// 	ca.Load()
// 	println("----- stored records ------")
// 	for i:=0; i<len(ca.Rows.EntryDates); i++ {
// 		ent := time.Unix(ca.Rows.EntryDates[i], 0)
// 		eff := time.Unix(ca.Rows.EffectiveDates[i], 0)
// 		rec := time.Unix(ca.Rows.RecordDates[i], 0)

// 		var ref int64
// 		if ca.Rows.Statuses[i] == uda.UpdateRecord {
// 			ref = ca.Rows.UpdateTextNumbers[i]
// 		} else if ca.Rows.Statuses[i] == uda.DeleteRecord {
// 			ref = ca.Rows.DeleteTextNumbers[i]
// 		}

// 		fmt.Printf("%c %c %c\tTEXTNUM: %d\tENT: %s, EFF: %s, REC: %s\tRATE: %.4f, REF: %d\n", 
// 			ca.Rows.Statuses[i],
// 			ca.Rows.SecurityTypes[i],
// 			ca.Rows.NotificationTypes[i],
// 			ca.Rows.TextNumbers[i],
// 			ent.Format("2006-01-02"),
// 			eff.Format("2006-01-02"),
// 			rec.Format("2006-01-02"),
// 			ca.Rows.Rates[i],
// 			ref)
// 	} 
// 	rate_changes := ca.RateChangeEvents()
// 	println("----- effective rate changes ---")
// 	for _, r := range rate_changes {
// 		fmt.Printf("DATE: %s, TEXTNUM: %d, RATE: %.4f\n", time.Unix(r.Epoch, 0).Format("2006-01-02"), r.Textnumber, r.Rate)
// 	}
// }

// func main() {
// 	if reorgDir != "" && dataDir != "" {
// 		executor.NewInstanceSetup(dataDir, true, true, true, true)
// 		import_reorg_files()
// 	} else if cusip != "" && dataDir != "" {
// 		// cusip = //  "75079T104" 654090109    371485301  56382R274 15930P800  409076106 90916U107 76133H102
// 		executor.NewInstanceSetup(dataDir, true, true, true, true)
// 		show_records(cusip)
// 	} else {
// 		log.Fatal("Please set reorgDir and dataDir parameters!")
// 	}
// }
