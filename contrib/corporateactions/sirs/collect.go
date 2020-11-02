package sirs

import (
	"os"
	"time"
	//"strings"
	"path/filepath"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/models"
)


func PreviousFriday(t time.Time) (time.Time, error) {
	if t.Weekday() == time.Friday {
		return t, nil
	}
	diff := -1 * (t.Weekday() + 2)
	prevFriday := t.AddDate(0, 0, int(diff))
	return prevFriday, nil
}

func exists(filename string ) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

func CollectSirsFilesFor(sirsFile string) ([]string, error) {
	basePath, fileName := filepath.Split(sirsFile)
	currentDateStr := filepath.Ext(fileName)
	currentDate, err := time.Parse("20060102", currentDateStr[1:])
	if err != nil {
		return []string{}, err
	}
	begin, _ := PreviousFriday(currentDate)
	masterfile := filepath.Join(basePath, "sirs.refresh." + begin.Format("20060102"))
	if !exists(masterfile) {
		return []string{}, nil
	}
	filenames := make([]string, 0)
	filenames = append(filenames, masterfile)
	t := begin
	for {
		t = t.AddDate(0, 0, 1)
		if t.After(currentDate) {
			break
		}
		filename := filepath.Join(basePath, "sirs." + t.Format("20060102"))
		if exists(filename) {
			filenames = append(filenames, filename)
		}
	}
	return filenames, nil
}

func LoadSirsFile(fileName string) ([]*models.SecurityMaster, error) {
	records := []*models.SecurityMaster{}
	file, err := os.Open(fileName)
	defer file.Close()
	if err == nil {
		records, err = Load(file) 
	} 
	return records, nil

}

func BuildSecurityMasterMap(sirsFiles []string) (map[string]string, error) {
	master := map[string]string{}
	for _, filename := range sirsFiles {
		records, err := LoadSirsFile(filename)
		if err == nil {
			log.Info("%s # of records: %d", filename, len(records))
			for _, r := range records {
				master[r.Cusip] = r.Symbol
			}
		} else {
			return master, err
		}
	}
	return master, nil
}
