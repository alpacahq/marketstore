package sirs

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// previousFriday Locate the previous Friday for a given time.
func previousFriday(t time.Time) (time.Time, error) {
	if t.Weekday() == time.Friday {
		return t, nil
	}
	diff := -1 * (t.Weekday() + 2)
	prevFriday := t.AddDate(0, 0, int(diff))
	return prevFriday, nil
}

// CollectSirsFilesFor returns a list of security master files for a given date.
// File names start from last Friday (the latest complete snapshot) and includes
// all incremental updates till we reach the date encoded in the reorg filename.
func CollectSirsFiles(basePath, currentDateStr string) ([]string, error) {
	currentDate, err := time.Parse("20060102", currentDateStr)
	if err != nil {
		log.Error("Unable to parse date: %s", currentDateStr)
		return nil, err
	}
	// ICE releases a full snapshot of security master information on each Friday in sirs.refresh files.
	begin, _ := previousFriday(currentDate)
	masterfile := filepath.Join(basePath, "sirs.refresh."+begin.Format("20060102"))
	if !exists(masterfile) {
		log.Warn("Master file not found: %s", masterfile)
		return []string{}, nil
	}

	filenames := make([]string, 0)
	filenames = append(filenames, masterfile)
	// beginning from last friday we find each incremental update, and add them to the list.
	t := begin
	for {
		t = t.AddDate(0, 0, 1)
		if t.After(currentDate) {
			break
		}
		filename := filepath.Join(basePath, "sirs."+t.Format("20060102"))
		if exists(filename) {
			filenames = append(filenames, filename)
		}
	}
	return filenames, nil
}

// LoadSirsFile loads a single Security info file.
func LoadSirsFile(fileName string) ([]*SecurityMaster, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("open a security info file %s: %w", fileName, err)
	}
	defer file.Close()
	return ParseFile(file)
}

// BuildSecurityMasterMap loads the listed security files and returns a map of cusipid -> symbol pairs.
// The first element of the input slice should be a complete snapshot (sirs.refresh.YYYYMMDD),
// and the following entries updates for this file (sirs.YYYYMMDD).
func BuildSecurityMasterMap(sirsFiles []string) (map[string]string, error) {
	master := map[string]string{}
	for _, filename := range sirsFiles {
		records, err := LoadSirsFile(filename)
		if err == nil {
			for _, r := range records {
				master[r.Cusip] = r.Symbol
			}
		} else {
			return master, err
		}
	}
	return master, nil
}

// Utility function to help out go's incredible standard library...
func exists(filename string) bool {
	fileinfo, err := os.Stat(filename)
	return !os.IsNotExist(err) && !fileinfo.IsDir()
}
