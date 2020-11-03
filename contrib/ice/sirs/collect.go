package sirs

import (
	"os"
	"time"
	"path/filepath"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/contrib/ice/models"
)



/*
	Locate the previous Friday for a given time. 
*/

func PreviousFriday(t time.Time) (time.Time, error) {
	if t.Weekday() == time.Friday {
		return t, nil
	}
	diff := -1 * (t.Weekday() + 2)
	prevFriday := t.AddDate(0, 0, int(diff))
	return prevFriday, nil
}

/* 
	Returns a list of security master files for a given reorg file. File names start from last Friday (the latest complete snapshot) 
	and includes all incremental updates till we reach the date encoded in the reorg filename
*/

func CollectSirsFilesFor(sirsFile string) ([]string, error) {
	basePath, fileName := filepath.Split(sirsFile)
	// getting the date of the reorg file from the filename
	currentDateStr := filepath.Ext(fileName)
	currentDate, err := time.Parse("20060102", currentDateStr[1:])
	if err != nil {
		log.Fatal("Unable to parse date from the reorg filename: %s", fileName)
		return []string{}, err
	}
	// ICE releases a full snapshot of security master information on each Friday in sirs.refresh files. 
	begin, _ := PreviousFriday(currentDate)
	masterfile := filepath.Join(basePath, "sirs.refresh." + begin.Format("20060102"))
	if !exists(masterfile) {
		// no master file, no chocolate
		log.Error("Master file not found: ", masterfile)
		return []string{}, nil
	}

	filenames := make([]string, 0)
	filenames = append(filenames, masterfile)
	// begining from last friday we find each incremental update, and add them to the list. 
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

/*
Loads a single Security info file and returns it's entries as a Cusip indexed map
*/

func LoadSirsFile(fileName string) ([]*models.SecurityMaster, error) {
	records := []*models.SecurityMaster{}
	file, err := os.Open(fileName)
	defer file.Close()
	if err == nil {
		records, err = Load(file) 
	} 
	return records, nil

}

/*
Loads the listed security files and returns a map of cusipid -> symbol pairs. 
The first element of the input slice should be a complete snapshot (sirs.refresh.YYYYMMDD), and the following entries updates for this file (sirs.YYYYMMDD)
*/

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



/* 
	Utility function to help out go's incedible standard library...
*/
func exists(filename string ) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}
