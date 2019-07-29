package cache

import (
	"io/ioutil"
	"path/filepath"

	"github.com/alpacahq/slait/commitlog"
	"github.com/alpacahq/slait/utils/log"
)

func (c *Cache) fill() error {
	rootDir := c.dataDir
	finfos, err := ioutil.ReadDir(rootDir)
	if err != nil {
		return err
	}
	for _, finfo := range finfos {
		tname := finfo.Name()
		c.addTopic(tname)
		if err := c.fillTopic(tname, filepath.Join(rootDir, tname)); err != nil {
			log.Error("%v", err)
		}
	}
	return nil
}

func (c *Cache) fillTopic(tname, topicDir string) error {
	finfos, err := ioutil.ReadDir(topicDir)
	if err != nil {
		return err
	}
	for _, finfo := range finfos {
		pname := finfo.Name()
		c.updateTopic(tname, pname, AddPartition)
		if err := c.fillPartition(tname, pname, filepath.Join(topicDir, pname)); err != nil {
			log.Error("failed to fill partition: %v (%s/%s)", err, tname, pname)
		}
	}
	return nil
}

func (c *Cache) fillPartition(tname, pname, path string) error {
	reader, err := commitlog.NewReader(path)
	if err != nil {
		return err
	}
	defer reader.Close()
	entries := Entries{}
	for {
		entry, err := reader.Read()
		if entry == nil && err == nil {
			break
		} else if err != nil {
			return err
		}
		entries = append(entries, &Entry{
			entry.Timestamp, entry.Data,
		})
	}
	if len(entries) > 0 {
		return c.appendEntries(tname, pname, entries, false)
	} else {
		return nil
	}
}
