package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"code.cloudfoundry.org/bytefmt"

	"github.com/alpacahq/slait/commitlog"
	"github.com/alpacahq/slait/utils"
	"github.com/alpacahq/slait/utils/log"
	"github.com/eapache/channels"
)

var masterCache Cache
var cacheStructure = make(map[string]map[string]uint64)

const (
	AddPartition = iota
	RemovePartition
	ClearPartition
)

type Cache struct {
	topics     *sync.Map
	LastCommit CacheCommit
	dataDir    string
	router     Router
}

type Topic struct {
	partitions *sync.Map
}

type Entries []*Entry

func (e Entries) Len() int           { return len(e) }
func (e Entries) Less(i, j int) bool { return e[i].Timestamp.Before(e[j].Timestamp) }
func (e Entries) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

type Partition struct {
	entries Entries
	mu      sync.RWMutex
	clog    *commitlog.CommitLog
}

type Entry struct {
	Timestamp time.Time
	Data      json.RawMessage
}

// slice searches entries in the partition qualified by from and to
func (p *Partition) slice(from, to *time.Time) Entries {
	// take a snapshot to avoid concurrent modification (a slice is immutable)
	p.mu.RLock()
	entries := p.entries
	p.mu.RUnlock()

	start := 0
	end := len(entries)

	if from != nil {
		start = sort.Search(len(entries), func(i int) bool {
			return entries[i].Timestamp.After(*from) || entries[i].Timestamp.Equal(*from)
		})
	}
	if to != nil {
		end = sort.Search(len(entries), func(i int) bool {
			return entries[i].Timestamp.After(*to)
		})
	}
	if end < start {
		// should return empty slice?
		return nil
	}
	return entries[start:end]
}

// clear clears the content of partition, both on-disk and memory
func (p *Partition) clear() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.clog.DeleteAll()
	p.entries = Entries{}
	return err
}

// newPartition creates a new Partition without data in it
func (c *Cache) newPartition(topic, key string) (*Partition, error) {
	duration := "120h"
	for _, plan := range utils.GlobalConfig.TrimConfig {
		re := regexp.MustCompile(plan.TopicMatch)
		match := re.FindStringSubmatch(topic)
		if len(match) != 0 {
			duration = plan.Duration
			break
		}
	}
	clog, err := commitlog.New(commitlog.Options{
		Path: filepath.Join(c.dataDir, topic, key),
		CleanerOptions: commitlog.CleanerOptions{
			"Name":     "Duration",
			"Duration": duration,
		},
	})
	if err != nil {
		return nil, err
	}

	return &Partition{
		entries: Entries{},
		clog:    clog,
	}, err
}

func (c *Cache) get(topic, key string, from, to *time.Time, last int) (entries Entries) {
	t, ok := c.topics.Load(topic)
	if !ok {
		return nil
	}
	top := t.(*Topic)
	p, ok := top.partitions.Load(key)
	if !ok {
		return nil
	}
	partition := p.(*Partition)
	entries = partition.slice(from, to)
	if last > 0 && len(entries) >= last {
		return entries[len(entries)-last:]
	} else {
		return entries
	}
}

func (c *Cache) getAll(topic string, from, to *time.Time, last int) map[string]Entries {
	data := make(map[string]Entries)
	t, ok := c.topics.Load(topic)
	if !ok {
		return nil
	}
	top := t.(*Topic)
	top.partitions.Range(
		func(key, value interface{}) bool {
			p := value.(*Partition)

			entries := p.slice(from, to)
			if entries == nil {
				return true
			}
			if last > 0 {
				data[key.(string)] = entries[len(entries)-last:]
			} else {
				data[key.(string)] = entries
			}
			return true
		})
	return data
}

// appendEntries appends entries to the end of partition entries.  Returns error
// if entries are not ordered in ascending order (duplicate timestamps are allowed).
// If an entry is missing its timestamp, it is filled here.  Note that this operation
// is atomic and if one of the entries fail to append, no entries are appended.
func (c *Cache) appendEntries(topic, key string, entries Entries, new bool) error {
	t, ok := c.topics.Load(topic)
	if !ok {
		return errors.New("Topic does not exist")
	}
	top := t.(*Topic)

	p, ok := top.partitions.Load(key)
	if !ok {
		newPart, err := c.newPartition(topic, key)
		if err != nil {
			return err
		}
		top.partitions.Store(key, newPart)
		p = newPart
		c.router.Update(topic, key, AddPartition)
	}
	partition := p.(*Partition)

	partition.mu.Lock()
	defer partition.mu.Unlock()
	fpos := partition.clog.Tell()

	var (
		firstAppend *int
		lastEntry   *Entry
		lastTime    time.Time
	)
	if len(partition.entries) > 0 {
		lastEntry = partition.entries[len(partition.entries)-1]
		lastTime = lastEntry.Timestamp
	}

	for i, entry := range entries {
		if entry.Timestamp.IsZero() {
			// maybe we want to error out in some cases in the future.
			entry.Timestamp = time.Now()
			if entry.Timestamp.Equal(lastTime) {
				// make sure it is unique (in some platform like play.golang.org,
				// time.Now() is second-precision)
				entry.Timestamp = entry.Timestamp.Add(time.Duration(1))
			}
			lastTime = entry.Timestamp
		}

		if i > 0 {
			lastEntry = entries[i-1]
		}
		// the behavior is to discard the entries that are before the latest
		// entry in the partition. if other entries in the request are after
		// though, they are still appended.
		if lastEntry != nil && entry.Timestamp.Before(lastEntry.Timestamp) {
			continue
		}
		if firstAppend == nil {
			tmp := i
			firstAppend = &tmp
		}
		if new {
			if err := partition.clog.Append(&commitlog.Entry{
				Timestamp: entry.Timestamp,
				Data:      entry.Data}); err != nil {
				log.Error("Failed to persist %v: %v", entry, err)
				partition.clog.Truncate(fpos)
				return err
			}
		}
	}
	if firstAppend != nil {
		partition.entries = append(partition.entries, entries[*firstAppend:]...)
	} else {
		return errors.New("Nothing new to append")
	}

	c.LastCommit = CacheCommit{
		Key:       fmt.Sprintf("%v_%v", topic, key),
		Timestamp: entries[entries.Len()-1].Timestamp,
	}
	return nil
}

func (c *Cache) addTopic(topic string) error {
	if _, ok := c.topics.Load(topic); !ok {
		c.topics.Store(topic, &Topic{partitions: &sync.Map{}})
		return nil
	} else {
		return errors.New("Topic already exists")
	}
}

func (c *Cache) removeTopic(topic string) {
	c.topics.Delete(topic)
}

func (c *Cache) updateTopic(topic, key string, action int) error {
	t, ok := c.topics.Load(topic)
	if !ok {
		return errors.New("Topic does not exist")
	}
	top := t.(*Topic)
	p, ok := top.partitions.Load(key)
	switch action {
	case AddPartition:
		if ok {
			return errors.New("Partition already exists")
		}
		partition, err := c.newPartition(topic, key)
		if err != nil {
			return err
		}
		top.partitions.Store(key, partition)
	case RemovePartition:
		if !ok {
			return errors.New("Partition does not exist")
		}
		partition := p.(*Partition)
		partition.clear()
		top.partitions.Delete(key)
	case ClearPartition:
		if !ok {
			return errors.New("Partition does not exist")
		}
		partition := p.(*Partition)
		partition.clear()
	default:
		return errors.New("Invalid update action")
	}
	c.topics.Store(topic, top)
	return nil
}

func (c *Cache) trimTopic(topic string) {
	if t, ok := c.topics.Load(topic); !ok {
		return
	} else {
		top := t.(*Topic)
		top.partitions.Range(func(key, value interface{}) bool {
			p := value.(*Partition)

			// make sure nobody modifies entries
			p.mu.Lock()
			defer p.mu.Unlock()

			upto, err := p.clog.Trim()
			if err != nil {
				log.Error("Error while trimming %v: %v", topic, err)
				return true
			}
			if upto.IsZero() {
				return true
			}
			start := sort.Search(len(p.entries), func(i int) bool {
				return p.entries[i].Timestamp.After(upto) || p.entries[i].Timestamp.Equal(upto)
			})
			// free the memory
			e := make(Entries, len(p.entries[start:]))
			copy(e, p.entries[start:])
			p.entries = e
			return true
		})
	}
}

func Build(dataDir string) {
	r := Router{
		pub:    channels.NewInfiniteChannel(),
		add:    make(chan *Publication, 100),
		remove: make(chan *Publication, 100),
	}
	masterCache = Cache{
		topics:  &sync.Map{},
		dataDir: dataDir,
		router:  r,
	}
}

func Catalog() map[string]map[string]int {
	catalog := make(map[string]map[string]int)
	var currentTopic string
	masterCache.topics.Range(
		func(key, value interface{}) bool {
			currentTopic = key.(string)
			catalog[currentTopic] = make(map[string]int)
			t := value.(*Topic)
			t.partitions.Range(
				func(key, value interface{}) bool {
					p := value.(*Partition)
					catalog[currentTopic][key.(string)] = p.entries.Len()
					return true
				})
			return true
		})
	return catalog
}

func Get(topic, key string, from, to *time.Time, last int) (entries Entries) {
	return masterCache.get(topic, key, from, to, last)
}

func GetAll(topic string, from, to *time.Time, last int) map[string]Entries {
	return masterCache.getAll(topic, from, to, last)
}

func Append(topic, partition string, entries Entries) (err error) {
	err = masterCache.appendEntries(topic, partition, entries, true)
	if err == nil {
		masterCache.router.Publish(topic, partition, entries)
	}
	return err
}

func Add(topic string) (err error) {
	err = masterCache.addTopic(topic)
	if err == nil {
		masterCache.router.Add(topic)
	}
	return err
}

func Remove(topic string) {
	masterCache.removeTopic(topic)
	masterCache.router.Remove(topic)
}

func Update(topic, partition string, action int) (err error) {
	err = masterCache.updateTopic(topic, partition, action)
	if err == nil {
		masterCache.router.Update(topic, partition, action)
	}
	return err
}

func Trim() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memStart := m.Alloc
	start := time.Now()
	log.Info("Trimming cache...")
	f := func(key, value interface{}) bool {
		masterCache.trimTopic(key.(string))
		return true
	}
	masterCache.topics.Range(f)
	debug.FreeOSMemory()
	runtime.ReadMemStats(&m)
	memEnd := m.Alloc
	log.Info("Cache trimmed in %v", time.Now().Sub(start))
	log.Info(
		"Trim stats | MemStart: %v MemEnd: %v MemFreed: %v",
		bytefmt.ByteSize(memStart),
		bytefmt.ByteSize(memEnd),
		bytefmt.ByteSize(memStart-memEnd),
	)
}

func Fill() error {
	return masterCache.fill()
}

type CacheCommit struct {
	Timestamp time.Time
	Key       string
}

func LastCommit() CacheCommit {
	return masterCache.LastCommit
}

// TODO: implement cache size calculation
func Size() (size int) {
	return size
}
