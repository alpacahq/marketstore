
package reorg

import (
	"math"
	"time"
	"sort"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
)

const (
	NewRecord = 'N'
	UpdateRecord = 'U'
	DeleteRecord = 'D'
	Split = '7'
	ReverseSplit = '+'
	Dividend = '/'
)

type CARows struct {
	EntryDates []int64
	TextNumbers []int64
	UpdateTextNumbers []int64
	DeleteTextNumbers []int64
	NotificationTypes []byte
	Statuses []byte
	SecurityTypes []byte
	EffectiveDates []int64
	RecordDates []int64
	NewRates []float64
	OldRates []float64
	Rates []float64
}

func NewCARows() *CARows {
	return &CARows{
		EntryDates: []int64{},
		TextNumbers: []int64{},
		UpdateTextNumbers: []int64{},
		DeleteTextNumbers: []int64{},
		NotificationTypes: []byte{},
		Statuses: []byte{},
		SecurityTypes: []byte{},
		RecordDates: []int64{},
		EffectiveDates: []int64{},
		NewRates: []float64{},
		OldRates: []float64{},
		Rates: []float64{},
	}
}

type RateChange struct {
	Epoch int64
	Rate float64
	Textnumber int64
	Type byte
}

type Actions struct {
	Cusip string
	Tbk *io.TimeBucketKey
	Rows *CARows
}

type RateChangeCache struct {
	Changes []RateChange
	Access int64
	CreatedAt time.Time
}
type CacheKey struct {
	cusip string
	splits bool 
	dividends bool
}

const cacheLifetime = 24 * time.Hour
var cache = map[CacheKey]RateChangeCache{}

func GetRateChanges(cusip string, includeSplits, includeDividends bool) []RateChange {
	key := CacheKey{cusip: cusip, splits: includeSplits, dividends: includeDividends}
	rate_cache, present := cache[key]
	if present && time.Now().Sub(rate_cache.CreatedAt) > cacheLifetime  {
		present = false
	}
	if !present {
		ca := NewCorporateActions(cusip)
		ca.Load()
		rate_cache = RateChangeCache{
			Changes: ca.RateChangeEvents(includeSplits, includeDividends),
			Access: 0,
			CreatedAt: time.Now(),
		}
		cache[key] = rate_cache
	} 
	return rate_cache.Changes
}



func NewCorporateActions(cusip string) (*Actions) {
	return &Actions{
		Cusip: cusip, 
		Tbk: io.NewTimeBucketKeyFromString(cusip + bucketkeySuffix),
		Rows: NewCARows(),
	}
}

func (a *Actions) Load() error {
	query := planner.NewQuery(executor.ThisInstance.CatalogDir)
	tbk := io.NewTimeBucketKeyFromString(a.Cusip + bucketkeySuffix)
	tf := tbk.GetItemInCategory("Timeframe")
	cd := utils.CandleDurationFromString(tf)
	queryableTimeframe := cd.QueryableTimeframe()
	tbk.SetItemInCategory("Timeframe", queryableTimeframe)
	
	epochStart := int64(0)
	epochEnd := int64(math.MaxInt64)
	start := io.ToSystemTimezone(time.Unix(epochStart, 0))
	end := io.ToSystemTimezone(time.Unix(epochEnd, 0))

	query.AddTargetKey(tbk)
	query.SetRange(start, end)

	parseResult, err := query.Parse()
	if err != nil {
		// probably it is a no files returned from query phase
		// fmt.Printf("%+v\n", err)
		return nil
	}
	scanner, err := executor.NewReader(parseResult)
	if err != nil {
		log.Fatal("Unable to create scanner: %s\n", err)
		return err
	}
	csm, err := scanner.Read()
	if err != nil {
		log.Fatal("Error returned from query scanner: %s\n", err)
		return err
	}
	a.FromColumnSeries(csm[*tbk])
	return nil
}

func (a *Actions) FromColumnSeries(cs *io.ColumnSeries) {
	a.Rows.EntryDates = cs.GetColumn("Epoch").([]int64)
	a.Rows.TextNumbers = cs.GetColumn("TextNumber").([]int64)
	a.Rows.UpdateTextNumbers = cs.GetColumn("UpdateTextNumber").([]int64)
	a.Rows.DeleteTextNumbers = cs.GetColumn("DeleteTextNumber").([]int64)
	a.Rows.NotificationTypes = cs.GetColumn("NotificationType").([]byte)
	a.Rows.Statuses = cs.GetColumn("Status").([]byte)
	a.Rows.SecurityTypes = cs.GetColumn("SecurityType").([]byte)
	a.Rows.RecordDates = cs.GetColumn("RecordDate").([]int64)
	a.Rows.EffectiveDates = cs.GetColumn("EffectiveDate").([]int64)
	a.Rows.NewRates = cs.GetColumn("NewRate").([]float64)
	a.Rows.OldRates = cs.GetColumn("OldRate").([]float64)
	a.Rows.Rates = cs.GetColumn("Rate").([]float64)
}

func (a *Actions) Len() int {
	return len(a.Rows.EntryDates)
}

func (a *Actions) RateChangeEvents(includeSplits, includeDividends bool) []RateChange {
	if a.Len() == 0 {
		return []RateChange{}
	}
	ca_map := map[int64]int{}
	for i:=0; i<a.Len(); i++ {
		var textnumber int64 
		status := a.Rows.Statuses[i]
		switch status {
		case NewRecord:
			textnumber = a.Rows.TextNumbers[i]
			ca_map[textnumber] = i
		case UpdateRecord:
			textnumber = a.Rows.UpdateTextNumbers[i]
			prev, present := ca_map[textnumber] 
			// being extremely paranoid here, allow updates for newer records only
			if present {
				if a.Rows.EntryDates[i] > a.Rows.EntryDates[prev] {
					ca_map[textnumber] = i
				}
			} else {
				// sometimes notifications start with an update, so just keep it 
				ca_map[textnumber] = i
			}
		case DeleteRecord:
			textnumber = a.Rows.DeleteTextNumbers[i]
			delete(ca_map, textnumber)
		}
		// log.Info("ID: %d, date: %+v, status: %d, rate: %+v\n", a.Rows.TextNumbers[i], a.Rows.entrydates[i], a.Rows.Statuses[i], ca.Rows.Rates[i])
	}
	action_index := []int{}
	for _, index := range ca_map {
		action_index = append(action_index, index)
	} 
	sort.Slice(action_index, func(i, j int) bool { return a.Rows.EffectiveDates[i] < a.Rows.EffectiveDates[j] })
	changes := make([]RateChange, 0, len(action_index)+1)
	for _, index := range action_index {
		if includeSplits && (a.Rows.NotificationTypes[index] == Split || a.Rows.NotificationTypes[index] == ReverseSplit) {
			changes = append(changes, RateChange{Epoch: a.Rows.EffectiveDates[index], Rate: a.Rows.Rates[index], Textnumber: a.Rows.TextNumbers[index], Type: a.Rows.NotificationTypes[index]})
		}
		if includeDividends && (a.Rows.NotificationTypes[index] == Dividend) {
			changes = append(changes, RateChange{Epoch: a.Rows.EffectiveDates[index], Rate: a.Rows.Rates[index], Textnumber: a.Rows.TextNumbers[index], Type: a.Rows.NotificationTypes[index]})
		}
		//FIXME: when to use RecordDates and when EffectiveDates?????
		//lehet hogy a SecurityType ???
		// A = Effective
		// C = RecordDate?
	}
	changes = append(changes, RateChange{Epoch: math.MaxInt64, Rate: 1, Textnumber: 0, Type: 0})
	return changes
}
