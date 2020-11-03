
package adjust

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
	ExpirationDates []int64
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
		ExpirationDates: []int64{},
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

func (act *Actions) Load() error {
	query := planner.NewQuery(executor.ThisInstance.CatalogDir)
	tbk := io.NewTimeBucketKeyFromString(act.Cusip + bucketkeySuffix)
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
		// probably no files returned from query phase
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
	act.FromColumnSeries(csm[*tbk])
	return nil
}

func (act *Actions) FromColumnSeries(cs *io.ColumnSeries) {
	act.Rows.EntryDates = cs.GetColumn("Epoch").([]int64)
	act.Rows.TextNumbers = cs.GetColumn("TextNumber").([]int64)
	act.Rows.UpdateTextNumbers = cs.GetColumn("UpdateTextNumber").([]int64)
	act.Rows.DeleteTextNumbers = cs.GetColumn("DeleteTextNumber").([]int64)
	act.Rows.NotificationTypes = cs.GetColumn("NotificationType").([]byte)
	act.Rows.Statuses = cs.GetColumn("Status").([]byte)
	act.Rows.SecurityTypes = cs.GetColumn("SecurityType").([]byte)
	act.Rows.RecordDates = cs.GetColumn("RecordDate").([]int64)
	act.Rows.EffectiveDates = cs.GetColumn("EffectiveDate").([]int64)
	act.Rows.ExpirationDates = cs.GetColumn("ExpirationDate").([]int64)
	act.Rows.NewRates = cs.GetColumn("NewRate").([]float64)
	act.Rows.OldRates = cs.GetColumn("OldRate").([]float64)
	act.Rows.Rates = cs.GetColumn("Rate").([]float64)
}

func (act *Actions) Len() int {
	return len(act.Rows.EntryDates)
}

func (act *Actions) RateChangeEvents(includeSplits, includeDividends bool) []RateChange {
	if act.Len() == 0 {
		return []RateChange{}
	}
	ca_map := map[int64]int{}
	for i:=0; i<act.Len(); i++ {
		var textnumber int64 
		status := act.Rows.Statuses[i]
		switch status {
		case NewRecord:
			textnumber = act.Rows.TextNumbers[i]
			ca_map[textnumber] = i
		case UpdateRecord:
			textnumber = act.Rows.UpdateTextNumbers[i]
			prev, present := ca_map[textnumber] 
			// being extremely paranoid here, allow updates for newer records only
			if present {
				if act.Rows.EntryDates[i] > act.Rows.EntryDates[prev] {
					ca_map[textnumber] = i
				}
			} else {
				// sometimes notifications start with an update, so just keep it 
				ca_map[textnumber] = i
			}
		case DeleteRecord:
			textnumber = act.Rows.DeleteTextNumbers[i]
			delete(ca_map, textnumber)
		}
		// log.Info("ID: %d, date: %+v, status: %d, rate: %+v\n", act.Rows.TextNumbers[i], act.Rows.entrydates[i], act.Rows.Statuses[i], ca.Rows.Rates[i])
	}
	action_index := []int{}
	for _, index := range ca_map {
		action_index = append(action_index, index)
	} 
	sort.Slice(action_index, func(i, j int) bool { return act.Rows.EffectiveDates[i] < act.Rows.EffectiveDates[j] })
	changes := make([]RateChange, 0, len(action_index)+1)
	for _, index := range action_index {
		// use Expiration date 
		if includeSplits && (act.Rows.NotificationTypes[index] == Split || act.Rows.NotificationTypes[index] == ReverseSplit) {
			changes = append(changes, RateChange{Epoch: act.Rows.ExpirationDates[index], Rate: act.Rows.Rates[index], Textnumber: act.Rows.TextNumbers[index], Type: act.Rows.NotificationTypes[index]})
		}
		if includeDividends && (act.Rows.NotificationTypes[index] == Dividend) {
			changes = append(changes, RateChange{Epoch: act.Rows.ExpirationDates[index], Rate: act.Rows.Rates[index], Textnumber: act.Rows.TextNumbers[index], Type: act.Rows.NotificationTypes[index]})
		}
	}
	changes = append(changes, RateChange{Epoch: math.MaxInt64, Rate: 1, Textnumber: 0, Type: 0})
	return changes
}
