package adjust

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type CARows struct {
	EntryDates               []int64
	TextNumbers              []int64
	UpdateTextNumbers        []int64
	DeleteTextNumbers        []int64
	NotificationTypes        []byte
	Statuses                 []byte
	UpdatedNotificationTypes []byte
	SecurityTypes            []byte
	VoluntaryMandatoryCodes  []byte
	EffectiveDates           []int64
	RecordDates              []int64
	ExpirationDates          []int64
	NewRates                 []float64
	OldRates                 []float64
	Rates                    []float64
}

func NewCARows(length int) *CARows {
	return &CARows{
		EntryDates:               make([]int64, length),
		TextNumbers:              make([]int64, length),
		UpdateTextNumbers:        make([]int64, length),
		DeleteTextNumbers:        make([]int64, length),
		NotificationTypes:        make([]byte, length),
		Statuses:                 make([]byte, length),
		UpdatedNotificationTypes: make([]byte, length),
		SecurityTypes:            make([]byte, length),
		VoluntaryMandatoryCodes:  make([]byte, length),
		RecordDates:              make([]int64, length),
		EffectiveDates:           make([]int64, length),
		ExpirationDates:          make([]int64, length),
		NewRates:                 make([]float64, length),
		OldRates:                 make([]float64, length),
		Rates:                    make([]float64, length),
	}
}

type RateChange struct {
	Textnumber int64
	Epoch      int64
	Type       enum.NotificationType
	Rate       float64
}

type Actions struct {
	Symbol string
	Tbk    *io.TimeBucketKey
	Rows   *CARows
}

type RateChangeCache struct {
	Changes   []RateChange
	Access    int64
	CreatedAt time.Time
}

type CacheKey struct {
	Symbol    string
	Splits    bool
	Dividends bool
}

type RateChangeGetter func(string, bool, bool) []RateChange

const CacheLifetime = 24 * time.Hour

var rateChangeCache = map[CacheKey]RateChangeCache{}

func GetRateChanges(symbol string, includeSplits, includeDividends bool,
	catalogDir *catalog.Directory,
) []RateChange {
	key := CacheKey{Symbol: symbol, Splits: includeSplits, Dividends: includeDividends}
	rateCache, present := rateChangeCache[key]
	if present && time.Since(rateCache.CreatedAt) > CacheLifetime {
		present = false
	}
	if !present {
		ca := NewCorporateActions(symbol)
		err := ca.Load(catalogDir)
		if err != nil {
			log.Error("load corporate actions from catalog: %v", err)
		}
		rateCache = RateChangeCache{
			Changes:   ca.RateChangeEvents(includeSplits, includeDividends),
			Access:    0,
			CreatedAt: time.Now(),
		}
		rateChangeCache[key] = rateCache
	}
	return rateCache.Changes
}

func NewCorporateActions(symbol string) *Actions {
	return &Actions{
		Symbol: symbol,
		Tbk:    io.NewTimeBucketKeyFromString(symbol + enum.BucketkeySuffix),
		Rows:   NewCARows(0),
	}
}

func (act *Actions) Load(catalogDir *catalog.Directory) error {
	if executor.ThisInstance == nil || catalogDir == nil {
		return nil
	}
	query := planner.NewQuery(catalogDir)
	tbk := io.NewTimeBucketKeyFromString(act.Symbol + enum.BucketkeySuffix)
	tf := tbk.GetItemInCategory("Timeframe")
	cd, err := utils.CandleDurationFromString(tf)
	if err != nil {
		return fmt.Errorf("timeframe is not found in %s: %w", tf, err)
	}
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
		if err.Error() == "no files returned from query parse" {
			return nil
		}
		log.Error("Unable to create parser: %s", err)
		return err
	}
	scanner, err := executor.NewReader(parseResult)
	if err != nil {
		log.Error("Unable to create scanner: %s", err)
		return err
	}
	csm, err := scanner.Read()
	if err != nil {
		log.Error("Error returned from query scanner: %s", err)
		return err
	}

	if err2 := act.fromColumnSeries(csm[*tbk]); err2 != nil {
		return err2
	}

	return nil
}

func (act *Actions) fromColumnSeries(cs *io.ColumnSeries) error {
	var ok1, ok2, ok3, ok4, ok5, ok6, ok7, ok8, ok9, ok10, ok11, ok12, ok13, ok14, ok15 bool
	act.Rows.EntryDates, ok1 = cs.GetColumn("Epoch").([]int64)
	act.Rows.TextNumbers, ok2 = cs.GetColumn("TextNumber").([]int64)
	act.Rows.UpdateTextNumbers, ok3 = cs.GetColumn("UpdateTextNumber").([]int64)
	act.Rows.DeleteTextNumbers, ok4 = cs.GetColumn("DeleteTextNumber").([]int64)
	act.Rows.NotificationTypes, ok5 = cs.GetColumn("NotificationType").([]byte)
	act.Rows.Statuses, ok6 = cs.GetColumn("Status").([]byte)
	act.Rows.UpdatedNotificationTypes, ok7 = cs.GetColumn("UpdatedNotificationType").([]byte)
	act.Rows.SecurityTypes, ok8 = cs.GetColumn("SecurityType").([]byte)
	act.Rows.VoluntaryMandatoryCodes, ok9 = cs.GetColumn("VoluntaryMandatoryCode").([]byte)
	act.Rows.RecordDates, ok10 = cs.GetColumn("RecordDate").([]int64)
	act.Rows.EffectiveDates, ok11 = cs.GetColumn("EffectiveDate").([]int64)
	act.Rows.ExpirationDates, ok12 = cs.GetColumn("ExpirationDate").([]int64)
	act.Rows.NewRates, ok13 = cs.GetColumn("NewRate").([]float64)
	act.Rows.OldRates, ok14 = cs.GetColumn("OldRate").([]float64)
	act.Rows.Rates, ok15 = cs.GetColumn("Rate").([]float64)
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7 && ok8 && ok9 && ok10 && ok11 && ok12 && ok13 && ok14 && ok15) {
		return fmt.Errorf("cast a column series to a corporate action object: %v", cs)
	}
	return nil
}

func (act *Actions) Len() int {
	return len(act.Rows.EntryDates)
}

func (act *Actions) getEffectiveActionsIndex() []int {
	caMap := map[int64]int{}
	for i := 0; i < act.Len(); i++ {
		var textnumber int64
		status := enum.StatusCode(act.Rows.Statuses[i])
		switch status {
		case enum.NewAnnouncement:
			textnumber = act.Rows.TextNumbers[i]
			caMap[textnumber] = i
		case enum.UpdatedAnnouncement:
			textnumber = act.Rows.UpdateTextNumbers[i]
			prev, present := caMap[textnumber]
			// being extremely paranoid here, allow updates for newer records only
			if present {
				if act.Rows.EntryDates[i] > act.Rows.EntryDates[prev] {
					caMap[textnumber] = i
				}
			} else {
				// sometimes notifications start with an update, so just keep it
				caMap[textnumber] = i
			}
		case enum.DeletedAnnouncement:
			textnumber = act.Rows.DeleteTextNumbers[i]
			delete(caMap, textnumber)
		}
		// log.Info("ID: %d, date: %+v, status: %d, rate: %+v\n",
		//   act.Rows.TextNumbers[i], act.Rows.entrydates[i], act.Rows.Statuses[i], ca.Rows.Rates[i])
	}
	actionIndex := make([]int, 0, len(caMap))
	for _, index := range caMap {
		actionIndex = append(actionIndex, index)
	}
	sortByDate := func(i, j int) bool {
		return act.Rows.ExpirationDates[actionIndex[i]] <= act.Rows.ExpirationDates[actionIndex[j]]
	}
	sort.SliceStable(actionIndex, sortByDate)
	return actionIndex
}

func (act *Actions) RateChangeEvents(includeSplits, includeDividends bool) []RateChange {
	if act.Len() == 0 {
		return []RateChange{}
	}
	actionIndex := act.getEffectiveActionsIndex()

	changes := make([]RateChange, 0, len(actionIndex))
	for _, index := range actionIndex {
		notificationType := enum.NotificationType(act.Rows.NotificationTypes[index])
		// use Expiration date
		if includeSplits && (notificationType == enum.StockSplit || notificationType == enum.ReverseStockSplit) {
			changes = append(changes,
				RateChange{
					Epoch:      act.Rows.ExpirationDates[index],
					Rate:       act.Rows.Rates[index],
					Textnumber: act.Rows.TextNumbers[index],
					Type:       notificationType,
				})
		}
		if includeDividends && notificationType == enum.StockDividend {
			changes = append(changes,
				RateChange{
					Epoch:      act.Rows.ExpirationDates[index],
					Rate:       act.Rows.Rates[index],
					Textnumber: act.Rows.TextNumbers[index],
					Type:       notificationType,
				})
		}
	}
	return changes
}
