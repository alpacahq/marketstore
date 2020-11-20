package adjust

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"
	"github.com/alpacahq/marketstore/v4/contrib/ice/reorg"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func announcementsToColumnSeries(announcements []reorg.Announcement) *io.ColumnSeries {
	length := len(announcements)
	rows := NewCARows(length)
	for i, announcement := range announcements {
		rows.EntryDates[i] = announcement.EntryDate.Unix()
		rows.TextNumbers[i] = announcement.TextNumber
		rows.UpdateTextNumbers[i] = announcement.UpdateTextNumber
		rows.DeleteTextNumbers[i] = announcement.DeleteTextNumber
		if len(announcement.NotificationType) > 0 {
			rows.NotificationTypes[i] = announcement.NotificationType[0]
		} else {
			rows.NotificationTypes[i] = 0
		}
		if len(announcement.Status) > 0 {
			rows.Statuses[i] = announcement.Status[0]
		} else {
			rows.Statuses[i] = 0
		}
		if len(announcement.SecurityType) > 0 {
			rows.SecurityTypes[i] = announcement.SecurityType[0]
		} else {
			rows.SecurityTypes[i] = 0
		}
		rows.RecordDates[i] = announcement.RecordDate.Unix()
		rows.EffectiveDates[i] = announcement.EffectiveDate.Unix()
		rows.ExpirationDates[i] = announcement.ExpirationDate.Unix()
		rows.NewRates[i] = announcement.NewRate
		rows.OldRates[i] = announcement.OldRate
		rows.Rates[i] = announcement.Rate
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", rows.EntryDates)
	cs.AddColumn("TextNumber", rows.TextNumbers)
	cs.AddColumn("UpdateTextNumber", rows.UpdateTextNumbers)
	cs.AddColumn("DeleteTextNumber", rows.DeleteTextNumbers)
	cs.AddColumn("NotificationType", rows.NotificationTypes)
	cs.AddColumn("Status", rows.Statuses)
	cs.AddColumn("SecurityType", rows.SecurityTypes)
	cs.AddColumn("RecordDate", rows.RecordDates)
	cs.AddColumn("EffectiveDate", rows.EffectiveDates)
	cs.AddColumn("ExpirationDate", rows.ExpirationDates)
	cs.AddColumn("NewRate", rows.NewRates)
	cs.AddColumn("OldRate", rows.OldRates)
	cs.AddColumn("Rate", rows.Rates)
	return cs
}

func announcement(textnumber int, entrydate, expdate time.Time, notificationtype, status byte, rate float64) reorg.Announcement {
	return reorg.Announcement{
		TextNumber:       int64(textnumber),
		EntryDate:        entrydate,
		ExpirationDate:   expdate,
		NotificationType: string(notificationtype),
		Status:           string(status),
		SecurityType:     string(enum.CommonStock),
		Rate:             rate,
	}
}

func date(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func unixDate(year int, month time.Month, day int) int64 {
	return date(year, month, day).Unix()
}

func defineCorporateActions(announcements ...reorg.Announcement) *Actions {
	cs := announcementsToColumnSeries(announcements)
	ca := NewCorporateActions("AAPL")
	ca.FromColumnSeries(cs)
	return ca
}

type Params = struct {
	dividends bool
	splits    bool
}

var filteringFixtures = []struct {
	description string
	in          []reorg.Announcement
	params      Params
	out         []RateChange
}{
	{
		description: `returns empty RateChange list when there's no announcements to process`,
		in:          []reorg.Announcement{},
		params: Params{
			dividends: true,
			splits:    true,
		},
		out: []RateChange{},
	},

	{
		description: `returns an empty list when includeSplits and includeDividends are false`,
		in: []reorg.Announcement{
			{
				TextNumber:       1111,
				EntryDate:        date(2019, time.July, 6),
				ExpirationDate:   date(2019, time.July, 8),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.095,
			},
			{
				TextNumber:       2222,
				EntryDate:        date(2020, time.March, 15),
				ExpirationDate:   date(2020, time.March, 19),
				NotificationType: string(enum.StockSplit),
				Status:           string(enum.NewAnnouncement),
				Rate:             3,
			},
			{
				TextNumber:       3333,
				EntryDate:        date(2020, time.June, 28),
				ExpirationDate:   date(2020, time.June, 30),
				NotificationType: string(enum.ReverseStockSplit),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.4,
			},
		},
		params: Params{
			dividends: false,
			splits:    false,
		},
		out: []RateChange{},
	},

	{
		description: `returns only Dividend type RateChanges when includeSplits is false and includeDividends is true`,
		in: []reorg.Announcement{
			{
				TextNumber:       1111,
				EntryDate:        date(2019, time.July, 6),
				ExpirationDate:   date(2019, time.July, 8),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.095,
			},
			{
				TextNumber:       2222,
				EntryDate:        date(2020, time.March, 15),
				ExpirationDate:   date(2020, time.March, 19),
				NotificationType: string(enum.StockSplit),
				Status:           string(enum.NewAnnouncement),
				Rate:             3,
			},
			{
				TextNumber:       3333,
				EntryDate:        date(2020, time.June, 28),
				ExpirationDate:   date(2020, time.June, 30),
				NotificationType: string(enum.ReverseStockSplit),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.4,
			},
		},
		params: Params{
			dividends: true,
			splits:    false,
		},
		out: []RateChange{
			{1111, unixDate(2019, time.July, 8), enum.StockDividend, 0.095},
		},
	},

	{
		description: `returns only Split type RateChanges when includeSplits is true and includeDividends is false`,
		in: []reorg.Announcement{
			{
				TextNumber:       1111,
				EntryDate:        date(2019, time.July, 6),
				ExpirationDate:   date(2019, time.July, 8),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.095,
			},
			{
				TextNumber:       2222,
				EntryDate:        date(2020, time.March, 15),
				ExpirationDate:   date(2020, time.March, 19),
				NotificationType: string(enum.StockSplit),
				Status:           string(enum.NewAnnouncement),
				Rate:             3,
			},
			{
				TextNumber:       3333,
				EntryDate:        date(2020, time.June, 28),
				ExpirationDate:   date(2020, time.June, 30),
				NotificationType: string(enum.ReverseStockSplit),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.4,
			},
		},
		params: Params{
			dividends: false,
			splits:    true,
		},
		out: []RateChange{
			{2222, unixDate(2020, time.March, 19), enum.StockSplit, 3},
			{3333, unixDate(2020, time.June, 30), enum.ReverseStockSplit, 0.4},
		},
	},

	{
		description: `returns both Split and Dividend type RateChanges when both includeSplits and includeDividends args are true`,
		in: []reorg.Announcement{
			{
				TextNumber:       1111,
				EntryDate:        date(2019, time.July, 6),
				ExpirationDate:   date(2019, time.July, 8),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.095,
			},
			{
				TextNumber:       2222,
				EntryDate:        date(2020, time.March, 15),
				ExpirationDate:   date(2020, time.March, 19),
				NotificationType: string(enum.StockSplit),
				Status:           string(enum.NewAnnouncement),
				Rate:             3,
			},
			{
				TextNumber:       3333,
				EntryDate:        date(2020, time.June, 28),
				ExpirationDate:   date(2020, time.June, 30),
				NotificationType: string(enum.ReverseStockSplit),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.4,
			},
		},
		params: Params{
			dividends: true,
			splits:    true,
		},
		out: []RateChange{
			{1111, unixDate(2019, time.July, 8), enum.StockDividend, 0.095},
			{2222, unixDate(2020, time.March, 19), enum.StockSplit, 3},
			{3333, unixDate(2020, time.June, 30), enum.ReverseStockSplit, 0.4},
		},
	},
}

func (s *TestSuite) TestRateChangeEventsFiltering(c *C) {
	for _, tt := range filteringFixtures {
		ca := defineCorporateActions(tt.in...)
		events := ca.RateChangeEvents(tt.params.splits, tt.params.dividends)
		c.Assert(events, DeepEquals, tt.out, Commentf("FAILED: %s, %+v\n", tt.description, tt.params))
	}
}

var statusHandlingFixtures = []struct {
	description string
	in          []reorg.Announcement
	params      Params
	out         []RateChange
}{
	{
		description: `if an Update is present, it should return the Update instead of the New one`,
		in: []reorg.Announcement{
			{
				TextNumber:       1111,
				EntryDate:        date(2019, time.July, 6),
				ExpirationDate:   date(2019, time.July, 8),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.095,
			},
			{
				TextNumber:       2222,
				UpdateTextNumber: 1111,
				EntryDate:        date(2019, time.July, 7),
				ExpirationDate:   date(2019, time.July, 10),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.UpdatedAnnouncement),
				Rate:             0.098,
			},
		},
		params: Params{
			dividends: true,
			splits:    true,
		},
		out: []RateChange{
			{2222, unixDate(2019, time.July, 10), enum.StockDividend, 0.098},
		},
	},

	{
		description: `if an Update is present, it should return the latest Update only`,
		in: []reorg.Announcement{
			{
				TextNumber:       1111,
				EntryDate:        date(2019, time.July, 6),
				ExpirationDate:   date(2019, time.July, 8),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.095,
			},
			{
				TextNumber:       2222,
				UpdateTextNumber: 1111,
				EntryDate:        date(2019, time.July, 7),
				ExpirationDate:   date(2019, time.July, 10),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.UpdatedAnnouncement),
				Rate:             0.098,
			},
			{
				TextNumber:       3333,
				UpdateTextNumber: 1111,
				EntryDate:        date(2019, time.July, 9),
				ExpirationDate:   date(2019, time.July, 15),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.UpdatedAnnouncement),
				Rate:             0.099,
			},
		},
		params: Params{
			dividends: true,
			splits:    true,
		},
		out: []RateChange{
			{3333, unixDate(2019, time.July, 15), enum.StockDividend, 0.099},
		},
	},

	{
		description: `if a Deleted announcement is present for a Textnumber, it should return an empty list`,
		in: []reorg.Announcement{
			{
				TextNumber:       1111,
				EntryDate:        date(2019, time.July, 6),
				ExpirationDate:   date(2019, time.July, 8),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.095,
			},
			{
				TextNumber:       2222,
				UpdateTextNumber: 1111,
				EntryDate:        date(2019, time.July, 7),
				ExpirationDate:   date(2019, time.July, 10),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.UpdatedAnnouncement),
				Rate:             0.098,
			},
			{
				TextNumber:       3333,
				UpdateTextNumber: 1111,
				EntryDate:        date(2019, time.July, 9),
				ExpirationDate:   date(2019, time.July, 15),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.UpdatedAnnouncement),
				Rate:             0.099,
			},
			{
				TextNumber:       4444,
				DeleteTextNumber: 1111,
				EntryDate:        date(2019, time.July, 9),
				ExpirationDate:   date(2019, time.July, 15),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.DeletedAnnouncement),
				Rate:             0.099,
			},
		},
		params: Params{
			dividends: true,
			splits:    true,
		},
		out: []RateChange{},
	},
}

func (s *TestSuite) TestRateChangeEventsAnnouncementStatusHandling(c *C) {
	for _, tt := range statusHandlingFixtures {
		ca := defineCorporateActions(tt.in...)
		events := ca.RateChangeEvents(tt.params.splits, tt.params.dividends)
		c.Assert(events, DeepEquals, tt.out, Commentf("FAILED: %s, %+v\n", tt.description, tt.params))
	}
}

var sortingFixtures = []struct {
	description string
	in          []reorg.Announcement
	params      Params
	out         []RateChange
}{
	{
		description: `returns a list of RateChanges ordered by ExpirationDate`,
		in: []reorg.Announcement{
			{
				TextNumber:       1111,
				EntryDate:        date(2019, time.July, 6),
				ExpirationDate:   date(2019, time.July, 9),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.095,
			},
			{
				TextNumber:       2222,
				EntryDate:        date(2019, time.July, 7),
				ExpirationDate:   date(2019, time.July, 7),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.098,
			},
			{
				TextNumber:       3333,
				EntryDate:        date(2019, time.July, 8),
				ExpirationDate:   date(2019, time.July, 12),
				NotificationType: string(enum.StockDividend),
				Status:           string(enum.NewAnnouncement),
				Rate:             0.098,
			},
		},
		params: Params{
			dividends: true,
			splits:    true,
		},
		out: []RateChange{
			{2222, unixDate(2019, time.July, 7), enum.StockDividend, 0.098},
			{1111, unixDate(2019, time.July, 9), enum.StockDividend, 0.095},
			{3333, unixDate(2019, time.July, 12), enum.StockDividend, 0.098},
		},
	},
}

func (s *TestSuite) TestRateChangeEventsProperSorting(c *C) {
	for _, tt := range sortingFixtures {
		ca := defineCorporateActions(tt.in...)
		events := ca.RateChangeEvents(tt.params.splits, tt.params.dividends)
		c.Assert(events, DeepEquals, tt.out, Commentf("FAILED: %s, %+v\n", tt.description, tt.params))
	}
}

func (s *TestSuite) TestCache(c *C) {
	{
		// GetRateChange should create a separate cache entry for each parameter combination
		rateChangeCache = map[CacheKey]RateChangeCache{}

		GetRateChanges("AAPL", true, true)
		GetRateChanges("AAPL", false, true)
		GetRateChanges("AAPL", true, false)
		GetRateChanges("AAPL", false, false)

		c.Assert(len(rateChangeCache), Equals, 4)
	}

	{
		// repeated calls with the same signature should not increase the number of cache entries
		rateChangeCache = map[CacheKey]RateChangeCache{}

		GetRateChanges("AAPL", true, true)
		GetRateChanges("AAPL", true, true)
		GetRateChanges("AAPL", true, true)

		c.Assert(len(rateChangeCache), Equals, 1)
	}

}
