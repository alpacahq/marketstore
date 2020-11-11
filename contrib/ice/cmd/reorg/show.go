package reorg

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/uda/adjust"
	"github.com/spf13/cobra"
)

// ShowRecordsCmd shows a stored corporate action records in marketstore. It's main purpose is to provide a way
// of verification of the imported data.
var ShowRecordsCmd = &cobra.Command{
	Use: "show <datadir> <cusip>",

	SilenceUsage: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			cmd.Help()
			return nil
		}
		cusip := args[1]
		dataDir := args[0]
		executor.NewInstanceSetup(dataDir, true, true, true, true)
		showRecords(cusip)
		return nil
	},
}

func showRecords(cusip string) {
	ca := adjust.NewCorporateActions(cusip)
	ca.Load()
	println("----- stored records ------")
	for i := 0; i < len(ca.Rows.EntryDates); i++ {
		ent := time.Unix(ca.Rows.EntryDates[i], 0)
		eff := time.Unix(ca.Rows.EffectiveDates[i], 0)
		rec := time.Unix(ca.Rows.RecordDates[i], 0)

		var ref int64
		if ca.Rows.Statuses[i] == adjust.UpdateRecord {
			ref = ca.Rows.UpdateTextNumbers[i]
		} else if ca.Rows.Statuses[i] == adjust.DeleteRecord {
			ref = ca.Rows.DeleteTextNumbers[i]
		}

		fmt.Printf("%c %c %c\tTEXTNUM: %d\tENT: %s, EFF: %s, REC: %s\tRATE: %.4f, REF: %d\n",
			ca.Rows.Statuses[i],
			ca.Rows.SecurityTypes[i],
			ca.Rows.NotificationTypes[i],
			ca.Rows.TextNumbers[i],
			ent.Format("2006-01-02"),
			eff.Format("2006-01-02"),
			rec.Format("2006-01-02"),
			ca.Rows.Rates[i],
			ref)
	}
	rateChanges := ca.RateChangeEvents(true, true)
	println("----- effective rate changes ---")
	for _, r := range rateChanges {
		fmt.Printf("DATE: %s, TEXTNUM: %d, RATE: %.4f\n", time.Unix(r.Epoch, 0).Format("2006-01-02"), r.Textnumber, r.Rate)
	}
}
