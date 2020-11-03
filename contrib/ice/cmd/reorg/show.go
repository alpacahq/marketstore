package reorg

import (
	"time"
	"fmt"

	"github.com/spf13/cobra"
	uda "github.com/alpacahq/marketstore/v4/uda/reorg"
	"github.com/alpacahq/marketstore/v4/executor"
)

var ShowRecordsCmd = &cobra.Command{
	Use: "show <datadir> <cusip>",

	SilenceUsage: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 2 {
			cusip := args[1]
			dataDir := args[0] 
			executor.NewInstanceSetup(dataDir, true, true, true, true)
			show_records(cusip)
		} else {
			cmd.Help()
		}
		return nil
	},
}

func show_records(cusip string) {
	ca := uda.NewCorporateActions(cusip)
	ca.Load()
	println("----- stored records ------")
	for i:=0; i<len(ca.Rows.EntryDates); i++ {
		ent := time.Unix(ca.Rows.EntryDates[i], 0)
		eff := time.Unix(ca.Rows.EffectiveDates[i], 0)
		rec := time.Unix(ca.Rows.RecordDates[i], 0)

		var ref int64
		if ca.Rows.Statuses[i] == uda.UpdateRecord {
			ref = ca.Rows.UpdateTextNumbers[i]
		} else if ca.Rows.Statuses[i] == uda.DeleteRecord {
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
	rate_changes := ca.RateChangeEvents(true, true)
	println("----- effective rate changes ---")
	for _, r := range rate_changes {
		fmt.Printf("DATE: %s, TEXTNUM: %d, RATE: %.4f\n", time.Unix(r.Epoch, 0).Format("2006-01-02"), r.Textnumber, r.Rate)
	}
}
