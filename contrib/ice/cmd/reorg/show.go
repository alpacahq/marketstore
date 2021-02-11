package reorg

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/uda/adjust"
	"github.com/spf13/cobra"
)

// ShowRecordsCmd shows stored corporate action announcements in marketstore. Its main purpose is to provide a way
// of verification of the imported data.
var ShowRecordsCmd = &cobra.Command{
	Use:   "show <datadir> <cusip/symbol>",
	Short: "Shows corporate action announcement",
	Long: `This command shows accouncements stored for a given symbol or cusip
	<datadir> must point to Marketstore's data directory
	<cusip/symbol> is euther a CUSIP id or a symbol name

	Mainly for debugging / verification purposes.
	`,
	SilenceUsage: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			cmd.Help()
			return nil
		}
		cusip := args[1]
		dataDir := args[0]
		metadata, _, _ := executor.NewInstanceSetup(dataDir, nil, 5, true, true, true, true)
		showRecords(cusip, metadata.CatalogDir)
		return nil
	},
}

func showRecords(cusip string, catalogDir *catalog.Directory) {
	ca := adjust.NewCorporateActions(cusip)
	ca.Load(false, catalogDir)
	fmt.Println("----- stored announcements ------")
	for i := 0; i < len(ca.Rows.EntryDates); i++ {
		ent := time.Unix(ca.Rows.EntryDates[i], 0)
		eff := time.Unix(ca.Rows.EffectiveDates[i], 0)
		rec := time.Unix(ca.Rows.RecordDates[i], 0)
		exp := time.Unix(ca.Rows.ExpirationDates[i], 0)

		var ref int64
		status := enum.StatusCode(ca.Rows.Statuses[i])
		if status == enum.UpdatedAnnouncement {
			ref = ca.Rows.UpdateTextNumbers[i]
		} else if status == enum.DeletedAnnouncement {
			ref = ca.Rows.DeleteTextNumbers[i]
		}

		fmt.Printf("%c %c %c\tTEXTNUM: %d\tENT: %s, EFF: %s, REC: %s, EXP: %s\tRATE: %.4f, REF: %d\n",
			ca.Rows.Statuses[i],
			ca.Rows.SecurityTypes[i],
			ca.Rows.NotificationTypes[i],
			ca.Rows.TextNumbers[i],
			ent.Format("2006-01-02"),
			eff.Format("2006-01-02"),
			rec.Format("2006-01-02"),
			exp.Format("2006-01-02"),
			ca.Rows.Rates[i],
			ref)
	}
	rateChanges := ca.RateChangeEvents(true, true)
	fmt.Println("----- effective rate changes ---")
	for _, r := range rateChanges {
		fmt.Printf("DATE: %s, TEXTNUM: %d, RATE: %.4f\n", time.Unix(r.Epoch, 0).Format("2006-01-02"), r.Textnumber, r.Rate)
	}
}
