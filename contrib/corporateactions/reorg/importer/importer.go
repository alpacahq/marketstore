package main

import (
	"fmt"
	"strings"
	"time"
	//"os"
	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/reorg"
	// "regexp"
	// "io/ioutil"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var record string = 
`........................................................... ENT:10/09/20:(2020) 
DEAL#: 1: TEXT#:2129215: REMARKS:REDEMPTION  :   IND:*U*1G: EFF:10/16/20:(2020) 
TARGET:3130ACMH4: :FEDERAL HO: INIT:999999999: :9999999999: EXP:10/16/20:(2020) 
CASH:1000.00000:  :A:   STATE:  :  T.AGENT:    : INFO:    : MAT:10/16/24:(2024) 
RATE:   2.50000:  : : :  1.00000:M:DTC: : PRATE:  0.000000: PRO:10/08/20:(2020) 
DESC. 1: FEDERAL HOME LN BKS                          UPDTEXT:2128893:          
DESC. 2: CONS BD DTD 10/16/2017 2.5% DUE 10/16/2024                             
DESC. 3: UPDATED ACCRUED INTEREST                                               
REDEMPTION DATE:  OCTOBER 16, 2020                                              
REDEMPTION PRICE: 1000.00000           ACCRUED INTEREST:  12.50000000:          
MATURITY DATE:    OCTOBER 16, 2024     ACCRUED INTEREST CODE:1:                 
COUPON:              2.500000          (ACCRUED INTEREST REPORTED)              
DATED DATE:       OCTOBER 16, 2017                                              
TYPE OF CALL:     FULL                                                          
PUBLICATION DATE: OCTOBER 08, 2020                                              
DEPO. 1: FEDERAL RESERVE BANK                                                   
DEPO. 2: 100 ORCHID STREET EAST RUTHERFORD NJ 07073                             
DEPO. 3: TEL. 201-531-3550                                          **          
........................................................... ENT:10/09/20:(2020) 
DEAL#: 1: TEXT#:2129216: REMARKS:EXCHANGE    :   IND:XN 1B: EFF:99/99/99:(9999) 
TARGET:29911QAB5: :EVANS BANC: INIT:29911QAA7: :EVANS BANC: EXP:11/09/20:(2020) 
CASH:   0.00000:  : : PROTECT:  :  T.AGENT:    : INFO:    : WDR:11/09/20:(2020) 
RATE:   1.00000:  :A: :  1.00000:V:DTC: : PRATE:  0.000000: PRO:99/99/99:(9999) 
OPT:01: "EVANS BANCORP INC"                                                     
ANNOUNCED AN EXCHANGE OFFER FOR ANY AND ALL SUB NT FXD/FLTG 144A DUE            
07/15/2030                                                                      
TERMS: FOR EVERY $1,000.00 PRINCIPAL AMOUNT OF NOTES HOLDERS WILL               
RECEIVE $1,000.00 PRINCIPAL AMOUNT OF EVANS BANCORP INC SUB NT                  
FXD/FLTG DUE 07/15/2030:                                                        
THE OFFER AND WITHDRAWAL RIGHTS WILL EXPIRE AT:                                 
:TIME: :05:00: :PM: :EST: :MONDAY, NOVEMBER 9, 2020                             
THERE IS NO PROTECT PERIOD AVAILABLE                                            
NOTE: THE EXCHANGE OFFER IS NOT CONDITIONED ON ANY MINIMUM PRINCIPAL            
AMOUNT OF NOTES BEING VALIDLY TENDERED. NOTES TENDERED MUST BE IN               
MINIMUM DENOMINATIONS OF $100,000.00 AND INTEGRAL MULTIPLES OF                  
CONTINUED ON TEXT NO. 2129217, DATED 10-09-2020                    ***          
........................................................... ENT:10/09/20:(2020) 
DEAL#: 1: TEXT#:2129217: REMARKS:EXCHANGE    :   IND:XN 1B: EFF:99/99/99:(9999) 
TARGET:29911QAB5: :EVANS BANC: INIT:29911QAA7: :EVANS BANC: EXP:11/09/20:(2020) 
CASH:   0.00000:  : : PROTECT:  :  T.AGENT:    : INFO:    : WDR:11/09/20:(2020) 
RATE:   1.00000:  :A: :  1.00000:V:DTC: : PRATE:  0.000000: PRO:99/99/99:(9999) 
OPT:01: CONTINUATION OF TEXT NO. 2129216                                        
$1,000.00 IN EXCESS THEREOF. PLEASE REFER TO THE PROSPECTUS FOR                 
FURTHER DETAILED CONDITIONS.:                                                   
EXCHANGE AGENT: UMB BANK, N.A.                                                  
ADDRESS: 5555 SAN FELIPE ST., SUITE 870                                         
HOUSTON, TEXAS 77056                                                            
TEL: (713) 300-0587                                                 **         
**********************************************************************          
99999999999999999999999999999999999999999999999999999999999999999999999999999999
`


type Item struct {
	EntryDate time.Time 					`reorg:"line:0 pos:64-72 format:01/02/06"`

	DealNumber string						`reorg:"line:1 pos:6-8"`
	TextNumber string						`reorg:"line:1 pos:16-23"`
	ReorgID	string							`reorg:"line:1 pos:16-23"`
	NotificationNumer string 				`reorg:"line:1 pos:33-45"`
	NotificationCode string					`reorg:"line:1 pos:53"`
	Status string							`reorg:"line:1 pos:54"`
	UpdatedNotificationType string			`reorg:"line:1 pos:55"`
	NrOfOptions string						`reorg:"line:1 pos:56"`
	SecurityType string						`reorg:"line:1 pos:57"`
	EffectiveDate time.Time					`reorg:"line:1 pos:64-72 format:01/02/06"`

	TargetCusip string						`reorg:"line:2 pos:7-16"`
	TargetDescription string				`reorg:"line:2 pos:19-29"`
	InitiatingCusip string					`reorg:"line:2 pos:36-45"`
	InitiatingDescription string			`reorg:"line:2 pos:48-58"`
	ExpirationDate time.Time				`reorg:"line:2 pos:64-72 format:01/02/06"`

	Cash float64							`reorg:"line:3 pos:5-15"`
	CashCode string 						`reorg:"line:3 pos:19"`
	StateCode string						`reorg:"line:3 pos:30-32"`
	RecordDate time.Time					`reorg:"line:3 pos:64-72 format:01/02/06"`

	RateCode string							`reorg:"line:4 pos:19"`
	VoluntaryMandatoryCode string			`reorg:"line:4 pos:33"`

	UpdateTextNumber string					`reorg:"func:ParseUpdateTextNumber"`
	DeleteTextNumber string					`reorg:"func:ParseDeleteTextNumber"`
	NewRate float64							`reorg:"func:ParseNewRate"`
	OldRate	float64							`reorg:"func:ParseOldRate"`
	DueRedemptionDate time.Time				`reorg:"func:ParseDueRedemptionDate"`
	// Detail string							`reorg:"func:parseDetail"`
}


func (i Item) ParseUpdateTextNumber(lines []string) string {
	if strings.TrimSpace(lines[5][54:61]) == "UPDTEXT" {
		return strings.TrimSpace(lines[5][62:69])
	} else {
		return ""
	}
}

func (i Item) ParseDeleteTextNumber(lines []string) string {
	if strings.TrimSpace(lines[5][54:61]) == "DELTEXT" {
		return strings.TrimSpace(lines[5][62:69])
	} else {
		return ""
	}
}

func (i Item) ParseNewRate(lines []string) string {
	if i.NotificationCode == "7" || i.NotificationCode == "+" {
		return lines[7][56:69]
	} else {
		return lines[4][5:15]
	}
}

func (i Item) ParseOldRate(lines []string) string {
	if i.NotificationCode == "7" || i.NotificationCode == "+" {
		return lines[8][56:69]
	} else {
		return lines[4][23:32]
	}
}


func (i Item) ParseDueRedemptionDate(lines []string) string {
	if len(lines) > 12 && lines[12][0:25] == "DUE BILL REDEMPTION DATE:" {
		return lines[12][25:33]
	} else {
		return ""
	}
}


// // StringToDecimal returns a decimal pointer created from s, or nil.
// func StringToDecimal(s string) *decimal.Decimal {
// 	if s == "" {
// 		return nil
// 	}

// 	v, err := decimal.NewFromString(s)
// 	if err != nil {
// 		return nil
// 	}
// 	return &v
// }


func main() {
	//buff, _ := ioutil.ReadFile("../ms/ALPACAFTPH1/reorg.20200109")
	//content := string(buff)
	content := record 
	var items = []Item{}
	reorg.ReadRecords(content, &items)
	println("Read ", len(items), "records")
	fmt.Printf("%+v\n", items)

	executor.NewInstanceSetup(".", true, true, true, true)
	
	csm := io.NewColumnSeriesMap()
	tbk := io.NewTimeBucketKeyFromString("AAPL/1D/ACTIONS")
	cs := io.NewColumnSeries()

	reorgid := make([]string, len(items))

	for i, item := range items {
		reorgid[i] = item.ReorgID
	}
	
	cs.AddColumn("ReorgID", reorgid)
	
	csm.AddColumnSeries(*tbk, cs)

	err := executor.WriteCSM(csm, true)

	println(err)

}