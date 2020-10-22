package main

import (
	"fmt"
	"strings"
	"time"
	"flag"
	// "os"
	"math"
	"github.com/alpacahq/marketstore/v4/contrib/corporateactions/reorg"
	// "regexp"
	"io/ioutil"
	"path/filepath"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/uda"
	// "github.com/alpacahq/marketstore/v4/uda/min"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils"
	//"math/rand"
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
	TextNumber int64						`reorg:"line:1 pos:16-23"`
	ReorgID	int								`reorg:"line:1 pos:16-23"`
	Remarks string 				`reorg:"line:1 pos:33-45"`
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

	Rate float64							`reorg:"line:4 pos:6-15"`
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


type ColumnBuffer struct {
	entrydates []int64
	textnumbers []int64
	// dealnumbers := make([]string, l
	// reorgids := make([]string, l)
	// notificationnumbers := make([]string, l)
	// notificationcodes := make([]string, l)
	// statuses := make([]string, l)
	// updatenotificationtypes := make([]string, l)
	// nrofoptions := make([]string, l)
	// securitytypes := make([]string, l)
	// effectivedates := make([]int64, l)
	// targetcusips := make([]string, l)
	// targetdescriptions := make([]string, l)
	// initiatingcusips := make([]string, l)
	// initiatingdescriptions := make([]string, l)
	// expirationdates := make([]int64, l)
	// cashes := make([]float64, l)
	// cashcodes := make([]string, l)
	// statecodes := make([]string, l)
	recorddates []int64
	// ratecodes := make([]string, l)
	// voluntarymandatorycodes := make([]string, l)
	// updatetextnumbers := make([]string, l)
	// deletetextnumbers := make([]string, l)
	newrates []float64
	oldrates []float64
	rates []float64
	// dueredemptiondates := make([]int64, l)
}

func NewColumnBuffer() *ColumnBuffer {
	return &ColumnBuffer{
		entrydates: []int64{},
		textnumbers: []int64{},
	// 	dealnumbers := make([]string, l
	// reorgids := make([]string, l)
	// notificationnumbers := make([]string, l)
	// notificationcodes := make([]string, l)
	// statuses := make([]string, l)
	// updatenotificationtypes := make([]string, l)
	// nrofoptions := make([]string, l)
	// securitytypes := make([]string, l)
	// effectivedates := make([]int64, l)
	// targetcusips := make([]string, l)
	// targetdescriptions := make([]string, l)
	// initiatingcusips := make([]string, l)
	// initiatingdescriptions := make([]string, l)
	// expirationdates := make([]int64, l)
	// cashes := make([]float64, l)
	// cashcodes := make([]string, l)
	// statecodes := make([]string, l)
	   recorddates: []int64{},
	// ratecodes := make([]string, l)
	// voluntarymandatorycodes := make([]string, l)
	// updatetextnumbers := make([]string, l)
	// deletetextnumbers := make([]string, l)
		newrates: []float64{},
		oldrates: []float64{},
		rates: []float64{},
	// dueredemptiondates := make([]int64, l)
	}
}

func readItems(path string) (*[]Item) {
	println("processing ", path)
	buff, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Printf("%+v\n", err)
		return nil
	}
	content := string(buff)
	var items = []Item{}
	reorg.ReadRecords(content, &items)
	println("Read ", len(items), "records")
	return &items
}

var ca = map[string][]Item{}

func storeItems(items []Item) error {
	var buffers = map[string]*ColumnBuffer{}
	var cb *ColumnBuffer
	var exists bool

	for _, item := range items {
		// fmt.Printf("%+v\n", i)
		if item.TargetCusip == "" {
			continue
		}

		if item.NotificationCode == "7" || item.NotificationCode == "+" {
			println()
			println(item.Remarks, " - ", item.TargetCusip, ":", item.OldRate, item.NewRate, item.Rate)
			ca[item.TargetCusip] = append(ca[item.TargetCusip], item)

			fmt.Printf("%+v\n", item)
			key := item.TargetCusip + "/1D/ACTIONS"

			// println(key)
			cb, exists = buffers[key]
			if !exists {
				cb = NewColumnBuffer()
				buffers[key] = cb
			}
			//println(i.TargetCusip, i.EntryDate.Unix())
			cb.entrydates = append(cb.entrydates, item.EntryDate.Unix())
			cb.recorddates = append(cb.recorddates, item.RecordDate.Unix())
			cb.textnumbers = append(cb.textnumbers, item.TextNumber)
			cb.newrates = append(cb.newrates, item.NewRate)
			cb.oldrates = append(cb.oldrates, item.OldRate)
			cb.rates = append(cb.rates, item.Rate)
			//fmt.Printf("%+v\n", cb.entrydates)
		}
	}

	csm := io.NewColumnSeriesMap()
	for key, buffer := range buffers {
		println(key, "items: ", len(buffer.entrydates))
		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", buffer.recorddates)
		cs.AddColumn("NewRate", buffer.newrates)
		cs.AddColumn("OldRate", buffer.oldrates)
		cs.AddColumn("Rate", buffer.rates)
		cs.AddColumn("TextNumber", buffer.textnumbers)
		tbk := io.NewTimeBucketKeyFromString(key)
		csm.AddColumnSeries(*tbk, cs)
	}
	println()
	err := executor.WriteCSM(csm, true)
	if err != nil {
		fmt.Printf("%+v\n", err)
	}
	return err
}

var reorg_dir string
var data_dir string 

func init() {
	flag.StringVar(&reorg_dir, "reorg", "./reorg", "path to the reorg files")
	flag.StringVar(&data_dir, "data", "./data", "path to store marketstore files")
	flag.Parse()
	println("data dir:", data_dir)
	println("reorg files:", reorg_dir)
}

func file_list(path string, prefix string) (out []string, err error) {
	localfiles, err := ioutil.ReadDir(path)
	if err == nil {
		for _, file := range localfiles {
			if strings.HasPrefix(file.Name(), prefix) { // && !strings.HasSuffix(file.Name(), ".processed") {
				out = append(out, file.Name())
			}
		}
	}
	return 
}



func import_reorg_files() {
	reorg_files, err := file_list(reorg_dir, "reorg")
	if err != nil {
	 	fmt.Printf("%+v\n", err)
	 	return 
	}
	for _, reorg_file := range reorg_files {
		path_to_file := filepath.Join(reorg_dir, reorg_file)
		items := readItems(path_to_file)
		if err := storeItems(*items); err == nil {
		// 	os.Rename(path_to_file, path_to_file+".processed")
		}
	}	


}


var (
	requiredColumns = []io.DataShape{
		{Name: "*", Type: io.FLOAT32},
		{Name: "*", Type: io.FLOAT32},
	}

	optionalColumns = []io.DataShape{
		{Name: "*", Type: io.FLOAT32},
	}

	initArgs = []io.DataShape{
		{Name: "tbk", Type: io.STRING},
	}
)


type ReorgAgg struct {
	uda.AggInterface
	ArgMap *functions.ArgumentMap

	output []float64
	tbk *io.TimeBucketKey
}


func (ra *ReorgAgg) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}
func (ra *ReorgAgg) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}
func (ra *ReorgAgg) GetInitArgs() []io.DataShape {
	return initArgs
}


func (ra *ReorgAgg) New() (uda.AggInterface, *functions.ArgumentMap) {
	r := new(ReorgAgg)
	r.ArgMap = functions.NewArgumentMap(requiredColumns, optionalColumns...)
	return r, r.ArgMap
}


func (ra *ReorgAgg) Init(args ...interface{}) error {
	if len(args) > 0 {
		var key string
		switch val := args[0].(type) {
		case string: 
			key = val
		case *string:
			key = *val
		case *[]string:
			if len(*val) != 1 {
				return fmt.Errorf("Argument passed to Init() is not a string")
			}
			key = (*val)[0]
		case []string:
			if len(val) != 1 {
				return fmt.Errorf("Argument passed to Init() is not a string")
			}
			key = val[0]
		default:
			return fmt.Errorf("Invalid parameter type for time bucket key!")
		}
		ra.tbk = io.NewTimeBucketKeyFromString(key)
		fmt.Printf("Timebucketkey: %+v\n", ra.tbk)
	}
	ra.Reset()
	return nil
}


func (ra *ReorgAgg) Reset() {
	// rreset some inner state here
}

func (ra *ReorgAgg) Accum(cols io.ColumnInterface) error {
	println("------ Accumulating -----")
	tbk := &io.TimeBucketKey{Key: ra.tbk.Key}

	tbk.SetItemInCategory("Symbol", "75079T104") //  654090109    371485301  56382R274 15930P800
	tbk.SetItemInCategory("AttributeGroup", "ACTIONS")
	tbk.SetItemInCategory("Timeframe", "1D")

	println("using ", tbk.Key)
	println(tbk.GetPathToYearFiles(data_dir))

	query := planner.NewQuery(executor.ThisInstance.CatalogDir)
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
		fmt.Printf("No files returned from parse! %+v\n", err)
		return err
	}

	fmt.Printf("parseresult: %+v\n", parseResult)

	scanner, err := executor.NewReader(parseResult)
	if err != nil {
		fmt.Printf("Unable to create scanner: %s\n", err)
		return err
	}
	// fmt.Printf("scanner: %+v\n", scanner)

	csm, err := scanner.Read()
	if err != nil {
		fmt.Printf("Error returned from query scanner: %s\n", err)
		return err
	}

	fmt.Printf("csm data shape: %+v\n", csm.GetMetadataKeys())

	cs2 := csm[*tbk]

	println("split data len:", cs2.Len())
	fmt.Printf("shape: %+v\n", cs2.GetDataShapes())
	epoch, err := cs2.GetTime()
	if err != nil {
		println("cannot find Epoch column!")
		return err
	}

	newrate := cs2.GetColumn("NewRate").([]float64)
	oldrate := cs2.GetColumn("OldRate").([]float64)
	rate := cs2.GetColumn("Rate").([]float64)
	textnumber := cs2.GetColumn("TextNumber").([]int64)

	for i:=0; i<len(epoch); i++ {
		fmt.Printf("ID: %d, date: %+v, new: %.2f, old: %.2f, rate: %+v\n",  textnumber[i], epoch[i], newrate[i], oldrate[i], rate[i])
	}

	// cols.Len()
	// cols.GetColumn("Close")
	// cols.GetDataShapes()
	// cols.GetTime() 

	// cols.Get
	return nil
}

func (ra *ReorgAgg) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("DividendAdjustedPrice", ra.output)
	return cs
}



func parseFunctionCall(call string) (funcName string, literalList, parameterList []string, err error) {
	call = strings.Trim(call, " ")
	left := strings.Index(call, "(")
	right := strings.LastIndex(call, ")")
	if left == -1 || right == -1 {
		return "", nil, nil, fmt.Errorf("unable to parse function call %s", call)
	}
	funcName = strings.Trim(call[:left], " ")
	call = call[left+1 : right]
	/*
		First parse for literals and re-form a string without them for the last stage of parsing
	*/
	var newCall string
	for {
		left = strings.Index(call, "'")
		if left == -1 {
			newCall = newCall + call
			break
		} else if left != 0 {
			newCall = newCall + call[:left]
		}
		call = call[left+1:]
		right = strings.Index(call, "'")
		if right == -1 {
			return "", nil, nil, fmt.Errorf("unclosed literal %s", call)
		}
		literalList = append(literalList, call[:right])
		call = call[right+1:]
	}
	pList := strings.Split(newCall, ",")
	for _, val := range pList {
		trimmed := strings.Trim(val, " ")
		if len(trimmed) != 0 {
			parameterList = append(parameterList, trimmed)
		}
	}
	return funcName, literalList, parameterList, nil
}




func main() {
	executor.NewInstanceSetup(data_dir, true, true, true, true)

	// import_reorg_files()

	// for cusip, items := range ca {
	// 	println("----------------------------------")
	// 	println(cusip)
	// 	for _, it := range items {
	// 		fmt.Printf("%+v\n", it)
	// 	}
	// }

	// panic("")

	tbk := io.NewTimeBucketKeyFromString("1244235/1D/ACTIONS:Symbol/Timeframe/Tag")
	println(tbk.String())
	// fmt.Printf("%+v\n", tbk.GetCategories())
	// fmt.Printf("%+v\n", tbk.GetItems())
	// fmt.Printf("%+v\n", tbk.GetMultiItemInCategory("Symbol"))
	println(tbk.GetItemInCategory("Timeframe"))
	// tf, err := tbk.GetTimeFrame()
	// fmt.Printf("%+v, %+v\n", tf, err)

	tbk.SetItemInCategory("Tag", "VALAMI")

	println(tbk.GetPathToYearFiles(data_dir))

	//fmt.Printf("write finished. %+v\n", err)


	var row []int64 = make([]int64, 20, 20)
	for i:=0; i<len(row); i++ {
		row[i] = int64(1)
	}

	// fmt.Printf("row: %+v\n", row)
	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()
	cs.AddColumn("Price", row)
	csm.AddColumnSeries(*tbk, cs)

	funcname, literals, params, err := parseFunctionCall("min('EURUSD/1D/OHLC', CLOSE, OP£N)")
	println("----- parse function call ------")
	fmt.Printf("funcname: %+v\n literals: %+v\nparams: %+v\nerr: %+v\n", funcname, literals, params, err)

	// var agg uda.AggInterface = &min.Min{}
	var agg uda.AggInterface = &ReorgAgg{}

	// fn = &min.Min{}
	aggfunc, argmap := agg.New()
	if err = argmap.PrepareArguments(params); err != nil {
		println("Preparearguments error:")
		fmt.Printf("%+v\n", err)
		return 
	}
	println("------- argmap things: --------- ")
	// fmt.Printf(" %+v\n", argmap.GetAliasedColumnNames())
	// fmt.Printf("MappedColumns: %+v\n", argmap.GetMappedColumns("BASE"))
	// fmt.Printf("MappedColumns: %+v\n", argmap.GetMappedColumns("EXT"))
	// fmt.Printf("MappedColumns: %+v\n", argmap.GetMappedColumns("*"))


	println()

	requiredInitDSV := aggfunc.GetInitArgs()
	requiredInitNames := io.GetNamesFromDSV(requiredInitDSV)
	fmt.Printf("Required InitNames: %+v\n", requiredInitNames)

	if err = aggfunc.Init(literals); err != nil {
		fmt.Printf("init error: %+v\n", err)
		return 
	}

	if err := aggfunc.Accum(cs); err != nil {
		fmt.Printf("Failed to run agg function: %+v\n", err)
		return 
	}

	out_cs := aggfunc.Output() 

	fmt.Printf("Output: %+v\n", out_cs.GetDataShapes())
	fmt.Printf("Min: %+v\n", out_cs.GetColumn("Epoch"))

}



	//fmt.Printf("%+v\n", items)
	
	// for i, item := range items {
	// 	entrydates[i] = item.EntryDate.Unix()
	// 	dealnumbers[i] = item.DealNumber
	// 	reorgids[i] = item.ReorgID
	// 	notificationnumbers[i] = item.NotificationNumer
	// 	notificationcodes[i] = item.NotificationNumer
	// 	statuses[i] = item.Status
	// 	updatenotificationtypes[i] = item.UpdatedNotificationType
	// 	nrofoptions[i] = item.NrOfOptions
	// 	securitytypes[i] = item.SecurityType
	// 	effectivedates[i] = item.EffectiveDate.Unix()
	// 	targetcusips[i] = item.TargetCusip
	// 	targetdescriptions[i] = item.TargetDescription
	// 	initiatingcusips[i] = item.InitiatingCusip
	// 	initiatingdescriptions[i] = item.InitiatingDescription
	// 	expirationdates[i] = item.ExpirationDate.Unix()
	// 	cashes[i] = item.Cash
	// 	cashcodes[i] = item.CashCode
	// 	statecodes[i] = item.StateCode
	// 	recorddates[i] = item.RecordDate.Unix()
	// 	ratecodes[i] = item.RateCode
	// 	voluntarymandatorycodes[i] = item.VoluntaryMandatoryCode
	// 	updatetextnumbers[i] = item.UpdateTextNumber
	// 	deletetextnumbers[i] = item.DeleteTextNumber
	// 	newrates[i] = item.NewRate
	// 	oldrates[i] = item.OldRate
	// 	dueredemptiondates[i] = item.DueRedemptionDate.Unix()
	// }
	
	// cs.AddColumn("Epoch", effectivedates)
	// cs.AddColumn("EntryDate", entrydates)
	// cs.AddColumn("DealNumbers", dealnumbers)
	// cs.AddColumn("ReorgID", reorgids)
	// cs.AddColumn("NotificationNumber", notificationnumbers)
	// cs.AddColumn("NotificationCode", notificationcodes)
	// cs.AddColumn("Status", statuses)
	// cs.AddColumn("UpdateNotificationType", updatenotificationtypes)
	// cs.AddColumn("NrOfOptions", nrofoptions)
	// cs.AddColumn("SecurityType", securitytypes)
	// cs.AddColumn("EffectiveDate", effectivedates)
	// cs.AddColumn("TargetCusip", targetcusips)
	// cs.AddColumn("TargetDescription", targetdescriptions)
	// cs.AddColumn("InitiatingCusip", initiatingcusips)
	// cs.AddColumn("InitiatingDescription", initiatingdescriptions)
	// cs.AddColumn("ExpirationDate", expirationdates)
	// cs.AddColumn("Cash", cashes)
	// cs.AddColumn("CashCode", cashcodes)
	// cs.AddColumn("StateCode", statecodes)
	// cs.AddColumn("RecordDate", recorddates)
	// cs.AddColumn("RateCode", ratecodes)
	// cs.AddColumn("VoluntaryMandatoryCodes", voluntarymandatorycodes)
	// cs.AddColumn("UpdateTextNumber", updatetextnumbers)
	// cs.AddColumn("DeleteTextNumber", deletetextnumbers)
	// cs.AddColumn("NewRate", newrates)
	// cs.AddColumn("OldRate", oldrates)
	// cs.AddColumn("DueRedemptionDate", dueredemptiondates)
	
	// csm.AddColumnSeries(*tbk, cs)

