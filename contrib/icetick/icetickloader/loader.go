package icetickloader

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	silent        bool
	mode          Mode
	fileDate      string
	currentTape   Tape
	symbolSkipper SymbolSkipper
)

const (
	tokenSep = "|"

	headerConfigPrefix = "#D=H"
	headerPrefix       = "H"
	bboConfigPrefix    = "#D=B"
	bboPrefix          = "B"
	tradeConfigPrefix  = "#D=T"
	tradePrefix        = "T"
)

// Tape represents the tape whose data is currently being processed
//
// Valid values are A, B and C
type Tape string

// SymbolSkipper is used to skip and report symbols whose lines are malformed
type SymbolSkipper struct {
	symbol string
	err    error
}

// IsActive returns whether s is currently active
func (s *SymbolSkipper) IsActive() bool {
	return s.symbol != ""
}

// Reset resets s
func (s *SymbolSkipper) Reset() {
	s.symbol = ""
	s.err = nil
}

// String returns s's string representation
func (s *SymbolSkipper) String() string {
	if s.symbol != "" {
		return fmt.Sprintf("skipping symbol %s because of error: %v", s.symbol, s.err)
	}
	return "not skipping"
}

// Skip sets s to skip symbol because of err
func (s *SymbolSkipper) Skip(symbol string, err error) {
	s.symbol = symbol
	s.err = err
}

// Mode represents the mode that the program runs in.
//
// 0: default, 1: trades, 2: quotes
type Mode int

var _ flag.Value = (*Mode)(nil)

func (m *Mode) String() string {
	if m == nil {
		return "<INVALID MODE>"
	}
	switch {
	case m.isDefault():
		return "default"
	case m.isTrades():
		return "trades"
	case m.isQuotes():
		return "quotes"
	}
	return "<INVALID_MODE>"
}

func (m *Mode) Set(s string) error {
	switch strings.ToLower(s) {
	case "d", "default":
		*m = Mode(0)
	case "t", "trade", "trades":
		*m = Mode(1)
	case "q", "quote", "quotes":
		*m = Mode(2)
	default:
		return errors.New("Invalid mode:" + s)
	}
	return nil
}

func (m Mode) isDefault() bool {
	return m == Mode(0)
}

func (m Mode) isTrades() bool {
	return m == Mode(1)
}

func (m Mode) isQuotes() bool {
	return m == Mode(2)
}

func init() {
	flag.BoolVar(&silent, "s", false, "If silent is true then the program won't create any output (besides errors and stats)")
	flag.Var(&mode, "m", `Mode has three settings: d(efault), t(rades), q(uotes).
In default both trades are quotes are reported (to stdout and stderr). The other 2 modes always use stdout and report the respective lines only.`)
}

// FileStats is used for writing stats
type FileStats struct {
	quotes   int
	trades   int
	date     string
	tape     Tape
	finished bool
}

// WriteTo writes f to file. File must be open already!
func (f *FileStats) WriteTo(file *os.File) {
	file.WriteString(fmt.Sprintf("Date: %s, Tape: %s, Finished: %t, Trades: %d, Quotes: %d\n", fileDate, string(f.tape), f.finished, f.trades, f.quotes))
}

// Buffer is used for internal buffering of lines that will be written
type Buffer struct {
	builder       strings.Builder
	itemsInBuffer int
}

func (b *Buffer) addToBuffer(s string) {
	b.builder.WriteString(s + "\n")
	b.itemsInBuffer++
}

// Write writes b's contents to stdout if stdout is true. Otherwise it writes to stderr
func (b *Buffer) Write(stdout bool) {
	if !silent {
		if b.itemsInBuffer > 0 {
			if stdout {
				fmt.Print(b.builder.String())
			} else {
				if _, err := os.Stderr.WriteString(b.builder.String()); err != nil {
					terminate("couldn't write to stderr, error: " + err.Error())
				}

			}
		}
	}

	b.Reset()
}

// Reset resets b's internal state
func (b *Buffer) Reset() {
	b.itemsInBuffer = 0
	b.builder.Reset()
}

// Trade is the struct that will be inserted into the DB for trades. Schema:
// id bigserial,
// epoch timestamptz NOT NULL,
// nanoseconds INTEGER NOT NULL,
// symbol varchar NOT NULL,
// exchange varchar(1) NOT NULL,
// price double precision NOT NULL,
// size bigint NOT NULL,
// conditions varchar NOT NULL,
// trade_id bigint NOT NULL,
// tape varchar(1) NOT NULL
type Trade struct {
	Timestamp  time.Time
	Symbol     string
	Exchange   string
	Price      string
	Size       string
	Conditions string
	TradeID    string
	Tape       Tape
}

func (t Trade) String() string {
	// 2020-01-03T09:00:00.063086-05:00;063086808;AAPL;P;297.35;1602;T;1;C
	return fmt.Sprintf("%s;%d;%s;%s;%s;%s;%s;%s;%s", t.Timestamp.UTC().Format(time.RFC3339), t.Timestamp.Nanosecond(), t.Symbol, t.Exchange, t.Price, t.Size, t.Conditions, t.TradeID, t.Tape)
}

// Quote is the struct that will be inserted into the DB for quotes (that are NBBOs to be more precise). Schema
// id bigserial,
// epoch timestamptz NOT NULL,
// nanoseconds integer NOT NULL,
// symbol varchar NOT NULL,
// ask_exchange varchar(1) NOT NULL,
// ask_price double precision NOT NULL,
// ask_size bigint NOT NULL,
// bid_exchange varchar(1) NOT NULL,
// bid_price double precision NOT NULL,
// bid_size bigint NOT NULL,
// condition varchar(1) NOT NULL,
// tape varchar(1) NOT NULL
type Quote struct {
	Timestamp   time.Time
	Symbol      string
	AskExchange string
	AskPrice    string
	AskSize     string
	BidExchange string
	BidPrice    string
	BidSize     string
	Condition   string // What's up with this?
	Tape        Tape
}

func (q Quote) String() string {
	// 2020-02-03T09:00:00.020394-05:00;020394647;AAPL;T;0.0;0;T;299.29;1;R;A
	return fmt.Sprintf("%s;%d;%s;%s;%s;%s;%s;%s;%s;%s;%s", q.Timestamp.UTC().Format(time.RFC3339), q.Timestamp.Nanosecond(), q.Symbol, q.AskExchange, q.AskPrice, q.AskSize, q.BidExchange, q.BidPrice, q.BidSize, q.Condition, q.Tape)
}

func terminate(msg string) {
	panic("> Date: " + fileDate + ", tape: " + string(currentTape) + ", msg: " + msg)
}

func splitLineAndCheckPrefix(line, prefix string) []string {
	parts := strings.Split(line, tokenSep)
	if prefix != "" && parts[0] != prefix {
		terminate(fmt.Sprintf("expected prefix %q for line %q but got %q", prefix, line, parts[0]))
	}
	return parts
}

func parseUint(s string) (uint, error) {
	u, err := strconv.ParseUint(s, 10, 0)
	if err != nil {
		return 0, fmt.Errorf(fmt.Sprintf("couldn't parse uint %q", s))
	}
	return uint(u), nil
}

func parseTS(s string) (time.Time, error) {
	var sec, nanoseconds uint
	var err error
	parts := strings.Split(s, ".")
	sec, err = parseUint(parts[0])
	if err != nil {
		return time.Time{}, err
	}
	if len(parts) == 2 && parts[1] != "" {
		zeroRightPadded := strings.ReplaceAll(fmt.Sprintf("%-9s", parts[1]), " ", "0")
		nanoseconds, err = parseUint(zeroRightPadded)
		if err != nil {
			return time.Time{}, err
		}
	}
	return time.Unix(int64(sec), int64(nanoseconds)), nil
}

func parseTradeCond1(s string, tape Tape) (string, bool) {
	if s == "" {
		// We don't want to accept empty conditions by padding them up to 00!
		return "", false
	}
	padded := fmt.Sprintf("%02s", s)

	switch tape {
	case Tape("A"), Tape("B"): // The ony difference is AMEX's 55 but I assume that's an error in the docs
		c, ok := tapeATapeBConditions[padded]
		return c, ok
	case Tape("C"):
		c, ok := tapeCConditions[padded]
		return c, ok
	default:
		terminate("invalid tape: " + string(tape))
	}

	return "", false
}

func parseExtCond(s string, tape Tape) (string, bool) {
	padded := fmt.Sprintf("%08s", s)
	first := padded[:2]
	second := padded[2:4]
	third := padded[4:6]
	fourth := padded[6:8]

	ok1 := true
	ok2 := true
	ok3 := true
	ok4 := true
	var (
		c1 string
		c2 string
		c3 string
		c4 string
	)

	var conditionMapping map[string]string

	switch tape {
	case Tape("A"), Tape("B"):
		// TODO: 07 is present in extended for tape A/C but not in TRADE.COND_1!!
		conditionMapping = tapeATapeBConditions
	case Tape("C"):
		conditionMapping = tapeCConditions
	default:
		terminate("invalid tape: " + string(tape))
	}

	c1, ok1 = conditionMapping[first]
	// For the rest of the segments we ignore 00
	if second != "00" {
		c2, ok2 = conditionMapping[second]
	}
	if third != "00" {
		c3, ok3 = conditionMapping[third]
	}
	if fourth != "00" {
		c4, ok4 = conditionMapping[fourth]
	}

	return c1 + c2 + c3 + c4, ok1 && ok2 && ok3 && ok4
}

func parseParticipant(s string, tape Tape) string {
	// If the participant is missing then the default is based on the tape
	//
	if s == "" {
		switch tape {
		case "A":
			return "N" // NOTE: https://www.finra.org/rules-guidance/rulebooks/finra-rules/7610a
		case "B":
			return "A" // NOTE: https://www.finra.org/rules-guidance/rulebooks/finra-rules/7610a
		case "C":
			return "T"
		}
	}

	return strings.ToUpper(s)
}

// ensureNoZeros makes sure no number in numbers is 0. If a number is 0 then it uses errorMsg to report the issue
func ensureNoZeros(errorMsg string, numbers ...int) {
	for _, n := range numbers {
		if n == 0 {
			terminate(errorMsg)
		}
	}
}

// HeaderConfiguration is the configuration for H| lines.
// The following configurations are likely to happen
// PLUSTICK_545_20151201.txt.gz
// #D=H|<ENUM.SRC.ID>|<SYMBOL.TICKER>|<ABRV.CURRENCY>|<ISIN>|<SEDOL>|<CUSIP>|<ENUM.INSTR.TYPE>|<LOT.SIZE>|<CONTRACT.SIZE>|<VARIABLE.TICK.SIZE>
// PLUSTICK_545_20210104.txt.gz
// #D=H|<ENUM.SRC.ID>|<SYMBOL.TICKER>|<CURRENCY.STRING>|<ISIN>|<SEDOL>|<CUSIP>|<ENUM.INSTR.TYPE>|<LOT.SIZE>|<CONTRACT.SIZE>|<VARIABLE.TICK.SIZE>|<ASK.CLOSE>|<BID.CLOSE>|<QUOTE.CLOSE.DATE>
type HeaderConfiguration struct {
	symbolIdx int
	srcIdx    int
}

func getHeaderConfiguration(line string) HeaderConfiguration {
	config := HeaderConfiguration{}
	parts := splitLineAndCheckPrefix(line, headerConfigPrefix)

	for idx, s := range parts {
		switch s {
		case "<ENUM.SRC.ID>":
			config.srcIdx = idx
		case "<SYMBOL.TICKER>":
			config.symbolIdx = idx
		}
	}

	ensureNoZeros("Invalid header configuration: "+line, config.symbolIdx, config.srcIdx)

	return config
}

func getTape(srcID string) Tape {
	switch srcID {
	case "545":
		return "B"
	case "558":
		return "A"
	case "564":
		return "C"
	default:
		terminate("invalid srcID: " + srcID)
		return ""
	}
}

func getSymbolAndTape(header string, config HeaderConfiguration) (string, Tape) {
	parts := splitLineAndCheckPrefix(header, headerPrefix)
	symbol := parts[config.symbolIdx]
	tape := getTape(parts[config.srcIdx])
	if symbol == "" {
		terminate("invalid header line, no symbol: " + header)
	}

	return symbol, tape
}

// BBOConfiguration is the configuration for B| lines.
// The following configurations are likely to happen
// PLUSTICK_545_20151201.txt.gz
// #D=B|<TAS.SEQ>|<ACTIVITY.DATETIME>|<BID.PRICE>|<BID.SIZE>|<BID.PART.CODE>|<ASK.PRICE>|<ASK.SIZE>|<ASK.PART.CODE>|<EXCH.MESSAGE.TIMESTAMP>
// PLUSTICK_545_20210104.txt.gz
// #D=B|<TAS.SEQ>|<ACTIVITY.DATETIME>|<BID.PRICE>|<BID.SIZE>|<BID.PART.CODE>|<ASK.PRICE>|<ASK.SIZE>|<ASK.PART.CODE>|<EXCH.MESSAGE.TIMESTAMP>
type BBOConfiguration struct {
	tsIdx       int
	bidPriceIdx int
	bidSizeIdx  int
	bidPartIdx  int
	askPriceIdx int
	askSizeIdx  int
	askPartIdx  int
}

func getBBOConfiguration(line string) BBOConfiguration {
	config := BBOConfiguration{}
	parts := splitLineAndCheckPrefix(line, bboConfigPrefix)

	for idx, s := range parts {
		switch s {
		case "<ACTIVITY.DATETIME>":
			config.tsIdx = idx
		case "<BID.PRICE>":
			config.bidPriceIdx = idx
		case "<BID.SIZE>":
			config.bidSizeIdx = idx
		case "<BID.PART.CODE>":
			config.bidPartIdx = idx
		case "<ASK.PRICE>":
			config.askPriceIdx = idx
		case "<ASK.SIZE>":
			config.askSizeIdx = idx
		case "<ASK.PART.CODE>":
			config.askPartIdx = idx
		}
	}

	ensureNoZeros("Invalid bbo configuration:"+line, config.tsIdx, config.bidPriceIdx, config.bidSizeIdx, config.bidPartIdx, config.askPriceIdx, config.askSizeIdx, config.askPartIdx)

	return config
}

// getBBO parses line using configuration and tape. It returns a Quote and a bool.
// If the bool is false then it's a one sided BBO meaning one side is missing.
// Error is not nil if there was an issue with the line's contents
func getBBO(line string, symbol string, configuration BBOConfiguration, tape Tape) (Quote, bool, error) {
	var err error
	parts := splitLineAndCheckPrefix(line, bboPrefix)
	q := Quote{Symbol: symbol, Tape: tape}

	q.Timestamp, err = parseTS(parts[configuration.tsIdx])
	if err != nil {
		return q, false, errors.New("invalid quote - invalid <ACTIVITY.DATETIME>: " + line)
	}
	q.BidPrice = parts[configuration.bidPriceIdx]
	q.BidSize = parts[configuration.bidSizeIdx]
	q.BidExchange = parseParticipant(parts[configuration.bidPartIdx], tape)
	q.AskPrice = parts[configuration.askPriceIdx]
	q.AskSize = parts[configuration.askSizeIdx]
	q.AskExchange = parseParticipant(parts[configuration.askPartIdx], tape)
	q.Condition = "?" // TODO: we also need the condition

	// Missing values are omitted from ICE data but we store them as 0
	if q.BidPrice == "" {
		q.BidPrice = "0"
	}
	if q.BidSize == "" {
		q.BidSize = "0"
	}
	if q.AskPrice == "" {
		q.AskPrice = "0"
	}
	if q.AskSize == "" {
		q.AskSize = "0"
	}

	return q, true, nil
}

// CorrectionConfiguration is the configuration for C| lines.
// The following configurations are likely to happen
// PLUSTICK_545_20151201.txt.gz
// #D=C|<TAS.SEQ>|<ACTIVITY.DATETIME>|<CORRECTION.PREV.TRADE.PRICE>|<CORRECTION.PREV.TRADE.SIZE>|<CORRECTION.PREV.TRADE.COND>|<CORRECTION.NEW.TRADE.PRICE>|<CORRECTION.NEW.TRADE.SIZE>|<CORRECTION.NEW.TRADE.COND>|<RNR.EXCH.ORIGINAL.SEQ>|<RNR.STREAM.ID>|<RNR.END.SEGMENT>|<RNR.END.TOTAL.SEGMENT>|<RNR.END.SESSION.ID>|<RNR.FLAG>|<EXCH.MESSAGE.TIMESTAMP>
// PLUSTICK_545_20210104.txt.gz
// #D=C|<TAS.SEQ>|<ACTIVITY.DATETIME>|<CORRECTION.PREV.TRADE.PRICE>|<CORRECTION.PREV.TRADE.SIZE>|<CORRECTION.PREV.TRADE.COND>|<CORRECTION.NEW.TRADE.PRICE>|<CORRECTION.NEW.TRADE.SIZE>|<CORRECTION.NEW.TRADE.COND>|<RNR.EXCH.ORIGINAL.SEQ>|<RNR.STREAM.ID>|<RNR.END.SEGMENT>|<RNR.END.TOTAL.SEGMENT>|<RNR.END.SESSION.ID>|<RNR.FLAG>|<EXCH.MESSAGE.TIMESTAMP>|<CANCEL.TRADE.SEQ>
// TODO: We have no way of telling what is being corrected where the <CANCEL.TRADE.SEQ> is not present
type CorrectionConfiguration struct {
}

// TradeConfiguration is the configuration for T| lines.
// The following configurations are likely to happen
// PLUSTICK_545_20151201.txt.gz
// #D=T|<TAS.SEQ>|<ACTIVITY.DATETIME>|<TRADE.PRICE>|<TRADE.SIZE>|<TRADE.COND_1>|<PART.CODE>|<EXTENDED.TRADE.COND>|<TRADE.DATETIME>|<RNR.EXCH.ORIGINAL.SEQ>|<RNR.STREAM.ID>|<RNR.END.SEGMENT>|<RNR.END.TOTAL.SEGMENT>|<RNR.END.SESSION.ID>|<RNR.FLAG>|<EXCH.MESSAGE.TIMESTAMP>|<TRADE.COND_2>|<TRADE.COND_3>
// PLUSTICK_545_20210104.txt.gz
// #D=T|<TAS.SEQ>|<ACTIVITY.DATETIME>|<TRADE.PRICE>|<TRADE.SIZE>|<TRADE.COND_1>|<PART.CODE>|<EXTENDED.TRADE.COND>|<TRADE.DATETIME>|<RNR.EXCH.ORIGINAL.SEQ>|<RNR.STREAM.ID>|<RNR.END.SEGMENT>|<RNR.END.TOTAL.SEGMENT>|<RNR.END.SESSION.ID>|<RNR.FLAG>|<EXCH.MESSAGE.TIMESTAMP>|<TRADE.COND_2>|<TRADE.COND_3>|<TRADE.OFFICIAL.TIME>|<TRADE.COND_4>|<TRADE.COND_5>|<TRADE.OFFICIAL.DATE>|<RETRANSMISSION.FLAG>|<TRADE.UNIQUE.ID>
type TradeConfiguration struct {
	tsIdx      int
	priceIdx   int
	sizeIdx    int
	cond1Idx   int
	extCondIdx int
	partIdx    int
	idIdx      int
	seqIdx     int
}

func getTradeConfiguration(line string) TradeConfiguration {
	config := TradeConfiguration{}
	parts := splitLineAndCheckPrefix(line, tradeConfigPrefix)

	for idx, s := range parts {
		switch s {
		case "<ACTIVITY.DATETIME>":
			config.tsIdx = idx
		case "<TRADE.PRICE>":
			config.priceIdx = idx
		case "<TRADE.SIZE>":
			config.sizeIdx = idx
		case "<TRADE.COND_1>":
			config.cond1Idx = idx
		case "<EXTENDED.TRADE.COND>":
			config.extCondIdx = idx
		case "<PART.CODE>":
			config.partIdx = idx
		case "<TRADE.UNIQUE.ID>":
			config.idIdx = idx
		case "<TAS.SEQ>":
			config.seqIdx = idx
		}
	}

	ensureNoZeros("Invalid trade configuration:"+line, config.tsIdx, config.priceIdx, config.sizeIdx, config.cond1Idx, config.extCondIdx, config.partIdx, config.seqIdx)

	return config
}

func getTrade(line string, symbol string, configuration TradeConfiguration, tape Tape) (Trade, error) {
	var err error
	parts := splitLineAndCheckPrefix(line, tradePrefix)
	t := Trade{Symbol: symbol, Tape: tape}

	t.Timestamp, err = parseTS(parts[configuration.tsIdx])
	if err != nil {
		return t, errors.New("invalid trade - invalid ACTIVITY.DATETIME: " + line)
	}

	t.Price = parts[configuration.priceIdx]
	t.Size = parts[configuration.sizeIdx]
	cond1, ok := parseTradeCond1(parts[configuration.cond1Idx], tape)
	if !ok {
		return t, errors.New("invalid trade - invalid TRADE.COND_1: " + line)
	}
	t.Conditions = cond1
	extendedConditions := parts[configuration.extCondIdx]
	// If extendedConditions is present we can use that for conditions
	if extendedConditions != "" {
		c, ok := parseExtCond(extendedConditions, tape)
		if !ok {
			return t, errors.New("invalid trade - invalid EXTENDED.TRADE.COND: " + line)
		}
		t.Conditions = c

	}
	t.Exchange = parseParticipant(parts[configuration.partIdx], tape)
	// If trade TRADE.UNIQUE.ID is not present we can use TAS.SEQ
	if configuration.idIdx != 0 {
		t.TradeID = parts[configuration.idIdx]
	} else {
		t.TradeID = parts[configuration.seqIdx]
	}

	switch {
	case t.Size == "", t.Price == "", t.TradeID == "":
		return t, errors.New("invalid trade - invalid SIZE, PRICE or ID: " + line)
	}
	// There were T| lines in PLUSTICK_545_20210104.txt.gz where <TRADE.UNIQUE.ID> should have been set but was missing
	// 23357510:T|3840|1609808393.5697|0.18|||||1609808393.5697|||||||||||||||. These are pretty ugly lines (and they're under C:TRINA), no other symbols are bad like this

	return t, nil
}

// CancelConfiguration is the configuration for X| lines.
// The following configurations are likely to happen
// PLUSTICK_545_20151201.txt.gz
// #D=X|<TAS.SEQ>|<ACTIVITY.DATETIME>|<TRADE.PRICE>|<TRADE.SIZE>|<TRADE.COND_1>|<PART.CODE>|<EXTENDED.TRADE.COND>|<TRADE.DATETIME>|<RNR.EXCH.ORIGINAL.SEQ>|<RNR.STREAM.ID>|<RNR.END.SEGMENT>|<RNR.END.TOTAL.SEGMENT>|<RNR.END.SESSION.ID>|<RNR.FLAG>|<EXCH.MESSAGE.TIMESTAMP>
// PLUSTICK_545_20210104.txt.gz
// #D=X|<TAS.SEQ>|<ACTIVITY.DATETIME>|<TRADE.PRICE>|<TRADE.SIZE>|<TRADE.COND_1>|<PART.CODE>|<EXTENDED.TRADE.COND>|<TRADE.DATETIME>|<RNR.EXCH.ORIGINAL.SEQ>|<RNR.STREAM.ID>|<RNR.END.SEGMENT>|<RNR.END.TOTAL.SEGMENT>|<RNR.END.SESSION.ID>|<RNR.FLAG>|<EXCH.MESSAGE.TIMESTAMP>
// TODO: How do we tell what was canceled?
type CancelConfiguration struct {
}

func ParseStream(reader io.Reader, mode Mode) (<-chan Trade, <-chan Quote) {

	var (
		currentSymbol string
		headerConfig  HeaderConfiguration
		bboConfig     BBOConfiguration
		tradeConfig   TradeConfiguration
	)

	chanSize := 1024 * 1024
	tradeStream := make(chan Trade, chanSize)
	quoteStream := make(chan Quote, chanSize)

	go func() {
		fileStats := FileStats{}

		modeString := mode.String()
		f, err := os.OpenFile("icetickloader_"+modeString+"_errors.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("couldn't open error file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)

		defer func() {
			// terminate calls panic and we want to write the reason to f
			if got := recover(); got != nil {
				log.Print(got)
			}
		}()

		f2, err := os.OpenFile("icetickloader_"+modeString+"_stats.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			terminate(fmt.Sprintf("couldn't open stats file: %v", err))
		}
		defer f2.Close()

		defer func() {
			// Flushing at the end
			// flush()
			close(tradeStream)
			close(quoteStream)
			fileStats.WriteTo(f2)
		}()

		uninitialized := true

		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			line := scanner.Text()
			idx := strings.Index(line, tokenSep)
			if idx < 0 {
				// The first two lines of a PLUSTICK files don't need to be processed
				continue
			}

			switch line[:idx] {
			case headerConfigPrefix:
				fileStats.finished = true
				// We shouldn't write stats the first time
				if !uninitialized {
					fileStats.WriteTo(f2)
				}
				fileStats.finished = false
				uninitialized = false
				headerConfig = getHeaderConfiguration(line)
				fileStats.trades = 0
				fileStats.quotes = 0
				fileDate = ""
			case headerPrefix:
				newSymbol, newTape := getSymbolAndTape(line, headerConfig)
				// Flushing before dealing with another symbol
				// flush()
				currentSymbol = newSymbol
				currentTape = newTape
				fileStats.tape = currentTape
				symbolSkipper.Reset()
			case bboConfigPrefix:
				bboConfig = getBBOConfiguration(line)
			case bboPrefix:
				if symbolSkipper.IsActive() {
					continue
				}

				q, doubleSided, err := getBBO(line, currentSymbol, bboConfig, currentTape)
				if err != nil {
					symbolSkipper.Skip(currentSymbol, err)
					continue
				}

				if fileDate == "" {
					fileDate = q.Timestamp.Format("2006/01/02")
					fileStats.date = fileDate
				}
				if doubleSided {
					fileStats.quotes++
					if !mode.isTrades() {
						quoteStream <- q
						// quoteBuffer.addToBuffer(q.String())
					}
				}
			case tradeConfigPrefix:
				tradeConfig = getTradeConfiguration(line)
			case tradePrefix:
				if symbolSkipper.IsActive() {
					continue
				}

				t, err := getTrade(line, currentSymbol, tradeConfig, currentTape)
				if err != nil {
					symbolSkipper.Skip(currentSymbol, err)
					continue
				}

				if fileDate == "" {
					fileDate = t.Timestamp.Format("2006/01/02")
					fileStats.date = fileDate
				}
				fileStats.trades++
				if !mode.isQuotes() {
					tradeStream <- t
					// tradeBuffer.addToBuffer(t.String())
				}
			default:
				continue
			}
		}

		if err := scanner.Err(); err != nil {
			terminate("Invalid termination")
		}

		fileStats.finished = true
	}()
	return tradeStream, quoteStream
}

// TODO: handle X/C - we have to be careful with this + batching -------- if we don't deal with these we could batch easily
