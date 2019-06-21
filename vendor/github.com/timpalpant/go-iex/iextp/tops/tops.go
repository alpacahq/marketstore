// Package tops implements an unmarshaler for the TOPS protocol, v1.6.
package tops

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/timpalpant/go-iex/iextp"
)

const (
	ChannelID               uint32 = 1
	V_1_5_MessageProtocolID uint16 = 0x8002
	V_1_6_MessageProtocolID uint16 = 0x8003
	FeedName                       = "TOPS"
)

const (
	// Administrative message formats.
	SystemEvent              = 0x53
	SecurityDirectory        = 0x44
	TradingStatus            = 0x48
	OperationalHaltStatus    = 0x4f
	ShortSalePriceTestStatus = 0x50

	// Trading message formats.
	QuoteUpdate   = 0x51
	TradeReport   = 0x54
	TradeBreak    = 0x42
	OfficialPrice = 0x58

	// Auction message formats.
	AuctionInformation = 0x41
)

func init() {
	// This package can parse both TOPS v1.5 and TOPS v1.6.
	iextp.RegisterProtocol(V_1_5_MessageProtocolID, Unmarshal)
	iextp.RegisterProtocol(V_1_6_MessageProtocolID, Unmarshal)
}

// Implements the TOPS protocol, v1.6.
func Unmarshal(buf []byte) (iextp.Message, error) {
	if len(buf) == 0 {
		return nil, fmt.Errorf("cannot unmarshal %v-length buffer", len(buf))
	}

	var msg iextp.Message

	messageType := buf[0]
	switch messageType {
	case SystemEvent:
		msg = &SystemEventMessage{}
	case SecurityDirectory:
		msg = &SecurityDirectoryMessage{}
	case TradingStatus:
		msg = &TradingStatusMessage{}
	case OperationalHaltStatus:
		msg = &OperationalHaltStatusMessage{}
	case ShortSalePriceTestStatus:
		msg = &ShortSalePriceTestStatusMessage{}
	case QuoteUpdate:
		msg = &QuoteUpdateMessage{}
	case TradeReport:
		msg = &TradeReportMessage{}
	case OfficialPrice:
		msg = &OfficialPriceMessage{}
	case TradeBreak:
		msg = &TradeBreakMessage{}
	case AuctionInformation:
		msg = &AuctionInformationMessage{}
	default:
		msg = &iextp.UnsupportedMessage{}
	}

	err := msg.Unmarshal(buf)
	return msg, err
}

// Parse the TOPS timestamp type: 8 bytes, signed integer containing
// a counter of nanoseconds since POSIX (Epoch) time UTC,
// into a native time.Time.
func ParseTimestamp(buf []byte) time.Time {
	timestampNs := int64(binary.LittleEndian.Uint64(buf))
	return time.Unix(0, timestampNs).In(time.UTC)
}

// Parse the TOPS event time: 4 bytes, unsigned integer containing
// a counter of seconds since POSIX (Epoch) time UTC,
// into a native time.Time
func ParseEventTime(buf []byte) time.Time {
	timestampSecs := binary.LittleEndian.Uint32(buf)
	return time.Unix(int64(timestampSecs), 0).In(time.UTC)
}

// Parse the TOPS price type: 8 bytes, signed integer containing
// a fixed-point number with 4 digits to the right of an implied
// decimal point, into a float64.
func ParseFloat(buf []byte) float64 {
	n := int64(binary.LittleEndian.Uint64(buf))
	return float64(n) / 10000
}

// Parse the TOPS string type: fixed-length ASCII byte sequence,
// left justified and space filled on the right.
func ParseString(buf []byte) string {
	return strings.TrimRight(string(buf), " ")
}

// SystemEventMessage is used to indicate events that apply
// to the market or the data feed.
//
// There will be a single message disseminated per channel for each
// System Event type within a given trading session.
type SystemEventMessage struct {
	MessageType uint8
	// System event identifier.
	SystemEvent uint8
	// Time stamp of the system event.
	Timestamp time.Time
}

func (m *SystemEventMessage) Unmarshal(buf []byte) error {
	if len(buf) < 10 {
		return fmt.Errorf(
			"cannot unmarshal SystemEventMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.SystemEvent = uint8(buf[1])
	m.Timestamp = ParseTimestamp(buf[2:10])

	return nil
}

const (
	// Outside of heartbeat messages on the lower level protocol,
	// the start of day message is the first message in any trading session.
	StartOfMessages uint8 = 0x4f
	// This message indicates that IEX is open and ready to start accepting
	// orders.
	StartOfSystemHours uint8 = 0x53
	// This message indicates that DAY and GTX orders, as well as
	// market orders and pegged orders, are available for execution on IEX.
	StartOfRegularMarketHours uint8 = 0x52
	// This message indicates that DAY orders, market orders, and pegged
	// orders are no longer accepted by IEX.
	EndOfRegularMarketHours uint8 = 0x4d
	// This message indicates that IEX is now closed and will not accept
	// any new orders during this trading session. It is still possible to
	// receive messages after the end of day.
	EndOfSystemHours uint8 = 0x45
	// This is always the last message sent in any trading session.
	EndOfMessages uint8 = 0x43
)

// IEX disseminates a full pre-market spin of SecurityDirectoryMessages for
// all IEX-listed securities. After the pre-market spin, IEX will use the
// SecurityDirectoryMessage to relay changes for an individual security.
type SecurityDirectoryMessage struct {
	MessageType uint8
	// See Appendix A for flag values.
	Flags uint8
	// The time of the update event as set by the IEX Trading System logic.
	Timestamp time.Time
	// IEX-listed security represented in Nasdaq Integrated symbology.
	Symbol string
	// The number of shares that represent a round lot for the security.
	RoundLotSize uint32
	// The corporate action adjusted previous official closing price for
	// the security (e.g. stock split, dividend, rights offering).
	// When no corporate action has occurred, the Adjusted POC Price
	// will be populated with the previous official close price. For
	// new issues (e.g., an IPO), this field will be the issue price.
	AdjustedPOCPrice float64
	// Indicates which Limit Up-Limit Down price band calculation
	// parameter is to be used.
	LULDTier uint8
}

func (m *SecurityDirectoryMessage) Unmarshal(buf []byte) error {
	if len(buf) < 31 {
		return fmt.Errorf(
			"cannot unmarshal SecurityDirectoryMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.Flags = uint8(buf[1])
	m.Timestamp = ParseTimestamp(buf[2:10])
	m.Symbol = ParseString(buf[10:18])
	m.RoundLotSize = binary.LittleEndian.Uint32(buf[18:22])
	m.AdjustedPOCPrice = ParseFloat(buf[22:30])
	m.LULDTier = uint8(buf[30])

	return nil
}

func (m *SecurityDirectoryMessage) IsTestSecurity() bool {
	return m.Flags&0x80 != 0
}

func (m *SecurityDirectoryMessage) IsWhenIssuedSecurity() bool {
	return m.Flags&0x40 != 0
}

func (m *SecurityDirectoryMessage) IsETP() bool {
	return m.Flags&0x20 != 0
}

const (
	// Not applicable.
	LULDTier0 uint8 = 0x0
	// Tier 1 NMS Stock.
	LULDTier1 uint8 = 0x1
	// Tier 2 NMS Stock.
	LULDTier2 uint8 = 0x2
)

// The Trading status message is used to indicate the current trading status
// of a security. For IEX-listed securities, IEX acts as the primary market
// and has the authority to institute a trading halt or trading pause in a
// security due to news dissemination or regulatory reasons. For
// non-IEX-listed securities, IEX abides by any regulatory trading halts
// and trading pauses instituted by the primary or listing market, as
// applicable.
//
// IEX disseminates a full pre-market spin of Trading status messages
// indicating the trading status of all securities. In the spin, IEX will
// send out a Trading status message with “T” (Trading) for all securities
// that are eligible for trading at the start of the Pre-Market Session.
// If a security is absent from the dissemination, firms should assume
// that the security is being treated as operationally halted in the IEX
// Trading System.
//
// After the pre-market spin, IEX will use the Trading status message to
// relay changes in trading status for an individual security. Messages
// will be sent when a security is:
//
//     Halted
//     Paused*
//     Released into an Order Acceptance Period*
//     Released for trading
//
// *The paused and released into an Order Acceptance Period status will be
// disseminated for IEX-listed securities only. Trading pauses on
// non-IEX-listed securities will be treated simply as a halt.
type TradingStatusMessage struct {
	MessageType uint8
	// Trading status.
	TradingStatus uint8
	// The time of the update event as set by the IEX Trading System logic.
	Timestamp time.Time
	// Security represented in Nasdaq integrated symbology.
	Symbol string
	// IEX populates the Reason field for IEX-listed securities when the
	// TradingStatus is TradingHalted or OrderAcceptancePeriod.
	// For non-IEX listed securities, the Reason field will be set to
	// ReasonNotAvailable when the trading status is TradingHalt.
	// The Reason will be blank when the trading status is TradingPause
	// or Trading.
	Reason string
}

func (m *TradingStatusMessage) Unmarshal(buf []byte) error {
	if len(buf) < 22 {
		return fmt.Errorf(
			"cannot unmarshal TradingStatusMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.TradingStatus = uint8(buf[1])
	m.Timestamp = ParseTimestamp(buf[2:10])
	m.Symbol = ParseString(buf[10:18])
	m.Reason = ParseString(buf[18:22])
	return nil
}

const (
	// Trading halted across all US equity markets.
	TradingHalt uint8 = 0x48
	// Trading halt released into an Order Acceptance Period
	// (IEX-listed securities only)
	TradingOrderAcceptancePeriod uint8 = 0x4f
	// Trading paused and Order Acceptance Period on IEX
	// (IEX-listed securities only)
	TradingPaused uint8 = 0x50
	// Trading on IEX
	Trading uint8 = 0x54
)

const (
	// Trading halt reasons.
	HaltNewsPending            = "T1"
	IPOIssueNotYetTrading      = "IPO1"
	IPOIssueDeferred           = "IPOD"
	MarketCircuitBreakerLevel3 = "MCB3"
	ReasonNotAvailable         = "NA"

	// Order Acceptance Period Reasons
	HaltNewsDisseminations           = "T2"
	IPONewIssueOrderAcceptancePeriod = "IPO2"
	IPOPreLaunchPeriod               = "IPO3"
	MarketCircuitBreakerLevel1       = "MCB1"
	MarketCircuitBreakerLevel2       = "MCB2"
)

// The Exchange may suspend trading of one or more securities on IEX for
// operational reasons and indicates such operational halt using the
// OperationalHaltStatusMessage.
//
// IEX disseminates a full pre-market spin of OperationalHaltStatusMessages
// indicating the operational halt status of all securities. In the spin,
// IEX will send out an OperationalHaltStatusMessage with
// NotOperationallyHalted for all securities that are eligible for trading
// at the start of the Pre-Market Session. If a security is absent from
// the dissemination, firms should assume that the security is being
// treated as operationally halted in the IEX Trading System at the start
// of the Pre-Market Session.
//
// After the pre-market spin, IEX will use the OperationalHaltStatusMessage
// to relay changes in operational halt status for an individual security.
type OperationalHaltStatusMessage struct {
	MessageType uint8
	// Operational halt status identifier
	OperationalHaltStatus uint8
	// The time of the update event as set by the IEX Trading System logic.
	Timestamp time.Time
	// Security represented in Nasdaq integrated symbology.
	Symbol string
}

func (m *OperationalHaltStatusMessage) Unmarshal(buf []byte) error {
	if len(buf) < 18 {
		return fmt.Errorf(
			"cannot unmarshal OperationalHaltStatusMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.OperationalHaltStatus = uint8(buf[1])
	m.Timestamp = ParseTimestamp(buf[2:10])
	m.Symbol = ParseString(buf[10:18])
	return nil
}

const (
	IEXSpecificOperationalHalt uint8 = 0x4f
	NotOperationallyHalted     uint8 = 0x4e
)

// In association with Rule 201 of Regulation SHO, the ShortSalePriceTestStatusMessage
// is used to indicate when a short sale price test restriction is in effect for a
// security.
//
// IEX disseminates a full pre-market spin of ShortSalePriceTestStatusMessages
// indicating the Rule 201 status of all securities. After the pre-market spin, IEX
// will use the ShortSalePriceTestStatusMessage in the event of an intraday
// status change.
//
// The IEX Trading system will process orders based on the latest short sale
// price test restriction status.
type ShortSalePriceTestStatusMessage struct {
	MessageType uint8
	// Whether or not the ShortSalePriceTest is in effect.
	ShortSalePriceTestStatus bool
	// The time of the update as set by the IEX Trading System logic.
	Timestamp time.Time
	// Security represented in Nasdaq integrated symbology.
	Symbol string
	// IEX populates the Detail field for IEX-listed securities;
	// this field will be set to DetailNotAvailable for non-IEX-listed
	// securities.
	Detail uint8
}

func (m *ShortSalePriceTestStatusMessage) Unmarshal(buf []byte) error {
	if len(buf) < 19 {
		return fmt.Errorf(
			"cannot unmarshal ShortSalePriceTestStatusMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.ShortSalePriceTestStatus = (uint8(buf[1]) != 0)
	m.Timestamp = ParseTimestamp(buf[2:10])
	m.Symbol = ParseString(buf[10:18])
	m.Detail = uint8(buf[18])
	return nil
}

const (
	// No price test in place.
	NoPriceTest uint8 = 0x20
	// Short sale price test restriction in effect due to an intraday
	// price drop in the security.
	ShortSalePriceTestActivated uint8 = 0x41
	// Short sale price test restriction remains in effect from prior day.
	ShortSalePriceTestContinued uint8 = 0x43
	// Short sale price test restriction deactivated.
	ShortSalePriceTestDeactivated uint8 = 0x44
	// Detail not available.
	DetailNotAvailable uint8 = 0x4e
)

// TOPS broadcasts a real-time QuoteUpdateMessage each time IEX's best bid
// or offer quotation is updated during the trading day. Prior to the start
// of trading, IEX publishes a "zero quote" (Bid Price, Bid Size, Ask Price,
// and Ask Size are zero) for all symbols in the IEX trading system.
type QuoteUpdateMessage struct {
	MessageType uint8
	Flags       uint8
	// The time an event triggered the quote update as set by the IEX Trading
	// System logic.
	Timestamp time.Time
	// Quoted symbol representation in Nasdaq integrated symbology.
	Symbol string
	// Size of the quote at the bid, in number of shares.
	BidSize uint32
	// Price of the quote at the bid.
	BidPrice float64
	// Price of the quote at the ask.
	AskPrice float64
	// Size of the quote at the ask, in number of shares.
	AskSize uint32
}

func (m *QuoteUpdateMessage) Unmarshal(buf []byte) error {
	if len(buf) < 42 {
		return fmt.Errorf(
			"cannot unmarshal QuoteUpdateMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.Flags = uint8(buf[1])
	m.Timestamp = ParseTimestamp(buf[2:10])
	m.Symbol = ParseString(buf[10:18])
	m.BidSize = binary.LittleEndian.Uint32(buf[18:22])
	m.BidPrice = ParseFloat(buf[22:30])
	m.AskPrice = ParseFloat(buf[30:38])
	m.AskSize = binary.LittleEndian.Uint32(buf[38:42])
	return nil
}

func (m *QuoteUpdateMessage) IsActive() bool {
	return m.Flags&0x80 == 0
}

func (m *QuoteUpdateMessage) IsRegularMarketSession() bool {
	return m.Flags&0x40 == 0
}

// TradeReportMessages are sent when an order on the IEX Order Book
// is executed in whole or in part. TOPS sends a TradeReportMessage
// for every individual fill.
type TradeReportMessage struct {
	MessageType        uint8
	SaleConditionFlags uint8
	// The time an event triggered the trade (i.e., execution) as set
	// by the IEX Trading System logic.
	Timestamp time.Time
	// Traded symbol represented in Nasdaq integrated symbology.
	Symbol string
	// Size of the trade, in number of shares.
	Size uint32
	// Execution price.
	Price float64
	// IEX generated trade identifier. A given trade is uniquely
	// identified within a day by its TradeID.
	TradeID int64
}

func (m *TradeReportMessage) Unmarshal(buf []byte) error {
	if len(buf) < 38 {
		return fmt.Errorf(
			"cannot unmarshal TradeReportMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.SaleConditionFlags = uint8(buf[1])
	m.Timestamp = ParseTimestamp(buf[2:10])
	m.Symbol = ParseString(buf[10:18])
	m.Size = binary.LittleEndian.Uint32(buf[18:22])
	m.Price = ParseFloat(buf[22:30])
	m.TradeID = int64(binary.LittleEndian.Uint64(buf[30:38]))
	return nil
}

// Trade resulted from an Intermarket Sweep Order.
func (m *TradeReportMessage) IsISO() bool {
	return m.SaleConditionFlags&0x80 != 0
}

// Trade occurred before or after the regular market session (i.e., Form T).
func (m *TradeReportMessage) IsExtendedHoursTrade() bool {
	return m.SaleConditionFlags&0x40 != 0
}

// Trade is less than one round lot.
func (m *TradeReportMessage) IsOddLot() bool {
	return m.SaleConditionFlags&0x20 != 0
}

// Whether the trade is subject to Rule 611 (Trade Through)
// of SEC Regulation NMS.
func (m *TradeReportMessage) IsTradeThroughExempt() bool {
	return m.SaleConditionFlags&0x10 != 0
}

func (m *TradeReportMessage) IsSinglePriceCrossTrade() bool {
	return m.SaleConditionFlags&0x08 != 0
}

func (m *TradeReportMessage) IsLastSaleEligible() bool {
	return !m.IsExtendedHoursTrade() && !m.IsOddLot()
}

func (m *TradeReportMessage) IsHighLowPriceEligible() bool {
	return !m.IsExtendedHoursTrade() && !m.IsOddLot()
}

func (m *TradeReportMessage) IsVolumeEligible() bool {
	return true
}

const (
	// IEX official opening price.
	OpeningPrice uint8 = 0x51
	// IEX official closing price.
	ClosingPrice uint8 = 0x4d
)

type OfficialPriceMessage struct {
	MessageType uint8
	// Price type identifier (OpeningPrice or ClosingPrice).
	PriceType uint8
	// The time an event triggered the official price calculation
	// (e.g., auction match) as set by the IEX Trading System logic.
	Timestamp time.Time
	// Security represented in Nasdaq Integrated symbology.
	Symbol string
	// IEX Official Opening or Closing Price of an IEX-listed security.
	OfficialPrice float64
}

func (m *OfficialPriceMessage) Unmarshal(buf []byte) error {
	if len(buf) < 26 {
		return fmt.Errorf(
			"cannot unmarshal OfficialMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.PriceType = uint8(buf[1])
	m.Timestamp = ParseTimestamp(buf[2:10])
	m.Symbol = ParseString(buf[10:18])
	m.OfficialPrice = ParseFloat(buf[18:26])
	return nil
}

// TradeBreakMessages are sent when an execution on IEX is broken
// on that same trading day. Trade breaks are rare and only affect
// applications that rely upon IEX execution based data.
type TradeBreakMessage struct {
	MessageType        uint8
	SaleConditionFlags uint8
	// The time an event triggered the trade (i.e., execution) as set
	// by the IEX Trading System logic.
	Timestamp time.Time
	// Traded symbol represented in Nasdaq integrated symbology.
	Symbol string
	// Size of the trade, in number of shares.
	Size uint32
	// Execution price.
	Price float64
	// IEX generated trade identifier. A given trade is uniquely
	// identified within a day by its TradeID.
	TradeID int64
}

func (m *TradeBreakMessage) Unmarshal(buf []byte) error {
	if len(buf) < 38 {
		return fmt.Errorf(
			"cannot unmarshal TradeBreakMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.SaleConditionFlags = uint8(buf[1])
	m.Timestamp = ParseTimestamp(buf[2:10])
	m.Symbol = ParseString(buf[10:18])
	m.Size = binary.LittleEndian.Uint32(buf[18:22])
	m.Price = ParseFloat(buf[22:30])
	m.TradeID = int64(binary.LittleEndian.Uint64(buf[30:38]))
	return nil
}

// DEEP broadcasts an AuctionInformationmessage every one second between
// the Lock-in Time and the auction match for Opening and Closing Auctions,
// and during the Display Only Period for IPO, Halt, and Volatility Auctions.
// Only IEX-listed securities are eligible for IEX Auctions.
type AuctionInformationMessage struct {
	MessageType uint8
	AuctionType uint8
	// The time of the update event as set by the IEX Trading System logic.
	Timestamp time.Time
	// IEX-listed security represented in Nasdaq integrated symbology.
	Symbol string
	// Number of shares paried at the Reference Price using orders on the
	// Auction Book.
	PairedShares uint32
	// Clearing price at or within the Reference Price Range using orders
	// on the Auction Book.
	ReferencePrice float64
	// Clearing price using Eligible Auction Orders.
	IndicativeClearingPrice float64
	// Number of unpaired shares at the Reference Price, using orders
	// on the Auction Book.
	ImbalanceShares uint32
	// Side of the imbalance.
	ImbalanceSide uint8
	// Total number of automatic extensions an IPO, Halt, or Volatility
	// auction has received.
	ExtensionNumber uint8
	// Projected time of the auction match.
	ScheduledAuctionTime time.Time
	// Clearing price using orders on the Auction Book.
	AuctionBookClearingPrice float64
	// Reference price used for the auction collar, if any.
	CollarReferencePrice float64
	// Lower threshold price of the auction collar, if any.
	LowerAuctionCollar float64
	// Upper threshold price of the auction caller, if any.
	UpperAuctionCollar float64
}

func (m *AuctionInformationMessage) Unmarshal(buf []byte) error {
	if len(buf) < 80 {
		return fmt.Errorf(
			"cannot unmarshal AuctionInformationMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.AuctionType = uint8(buf[1])
	m.Timestamp = ParseTimestamp(buf[2:10])
	m.Symbol = ParseString(buf[10:18])
	m.PairedShares = binary.LittleEndian.Uint32(buf[18:22])
	m.ReferencePrice = ParseFloat(buf[22:30])
	m.IndicativeClearingPrice = ParseFloat(buf[30:38])
	m.ImbalanceShares = binary.LittleEndian.Uint32(buf[38:42])
	m.ImbalanceSide = uint8(buf[42])
	m.ExtensionNumber = uint8(buf[43])
	m.ScheduledAuctionTime = ParseEventTime(buf[44:48])
	m.AuctionBookClearingPrice = ParseFloat(buf[48:56])
	m.CollarReferencePrice = ParseFloat(buf[56:64])
	m.LowerAuctionCollar = ParseFloat(buf[64:72])
	m.UpperAuctionCollar = ParseFloat(buf[72:80])
	return nil
}

// Auction types.
const (
	OpeningAuction    uint8 = 0x4f
	ClosingAuction    uint8 = 0x43
	IPOAuction        uint8 = 0x49
	HaltAuction       uint8 = 0x48
	VolatilityAuction uint8 = 0x56
)

// The side of the imbalance.
const (
	BuySideImbalance  uint8 = 0x42
	SellSideImbalance uint8 = 0x53
	NoImbalance       uint8 = 0x4e
)
