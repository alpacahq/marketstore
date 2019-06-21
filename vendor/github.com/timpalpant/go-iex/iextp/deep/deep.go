// Package deep implements an unmarshaler for the DEEP protocol, v1.0.
package deep

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/timpalpant/go-iex/iextp"
	"github.com/timpalpant/go-iex/iextp/tops"
)

const (
	ChannelID               uint32 = 1
	V_1_0_MessageProtocolID uint16 = 0x8004
	FeedName                       = "DEEP"
)

const (
	SystemEvent              = tops.SystemEvent
	SecurityDirectory        = tops.SecurityDirectory
	TradingStatus            = tops.TradingStatus
	OperationalHaltStatus    = tops.OperationalHaltStatus
	ShortSalePriceTestStatus = tops.ShortSalePriceTestStatus
	AuctionInformation       = tops.AuctionInformation
	TradeReport              = tops.TradeReport
	OfficialPrice            = tops.OfficialPrice
	TradeBreak               = tops.TradeBreak

	SecurityEvent            = 0x45
	PriceLevelUpdateBuySide  = 0x38
	PriceLevelUpdateSellSide = 0x35
)

func init() {
	iextp.RegisterProtocol(V_1_0_MessageProtocolID, Unmarshal)
}

// Implements the DEEP protocol, v1.0.
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
	case SecurityEvent:
		msg = &SecurityEventMessage{}
	case PriceLevelUpdateBuySide:
		msg = &PriceLevelUpdateMessage{}
	case PriceLevelUpdateSellSide:
		msg = &PriceLevelUpdateMessage{}
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

type SystemEventMessage = tops.SystemEventMessage
type SecurityDirectoryMessage = tops.SecurityDirectoryMessage
type TradingStatusMessage = tops.TradingStatusMessage
type OperationalHaltStatusMessage = tops.OperationalHaltStatusMessage
type ShortSalePriceTestStatusMessage = tops.ShortSalePriceTestStatusMessage
type TradeReportMessage = tops.TradeReportMessage
type OfficialPriceMessage = tops.OfficialPriceMessage
type TradeBreakMessage = tops.TradeBreakMessage
type AuctionInformationMessage = tops.AuctionInformationMessage

// The SecurityEventMessage is used to indicate events that apply
// to a security.
type SecurityEventMessage struct {
	MessageType uint8
	// Security event identifier.
	SecurityEvent uint8
	// The time of the update event as set by the IEX Trading System logic.
	Timestamp time.Time
	// IEX-listed security represented in Nasdaq Integrated symbology.
	Symbol string
}

func (m *SecurityEventMessage) Unmarshal(buf []byte) error {
	if len(buf) < 18 {
		return fmt.Errorf(
			"cannot unmarshal SecurityEventMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.SecurityEvent = uint8(buf[1])
	m.Timestamp = tops.ParseTimestamp(buf[2:10])
	m.Symbol = tops.ParseString(buf[10:18])
	return nil
}

// Security event types.
const (
	// Indicates that the opening process is complete in this security
	// and any orders queued during the Pre-Market Session are now
	// available for execution on the IEX Order Book for the subject
	// security.
	OpeningProcessComplete uint8 = 0x4f
	// For non-IEX-listed securities, this message indicates that
	// IEX has completed canceling orders from the IEX Order Book
	// for the subject security that are not eligible for the
	// Post-Market Session. For IEX-listed securities, this message
	// indicates that the closing process (e.g. Closing Auction)
	// has completed for this security and IEX has completed canceling
	// orders from the IEX Order Book for the subject security that
	// are not eligible for the Post-Market Session.
	ClosingProcessComplete uint8 = 0x43
)

type PriceLevelUpdateMessage struct {
	MessageType uint8
	EventFlags  uint8
	// The time of the update event as set by the IEX Trading System logic.
	Timestamp time.Time
	// IEX-listed security represented in Nasdaq Integrated symbology.
	Symbol string
	// Aggregated quoted size.
	Size uint32
	// Price level to add/update in the IEX Order Book.
	Price float64
}

func (m *PriceLevelUpdateMessage) IsBuySide() bool {
	return m.MessageType == PriceLevelUpdateBuySide
}

func (m *PriceLevelUpdateMessage) IsSellSide() bool {
	return m.MessageType == PriceLevelUpdateSellSide
}

func (m *PriceLevelUpdateMessage) EventProcessingComplete() bool {
	return m.EventFlags&0x1 != 0
}

func (m *PriceLevelUpdateMessage) Unmarshal(buf []byte) error {
	if len(buf) < 18 {
		return fmt.Errorf(
			"cannot unmarshal SecurityEventMessage from %v-length buffer",
			len(buf))
	}

	m.MessageType = uint8(buf[0])
	m.EventFlags = uint8(buf[1])
	m.Timestamp = tops.ParseTimestamp(buf[2:10])
	m.Symbol = tops.ParseString(buf[10:18])
	m.Size = binary.LittleEndian.Uint32(buf[18:22])
	m.Price = tops.ParseFloat(buf[22:30])
	return nil
}
