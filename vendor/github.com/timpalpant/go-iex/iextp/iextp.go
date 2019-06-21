package iextp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// Size of the segment header, in bytes.
const segmentHeaderSize uint16 = 40

// Protocol represents a higher-level IEXTP protocol, such as TOPS or DEEP.
// A Protocol unmarshals a Message received in an IEXTP segment.
// Note that buf contains only the message content and not the
// segment header.
type Protocol func(buf []byte) (Message, error)

var protocolRegistry = map[uint16]Protocol{}

// Register an IEXTP protocol to use for decoding Segment Messages.
// RegisterProtocol should be called at init time by packages that implement
// IEXTP protocols, such as TOPS and DEEP.
func RegisterProtocol(messageProtocolID uint16, p Protocol) {
	protocolRegistry[messageProtocolID] = p
}

// Segment represents an IEXTP Segment.
type Segment struct {
	Header   SegmentHeader
	Messages []Message
}

func (s *Segment) Unmarshal(buf []byte) error {
	// Unmarshal segment header.
	if err := s.Header.Unmarshal(buf); err != nil {
		return err
	}

	if int(s.Header.PayloadLength) != len(buf)-int(segmentHeaderSize) {
		return io.ErrUnexpectedEOF
	}

	protocol, ok := protocolRegistry[s.Header.MessageProtocolID]
	if !ok {
		return fmt.Errorf("unknown message protocol: %v",
			s.Header.MessageProtocolID)
	}

	cur := segmentHeaderSize // Current position in buf.
	s.Messages = make([]Message, s.Header.MessageCount)
	for i := uint16(0); i < s.Header.MessageCount; i++ {
		if int(cur+2) > len(buf) {
			return errors.New(
				"invalid segment: message exceeds payload length")
		}

		// Messages are variable-length depending on their type.
		// Get the length of the next message in the segment.
		messageLength := binary.LittleEndian.Uint16(buf[cur : cur+2])
		cur += 2

		if int(cur+messageLength) > len(buf) {
			return errors.New(
				"invalid segment: message exceeds payload length")
		}

		// Unmarshal the message.
		msgBuf := buf[cur : cur+messageLength]
		cur += messageLength
		msg, err := protocol(msgBuf)
		if err != nil {
			return err
		}

		s.Messages[i] = msg
	}

	return nil
}

// Message represents an IEXTP message.
type Message interface {
	// Unmarshal unmarshals the given byte content into the Message.
	// Note that buf includes the entire message content, including the
	// leading message type byte.
	//
	// IEX reserves the right to grow the message length without notice,
	// but only by adding additional data to the end of the message, so
	// decoders should handle messages that grow beyond the expected
	// length.
	Unmarshal(buf []byte) error
}

// UnsupportedMessage may be returned by a protocol for any
// message types it does not know how to decode.
type UnsupportedMessage struct {
	MessageType uint8
	Message     []byte
}

func (m *UnsupportedMessage) Unmarshal(buf []byte) error {
	m.MessageType = uint8(buf[0])
	m.Message = buf
	return nil
}

type SegmentHeader struct {
	// Version of the IEX-TP protocol.
	Version uint8
	// Reserved byte.
	_ uint8
	// A unique identifier for the higher-layer specification that describes
	// the messages contaiend within a segment. See the higher-layer protocol
	// specification for the protocol's message identification in IEX-TP.
	MessageProtocolID uint16
	// An identifier for a given stream of bytes/sequenced messages. Messages
	// received from multiple sources which use the same Channel ID are
	// guaranteed to be duplicates by sequence number and/or offset. See the
	// higher-layer protocol specification for the protocol's channel
	// identification on IEX-TP.
	ChannelID uint32
	// SessionID uniquely identifies a stream of messages produced by the
	// system. A given message is uniquely identified within a message
	// protocol by its Session ID and Sequence Number.
	SessionID uint32
	// PayloadLength is an unsigned binary count representing the number
	// of bytes contained in the segment's payload. Note that the Payload
	// Length field value does not include the length of the IEX-TP
	// header.
	PayloadLength uint16
	// MessageCount is a count representing the number of Message Blocks
	// in the segment.
	MessageCount uint16
	// StreamOffset is a counter representing the byte offset of the payload
	// in the data stream.
	StreamOffset int64
	// FirstMessageSequenceNumber is a counter representing the sequence
	// number of the first message in the segment. If there is more than one
	// message in a segment, all subsequent messages are implicitly
	// numbered sequentially.
	FirstMessageSequenceNumber int64
	// The time the outbound segment was sent as set by the sender.
	SendTime time.Time
}

func (sh *SegmentHeader) Unmarshal(buf []byte) error {
	if len(buf) < 40 {
		return fmt.Errorf(
			"cannot unmarshal SegmentHeader from %v-length buffer",
			len(buf))
	}

	sh.Version = uint8(buf[0])
	sh.MessageProtocolID = binary.LittleEndian.Uint16(buf[2:4])
	sh.ChannelID = binary.LittleEndian.Uint32(buf[4:8])
	sh.SessionID = binary.LittleEndian.Uint32(buf[8:12])
	sh.PayloadLength = binary.LittleEndian.Uint16(buf[12:14])
	sh.MessageCount = binary.LittleEndian.Uint16(buf[14:16])
	sh.StreamOffset = int64(binary.LittleEndian.Uint64(buf[16:24]))
	sh.FirstMessageSequenceNumber = int64(binary.LittleEndian.Uint64(buf[24:32]))
	timestampNs := int64(binary.LittleEndian.Uint64(buf[32:40]))
	sh.SendTime = time.Unix(0, timestampNs).In(time.UTC)
	return nil
}
