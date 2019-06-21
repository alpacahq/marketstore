package iex

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"io"
	"net"

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcapgo"

	"github.com/timpalpant/go-iex/iextp"
	_ "github.com/timpalpant/go-iex/iextp/deep"
	_ "github.com/timpalpant/go-iex/iextp/tops"
)

const (
	magicGzip1         = 0x1f
	magicGzip2         = 0x8b
	pcapNGMagic uint32 = 0x0A0D0D0A

	maxDatagramSize = 65536
)

// PacketDataSource represents a source of decoded network packets
// from a pcap dump or live network connection.
type PacketDataSource interface {
	// NextPayload returns the next decoded packet payload.
	//
	// NOTE: The underlying byte array may be reused in
	// subsequent calls to NextPayload.
	NextPayload() ([]byte, error)
}

// DEPRECATED: Use NewPacketConnDataSource or NewPcapDataSource.
func NewPacketDataSource(r io.Reader) (PacketDataSource, error) {
	// Check for live-streaming packet connection.
	if conn, ok := r.(net.PacketConn); ok {
		return NewPacketConnDataSource(conn), nil
	}

	// Otherwise it must be data from a pcap or pcap-ng dump.
	return NewPcapDataSource(r)
}

// PacketConnDataSource implements PacketDataSource for live UDP
// data connections that implement net.PacketConn.
type PacketConnDataSource struct {
	conn net.PacketConn
	buf  []byte
}

// NewPacketConnDataSource creates a new PacketConnDataSource
// from the given net.PacketConn.
func NewPacketConnDataSource(conn net.PacketConn) *PacketConnDataSource {
	return &PacketConnDataSource{
		conn: conn,
		buf:  make([]byte, maxDatagramSize),
	}
}

// NextPayload implements PacketDataSource.
func (pcds *PacketConnDataSource) NextPayload() ([]byte, error) {
	n, _, err := pcds.conn.ReadFrom(pcds.buf)
	return pcds.buf[:n], err
}

// GopacketDataSource implements PacketDataSource for gopacket.PacketSource.
// It can be used to source the packet payload data from a pcap or pcap-ng file.
type GopacketDataSource struct {
	packetSource *gopacket.PacketSource
}

func NewGopacketDataSource(packetSource *gopacket.PacketSource) *GopacketDataSource {
	return &GopacketDataSource{packetSource}
}

// Create a new GopacketDataSource from the given pcap or pcap-ng file data.
func NewPcapDataSource(r io.Reader) (*GopacketDataSource, error) {
	input := bufio.NewReader(r)
	gzipMagic, err := input.Peek(2)
	if err != nil {
		return nil, err
	}

	if gzipMagic[0] == magicGzip1 && gzipMagic[1] == magicGzip2 {
		if gzf, err := gzip.NewReader(input); err != nil {
			return nil, err
		} else {
			input = bufio.NewReader(gzf)
		}
	}

	magicBuf, err := input.Peek(4)
	if err != nil {
		return nil, err
	}
	magic := binary.LittleEndian.Uint32(magicBuf)

	var packetSource *gopacket.PacketSource
	if magic == pcapNGMagic {
		packetReader, err := pcapgo.NewNgReader(input, pcapgo.DefaultNgReaderOptions)
		if err != nil {
			return nil, err
		}
		packetSource = gopacket.NewPacketSource(packetReader, packetReader.LinkType())
	} else {
		packetReader, err := pcapgo.NewReader(input)
		if err != nil {
			return nil, err
		}
		packetSource = gopacket.NewPacketSource(packetReader, packetReader.LinkType())
	}

	return NewGopacketDataSource(packetSource), nil
}

// NextPayload implements PacketDataSource.
func (gds *GopacketDataSource) NextPayload() ([]byte, error) {
	for {
		packet, err := gds.packetSource.NextPacket()
		if err != nil {
			return nil, err
		}

		if app := packet.ApplicationLayer(); app != nil {
			return app.Payload(), nil
		}
	}
}

// PcapScanner is a high-level reader for iterating through messages from
// from IEX pcap dumps or streaming UDP connections.
type PcapScanner struct {
	packetSource    PacketDataSource
	currentSegment  []iextp.Message
	currentMsgIndex int
}

// Create a new PcapScanner with the given source of network packets.
func NewPcapScanner(packetDataSource PacketDataSource) *PcapScanner {
	return &PcapScanner{
		packetSource: packetDataSource,
	}
}

// Get the next Message in the pcap dump.
// Returns io.EOF if the underlying packet source has no more data.
func (p *PcapScanner) NextMessage() (iextp.Message, error) {
	for p.currentMsgIndex >= len(p.currentSegment) {
		if err := p.nextSegment(); err != nil {
			return nil, err
		}
	}

	msg := p.currentSegment[p.currentMsgIndex]
	p.currentMsgIndex++
	return msg, nil
}

// Read packets until we find the next one with > 0 messages.
// Returns an error if the underlying packet source returns an error,
// or if the payload cannot be decoded as an IEX-TP segment.
func (p *PcapScanner) nextSegment() error {
	for {
		payload, err := p.packetSource.NextPayload()
		if err != nil {
			return err
		}

		segment := iextp.Segment{}
		if err := segment.Unmarshal(payload); err != nil {
			return err
		}

		if len(segment.Messages) != 0 {
			p.currentSegment = segment.Messages
			p.currentMsgIndex = 0
			return nil
		}
	}
}
