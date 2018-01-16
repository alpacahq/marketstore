package executor

// Message Types for WAL Messages
// --- Message ID
type MIDEnum int8

const (
	TGDATA MIDEnum = iota
	TXNINFO
	STATUS
)

// --- Destination ID
type DestEnum int8

const (
	WAL DestEnum = iota
	CHECKPOINT
)

// --- Status
type TxnStatusEnum int8

const (
	PREPARING TxnStatusEnum = iota
	COMMITINTENDED
	COMMITCOMPLETE
)

type FileStatusEnum int8

const (
	Invalid FileStatusEnum = iota
	OPEN
	CLOSED
)

type ReplayStateEnum int8

const (
	Invalid2 ReplayStateEnum = iota
	NOTREPLAYED
	REPLAYED
	REPLAYINPROCESS
)
