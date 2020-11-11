package reorg

type NotificationTypeEnum byte

const (
	Split        NotificationTypeEnum = '7'
	ReverseSplit NotificationTypeEnum = '+'
	Dividend     NotificationTypeEnum = '/'
)
