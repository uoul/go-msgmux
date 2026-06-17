package msgmux

type MessageId string

type MessageType string

type Message[T any] struct {
	MsgId MessageId
	Type  MessageType
	Error *string
	Body  T
}
