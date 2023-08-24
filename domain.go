package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// ProviderKafka
// kafka provider entity
type ProviderKafka struct {
	ctx  context.Context
	conn *kafka.Conn
}

// Event
// kafka event entity
type Event struct {
	Topic        string
	EventID      string
	EventValue   interface{}
	IsGuaranteed bool
}

// Message
// kafka message entity
type Message struct {
	SourceMsg kafka.Message
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Value     []byte
}
