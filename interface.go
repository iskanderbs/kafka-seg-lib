package kafka

import "github.com/segmentio/kafka-go"

// ProviderKafkaInterface
// main kafka provider interface
type ProviderKafkaInterface interface {
	GetConn() *kafka.Conn
	InitTopic(topicName string) error
	SendEvent(event Event) error
	FetchMessages(topic, groupID string, autoCommit bool, msgChan chan<- Message, errChan chan<- error)
	CommitMessage(topic, groupID string, msg *Message) error
}
