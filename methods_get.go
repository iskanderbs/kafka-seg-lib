package kafka

import "github.com/segmentio/kafka-go"

// GetConn
// get apache kafka conn
func (p *ProviderKafka) GetConn() *kafka.Conn {
	return p.conn
}
