package kafka

import (
	"errors"

	"github.com/segmentio/kafka-go"
)

// InitTopic
// create topic if not exist
func (p *ProviderKafka) InitTopic(topicName string) error {
	topicsMap, err := p.getTopicsMap()
	if err != nil {
		return errors.New("InitTopic.p.getTopicsMap: " + err.Error())
	}

	_, ok := topicsMap[topicName]
	if ok {
		return nil
	}

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     defaultPartitionsCount,
			ReplicationFactor: defaultReplicationFactor,
		},
	}

	err = p.conn.CreateTopics(topicConfigs...)
	if err != nil {
		return errors.New("InitTopic.p.conn.CreateTopics: " + err.Error())
	}

	return nil
}
