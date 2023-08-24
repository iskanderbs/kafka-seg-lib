package kafka

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
)

// SendEvent
// send event to topic
func (p *ProviderKafka) SendEvent(event Event) error {
	w := p.getWriter(event.IsGuaranteed)

	jsonData, err := json.Marshal(event.EventValue)
	if err != nil {
		return errors.New("SendEvent.json.Marshal: " + err.Error())
	}

	msg := kafka.Message{
		Key:   []byte(event.EventID),
		Value: jsonData,
		Time:  time.Now().UTC(),
		Topic: event.Topic,
	}

	err = w.WriteMessages(p.ctx, msg)
	if err != nil {
		return errors.New("SendEvent.w.WriteMessages: " + err.Error())
	}

	return nil
}
