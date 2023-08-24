package kafka

import (
	"errors"

	"github.com/segmentio/kafka-go"
)

// FetchMessages
// fetch incoming messages worker
func (p *ProviderKafka) FetchMessages(topic, groupID string, autoCommit bool, msgChan chan<- Message, errChan chan<- error) {
	reader := p.getReader(topic, groupID)

	for {
		select {
		case <-p.ctx.Done():
			return

		default:
			var (
				msg kafka.Message
				err error
			)

			if autoCommit {
				msg, err = reader.ReadMessage(p.ctx)
				if err != nil {
					errChan <- errors.New("FetchMessages.reader.ReadMessage: " + err.Error())
					continue
				}
			} else {
				msg, err = reader.FetchMessage(p.ctx)
				if err != nil {
					errChan <- errors.New("FetchMessages.reader.FetchMessage: " + err.Error())
					continue
				}
			}

			msgChan <- Message{
				SourceMsg: msg,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       string(msg.Key),
				Value:     msg.Value,
			}
		}
	}
}

// CommitMessage
// commit incoming message
func (p *ProviderKafka) CommitMessage(topic, groupID string, msg *Message) error {
	reader := p.getReader(topic, groupID)

	err := reader.CommitMessages(p.ctx, msg.SourceMsg)
	if err != nil {
		return errors.New("CommitMessage.reader.CommitMessages: " + err.Error())
	}

	return nil
}
