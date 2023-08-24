package kafka

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"

	"github.com/segmentio/kafka-go"
)

// Global default constants
const (
	defaultPartitionsCount   = 12
	defaultReplicationFactor = 3
)

// Writers
// private writers pool entity
type writers struct {
	fastWriter *kafka.Writer // ack:  1, balancer: round-robin (by hash)
	slowWriter *kafka.Writer // ack: -1, balancer: round-robin (by hash)
}

// Global writers instance
// with concurrency-safe init
var (
	writersSync     sync.Once
	writersInstance *writers
)

// Global readers map instance
// with concurrency-safe init
var (
	readersSync  sync.Once
	readersMutex sync.Mutex
	readersMap   map[string]*kafka.Reader
)

// initConnect
// init Apache Kafka conn
func initConnect(ctx context.Context, host string) (*kafka.Conn, error) {
	conn, err := kafka.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, errors.New("initConnect.kafka.DialContext: " + err.Error())
	}

	controller, err := conn.Controller()
	if err != nil {
		return nil, errors.New("initConnect.conn.Controller: " + err.Error())
	}

	connLeader, err := kafka.Dial("tcp",
		net.JoinHostPort(
			controller.Host,
			strconv.Itoa(controller.Port),
		),
	)
	if err != nil {
		return nil, errors.New("initConnect.kafka.Dial: " + err.Error())
	}

	return connLeader, nil
}

// getTopicsMap
// get map of existing topics
func (p *ProviderKafka) getTopicsMap() (map[string]string, error) {
	topics := make(map[string]string)

	partitions, err := p.conn.ReadPartitions()
	if err != nil {
		return nil, errors.New("getTopicsMap.p.conn.ReadPartitions: " + err.Error())
	}

	for i := 0; i < len(partitions); i++ {
		topics[partitions[i].Topic] = partitions[i].Topic
	}

	return topics, nil
}

// getWriter
// writers constructor & getter
func (p *ProviderKafka) getWriter(isGuaranteed bool) *kafka.Writer {
	writersSync.Do(func() {
		writersInstance = new(writers)

		writersInstance.fastWriter = &kafka.Writer{
			Addr:         p.conn.RemoteAddr(),
			Balancer:     &kafka.Hash{},
			RequiredAcks: 1,
		}

		writersInstance.slowWriter = &kafka.Writer{
			Addr:         p.conn.RemoteAddr(),
			Balancer:     &kafka.Hash{},
			RequiredAcks: -1,
		}
	})

	if isGuaranteed {
		return writersInstance.slowWriter
	}

	return writersInstance.fastWriter
}

// getReader
// reader constructor
func (p *ProviderKafka) getReader(topic, groupID string) *kafka.Reader {
	key := topic + groupID

	readersSync.Do(func() {
		readersMap = make(map[string]*kafka.Reader)
	})

	_, ok := readersMap[key]
	if !ok {
		cfg := kafka.ReaderConfig{
			Brokers:  []string{p.conn.RemoteAddr().String()},
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}

		readersMutex.Lock()
		defer readersMutex.Unlock()

		readersMap[key] = kafka.NewReader(cfg)
	}

	return readersMap[key]
}
