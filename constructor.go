package kafka

import (
	"context"
)

// NewKafkaProvider
// kafka provider constructor
func NewKafkaProvider(ctx context.Context, host string) (ProviderKafkaInterface, error) {
	provider := new(ProviderKafka)

	conn, err := initConnect(ctx, host)
	if err != nil {
		return nil, err
	}

	provider.conn = conn
	provider.ctx = ctx

	return provider, nil
}
