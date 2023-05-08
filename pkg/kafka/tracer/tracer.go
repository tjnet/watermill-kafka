package tracer

import (
	"context"
	"github.com/Shopify/sarama"
)

type Tracer interface {
	WrapSyncProducer(saramaConfig *sarama.Config, producer sarama.SyncProducer) sarama.SyncProducer
	TraceSendStart(ctx context.Context, message *sarama.ProducerMessage) (func(), error)
}
