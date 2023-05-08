package noop

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka/tracer"
)

type noOpTracer struct {
}

func (n *noOpTracer) WrapSyncProducer(saramaConfig *sarama.Config, producer sarama.SyncProducer) sarama.SyncProducer {
	return producer
}

func (n *noOpTracer) TraceSendStart(ctx context.Context, _ *sarama.ProducerMessage) (func(), error) {
	return func() {

	}, nil
}

func NewNoOpTracer() tracer.Tracer {
	return &noOpTracer{}
}
