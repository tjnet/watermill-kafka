package kafka

import (
	"context"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka/tracer"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka/tracer/noop"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
	config   PublisherConfig
	producer sarama.SyncProducer
	logger   watermill.LoggerAdapter

	closed bool
}

// NewPublisher creates a new Kafka Publisher.
func NewPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
) (*Publisher, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	producer, err := sarama.NewSyncProducer(config.Brokers, config.OverwriteSaramaConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Kafka producer")
	}

	producer = config.Tracer.WrapSyncProducer(config.OverwriteSaramaConfig, producer)

	return &Publisher{
		config:   config,
		producer: producer,
		logger:   logger,
	}, nil
}

type PublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config

	Tracer tracer.Tracer
}

func (c *PublisherConfig) setDefaults() {
	if c.OverwriteSaramaConfig == nil {
		c.OverwriteSaramaConfig = DefaultSaramaSyncPublisherConfig()
	}
	if c.Tracer == nil {
		c.Tracer = noop.NewNoOpTracer()
	}
}

func (c PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Marshaler == nil {
		return errors.New("missing marshaler")
	}

	return nil
}

func DefaultSaramaSyncPublisherConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Version = sarama.V1_0_0_0
	config.Metadata.Retry.Backoff = time.Second * 2
	config.ClientID = "watermill"

	return config
}

// Publish publishes message to Kafka.
//
// Publish is blocking and wait for ack from Kafka.
// When one of messages delivery fails - function is interrupted.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	for _, msg := range msgs {
		p.sendMessage(logFields, msg, topic)
	}

	return nil
}

func (p *Publisher) sendMessage(logFields watermill.LogFields, msg *message.Message, topic string) error {
	logFields["message_uuid"] = msg.UUID
	p.logger.Trace("Sending message to Kafka", logFields)

	kafkaMsg, err := p.config.Marshaler.Marshal(topic, msg)
	if err != nil {
		return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
	}
	cleanup, err := p.config.Tracer.TraceSendStart(context.Background(), kafkaMsg)

	defer cleanup()
	if err != nil {
		return errors.Wrapf(err, "cannot start trace.")
	}

	partition, offset, err := p.producer.SendMessage(kafkaMsg)
	if err != nil {
		return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
	}

	logFields["kafka_partition"] = partition
	logFields["kafka_partition_offset"] = offset

	p.logger.Trace("Message sent to Kafka", logFields)

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	if err := p.producer.Close(); err != nil {
		return errors.Wrap(err, "cannot close Kafka producer")
	}

	return nil
}
