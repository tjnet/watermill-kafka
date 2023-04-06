package kafka

import (
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

var propagators = propagation.TraceContext{}

type Publisher struct {
	config      PublisherConfig
	producer    sarama.SyncProducer
	logger      watermill.LoggerAdapter
	interceptor PublishInterceptor
	closed      bool
}

// NewPublisher creates a new Kafka Publisher.
func NewPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
	interceptors []PublishInterceptor,
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

	if config.OTELEnabled {
		traceProvider := config.OTELTraceProvider
		opts := []otelsarama.Option{
			otelsarama.WithTracerProvider(traceProvider),
			otelsarama.WithPropagators(propagators),
		}
		producer = otelsarama.WrapSyncProducer(config.OverwriteSaramaConfig, producer, opts...)
	}

	return &Publisher{
		config:      config,
		producer:    producer,
		logger:      logger,
		interceptor: ChainedInterceptor(interceptors...),
	}, nil
}

type PublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler

	// OverwriteSaramaConfig holds additional sarama settings.
	OverwriteSaramaConfig *sarama.Config

	// If true then each sent message will be wrapped with Opentelemetry tracing, provided by otelsarama.
	OTELEnabled bool

	OTELTraceProvider oteltrace.TracerProvider
}

func (c *PublisherConfig) setDefaults() {
	if c.OverwriteSaramaConfig == nil {
		c.OverwriteSaramaConfig = DefaultSaramaSyncPublisherConfig()
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
		if err := p.interceptor(msg, func(message *message.Message) error {

			logFields["message_uuid"] = msg.UUID
			p.logger.Trace("Sending message to Kafka", logFields)

			kafkaMsg, err := p.config.Marshaler.Marshal(topic, msg)
			if err != nil {
				return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
			}

			if p.config.OTELEnabled {
				// see https://github.com/open-telemetry/opentelemetry-go-contrib/blob/c52452d90ab05a5a92f54398c0093376883cca2a/instrumentation/github.com/Shopify/sarama/otelsarama/test/producer_test.go#L164
				// create message span with context
				// ctx, _ := p.config.OTELTraceProvider.Tracer("watermill-kafka-producer").Start(context.Background(), "")
				// propagators.Inject(ctx, otelsarama.NewProducerMessageCarrier(kafkaMsg))
			}

			partition, offset, err := p.producer.SendMessage(kafkaMsg)
			if err != nil {
				return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
			}

			logFields["kafka_partition"] = partition
			logFields["kafka_partition_offset"] = offset

			p.logger.Trace("Message sent to Kafka", logFields)
			return nil
		}); err != nil {
			return errors.Wrapf(err, "cannot intercept Kafka Producer")
		}
	}

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
