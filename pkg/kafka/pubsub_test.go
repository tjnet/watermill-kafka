package kafka_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func kafkaBrokers() []string {
	brokers := os.Getenv("WATERMILL_TEST_KAFKA_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, ",")
	}
	return []string{"localhost:9091", "localhost:9092", "localhost:9093", "localhost:9094", "localhost:9095"}
}

// publisherConfigOption is a functional option for the PublisherConfig struct
type publisherConfigOption func(config *kafka.PublisherConfig)

func newPubSub(t *testing.T, marshaler kafka.MarshalerUnmarshaler, consumerGroup string, pubOpt ...publisherConfigOption) (*kafka.Publisher, *kafka.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	var err error
	var publisher *kafka.Publisher

	retriesLeft := 5
	for {

		pubConf := &kafka.PublisherConfig{
			Brokers:   kafkaBrokers(),
			Marshaler: marshaler,
		}
		for _, opt := range pubOpt {
			opt(pubConf)
		}
		publisher, err = kafka.NewPublisher(*pubConf, logger, nil)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Publisher: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}
	require.NoError(t, err)

	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	saramaConfig.Admin.Timeout = time.Second * 30
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.ChannelBufferSize = 10240
	saramaConfig.Consumer.Group.Heartbeat.Interval = time.Millisecond * 500
	saramaConfig.Consumer.Group.Rebalance.Timeout = time.Second * 3

	var subscriber *kafka.Subscriber

	retriesLeft = 5
	for {
		subscriber, err = kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:               kafkaBrokers(),
				Unmarshaler:           marshaler,
				OverwriteSaramaConfig: saramaConfig,
				ConsumerGroup:         consumerGroup,
				InitializeTopicDetails: &sarama.TopicDetail{
					NumPartitions:     8,
					ReplicationFactor: 1,
				},
			},
			logger,
		)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Subscriber: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}

	require.NoError(t, err)

	return publisher, subscriber
}

func generatePartitionKey(topic string, msg *message.Message) (string, error) {
	return msg.Metadata.Get("partition_key"), nil
}

func createPubSubWithConsumerGrup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.DefaultMarshaler{}, consumerGroup)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGrup(t, "test")
}

func createPartitionedPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.NewWithPartitioningMarshaler(generatePartitionKey), "test")
}

func createNoGroupPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.DefaultMarshaler{}, "")
}

func TestPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestPublishSubscribe_ordered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     true,
			Persistent:          true,
		},
		createPartitionedPubSub,
		createPubSubWithConsumerGrup,
	)
}

func TestNoGroupSubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                   false,
			ExactlyOnceDelivery:              false,
			GuaranteedOrder:                  false,
			Persistent:                       true,
			NewSubscriberReceivesOldMessages: true,
		},
		createNoGroupPubSub,
		nil,
	)
}

func TestCtxValues(t *testing.T) {
	pub, sub := newPubSub(t, kafka.DefaultMarshaler{}, "")
	topicName := "topic_" + watermill.NewUUID()

	var messagesToPublish []*message.Message

	for i := 0; i < 20; i++ {
		id := watermill.NewUUID()
		messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))
	}
	err := pub.Publish(topicName, messagesToPublish...)
	require.NoError(t, err, "cannot publish message")

	messages, err := sub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkReadWithDeduplication(messages, len(messagesToPublish), time.Second*10)
	require.True(t, all)

	expectedPartitionsOffsets := map[int32]int64{}
	for _, msg := range receivedMessages {
		partition, ok := kafka.MessagePartitionFromCtx(msg.Context())
		assert.True(t, ok)

		messagePartitionOffset, ok := kafka.MessagePartitionOffsetFromCtx(msg.Context())
		assert.True(t, ok)

		kafkaMsgTimestamp, ok := kafka.MessageTimestampFromCtx(msg.Context())
		assert.True(t, ok)
		assert.NotZero(t, kafkaMsgTimestamp)

		if expectedPartitionsOffsets[partition] <= messagePartitionOffset {
			// kafka partition offset is offset of the last message + 1
			expectedPartitionsOffsets[partition] = messagePartitionOffset + 1
		}
	}
	assert.NotEmpty(t, expectedPartitionsOffsets)

	offsets, err := sub.PartitionOffset(topicName)
	require.NoError(t, err)
	assert.NotEmpty(t, offsets)

	assert.EqualValues(t, expectedPartitionsOffsets, offsets)

	require.NoError(t, pub.Close())
}

func TestWrapSyncProducer(t *testing.T) {
	// TODO: how about adding pub.WithWrapSyncProducer() or pub.WithInterceptors()?
	// https://github.com/gojek/courier-go/blob/252df2ee3a206bc5d918413fd60859b043c41999/otelcourier/publish_test.go#L64
	sr := tracetest.NewSpanRecorder()
	// see https://github.com/evantorrie/contrib-orig/blob/15218d62aa600c21cc701cc001c81d425e186672/instrumentation/github.com/Shopify/sarama/producer_test.go#L44
	pub, sub := newPubSub(t, kafka.DefaultMarshaler{}, "",
		func(config *kafka.PublisherConfig) {
			config.OTELEnabled = true
		},
		func(config *kafka.PublisherConfig) {
			// see https://github.com/tomMoulard/traefik-issues/blob/8fd14c68114c8c96e5023a469115bdfc6c27f74e/pull-8999-opentelemetry/try-1/poc-trace.go#L103
			provider := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
			config.OTELTraceProvider = provider
		},
	)

	topicName := "topic_" + watermill.NewUUID()

	var messagesToPublish []*message.Message

	for i := 0; i < 1; i++ {
		id := watermill.NewUUID()
		messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))
	}
	err := pub.Publish(topicName, messagesToPublish...)
	require.NoError(t, err, "cannot publish message")

	messages, err := sub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkReadWithDeduplication(messages, len(messagesToPublish), time.Second*10)
	require.True(t, all)

	for _, msg := range receivedMessages {
		// To test the tracing feature,
		// we can use a Kafka consumer to read the message from the Kafka topic and
		// check if the trace ID and span ID are correct.
		// TODO: must receive trace ID now. existing implementation is not good enough.
		// see this test
		// https://github.com/open-telemetry/opentelemetry-go-contrib/blob/c52452d90ab05a5a92f54398c0093376883cca2a/instrumentation/github.com/Shopify/sarama/otelsarama/test/producer_test.go#L164
		kafkaHeaders, ok := kafka.MessageHeadersFromCtx(msg.Context())
		assert.True(t, len(kafkaHeaders) > 0)
		t.Logf("headers %+v", kafkaHeaders)

		kafkaMsgTimestamp, ok := kafka.MessageTimestampFromCtx(msg.Context())
		assert.True(t, ok)
		assert.NotZero(t, kafkaMsgTimestamp)

		spanList := sr.Ended()
		for _, span := range spanList {
			assert.True(t, span.SpanContext().IsValid())
			assert.NotEmpty(t, span.SpanContext().TraceID())
			assert.Equal(t, "kafka.produce", span.Name())
			// assert.Empty(t, span.Attributes()) // fail
		}
	}

}
