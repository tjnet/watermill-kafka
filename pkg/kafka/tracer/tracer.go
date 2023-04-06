package tracer

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
)

// PubSubTracer traces Publish, Subscribe, and so on.
//
//	inspired by https://github.com/kubescape/messaging/blob/c40f40f3f8108125c91d01c4f24cd0a63be3b203/pulsar/common/tracer/interceptors.go#L17
//
// https://github.com/377312117/AllCode/blob/aa032aad2f3687e35169e604ac1d893ec87f0055/GoCode/AllGitHub/jupiter/pkg/client/rocketmq/consumer.go#L41
// good example of interceptor
// https://github.com/DataWorkbench/common/blob/8364bed7bb7c5f9b21f9aae2ef740131b4b7b256/kafka/consumer_handler.go#L51
// https://github.com/gocollection/kafka-go/blob/master/kafka_handler.go
// good example of abstraction
// https://github.com/gocollection/kafka-go/blob/master/pubsub_api.go
type PubSubTracer interface {
	// TracePublish is called at the beginning or end of Publish calls.
	TracePublish(topic string, msgs ...*message.Message) error
	// TraceSubscribe is called at the beginning or end of Subscribe calls.
	TraceSubscribe(ctx context.Context, topic string) error
}

type TracePublishInterceptor struct {
}

type TraceSubscribeInterceptor struct {
}
