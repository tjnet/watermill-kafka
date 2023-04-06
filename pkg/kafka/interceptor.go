package kafka

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

// PublishInterceptor
// see https://github.com/apache/rocketmq-client-go/blob/master/primitive/interceptor.go
type PublishInterceptor func(msg *message.Message, handler PublisherHandler) error

func ChainedInterceptor(interceptors ...PublishInterceptor) func(msg *message.Message, handler PublisherHandler) error {
	if interceptors != nil || len(interceptors) > 0 {
		chainedInterceptor := func(msg *message.Message, handler PublisherHandler) error {
			for i := len(interceptors) - 1; i >= 0; i-- {
				currentInterceptor := interceptors[i]
				currentHandler := handler
				handler = func(msg *message.Message) error {
					return currentInterceptor(msg, currentHandler)
				}
			}
			return handler(msg)
		}
		return chainedInterceptor
	} else {
		return noOpInterceptor
	}
}

func noOpInterceptor(msg *message.Message, handler PublisherHandler) error {
	return handler(msg)
}
