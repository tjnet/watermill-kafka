package kafka

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
)

type ConsumerHandler func(ctx context.Context, message *message.Message) error

type PublisherHandler func(message *message.Message) error
