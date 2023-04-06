package kafka

// TODO: refactor move this to pubsub_test.go
/*
func TestPublishInterceptor(t *testing.T) {
	var logs []string

	// create new interceptor chain
	interceptor1 := func(msg *message.Message, handler PublisherHandler) error {
		logs = append(logs, "Interceptor1: Before message handling")
		err := handler(msg)
		logs = append(logs, "Interceptor1: After message handling")
		return err
	}
	interceptor2 := func(msg *message.Message, handler PublisherHandler) error {
		logs = append(logs, "Interceptor2: Before message handling")
		err := handler(msg)
		logs = append(logs, "Interceptor2: After message handling")
		return err
	}
	interceptors := []PublishInterceptor{interceptor1, interceptor2}

	// create a mock producer
	logger := watermill.NopLogger{}

	mockBroker := newMockKafkaBroker(t)
	defer func() { mockBroker.Close() }()

	publisher, err := NewPublisher(PublisherConfig{
		Brokers:   []string{mockBroker.Addr()},
		Marshaler: DefaultMarshaler{},
	}, logger, interceptors)
	if err != nil {
		panic(err)
	}

	var messagesToPublish []*message.Message
	for i := 0; i < 1; i++ {
		id := watermill.NewUUID()
		messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))
	}
	publisher.Publish("topic_dummy", messagesToPublish...)

	// check the logs to ensure that the interceptors were called in the correct order
	expectedLogs := []string{
		"Interceptor1: Before message handling",
		"Interceptor2: Before message handling",
		"Interceptor2: After message handling",
		"Interceptor1: After message handling",
	}
	if !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("Unexpected logs:\nExpected: %v\nActual: %v", expectedLogs, logs)
	}
}

func newMockKafkaBroker(t *testing.T) *sarama.MockBroker {
	mockBroker := sarama.NewMockBroker(t, 0)
	return mockBroker
}
*/
