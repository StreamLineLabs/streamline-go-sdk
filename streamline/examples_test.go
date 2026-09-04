package streamline_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streamlinelabs/streamline-go-sdk/streamline"
)

// Examples in this file intentionally omit Output comments. The Go toolchain
// compile-checks them without executing calls that require a live server.

func ExampleClient() {
	config := streamline.DefaultConfig()
	config.Brokers = []string{"localhost:9092"}

	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("close client: %v", closeErr)
		}
	}()

	result, err := client.Producer.Send(
		context.Background(),
		"my-topic",
		nil,
		[]byte("Hello, World!"),
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("produced to partition %d at offset %d", result.Partition, result.Offset)
}

func ExampleTracingProducer() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	tracingProducer := streamline.NewTracingProducer(client.Producer)

	result, err := tracingProducer.Send(ctx, "orders", []byte("key"), []byte("value"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("produced to partition %d at offset %d", result.Partition, result.Offset)
}

func ExampleTracingConsumer() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer, err := client.NewConsumer(ctx, "orders-service", []string{"orders"})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	tracingConsumer := streamline.NewTracingConsumer(consumer)
	messages, errs := tracingConsumer.Start(ctx)
	select {
	case msg := <-messages:
		processCtx, span := tracingConsumer.TraceProcess(ctx, msg)
		processMessage(processCtx, msg)
		span.End()
	case err := <-errs:
		log.Print(err)
	case <-ctx.Done():
	}
}

func ExampleProducer_SendMessage() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	result, err := client.Producer.SendMessage(context.Background(), &streamline.Message{
		Topic: "topic",
		Key:   []byte("key"),
		Value: []byte("value"),
		Headers: map[string][]byte{
			"trace-id": []byte("abc123"),
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("produced to partition %d at offset %d", result.Partition, result.Offset)
}

func ExampleProducer_SendBatch() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	messages := []*streamline.Message{
		{Topic: "topic", Value: []byte("msg1")},
		{Topic: "topic", Value: []byte("msg2")},
		{Topic: "topic", Value: []byte("msg3")},
	}
	results, err := client.Producer.SendBatch(context.Background(), messages)
	if err != nil {
		log.Printf("batch completed with partial results: %v", err)
	}
	log.Printf("received %d result slots", len(results))
}

func ExampleProducer_SendAsync() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	result := <-client.Producer.SendAsync(&streamline.Message{
		Topic: "topic",
		Value: []byte("async message"),
	})
	if result.Err != nil {
		log.Fatal(result.Err)
	}
	log.Printf("sent to partition %d", result.Partition)
}

func ExampleProducer_BeginTransaction() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	producer := client.Producer
	if beginErr := producer.BeginTransaction(); beginErr != nil {
		log.Fatal(beginErr)
	}
	if sendErr := producer.SendTransactional(ctx, &streamline.Message{
		Topic: "orders",
		Key:   []byte("k1"),
		Value: []byte("v1"),
	}); sendErr != nil {
		_ = producer.AbortTransaction()
		log.Fatal(sendErr)
	}
	if sendErr := producer.SendTransactional(ctx, &streamline.Message{
		Topic: "orders",
		Key:   []byte("k2"),
		Value: []byte("v2"),
	}); sendErr != nil {
		_ = producer.AbortTransaction()
		log.Fatal(sendErr)
	}

	results, err := producer.CommitTransaction(ctx)
	if err != nil {
		log.Printf("commit returned partial results: %v", err)
	}
	log.Printf("received %d result slots", len(results))
}

func ExampleConsumer() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer, err := client.NewConsumer(ctx, "my-group", []string{"my-topic"})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	messages, errs := consumer.Start(ctx)
	select {
	case msg, ok := <-messages:
		if ok {
			log.Printf("received: %s", msg.Value)
		}
	case err, ok := <-errs:
		if ok {
			log.Printf("consumer error: %v", err)
		}
	case <-ctx.Done():
	}
}

func ExampleConsumer_Poll() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, "my-group", []string{"my-topic"})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	messages, err := consumer.Poll(ctx, 100, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	for _, msg := range messages {
		log.Printf("received: %s", msg.Value)
	}
}

func ExampleAdmin() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	if createErr := client.Admin.CreateTopic(ctx, streamline.TopicConfig{
		Name:              "my-topic",
		NumPartitions:     3,
		ReplicationFactor: 1,
		Config: map[string]string{
			"retention.ms": "86400000",
		},
	}); createErr != nil {
		log.Fatal(createErr)
	}

	topics, err := client.Admin.ListTopics(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, topic := range topics {
		log.Printf("topic: %s, partitions: %d", topic.Name, topic.Partitions)
	}
}

func ExampleAdmin_DescribeTopic() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	info, partitions, err := client.Admin.DescribeTopic(context.Background(), "my-topic")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("topic: %s", info.Name)
	for _, partition := range partitions {
		log.Printf(
			"partition %d: leader=%d, replicas=%v",
			partition.ID,
			partition.Leader,
			partition.Replicas,
		)
	}
}

func ExampleHTTPAdmin() {
	ctx := context.Background()
	admin := streamline.NewHTTPAdmin("http://localhost:9094")

	cluster, err := admin.ClusterInfo(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("cluster: %s, brokers: %d\n", cluster.ClusterID, len(cluster.Brokers))

	lag, err := admin.ConsumerGroupLag(ctx, "my-group")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("total lag: %d\n", lag.TotalLag)

	messages, err := admin.InspectMessages(ctx, "events", 0, nil, 10)
	if err != nil {
		log.Fatal(err)
	}
	for _, message := range messages {
		fmt.Printf("offset=%d value=%s\n", message.Offset, message.Value)
	}
}

func ExampleQueryClient() {
	ctx := context.Background()
	queryClient := streamline.NewQueryClient("http://localhost:9094")

	result, err := queryClient.Query(ctx, "SELECT * FROM topic('events') LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range result.Rows {
		fmt.Println(row)
	}

	result, err = queryClient.QueryWithOptions(
		ctx,
		"SELECT * FROM topic('events') ORDER BY offset DESC",
		streamline.QueryOptions{TimeoutMs: 5000, MaxRows: 100},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("scanned %d rows\n", result.Metadata.RowsScanned)
}

func ExampleConfig() {
	config := streamline.DefaultConfig()
	config.Brokers = []string{"localhost:9092"}
	config.Producer = streamline.ProducerConfig{
		RequiredAcks: -1,
		Compression:  1,
		BatchSize:    16384,
		BatchTimeout: 10 * time.Millisecond,
		Idempotent:   true,
		Retries:      3,
	}
	config.Consumer = streamline.ConsumerConfig{
		GroupID:           "my-group",
		AutoOffsetReset:   "earliest",
		SessionTimeout:    30 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		MaxPollRecords:    500,
		IsolationLevel:    1,
	}

	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}

func ExampleStreamlineError() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	_, err = client.Producer.Send(context.Background(), "my-topic", nil, []byte("data"))
	if err == nil {
		return
	}

	var streamlineErr *streamline.StreamlineError
	if errors.As(err, &streamlineErr) {
		log.Printf(
			"error [%s]: %s; retryable=%v",
			streamlineErr.Code,
			streamlineErr.Message,
			streamlineErr.Retryable,
		)
	}
}

func ExampleCircuitBreaker() {
	cb := streamline.NewCircuitBreaker(streamline.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      30 * time.Second,
		OnStateChange: func(from, to streamline.CircuitState) {
			log.Printf("circuit: %s -> %s", from, to)
		},
	})

	if cb.Allow() {
		if err := doSomething(); err != nil {
			cb.RecordFailure()
		} else {
			cb.RecordSuccess()
		}
	}
}

func ExampleConsumer_Search() {
	config := streamline.DefaultConfig()
	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	consumer, err := client.NewConsumer(ctx, "search-service", []string{"logs.app"})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	results, err := consumer.Search(ctx, "logs.app", "payment failure", 10)
	if err != nil {
		log.Fatal(err)
	}
	for _, hit := range results {
		log.Printf("[p%d] offset=%d score=%.2f", hit.Partition, hit.Offset, hit.Score)
	}
}

func ExampleVerifier() {
	publicKey := make(ed25519.PublicKey, ed25519.PublicKeySize)
	verifier := streamline.NewVerifier(publicKey)
	record := &streamline.ConsumerMessage{
		Topic:   "events",
		Headers: map[string][]byte{},
	}

	result, err := verifier.Verify(record)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("verified: %v, producer: %s", result.Verified, result.ProducerID)
}

func processMessage(context.Context, *streamline.ConsumerMessage) {}

func doSomething() error {
	return nil
}
