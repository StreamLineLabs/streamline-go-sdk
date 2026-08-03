package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streamlinelabs/streamline-go-sdk/streamline"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run holds the example body so deferred cleanup still runs when it fails:
// log.Fatal in main would skip every pending defer.
func run() error {
	// Create client with default configuration
	config := streamline.DefaultConfig()
	brokers := os.Getenv("STREAMLINE_BOOTSTRAP_SERVERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	config.Brokers = []string{brokers}

	client, err := streamline.NewClient(config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("Failed to close client: %v", closeErr)
		}
	}()

	ctx := context.Background()

	// Create a topic
	fmt.Println("Creating topic...")
	if createErr := client.Admin.CreateTopic(ctx, streamline.TopicConfig{
		Name:              "example-topic",
		NumPartitions:     3,
		ReplicationFactor: 1,
	}); createErr != nil {
		log.Printf("Warning: failed to create topic (may already exist): %v", createErr)
	}

	// Produce messages
	fmt.Println("Producing messages...")
	for i := 0; i < 10; i++ {
		result, sendErr := client.Producer.Send(ctx, "example-topic",
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("Hello, Streamline! Message %d", i)),
		)
		if sendErr != nil {
			log.Printf("Failed to send message: %v", sendErr)
			continue
		}
		fmt.Printf("Produced message to partition %d at offset %d\n",
			result.Partition, result.Offset)
	}

	// Produce with headers
	result, err := client.Producer.SendMessage(ctx, &streamline.Message{
		Topic: "example-topic",
		Key:   []byte("with-headers"),
		Value: []byte("Message with headers"),
		Headers: map[string][]byte{
			"trace-id":     []byte("abc123"),
			"content-type": []byte("application/json"),
		},
	})
	if err != nil {
		log.Printf("Failed to send message with headers: %v", err)
	} else {
		fmt.Printf("Produced message with headers to partition %d at offset %d\n",
			result.Partition, result.Offset)
	}

	// Create consumer
	fmt.Println("Starting consumer...")
	consumer, err := client.NewConsumer(ctx, "example-group", []string{"example-topic"})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer func() {
		if closeErr := consumer.Close(); closeErr != nil {
			log.Printf("Failed to close consumer: %v", closeErr)
		}
	}()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consuming
	messages, consumerErrs := consumer.Start(ctx)

	fmt.Println("Consuming messages (press Ctrl+C to stop)...")
	timeout := time.After(10 * time.Second)

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				return nil
			}
			fmt.Printf("Received: topic=%s partition=%d offset=%d key=%s value=%s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			if len(msg.Headers) > 0 {
				fmt.Printf("  Headers: %v\n", msg.Headers)
			}
		case consumeErr := <-consumerErrs:
			log.Printf("Consumer error: %v", consumeErr)
		case <-sigChan:
			fmt.Println("\nShutting down...")
			return nil
		case <-timeout:
			fmt.Println("\nTimeout reached")
			return nil
		}
	}
}
