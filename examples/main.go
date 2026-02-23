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
	// Create client with default configuration
	config := streamline.DefaultConfig()
	brokers := os.Getenv("STREAMLINE_BOOTSTRAP_SERVERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	config.Brokers = []string{brokers}

	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Create a topic
	fmt.Println("Creating topic...")
	err = client.Admin.CreateTopic(ctx, streamline.TopicConfig{
		Name:              "example-topic",
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		log.Printf("Warning: failed to create topic (may already exist): %v", err)
	}

	// Produce messages
	fmt.Println("Producing messages...")
	for i := 0; i < 10; i++ {
		result, err := client.Producer.Send(ctx, "example-topic",
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("Hello, Streamline! Message %d", i)),
		)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
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
			"trace-id":    []byte("abc123"),
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
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consuming
	messages, errors := consumer.Start(ctx)

	fmt.Println("Consuming messages (press Ctrl+C to stop)...")
	timeout := time.After(10 * time.Second)

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				return
			}
			fmt.Printf("Received: topic=%s partition=%d offset=%d key=%s value=%s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			if len(msg.Headers) > 0 {
				fmt.Printf("  Headers: %v\n", msg.Headers)
			}
		case err := <-errors:
			log.Printf("Consumer error: %v", err)
		case <-sigChan:
			fmt.Println("\nShutting down...")
			return
		case <-timeout:
			fmt.Println("\nTimeout reached")
			return
		}
	}
}
