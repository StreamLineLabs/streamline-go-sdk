// Circuit breaker example for Streamline Go SDK.
//
// The circuit breaker prevents repeated attempts against a failing server.
// After consecutive failures it "opens" and rejects requests immediately.
//
// Run with:
//
//	go run examples/circuit_breaker/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
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
	fmt.Println("Circuit Breaker Example")
	fmt.Println("========================================")

	brokers := os.Getenv("STREAMLINE_BOOTSTRAP_SERVERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	client, err := streamline.NewClient(streamline.Config{
		Brokers: []string{brokers},
	})
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("Failed to close client: %v", closeErr)
		}
	}()

	// Configure the circuit breaker
	cb := streamline.NewCircuitBreaker(streamline.CircuitBreakerConfig{
		FailureThreshold:    5,
		SuccessThreshold:    2,
		OpenTimeout:         10 * time.Second,
		HalfOpenMaxRequests: 3,
		OnStateChange: func(from, to streamline.CircuitState) {
			fmt.Printf("  [Circuit Breaker] %s → %s\n", from, to)
		},
	})

	ctx := context.Background()

	// Create a topic for the example; an existing topic is not an error here.
	if createErr := client.Admin.CreateTopic(ctx, streamline.TopicConfig{
		Name:              "cb-example",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); createErr != nil {
		log.Printf("Create topic (may already exist): %v", createErr)
	}

	// Send messages through the circuit breaker
	for i := 0; i < 20; i++ {
		if !cb.Allow() {
			fmt.Printf("  Message %d: REJECTED (circuit open)\n", i)
			time.Sleep(1 * time.Second)
			continue
		}

		result, sendErr := client.Producer.Send(ctx, "cb-example", []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("message-%d", i)))
		if sendErr != nil {
			cb.RecordFailure()
			fmt.Printf("  Message %d: FAILED (%v) (circuit: %s)\n", i, sendErr, cb.State())
		} else {
			cb.RecordSuccess()
			fmt.Printf("  Message %d: sent to partition=%d offset=%d (circuit: %s)\n",
				i, result.Partition, result.Offset, cb.State())
		}
	}

	fmt.Printf("\nFinal circuit state: %s\n", cb.State())
	fmt.Println("Done!")

	return nil
}
