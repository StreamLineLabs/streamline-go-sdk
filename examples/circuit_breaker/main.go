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
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

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

	// Create a topic for the example
	_ = client.Admin.CreateTopic(ctx, "cb-example", 1, 1, nil)

	// Send messages through the circuit breaker
	for i := 0; i < 20; i++ {
		if !cb.Allow() {
			fmt.Printf("  Message %d: REJECTED (circuit open)\n", i)
			time.Sleep(1 * time.Second)
			continue
		}

		result, err := client.Producer.Send(ctx, "cb-example", []byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("message-%d", i)))
		if err != nil {
			cb.RecordFailure()
			fmt.Printf("  Message %d: FAILED (%v) (circuit: %s)\n", i, err, cb.State())
		} else {
			cb.RecordSuccess()
			fmt.Printf("  Message %d: sent to partition=%d offset=%d (circuit: %s)\n",
				i, result.Partition, result.Offset, cb.State())
		}
	}

	fmt.Printf("\nFinal circuit state: %s\n", cb.State())
	fmt.Println("Done!")
}
