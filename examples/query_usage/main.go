// Streamline SQL Query Example
//
// Demonstrates using Streamline's embedded analytics engine (DuckDB)
// to run SQL queries on streaming data.
//
// Prerequisites:
//   - Streamline server running
//   - go get github.com/streamlinelabs/streamline-go-sdk
//
// Run:
//
//	go run examples/query_usage/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

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
	bootstrap := os.Getenv("STREAMLINE_BOOTSTRAP")
	if bootstrap == "" {
		bootstrap = "localhost:9092"
	}
	httpURL := os.Getenv("STREAMLINE_HTTP")
	if httpURL == "" {
		httpURL = "http://localhost:9094"
	}

	// Produce sample data
	cfg := streamline.DefaultConfig()
	cfg.Brokers = []string{bootstrap}
	client, err := streamline.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("close client: %v", closeErr)
		}
	}()

	ctx := context.Background()

	if createErr := client.Admin.CreateTopic(ctx, streamline.TopicConfig{
		Name:              "events",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); createErr != nil {
		log.Printf("create topic (may already exist): %v", createErr)
	}

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf(`{"user":"user-%d","action":"click","value":%d}`, i, i*10)
		if _, sendErr := client.Producer.Send(ctx, "events", nil, []byte(msg)); sendErr != nil {
			return fmt.Errorf("produce: %w", sendErr)
		}
	}
	fmt.Println("Produced 10 events")

	// Query the data
	queryClient := streamline.NewQueryClient(httpURL)

	// Simple SELECT
	fmt.Println("\n--- All events (limit 5) ---")
	result, err := queryClient.Query(ctx, "SELECT * FROM topic('events') LIMIT 5")
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	fmt.Printf("Columns: %d, Rows: %d\n", len(result.Columns), len(result.Rows))
	for _, row := range result.Rows {
		fmt.Printf("  %v\n", row)
	}

	// Aggregation
	fmt.Println("\n--- Count by action ---")
	result, err = queryClient.Query(ctx, "SELECT action, COUNT(*) as cnt FROM topic('events') GROUP BY action")
	if err != nil {
		return fmt.Errorf("aggregate query: %w", err)
	}
	for _, row := range result.Rows {
		fmt.Printf("  %v\n", row)
	}

	// Query with options
	fmt.Println("\n--- With custom timeout and limit ---")
	opts := streamline.QueryOptions{TimeoutMs: 5000, MaxRows: 3}
	result, err = queryClient.QueryWithOptions(
		ctx,
		"SELECT * FROM topic('events') ORDER BY offset DESC",
		opts,
	)
	if err != nil {
		return fmt.Errorf("query with options: %w", err)
	}
	fmt.Printf("Returned %d rows\n", len(result.Rows))

	// Explain query plan
	fmt.Println("\n--- Explain query plan ---")
	plan, err := queryClient.Explain(ctx, "SELECT * FROM topic('events') LIMIT 10")
	if err != nil {
		return fmt.Errorf("explain: %w", err)
	}
	fmt.Println(plan)

	return nil
}
