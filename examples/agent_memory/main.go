// Streamline Agent Memory Example
//
// Demonstrates the memory MCP tools (remember, recall) for building
// agents with persistent, semantically searchable memory. Shows both
// single-agent memory and multi-agent shared memory via namespaces.
//
// Prerequisites:
//   - Streamline server running with memory features enabled
//   - go get github.com/streamlinelabs/streamline-go-sdk
//
// Run:
//
//	go run examples/agent_memory/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/streamlinelabs/streamline-go-sdk/streamline"
)

func main() {
	config := streamline.DefaultConfig()
	brokers := os.Getenv("STREAMLINE_BOOTSTRAP_SERVERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	config.Brokers = []string{brokers}

	httpEndpoint := os.Getenv("STREAMLINE_HTTP")
	if httpEndpoint == "" {
		httpEndpoint = "http://localhost:9094"
	}
	config.HTTPEndpoint = httpEndpoint

	client, err := streamline.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	singleAgentMemory(ctx, client)
	multiAgentSharedMemory(ctx, client)

	fmt.Println("\nDone!")
}

func singleAgentMemory(ctx context.Context, client *streamline.Client) {
	fmt.Println("=== Single Agent Memory ===")

	// Store architectural decisions
	err := client.MemoryRemember(ctx, streamline.MemoryEntry{
		AgentID:    "demo-agent",
		Content:    "We chose PostgreSQL for its JSONB support and mature ecosystem",
		Kind:       "fact",
		Importance: 0.8,
		Tags:       []string{"architecture", "database"},
	})
	if err != nil {
		log.Fatalf("Failed to remember: %v", err)
	}

	err = client.MemoryRemember(ctx, streamline.MemoryEntry{
		AgentID:    "demo-agent",
		Content:    "Redis is used as a caching layer with a 15-minute TTL",
		Kind:       "fact",
		Importance: 0.7,
		Tags:       []string{"architecture", "caching"},
	})
	if err != nil {
		log.Fatalf("Failed to remember: %v", err)
	}

	err = client.MemoryRemember(ctx, streamline.MemoryEntry{
		AgentID:    "demo-agent",
		Content:    "User requested dark mode support in the dashboard",
		Kind:       "preference",
		Importance: 0.6,
		Tags:       []string{"ui", "user-request"},
	})
	if err != nil {
		log.Fatalf("Failed to remember: %v", err)
	}

	fmt.Println("Stored 3 memories\n")

	// Recall by semantic similarity
	fmt.Println("--- Recall: 'why did we pick our database?' ---")
	results, err := client.MemoryRecall(ctx, streamline.MemoryQuery{
		AgentID: "demo-agent",
		Query:   "why did we pick our database?",
		K:       5,
	})
	if err != nil {
		log.Fatalf("Failed to recall: %v", err)
	}
	for _, hit := range results {
		fmt.Printf("  [%s] score=%.2f: %s\n", hit.Tier, hit.Score, hit.Content)
	}

	fmt.Println("\n--- Recall: 'caching strategy' ---")
	results, err = client.MemoryRecall(ctx, streamline.MemoryQuery{
		AgentID: "demo-agent",
		Query:   "caching strategy",
		K:       5,
	})
	if err != nil {
		log.Fatalf("Failed to recall: %v", err)
	}
	for _, hit := range results {
		fmt.Printf("  [%s] score=%.2f: %s\n", hit.Tier, hit.Score, hit.Content)
	}
}

func multiAgentSharedMemory(ctx context.Context, client *streamline.Client) {
	fmt.Println("\n=== Multi-Agent Shared Memory ===")

	// Agent A stores a decision in the shared namespace
	err := client.MemoryRemember(ctx, streamline.MemoryEntry{
		AgentID:    "agent-a",
		Namespace:  "team-shared",
		Content:    "Deploy target is Kubernetes on AWS EKS",
		Kind:       "fact",
		Importance: 0.9,
		Tags:       []string{"infra", "deployment"},
	})
	if err != nil {
		log.Fatalf("Failed to remember: %v", err)
	}
	fmt.Println("Agent A stored deployment decision")

	// Agent B stores related context in the same namespace
	err = client.MemoryRemember(ctx, streamline.MemoryEntry{
		AgentID:    "agent-b",
		Namespace:  "team-shared",
		Content:    "CI/CD pipeline uses GitHub Actions with OIDC auth to AWS",
		Kind:       "fact",
		Importance: 0.8,
		Tags:       []string{"infra", "ci-cd"},
	})
	if err != nil {
		log.Fatalf("Failed to remember: %v", err)
	}
	fmt.Println("Agent B stored CI/CD context")

	// Agent C recalls shared memories from the team namespace
	fmt.Println("\n--- Agent C recalls 'deployment infrastructure' from shared namespace ---")
	results, err := client.MemoryRecall(ctx, streamline.MemoryQuery{
		AgentID:   "agent-c",
		Namespace: "team-shared",
		Query:     "deployment infrastructure",
		K:         5,
	})
	if err != nil {
		log.Fatalf("Failed to recall: %v", err)
	}
	for _, hit := range results {
		fmt.Printf("  [%s] score=%.2f: %s\n", hit.Tier, hit.Score, hit.Content)
	}
}
