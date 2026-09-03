package moonshot_test

import (
	"context"
	"log"

	"github.com/streamlinelabs/streamline-go-sdk/streamline/moonshot"
)

// These examples are compile-only because the experimental APIs require a
// feature-enabled Streamline server.

func ExampleMemoryClient() {
	ctx := context.Background()
	memory, err := moonshot.NewMemoryClient(moonshot.Options{
		HTTPURL: "http://localhost:9094",
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = memory.Remember(ctx, moonshot.RememberParams{
		AgentID: "assistant",
		Kind:    moonshot.MemoryFact,
		Content: "user prefers dark mode",
		Tags:    []string{"preferences"},
	})
	if err != nil {
		log.Fatal(err)
	}

	results, err := memory.Recall(ctx, moonshot.RecallParams{
		AgentID: "assistant",
		Query:   "user preferences",
		K:       5,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("recalled %d memories", len(results))
}

func ExampleBranchAdminClient() {
	ctx := context.Background()
	branches, err := moonshot.NewBranchAdminClient(moonshot.Options{
		HTTPURL: "http://localhost:9094",
	})
	if err != nil {
		log.Fatal(err)
	}

	branch, err := branches.Create(
		ctx,
		"events",
		"experiment-v2",
		[]int64{0},
		moonshot.CreateBranchOptions{CreatedBy: "example"},
	)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := branches.Append(ctx, branch.ID, 0, `{"variant":"v2"}`); err != nil {
		log.Fatal(err)
	}
}
