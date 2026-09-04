package streamline_test

import (
	"context"
	"log"

	streamline "github.com/streamlinelabs/streamline-go-sdk/testcontainers"
)

// This example is compile-only because running it requires Docker and a
// Streamline container image.
func Example() {
	ctx := context.Background()
	container, err := streamline.RunContainer(
		ctx,
		streamline.WithTag("0.4.0"),
		streamline.WithDebugLogging(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			log.Printf("terminate container: %v", err)
		}
	}()

	bootstrapServers, err := container.BootstrapServers(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("bootstrap servers: %s", bootstrapServers)
}
