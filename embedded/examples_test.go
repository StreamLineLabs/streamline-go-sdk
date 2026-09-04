package embedded_test

import (
	"log"
	"time"

	"github.com/streamlinelabs/streamline-go-sdk/embedded"
)

// This example is compile-only. Running it requires the embedded build tag,
// CGO, and a compatible libstreamline installation.
func Example() {
	instance, err := embedded.New(embedded.Config{InMemory: true})
	if err != nil {
		log.Fatal(err)
	}
	defer instance.Close()

	if err := instance.CreateTopic("events", 1); err != nil {
		log.Fatal(err)
	}
	if err := instance.Produce("events", []byte("hello")); err != nil {
		log.Fatal(err)
	}
	if _, err := instance.Consume("events", 5*time.Second); err != nil {
		log.Fatal(err)
	}
}
