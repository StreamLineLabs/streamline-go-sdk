// Security example for Streamline Go SDK.
//
// Demonstrates TLS and SASL authentication configuration.
//
// Run with:
//
//	SASL_USERNAME=admin SASL_PASSWORD=admin-secret go run examples/security/main.go
//	SECURITY_MODE=scram SASL_USERNAME=admin SASL_PASSWORD=admin-secret go run examples/security/main.go
//	SECURITY_MODE=tls CA_PATH=certs/ca.pem go run examples/security/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/streamlinelabs/streamline-go-sdk/streamline"
)

func saslPlainExample() error {
	fmt.Println("SASL/PLAIN Authentication")
	fmt.Println("----------------------------------------")

	client, err := streamline.NewClient(streamline.Config{
		Brokers: []string{envOr("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092")},
		SASL: &streamline.SASLConfig{
			Mechanism: "PLAIN",
			Username:  envOr("SASL_USERNAME", "admin"),
			Password:  envOr("SASL_PASSWORD", "admin-secret"),
		},
	})
	if err != nil {
		return fmt.Errorf("SASL/PLAIN connection failed: %w", err)
	}
	defer closeClient(client)

	ctx := context.Background()
	topics, err := client.Admin.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics failed: %w", err)
	}
	fmt.Printf("  Connected with SASL/PLAIN. Topics: %v\n", topics)

	result, err := client.Producer.Send(ctx, "secure-topic", nil, []byte("authenticated message"))
	if err != nil {
		return fmt.Errorf("produce failed: %w", err)
	}
	fmt.Printf("  Produced to partition=%d offset=%d\n\n", result.Partition, result.Offset)

	return nil
}

func scramExample() error {
	fmt.Println("SASL/SCRAM-SHA-256 Authentication")
	fmt.Println("----------------------------------------")

	client, err := streamline.NewClient(streamline.Config{
		Brokers: []string{envOr("STREAMLINE_BOOTSTRAP_SERVERS", "localhost:9092")},
		SASL: &streamline.SASLConfig{
			Mechanism: "SCRAM-SHA-256",
			Username:  envOr("SASL_USERNAME", "admin"),
			Password:  envOr("SASL_PASSWORD", "admin-secret"),
		},
	})
	if err != nil {
		return fmt.Errorf("SCRAM connection failed: %w", err)
	}
	defer closeClient(client)

	ctx := context.Background()
	topics, err := client.Admin.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics failed: %w", err)
	}
	fmt.Printf("  Connected with SCRAM-SHA-256. Topics: %v\n\n", topics)

	return nil
}

func tlsExample() error {
	fmt.Println("TLS Encrypted Connection")
	fmt.Println("----------------------------------------")

	client, err := streamline.NewClient(streamline.Config{
		Brokers: []string{envOr("STREAMLINE_TLS_BOOTSTRAP", "localhost:9093")},
		TLS: &streamline.TLSConfig{
			Enable:   true,
			CAFile:   envOr("CA_PATH", "certs/ca.pem"),
			CertFile: os.Getenv("CLIENT_CERT_PATH"),
			KeyFile:  os.Getenv("CLIENT_KEY_PATH"),
		},
	})
	if err != nil {
		return fmt.Errorf("TLS connection failed: %w", err)
	}
	defer closeClient(client)

	ctx := context.Background()
	topics, err := client.Admin.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics failed: %w", err)
	}
	fmt.Printf("  Connected with TLS. Topics: %v\n\n", topics)

	return nil
}

// closeClient closes the client and reports a close failure without masking
// the example's own result.
func closeClient(client *streamline.Client) {
	if err := client.Close(); err != nil {
		log.Printf("Failed to close client: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run holds the example body so deferred cleanup still runs when it fails:
// log.Fatal in main would skip every pending defer.
func run() error {
	fmt.Println("Streamline Security Examples")
	fmt.Println("========================================")
	fmt.Println()

	var err error
	switch envOr("SECURITY_MODE", "sasl_plain") {
	case "scram":
		err = scramExample()
	case "tls":
		err = tlsExample()
	default:
		err = saslPlainExample()
	}
	if err != nil {
		return err
	}

	fmt.Println("Done!")

	return nil
}
