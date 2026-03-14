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

func saslPlainExample() {
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
		log.Fatalf("SASL/PLAIN connection failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	topics, err := client.Admin.ListTopics(ctx)
	if err != nil {
		log.Fatalf("List topics failed: %v", err)
	}
	fmt.Printf("  Connected with SASL/PLAIN. Topics: %v\n", topics)

	result, err := client.Producer.Send(ctx, "secure-topic", nil, []byte("authenticated message"))
	if err != nil {
		log.Fatalf("Produce failed: %v", err)
	}
	fmt.Printf("  Produced to partition=%d offset=%d\n\n", result.Partition, result.Offset)
}

func scramExample() {
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
		log.Fatalf("SCRAM connection failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	topics, err := client.Admin.ListTopics(ctx)
	if err != nil {
		log.Fatalf("List topics failed: %v", err)
	}
	fmt.Printf("  Connected with SCRAM-SHA-256. Topics: %v\n\n", topics)
}

func tlsExample() {
	fmt.Println("TLS Encrypted Connection")
	fmt.Println("----------------------------------------")

	client, err := streamline.NewClient(streamline.Config{
		Brokers: []string{envOr("STREAMLINE_TLS_BOOTSTRAP", "localhost:9093")},
		TLS: &streamline.TLSConfig{
			CAPath:   envOr("CA_PATH", "certs/ca.pem"),
			CertPath: os.Getenv("CLIENT_CERT_PATH"),
			KeyPath:  os.Getenv("CLIENT_KEY_PATH"),
		},
	})
	if err != nil {
		log.Fatalf("TLS connection failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	topics, err := client.Admin.ListTopics(ctx)
	if err != nil {
		log.Fatalf("List topics failed: %v", err)
	}
	fmt.Printf("  Connected with TLS. Topics: %v\n\n", topics)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	fmt.Println("Streamline Security Examples")
	fmt.Println("========================================")
	fmt.Println()

	switch envOr("SECURITY_MODE", "sasl_plain") {
	case "scram":
		scramExample()
	case "tls":
		tlsExample()
	default:
		saslPlainExample()
	}

	fmt.Println("Done!")
}
