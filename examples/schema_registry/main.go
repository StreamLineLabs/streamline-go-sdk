package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/streamlinelabs/streamline-go-sdk/streamline"
)

// User represents the Avro-registered User record.
type User struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	CreatedAt string `json:"created_at"`
}

// Avro schema for the User record.
const userSchema = `{
  "type": "record",
  "name": "User",
  "namespace": "com.streamline.examples",
  "fields": [
    {"name": "id",         "type": "int"},
    {"name": "name",       "type": "string"},
    {"name": "email",      "type": "string"},
    {"name": "created_at", "type": "string"}
  ]
}`

const (
	subject = "users-value"
	topic   = "users"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run holds the example body so deferred cleanup still runs when it fails:
// log.Fatal in main would skip every pending defer.
func run() error {
	ctx := context.Background()

	brokers := os.Getenv("STREAMLINE_BOOTSTRAP_SERVERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	registryURL := os.Getenv("STREAMLINE_SCHEMA_REGISTRY_URL")
	if registryURL == "" {
		registryURL = "http://localhost:9094"
	}

	// === 1. Create Streamline client ===
	config := streamline.DefaultConfig()
	config.Brokers = []string{brokers}

	client, err := streamline.NewClient(config)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("Failed to close client: %v", closeErr)
		}
	}()

	// === 2. Create a schema registry client ===
	registry := streamline.NewSchemaRegistryClient(registryURL)

	// Ensure the topic exists
	if createErr := client.Admin.CreateTopic(ctx, streamline.TopicConfig{
		Name:              topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	}); createErr != nil {
		log.Printf("Warning: failed to create topic (may already exist): %v", createErr)
	}

	// === 3. Register an Avro schema ===
	fmt.Println("=== Registering Schema ===")
	schemaID, err := registry.RegisterSchema(subject, userSchema, streamline.SchemaTypeAvro)
	if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}
	fmt.Printf("Registered schema with id=%d for subject=%s\n", schemaID, subject)

	// Retrieve the schema back by id
	retrieved, err := registry.GetSchema(schemaID)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}
	fmt.Printf("Retrieved schema: type=%s schema=%s\n", retrieved.Type, retrieved.Schema)

	// === 4. Check schema compatibility ===
	fmt.Println("\n=== Checking Compatibility ===")
	compatible, err := registry.CheckCompatibility(subject, userSchema, streamline.SchemaTypeAvro)
	if err != nil {
		return fmt.Errorf("failed to check compatibility: %w", err)
	}
	fmt.Printf("Schema compatible: %v\n", compatible)

	// === 5. Produce messages with schema validation ===
	// In practice, the schema registry validates on the server side.
	// The client serializes to JSON and the server validates against the registered schema.
	fmt.Println("\n=== Producing Messages with Schema ===")
	for i := 0; i < 5; i++ {
		user := User{
			ID:        i,
			Name:      fmt.Sprintf("user-%d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			CreatedAt: "2025-01-15T10:00:00Z",
		}
		value, marshalErr := json.Marshal(user)
		if marshalErr != nil {
			return fmt.Errorf("failed to marshal user: %w", marshalErr)
		}

		result, sendErr := client.Producer.Send(ctx, topic,
			[]byte(fmt.Sprintf("user-%d", i)),
			value,
		)
		if sendErr != nil {
			log.Printf("Failed to send message: %v", sendErr)
			continue
		}
		fmt.Printf("Produced user-%d to partition %d at offset %d\n",
			i, result.Partition, result.Offset)
	}

	// === 6. Consume and deserialize with schema ===
	fmt.Println("\n=== Consuming Messages with Schema ===")
	consumer, err := client.NewConsumer(ctx, "go-schema-group", []string{topic})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer func() {
		if closeErr := consumer.Close(); closeErr != nil {
			log.Printf("Failed to close consumer: %v", closeErr)
		}
	}()

	messages, consumerErrs := consumer.Start(ctx)
	consumed := 0

	for consumed < 5 {
		select {
		case msg, ok := <-messages:
			if !ok {
				fmt.Println("Consumer channel closed")
				return nil
			}

			var user User
			if unmarshalErr := json.Unmarshal(msg.Value, &user); unmarshalErr != nil {
				log.Printf("Failed to deserialize message: %v", unmarshalErr)
				continue
			}

			fmt.Printf("Received: partition=%d offset=%d user={id:%d name:%s email:%s}\n",
				msg.Partition, msg.Offset, user.ID, user.Name, user.Email)
			consumed++

		case consumeErr := <-consumerErrs:
			log.Printf("Consumer error: %v", consumeErr)
		}
	}

	fmt.Println("\nDone!")

	return nil
}
