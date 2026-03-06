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
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// === 2. Create a schema registry client ===
	registry := streamline.NewSchemaRegistryClient(registryURL)

	// Ensure the topic exists
	err = client.Admin.CreateTopic(ctx, streamline.TopicConfig{
		Name:              topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		log.Printf("Warning: failed to create topic (may already exist): %v", err)
	}

	// === 3. Register an Avro schema ===
	fmt.Println("=== Registering Schema ===")
	schemaID, err := registry.RegisterSchema(subject, userSchema, streamline.SchemaTypeAvro)
	if err != nil {
		log.Fatalf("Failed to register schema: %v", err)
	}
	fmt.Printf("Registered schema with id=%d for subject=%s\n", schemaID, subject)

	// Retrieve the schema back by id
	retrieved, err := registry.GetSchema(schemaID)
	if err != nil {
		log.Fatalf("Failed to get schema: %v", err)
	}
	fmt.Printf("Retrieved schema: type=%s schema=%s\n", retrieved.Type, retrieved.Schema)

	// === 4. Check schema compatibility ===
	fmt.Println("\n=== Checking Compatibility ===")
	compatible, err := registry.CheckCompatibility(subject, userSchema, streamline.SchemaTypeAvro)
	if err != nil {
		log.Fatalf("Failed to check compatibility: %v", err)
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
		value, err := json.Marshal(user)
		if err != nil {
			log.Fatalf("Failed to marshal user: %v", err)
		}

		result, err := client.Producer.Send(ctx, topic,
			[]byte(fmt.Sprintf("user-%d", i)),
			value,
		)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}
		fmt.Printf("Produced user-%d to partition %d at offset %d\n",
			i, result.Partition, result.Offset)
	}

	// === 6. Consume and deserialize with schema ===
	fmt.Println("\n=== Consuming Messages with Schema ===")
	consumer, err := client.NewConsumer(ctx, "go-schema-group", []string{topic})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	messages, errors := consumer.Start(ctx)
	consumed := 0

	for consumed < 5 {
		select {
		case msg, ok := <-messages:
			if !ok {
				fmt.Println("Consumer channel closed")
				return
			}

			var user User
			if err := json.Unmarshal(msg.Value, &user); err != nil {
				log.Printf("Failed to deserialize message: %v", err)
				continue
			}

			fmt.Printf("Received: partition=%d offset=%d user={id:%d name:%s email:%s}\n",
				msg.Partition, msg.Offset, user.ID, user.Name, user.Email)
			consumed++

		case err := <-errors:
			log.Printf("Consumer error: %v", err)
		}
	}

	fmt.Println("\nDone!")
}
