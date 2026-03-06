package streamline

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const wireFormatMagicByte byte = 0x00

// SchemaSerializerOption configures a SchemaSerializer.
type SchemaSerializerOption func(*SchemaSerializer)

// WithAutoRegister enables or disables automatic schema registration on produce.
func WithAutoRegister(b bool) SchemaSerializerOption {
	return func(s *SchemaSerializer) {
		s.autoRegister = b
	}
}

// SchemaSerializer handles serialization and deserialization of messages
// with Confluent-compatible wire format (magic byte + 4-byte schema ID + payload).
type SchemaSerializer struct {
	registry     *SchemaRegistryClient
	subjectCache map[string]int
	schemaCache  map[int]string
	mu           sync.RWMutex
	autoRegister bool
}

// NewSchemaSerializer creates a new SchemaSerializer backed by the given registry client.
func NewSchemaSerializer(registry *SchemaRegistryClient, opts ...SchemaSerializerOption) *SchemaSerializer {
	s := &SchemaSerializer{
		registry:     registry,
		subjectCache: make(map[string]int),
		schemaCache:  make(map[int]string),
		autoRegister: true,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Serialize marshals value as JSON and prepends the wire format header.
// The subject and schema are used to look up (or register) the schema ID.
func (s *SchemaSerializer) Serialize(subject string, schema string, schemaType SchemaType, value interface{}) ([]byte, error) {
	id, err := s.resolveSchemaID(subject, schema, schemaType)
	if err != nil {
		return nil, fmt.Errorf("streamline: resolve schema ID: %w", err)
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("streamline: marshal value: %w", err)
	}

	// Wire format: magic byte (1) + schema ID (4) + payload (N)
	buf := make([]byte, 5+len(payload))
	buf[0] = wireFormatMagicByte
	binary.BigEndian.PutUint32(buf[1:5], uint32(id))
	copy(buf[5:], payload)

	return buf, nil
}

// resolveSchemaID returns the cached schema ID for the subject, or registers it.
func (s *SchemaSerializer) resolveSchemaID(subject, schema string, schemaType SchemaType) (int, error) {
	s.mu.RLock()
	if id, ok := s.subjectCache[subject]; ok {
		s.mu.RUnlock()
		return id, nil
	}
	s.mu.RUnlock()

	if !s.autoRegister {
		return 0, fmt.Errorf("schema not cached for subject %q and auto-register is disabled", subject)
	}

	id, err := s.registry.RegisterSchema(subject, schema, schemaType)
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	s.subjectCache[subject] = id
	s.schemaCache[id] = schema
	s.mu.Unlock()

	return id, nil
}

// Deserialize parses the wire format header and unmarshals the JSON payload into target.
// Returns the schema ID extracted from the header.
func (s *SchemaSerializer) Deserialize(data []byte, target interface{}) (int, error) {
	if len(data) < 5 {
		return 0, fmt.Errorf("streamline: data too short for wire format (got %d bytes, need >= 5)", len(data))
	}
	if data[0] != wireFormatMagicByte {
		return 0, fmt.Errorf("streamline: invalid magic byte: expected 0x%02x, got 0x%02x", wireFormatMagicByte, data[0])
	}

	id := int(binary.BigEndian.Uint32(data[1:5]))
	payload := data[5:]

	if err := json.Unmarshal(payload, target); err != nil {
		return 0, fmt.Errorf("streamline: unmarshal payload: %w", err)
	}

	return id, nil
}

// DeserializedMessage wraps a ConsumerMessage with the parsed schema ID
// and the raw JSON payload for further processing.
type DeserializedMessage struct {
	*ConsumerMessage
	SchemaID int
	Payload  json.RawMessage
}

// SchemaProducer wraps a Client producer with automatic schema serialization.
type SchemaProducer struct {
	client     *Client
	serializer *SchemaSerializer
	subject    string
	schema     string
	schemaType SchemaType
}

// NewSchemaProducer creates a producer that serializes values using the schema registry.
func NewSchemaProducer(client *Client, registry *SchemaRegistryClient, subject, schema string, schemaType SchemaType) *SchemaProducer {
	return &SchemaProducer{
		client:     client,
		serializer: NewSchemaSerializer(registry),
		subject:    subject,
		schema:     schema,
		schemaType: schemaType,
	}
}

// Send serializes value through the schema registry and produces the message.
func (sp *SchemaProducer) Send(ctx context.Context, topic string, key []byte, value interface{}) (*ProducerResult, error) {
	encoded, err := sp.serializer.Serialize(sp.subject, sp.schema, sp.schemaType, value)
	if err != nil {
		return nil, err
	}
	return sp.client.Producer.Send(ctx, topic, key, encoded)
}

// SchemaConsumer wraps a Consumer with automatic schema deserialization.
type SchemaConsumer struct {
	consumer   *Consumer
	serializer *SchemaSerializer
}

// NewSchemaConsumer creates a consumer that deserializes values using the schema registry wire format.
func NewSchemaConsumer(consumer *Consumer, serializer *SchemaSerializer) *SchemaConsumer {
	return &SchemaConsumer{
		consumer:   consumer,
		serializer: serializer,
	}
}

// Poll retrieves messages and deserializes their values from the wire format.
func (sc *SchemaConsumer) Poll(ctx context.Context, maxRecords int, timeout time.Duration) ([]*DeserializedMessage, error) {
	msgs, err := sc.consumer.Poll(ctx, maxRecords, timeout)
	if err != nil {
		return nil, err
	}

	result := make([]*DeserializedMessage, 0, len(msgs))
	for _, msg := range msgs {
		var raw json.RawMessage
		schemaID, err := sc.serializer.Deserialize(msg.Value, &raw)
		if err != nil {
			return nil, fmt.Errorf("streamline: deserialize message at offset %d: %w", msg.Offset, err)
		}
		result = append(result, &DeserializedMessage{
			ConsumerMessage: msg,
			SchemaID:        schemaID,
			Payload:         raw,
		})
	}
	return result, nil
}

// Close closes the underlying consumer.
func (sc *SchemaConsumer) Close() error {
	return sc.consumer.Close()
}
