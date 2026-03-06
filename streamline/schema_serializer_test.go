package streamline

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// sampleEvent is a test struct for serialization round-trips.
type sampleEvent struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestSchemaSerializer_Serialize_WireFormat(t *testing.T) {
	// Set up a fake registry that returns schema ID 42.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":42}`)
	}))
	defer srv.Close()

	registry := NewSchemaRegistryClient(srv.URL)
	serializer := NewSchemaSerializer(registry)

	event := sampleEvent{ID: 1, Name: "test"}
	data, err := serializer.Serialize("test-subject", `{"type":"record"}`, SchemaTypeJSON, event)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Verify magic byte
	if data[0] != 0x00 {
		t.Errorf("expected magic byte 0x00, got 0x%02x", data[0])
	}

	// Verify schema ID (big-endian 42)
	schemaID := binary.BigEndian.Uint32(data[1:5])
	if schemaID != 42 {
		t.Errorf("expected schema ID 42, got %d", schemaID)
	}

	// Verify JSON payload
	var decoded sampleEvent
	if err := json.Unmarshal(data[5:], &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if decoded.ID != 1 || decoded.Name != "test" {
		t.Errorf("payload mismatch: got %+v", decoded)
	}
}

func TestSchemaSerializer_Deserialize(t *testing.T) {
	payload, _ := json.Marshal(sampleEvent{ID: 7, Name: "hello"})
	data := make([]byte, 5+len(payload))
	data[0] = 0x00
	binary.BigEndian.PutUint32(data[1:5], 99)
	copy(data[5:], payload)

	serializer := NewSchemaSerializer(NewSchemaRegistryClient("http://unused"))

	var target sampleEvent
	schemaID, err := serializer.Deserialize(data, &target)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	if schemaID != 99 {
		t.Errorf("expected schema ID 99, got %d", schemaID)
	}
	if target.ID != 7 || target.Name != "hello" {
		t.Errorf("deserialized value mismatch: got %+v", target)
	}
}

func TestSchemaSerializer_Deserialize_TooShort(t *testing.T) {
	serializer := NewSchemaSerializer(NewSchemaRegistryClient("http://unused"))
	var target sampleEvent
	_, err := serializer.Deserialize([]byte{0x00, 0x01}, &target)
	if err == nil {
		t.Fatal("expected error for short data, got nil")
	}
}

func TestSchemaSerializer_Deserialize_BadMagic(t *testing.T) {
	data := []byte{0xFF, 0x00, 0x00, 0x00, 0x01, '{', '}'}
	serializer := NewSchemaSerializer(NewSchemaRegistryClient("http://unused"))
	var target sampleEvent
	_, err := serializer.Deserialize(data, &target)
	if err == nil {
		t.Fatal("expected error for bad magic byte, got nil")
	}
}

func TestSchemaSerializer_RoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":10}`)
	}))
	defer srv.Close()

	registry := NewSchemaRegistryClient(srv.URL)
	serializer := NewSchemaSerializer(registry)

	original := sampleEvent{ID: 42, Name: "round-trip"}
	data, err := serializer.Serialize("my-subject", `{}`, SchemaTypeJSON, original)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}

	var result sampleEvent
	schemaID, err := serializer.Deserialize(data, &result)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	if schemaID != 10 {
		t.Errorf("expected schema ID 10, got %d", schemaID)
	}
	if result != original {
		t.Errorf("round-trip mismatch: got %+v, want %+v", result, original)
	}
}

func TestSchemaSerializer_CacheBehavior(t *testing.T) {
	var registrations int
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		registrations++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":5}`)
	}))
	defer srv.Close()

	registry := NewSchemaRegistryClient(srv.URL)
	serializer := NewSchemaSerializer(registry)

	event := sampleEvent{ID: 1, Name: "cache-test"}

	// First call should hit the registry.
	_, err := serializer.Serialize("cached-subject", `{}`, SchemaTypeJSON, event)
	if err != nil {
		t.Fatalf("first Serialize: %v", err)
	}

	// Second call should use cache — no additional HTTP call.
	_, err = serializer.Serialize("cached-subject", `{}`, SchemaTypeJSON, event)
	if err != nil {
		t.Fatalf("second Serialize: %v", err)
	}

	mu.Lock()
	count := registrations
	mu.Unlock()

	if count != 1 {
		t.Errorf("expected 1 registry call (cached), got %d", count)
	}
}

func TestSchemaSerializer_AutoRegisterDisabled(t *testing.T) {
	registry := NewSchemaRegistryClient("http://unused")
	serializer := NewSchemaSerializer(registry, WithAutoRegister(false))

	event := sampleEvent{ID: 1, Name: "no-register"}
	_, err := serializer.Serialize("unknown-subject", `{}`, SchemaTypeJSON, event)
	if err == nil {
		t.Fatal("expected error when auto-register is disabled and subject is not cached")
	}
}

func TestSchemaSerializer_ConcurrentAccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":77}`)
	}))
	defer srv.Close()

	registry := NewSchemaRegistryClient(srv.URL)
	serializer := NewSchemaSerializer(registry)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			subject := fmt.Sprintf("subject-%d", n%5)
			_, err := serializer.Serialize(subject, `{}`, SchemaTypeJSON, sampleEvent{ID: n})
			if err != nil {
				t.Errorf("concurrent Serialize(%s): %v", subject, err)
			}
		}(i)
	}
	wg.Wait()
}
