//go:build embedded && cgo

package embedded

import (
	"errors"
	"testing"
	"time"
)

// These tests exercise the parts of the CGO bindings that do not need a running
// engine, so they pass anywhere libstreamline can be linked.

func TestClosedInstanceReturnsErrClosed(t *testing.T) {
	instance := &Instance{}

	if err := instance.Produce("topic", []byte("value")); !errors.Is(err, ErrClosed) {
		t.Errorf("Produce error = %v, want ErrClosed", err)
	}
	if err := instance.ProduceWithKey("topic", []byte("value"), []byte("key")); !errors.Is(err, ErrClosed) {
		t.Errorf("ProduceWithKey error = %v, want ErrClosed", err)
	}
	if err := instance.CreateTopic("topic", 1); !errors.Is(err, ErrClosed) {
		t.Errorf("CreateTopic error = %v, want ErrClosed", err)
	}
	if _, err := instance.consumeOnce("topic"); !errors.Is(err, ErrClosed) {
		t.Errorf("consumeOnce error = %v, want ErrClosed", err)
	}

	instance.Close() // must not panic on an already-closed instance
}

func TestQueryUnsupported(t *testing.T) {
	result, err := (&Instance{}).Query("SELECT 1")
	if result != "" {
		t.Errorf("Query result = %q, want empty", result)
	}
	if !errors.Is(err, ErrQueryUnsupported) {
		t.Errorf("Query error = %v, want ErrQueryUnsupported", err)
	}
}

func TestVersionLinked(t *testing.T) {
	if Version() == "" {
		t.Error("Version() returned an empty string; libstreamline should report a version")
	}
}

func TestByteSliceEmpty(t *testing.T) {
	ptr, length := byteSlice(nil)
	if ptr != nil || length != 0 {
		t.Errorf("byteSlice(nil) = (%v, %d), want (nil, 0)", ptr, length)
	}

	ptr, length = byteSlice([]byte("abc"))
	if ptr == nil || length != 3 {
		t.Errorf("byteSlice(\"abc\") = (%v, %d), want (non-nil, 3)", ptr, length)
	}
}

// TestConsumeOnClosedInstanceFailsFast covers the Consume polling loop: a closed
// instance must surface ErrClosed instead of spinning until the timeout.
func TestConsumeOnClosedInstanceFailsFast(t *testing.T) {
	start := time.Now()

	msg, err := (&Instance{}).Consume("topic", 5*time.Second)
	if msg != nil {
		t.Errorf("Consume message = %+v, want nil", msg)
	}
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Consume error = %v, want ErrClosed", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Consume blocked for %v, want an immediate ErrClosed", elapsed)
	}
}

// TestConsumeOffsetsAdvancePerTopic documents that Consume tracks a per-topic
// read position, which is what makes repeated calls walk the log in order.
func TestConsumeOffsetsAdvancePerTopic(t *testing.T) {
	instance := &Instance{offsets: map[string]int64{}}

	instance.offsets["a"] = 3
	instance.offsets["b"] = 7

	if instance.offsets["a"] == instance.offsets["b"] {
		t.Fatal("offsets must be tracked per topic")
	}
	if got := instance.offsets["missing"]; got != 0 {
		t.Errorf("offset for an unseen topic = %d, want 0 (read from the beginning)", got)
	}
}
