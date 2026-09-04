//go:build !embedded || !cgo

package embedded

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewReturnsNotEnabled(t *testing.T) {
	instance, err := New(Config{InMemory: true})
	if instance != nil {
		t.Fatalf("expected a nil instance, got %+v", instance)
	}
	if !errors.Is(err, ErrNotEnabled) {
		t.Fatalf("expected ErrNotEnabled, got %v", err)
	}
}

func TestOperationsReturnNotEnabled(t *testing.T) {
	instance := &Instance{}

	tests := []struct {
		name string
		err  error
	}{
		{"Produce", instance.Produce("topic", []byte("value"))},
		{"ProduceWithKey", instance.ProduceWithKey("topic", []byte("value"), []byte("key"))},
		{"CreateTopic", instance.CreateTopic("topic", 1)},
	}
	for _, tt := range tests {
		if !errors.Is(tt.err, ErrNotEnabled) {
			t.Errorf("%s error = %v, want ErrNotEnabled", tt.name, tt.err)
		}
	}

	msg, err := instance.Consume("topic", time.Millisecond)
	if msg != nil {
		t.Errorf("Consume message = %+v, want nil", msg)
	}
	if !errors.Is(err, ErrNotEnabled) {
		t.Errorf("Consume error = %v, want ErrNotEnabled", err)
	}

	result, err := instance.Query("SELECT 1")
	if result != "" {
		t.Errorf("Query result = %q, want empty", result)
	}
	if !errors.Is(err, ErrNotEnabled) {
		t.Errorf("Query error = %v, want ErrNotEnabled", err)
	}

	instance.Close() // must not panic
}

func TestVersionEmptyWithoutTag(t *testing.T) {
	if v := Version(); v != "" {
		t.Errorf("Version() = %q, want empty string without the embedded build tag", v)
	}
}

func TestErrNotEnabledMentionsBuildTag(t *testing.T) {
	msg := ErrNotEnabled.Error()
	for _, want := range []string{"embedded", "CGO_ENABLED=1", "libstreamline"} {
		if !strings.Contains(msg, want) {
			t.Errorf("ErrNotEnabled message %q should mention %q", msg, want)
		}
	}
}

// TestConsumeDoesNotWaitForTimeout keeps the stub honest: without the build tag
// Consume must fail immediately instead of blocking for the caller's timeout.
func TestConsumeDoesNotWaitForTimeout(t *testing.T) {
	start := time.Now()

	if _, err := (&Instance{}).Consume("topic", 5*time.Second); !errors.Is(err, ErrNotEnabled) {
		t.Fatalf("Consume error = %v, want ErrNotEnabled", err)
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Consume blocked for %v, want an immediate ErrNotEnabled", elapsed)
	}
}
