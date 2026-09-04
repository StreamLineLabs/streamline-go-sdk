package streamline

import (
	"os"
	"regexp"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var semanticVersionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func TestVersionIsStableSemanticVersion(t *testing.T) {
	if Version != "0.4.0" {
		t.Fatalf("Version = %q, want 0.4.0", Version)
	}
	if !semanticVersionPattern.MatchString(Version) {
		t.Fatalf("Version %q is not a stable semantic version", Version)
	}
}

func TestReleaseTagMatchesVersion(t *testing.T) {
	tag := os.Getenv("STREAMLINE_RELEASE_TAG")
	if tag == "" {
		t.Skip("STREAMLINE_RELEASE_TAG is set only by the release workflow")
	}
	if err := validateReleaseTag(tag); err != nil {
		t.Fatal(err)
	}
}

func TestValidateReleaseTag(t *testing.T) {
	if err := validateReleaseTag("v0.4.0"); err != nil {
		t.Fatalf("validateReleaseTag(v0.4.0): %v", err)
	}
	for _, tag := range []string{"", "0.4.0", "v0.3.0", "v0.3.0-rc.1", "v0.4.1"} {
		if err := validateReleaseTag(tag); err == nil {
			t.Errorf("validateReleaseTag(%q) succeeded, want mismatch error", tag)
		}
	}
}

func TestTracingConstructorsUseSDKVersion(t *testing.T) {
	previous := otel.GetTracerProvider()
	recorder := &recordingTracerProvider{
		TracerProvider: noop.NewTracerProvider(),
	}
	otel.SetTracerProvider(recorder)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
	})

	NewTracingProducer(nil)
	NewTracingConsumer(nil)

	calls := recorder.callsSnapshot()
	if len(calls) != 2 {
		t.Fatalf("recorded %d tracer calls, want 2", len(calls))
	}
	for _, call := range calls {
		if call.name != instrumentationName {
			t.Errorf("tracer name = %q, want %q", call.name, instrumentationName)
		}
		if call.version != Version {
			t.Errorf("tracer version = %q, want %q", call.version, Version)
		}
	}
}

type tracerCall struct {
	name    string
	version string
}

type recordingTracerProvider struct {
	trace.TracerProvider

	mu    sync.Mutex
	calls []tracerCall
}

func (p *recordingTracerProvider) Tracer(name string, options ...trace.TracerOption) trace.Tracer {
	config := trace.NewTracerConfig(options...)

	p.mu.Lock()
	p.calls = append(p.calls, tracerCall{
		name:    name,
		version: config.InstrumentationVersion(),
	})
	p.mu.Unlock()

	return p.TracerProvider.Tracer(name, options...)
}

func (p *recordingTracerProvider) callsSnapshot() []tracerCall {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]tracerCall(nil), p.calls...)
}
