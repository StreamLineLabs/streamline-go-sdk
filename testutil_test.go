package streamline_test

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

// IntegrationBootstrap returns the Kafka bootstrap address from the
// STREAMLINE_BOOTSTRAP env var, falling back to localhost:9092.
func IntegrationBootstrap() string {
	if v := os.Getenv("STREAMLINE_BOOTSTRAP"); v != "" {
		return v
	}
	return "localhost:9092"
}

// IntegrationHTTPURL returns the Streamline HTTP API URL from the
// STREAMLINE_HTTP env var, falling back to http://localhost:9094.
func IntegrationHTTPURL() string {
	if v := os.Getenv("STREAMLINE_HTTP"); v != "" {
		return v
	}
	return "http://localhost:9094"
}

// SkipIfNoServer skips the test when no Streamline server is reachable,
// unless -short is used (which also skips). Useful for integration tests
// that should be silently skipped in local development.
func SkipIfNoServer(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("%s/health", IntegrationHTTPURL()))
	if err != nil {
		t.Skipf("skipping: Streamline server not reachable at %s: %v", IntegrationHTTPURL(), err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("skipping: Streamline health check returned %d", resp.StatusCode)
	}
}
