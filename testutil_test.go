package streamline_test

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

// Environment variables that control the live-server test suite.
const (
	// EnvBootstrap overrides the Kafka bootstrap address.
	EnvBootstrap = "STREAMLINE_BOOTSTRAP"

	// EnvHTTP overrides the Streamline HTTP API base URL.
	EnvHTTP = "STREAMLINE_HTTP"

	// EnvSkipIntegration skips the live-server suite even when it was compiled
	// in with `-tags=integration`.
	EnvSkipIntegration = "STREAMLINE_SKIP_INTEGRATION"
)

// Defaults used when the corresponding environment variable is unset.
const (
	defaultBootstrap = "localhost:9092"
	defaultHTTPURL   = "http://localhost:9094"
)

// healthProbeTimeout bounds the readiness probe performed by SkipIfNoServer.
const healthProbeTimeout = 2 * time.Second

// IntegrationBootstrap returns the Kafka bootstrap address from the
// STREAMLINE_BOOTSTRAP env var, falling back to localhost:9092.
func IntegrationBootstrap() string {
	if v := os.Getenv(EnvBootstrap); v != "" {
		return v
	}
	return defaultBootstrap
}

// IntegrationHTTPURL returns the Streamline HTTP API URL from the
// STREAMLINE_HTTP env var, falling back to http://localhost:9094.
func IntegrationHTTPURL() string {
	if v := os.Getenv(EnvHTTP); v != "" {
		return v
	}
	return defaultHTTPURL
}

// IntegrationSkipReason reports why the live-server suite must not run, or an
// empty string when it may run. Selection is a pure function of the -short flag
// and STREAMLINE_SKIP_INTEGRATION so it can be tested without a server.
//
// The suite itself is additionally guarded by the "integration" build tag, so
// `go test ./...` never compiles — let alone runs — tests that need a server.
func IntegrationSkipReason(short bool, skipEnv string) string {
	if short {
		return "skipping live-server test in -short mode"
	}
	if truthy(skipEnv) {
		return "skipping live-server test: " + EnvSkipIntegration + " is set"
	}
	return ""
}

// truthy reports whether an environment variable value opts in.
func truthy(value string) bool {
	if value == "" {
		return false
	}
	if enabled, err := strconv.ParseBool(value); err == nil {
		return enabled
	}
	// Any other non-empty value (for example "yes") counts as set.
	return true
}

// RequireIntegration skips the test unless the live-server suite is selected.
func RequireIntegration(t *testing.T) {
	t.Helper()
	if reason := IntegrationSkipReason(testing.Short(), os.Getenv(EnvSkipIntegration)); reason != "" {
		t.Skip(reason)
	}
}

// SkipIfNoServer skips the test when the live-server suite is not selected or
// when no Streamline server answers the health endpoint. Only call it from
// tests guarded by the "integration" build tag: it performs a network request.
func SkipIfNoServer(t *testing.T) {
	t.Helper()
	RequireIntegration(t)

	client := &http.Client{Timeout: healthProbeTimeout}
	resp, err := client.Get(fmt.Sprintf("%s/health", IntegrationHTTPURL()))
	if err != nil {
		t.Skipf("skipping: Streamline server not reachable at %s: %v", IntegrationHTTPURL(), err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("skipping: Streamline health check returned %d", resp.StatusCode)
	}
}
