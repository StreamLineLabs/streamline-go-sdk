package streamline_test

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
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

	// EnvRequireIntegration makes an unavailable live server a test failure
	// instead of a local-development skip.
	EnvRequireIntegration = "STREAMLINE_REQUIRE_INTEGRATION"

	// EnvAuthEnabled opts into auth conformance. When true, the suite requires
	// explicit modes and configuration and never silently skips a missing
	// server.
	EnvAuthEnabled = "STREAMLINE_AUTH_ENABLED"

	// EnvAuthModes is a comma-separated list of auth modes to test.
	EnvAuthModes = "STREAMLINE_AUTH_MODES"

	// EnvAuthUsername and EnvAuthPassword contain credentials for SASL modes.
	EnvAuthUsername = "STREAMLINE_AUTH_USERNAME"
	EnvAuthPassword = "STREAMLINE_AUTH_PASSWORD"

	// EnvAuthTLSBootstrap is the Kafka bootstrap address for TLS and mTLS.
	EnvAuthTLSBootstrap = "STREAMLINE_AUTH_TLS_BOOTSTRAP"

	// EnvAuthCAFile, EnvAuthClientCertFile, and EnvAuthClientKeyFile point to
	// certificate material supplied by the external auth fixture.
	EnvAuthCAFile         = "STREAMLINE_AUTH_CA_FILE"
	EnvAuthClientCertFile = "STREAMLINE_AUTH_CLIENT_CERT_FILE"
	EnvAuthClientKeyFile  = "STREAMLINE_AUTH_CLIENT_KEY_FILE"

	// EnvAuthSASLTLSEnabled enables TLS for the SASL conformance modes.
	EnvAuthSASLTLSEnabled = "STREAMLINE_AUTH_SASL_TLS_ENABLED"
)

// Defaults used when the corresponding environment variable is unset.
const (
	defaultBootstrap = "localhost:9092"
	defaultHTTPURL   = "http://localhost:9094"
)

// healthProbeTimeout bounds the readiness probe performed by SkipIfNoServer.
const healthProbeTimeout = 2 * time.Second

const (
	authModeTLS         = "tls"
	authModeMTLS        = "mtls"
	authModePlain       = "plain"
	authModeSCRAMSHA256 = "scram-sha-256"
	authModeSCRAMSHA512 = "scram-sha-512"
)

var supportedAuthModes = map[string]struct{}{
	authModeTLS:         {},
	authModeMTLS:        {},
	authModePlain:       {},
	authModeSCRAMSHA256: {},
	authModeSCRAMSHA512: {},
}

type authSelection struct {
	enabled bool
	modes   map[string]struct{}
}

func (s authSelection) includes(mode string) bool {
	_, ok := s.modes[mode]
	return ok
}

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

func parseStrictBool(name, value string) (bool, error) {
	if value == "" {
		return false, nil
	}
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean, got %q", name, value)
	}
	return enabled, nil
}

func integrationRequired(requireEnv, authEnabledEnv string) (bool, error) {
	required, err := parseStrictBool(EnvRequireIntegration, requireEnv)
	if err != nil {
		return false, err
	}
	authEnabled, err := parseStrictBool(EnvAuthEnabled, authEnabledEnv)
	if err != nil {
		return false, err
	}
	return required || authEnabled, nil
}

func parseAuthSelection(enabledEnv, modesEnv string) (authSelection, error) {
	enabled, err := parseStrictBool(EnvAuthEnabled, enabledEnv)
	if err != nil {
		return authSelection{}, err
	}
	selection := authSelection{
		enabled: enabled,
		modes:   make(map[string]struct{}),
	}
	if !enabled {
		return selection, nil
	}

	for _, rawMode := range strings.Split(modesEnv, ",") {
		mode := strings.ToLower(strings.TrimSpace(rawMode))
		if mode == "" {
			continue
		}
		if _, ok := supportedAuthModes[mode]; !ok {
			return authSelection{}, fmt.Errorf(
				"%s contains unsupported mode %q; supported modes: %s",
				EnvAuthModes,
				mode,
				strings.Join([]string{
					authModeTLS,
					authModeMTLS,
					authModePlain,
					authModeSCRAMSHA256,
					authModeSCRAMSHA512,
				}, ", "),
			)
		}
		selection.modes[mode] = struct{}{}
	}
	if len(selection.modes) == 0 {
		return authSelection{}, fmt.Errorf("%s must list at least one mode when %s=true", EnvAuthModes, EnvAuthEnabled)
	}
	return selection, nil
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
		required, err := integrationRequired(
			os.Getenv(EnvRequireIntegration),
			os.Getenv(EnvAuthEnabled),
		)
		if err != nil {
			t.Fatal(err)
		}
		if required {
			t.Fatalf("live-server conformance is required but selection requested a skip: %s", reason)
		}
		t.Skip(reason)
	}
}

func RequireAuth(t *testing.T) authSelection {
	t.Helper()

	selection, err := parseAuthSelection(
		os.Getenv(EnvAuthEnabled),
		os.Getenv(EnvAuthModes),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !selection.enabled {
		t.Skip("auth conformance is disabled; set " + EnvAuthEnabled + "=true with explicit " + EnvAuthModes)
	}
	RequireIntegration(t)
	return selection
}

func RequireAuthMode(t *testing.T, mode string) authSelection {
	t.Helper()

	selection := RequireAuth(t)
	if !selection.includes(mode) {
		t.Skipf("auth mode %q is not selected by %s", mode, EnvAuthModes)
	}
	return selection
}

func RequireAuthEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for the selected auth conformance mode", name)
	}
	return value
}

func AuthSASLTLSEnabled(t *testing.T) bool {
	t.Helper()
	enabled, err := parseStrictBool(EnvAuthSASLTLSEnabled, os.Getenv(EnvAuthSASLTLSEnabled))
	if err != nil {
		t.Fatal(err)
	}
	return enabled
}

// SkipIfNoServer skips the test when the live-server suite is not selected or
// when no Streamline server answers the health endpoint. Only call it from
// tests guarded by the "integration" build tag: it performs a network request.
func SkipIfNoServer(t *testing.T) {
	t.Helper()
	RequireIntegration(t)

	required, err := integrationRequired(
		os.Getenv(EnvRequireIntegration),
		os.Getenv(EnvAuthEnabled),
	)
	if err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Timeout: healthProbeTimeout}
	resp, err := client.Get(fmt.Sprintf("%s/health", IntegrationHTTPURL()))
	if err != nil {
		if required {
			t.Fatalf("required Streamline server is not reachable at %s: %v", IntegrationHTTPURL(), err)
		}
		t.Skipf("skipping: Streamline server not reachable at %s: %v", IntegrationHTTPURL(), err)
	}
	if closeErr := resp.Body.Close(); closeErr != nil {
		t.Errorf("close Streamline health response: %v", closeErr)
	}
	if resp.StatusCode != http.StatusOK {
		if required {
			t.Fatalf("required Streamline health check returned %d", resp.StatusCode)
		}
		t.Skipf("skipping: Streamline health check returned %d", resp.StatusCode)
	}
}
