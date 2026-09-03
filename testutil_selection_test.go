package streamline_test

import (
	"strings"
	"testing"
)

// These tests cover the live-server test selection itself, so the gating logic
// stays verifiable without a Streamline server (or Docker) available.

func TestIntegrationSkipReason(t *testing.T) {
	tests := []struct {
		name       string
		short      bool
		skipEnv    string
		wantSkip   bool
		wantReason string
	}{
		{name: "selected", short: false, skipEnv: "", wantSkip: false},
		{name: "short mode", short: true, skipEnv: "", wantSkip: true, wantReason: "-short"},
		{name: "skip env true", short: false, skipEnv: "true", wantSkip: true, wantReason: EnvSkipIntegration},
		{name: "skip env 1", short: false, skipEnv: "1", wantSkip: true, wantReason: EnvSkipIntegration},
		{name: "skip env yes", short: false, skipEnv: "yes", wantSkip: true, wantReason: EnvSkipIntegration},
		{name: "skip env false", short: false, skipEnv: "false", wantSkip: false},
		{name: "skip env 0", short: false, skipEnv: "0", wantSkip: false},
		{name: "short wins over skip env false", short: true, skipEnv: "false", wantSkip: true, wantReason: "-short"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason := IntegrationSkipReason(tt.short, tt.skipEnv)
			if gotSkip := reason != ""; gotSkip != tt.wantSkip {
				t.Fatalf("IntegrationSkipReason(%v, %q) = %q, want skip=%v", tt.short, tt.skipEnv, reason, tt.wantSkip)
			}
			if tt.wantReason != "" && !strings.Contains(reason, tt.wantReason) {
				t.Errorf("reason %q should mention %q", reason, tt.wantReason)
			}
		})
	}
}

func TestIntegrationBootstrapDefault(t *testing.T) {
	t.Setenv(EnvBootstrap, "")
	if got := IntegrationBootstrap(); got != "localhost:9092" {
		t.Errorf("IntegrationBootstrap() = %q, want localhost:9092", got)
	}

	t.Setenv(EnvBootstrap, "streamline.internal:19092")
	if got := IntegrationBootstrap(); got != "streamline.internal:19092" {
		t.Errorf("IntegrationBootstrap() = %q, want streamline.internal:19092", got)
	}
}

func TestIntegrationHTTPURLDefault(t *testing.T) {
	t.Setenv(EnvHTTP, "")
	if got := IntegrationHTTPURL(); got != "http://localhost:9094" {
		t.Errorf("IntegrationHTTPURL() = %q, want http://localhost:9094", got)
	}

	t.Setenv(EnvHTTP, "http://streamline.internal:9094")
	if got := IntegrationHTTPURL(); got != "http://streamline.internal:9094" {
		t.Errorf("IntegrationHTTPURL() = %q, want http://streamline.internal:9094", got)
	}
}

func TestRequireIntegrationSkipsWhenDisabled(t *testing.T) {
	t.Setenv(EnvSkipIntegration, "1")

	skipped := true
	t.Run("guarded", func(t *testing.T) {
		RequireIntegration(t)
		skipped = false
	})

	if !skipped {
		t.Error("RequireIntegration should skip when " + EnvSkipIntegration + " is set")
	}
}

func TestIntegrationRequired(t *testing.T) {
	tests := []struct {
		name       string
		requireEnv string
		authEnv    string
		want       bool
		wantErr    bool
	}{
		{name: "optional", want: false},
		{name: "explicitly required", requireEnv: "true", want: true},
		{name: "auth implies required", authEnv: "true", want: true},
		{name: "both false", requireEnv: "false", authEnv: "false", want: false},
		{name: "invalid require", requireEnv: "yes", wantErr: true},
		{name: "invalid auth", authEnv: "enabled", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := integrationRequired(tt.requireEnv, tt.authEnv)
			if (err != nil) != tt.wantErr {
				t.Fatalf("integrationRequired(%q, %q) error = %v, wantErr=%v", tt.requireEnv, tt.authEnv, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("integrationRequired(%q, %q) = %v, want %v", tt.requireEnv, tt.authEnv, got, tt.want)
			}
		})
	}
}

func TestParseAuthSelection(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		selection, err := parseAuthSelection("", "")
		if err != nil {
			t.Fatal(err)
		}
		if selection.enabled {
			t.Fatal("auth selection should be disabled")
		}
	})

	t.Run("explicit modes", func(t *testing.T) {
		selection, err := parseAuthSelection("true", "plain, SCRAM-SHA-256,mtls")
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []string{authModePlain, authModeSCRAMSHA256, authModeMTLS} {
			if !selection.includes(mode) {
				t.Errorf("selection should include %q", mode)
			}
		}
		if selection.includes(authModeTLS) {
			t.Error("selection should not include tls")
		}
	})

	for _, tt := range []struct {
		name    string
		enabled string
		modes   string
	}{
		{name: "invalid boolean", enabled: "yes", modes: "plain"},
		{name: "missing modes", enabled: "true"},
		{name: "unknown mode", enabled: "true", modes: "oauth"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseAuthSelection(tt.enabled, tt.modes); err == nil {
				t.Fatal("parseAuthSelection succeeded, want error")
			}
		})
	}
}
