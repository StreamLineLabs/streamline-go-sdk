package streamline

import (
	"strings"
	"testing"
)

func TestValidateTopicName(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid simple name",
			topic:   "my-topic",
			wantErr: false,
		},
		{
			name:    "valid with dots",
			topic:   "my.topic.name",
			wantErr: false,
		},
		{
			name:    "valid with underscores",
			topic:   "my_topic_name",
			wantErr: false,
		},
		{
			name:    "valid alphanumeric",
			topic:   "Topic123",
			wantErr: false,
		},
		{
			name:    "valid mixed separators",
			topic:   "my-topic_v2.events",
			wantErr: false,
		},
		{
			name:    "valid single character",
			topic:   "a",
			wantErr: false,
		},
		{
			name:    "valid max length",
			topic:   strings.Repeat("a", 249),
			wantErr: false,
		},
		{
			name:    "empty name",
			topic:   "",
			wantErr: true,
			errMsg:  "cannot be empty",
		},
		{
			name:    "exceeds max length",
			topic:   strings.Repeat("a", 250),
			wantErr: true,
			errMsg:  "exceeds maximum length",
		},
		{
			name:    "dot only",
			topic:   ".",
			wantErr: true,
			errMsg:  "is not allowed",
		},
		{
			name:    "double dot",
			topic:   "..",
			wantErr: true,
			errMsg:  "is not allowed",
		},
		{
			name:    "contains space",
			topic:   "my topic",
			wantErr: true,
			errMsg:  "invalid characters",
		},
		{
			name:    "contains slash",
			topic:   "my/topic",
			wantErr: true,
			errMsg:  "invalid characters",
		},
		{
			name:    "contains colon",
			topic:   "my:topic",
			wantErr: true,
			errMsg:  "invalid characters",
		},
		{
			name:    "contains at sign",
			topic:   "my@topic",
			wantErr: true,
			errMsg:  "invalid characters",
		},
		{
			name:    "contains hash",
			topic:   "my#topic",
			wantErr: true,
			errMsg:  "invalid characters",
		},
		{
			name:    "contains asterisk",
			topic:   "my*topic",
			wantErr: true,
			errMsg:  "invalid characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTopicName(tt.topic)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTopicName(%q) error = %v, wantErr %v", tt.topic, err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("validateTopicName(%q) error = %q, want containing %q", tt.topic, err.Error(), tt.errMsg)
				}
			}
			if tt.wantErr {
				var sErr *StreamlineError
				if !isStreamlineError(err, &sErr) {
					t.Errorf("validateTopicName(%q) expected StreamlineError, got %T", tt.topic, err)
				} else if sErr.Code != ErrConfiguration {
					t.Errorf("validateTopicName(%q) error code = %s, want CONFIGURATION_ERROR", tt.topic, sErr.Code)
				}
			}
		})
	}
}

func isStreamlineError(err error, target **StreamlineError) bool {
	if se, ok := err.(*StreamlineError); ok {
		*target = se
		return true
	}
	return false
}

func TestValidateTLSConfig(t *testing.T) {
	t.Run("nil config is valid", func(t *testing.T) {
		if err := validateTLSConfig(nil); err != nil {
			t.Errorf("expected nil error for nil config, got %v", err)
		}
	})

	t.Run("disabled TLS is valid", func(t *testing.T) {
		cfg := &TLSConfig{Enable: false}
		if err := validateTLSConfig(cfg); err != nil {
			t.Errorf("expected nil error for disabled TLS, got %v", err)
		}
	})

	t.Run("cert without key is rejected", func(t *testing.T) {
		cfg := &TLSConfig{Enable: true, CertFile: "/tmp/cert.pem"}
		err := validateTLSConfig(cfg)
		if err == nil {
			t.Error("expected error when cert provided without key")
		}
		if !strings.Contains(err.Error(), "both be provided") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("key without cert is rejected", func(t *testing.T) {
		cfg := &TLSConfig{Enable: true, KeyFile: "/tmp/key.pem"}
		err := validateTLSConfig(cfg)
		if err == nil {
			t.Error("expected error when key provided without cert")
		}
	})

	t.Run("nonexistent cert file is rejected", func(t *testing.T) {
		cfg := &TLSConfig{
			Enable:   true,
			CertFile: "/nonexistent/path/cert.pem",
			KeyFile:  "/nonexistent/path/key.pem",
		}
		err := validateTLSConfig(cfg)
		if err == nil {
			t.Error("expected error for nonexistent cert file")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("expected 'not found' in error, got: %v", err)
		}
	})

	t.Run("directory path is rejected", func(t *testing.T) {
		cfg := &TLSConfig{
			Enable:   true,
			CertFile: t.TempDir(),
			KeyFile:  t.TempDir(),
		}
		err := validateTLSConfig(cfg)
		if err == nil {
			t.Error("expected error when cert path is a directory")
		}
		if !strings.Contains(err.Error(), "directory") {
			t.Errorf("expected 'directory' in error, got: %v", err)
		}
	})
}
