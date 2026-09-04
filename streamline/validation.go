package streamline

import (
	"fmt"
	"os"
	"regexp"
)

const maxTopicNameLength = 249

var validTopicNamePattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// validateTopicName checks that a topic name conforms to Kafka naming rules:
// non-empty, at most 249 characters, only alphanumeric/'.'/'-'/'_', and not "." or "..".
func validateTopicName(topic string) error {
	if topic == "" {
		return &StreamlineError{
			Code:    ErrConfiguration,
			Message: "topic name cannot be empty",
		}
	}
	if len(topic) > maxTopicNameLength {
		return &StreamlineError{
			Code:    ErrConfiguration,
			Message: fmt.Sprintf("topic name exceeds maximum length of %d characters", maxTopicNameLength),
		}
	}
	if topic == "." || topic == ".." {
		return &StreamlineError{
			Code:    ErrConfiguration,
			Message: fmt.Sprintf("topic name %q is not allowed", topic),
		}
	}
	if !validTopicNamePattern.MatchString(topic) {
		return &StreamlineError{
			Code:    ErrConfiguration,
			Message: fmt.Sprintf("topic name %q contains invalid characters; only alphanumeric, '.', '_', and '-' are allowed", topic),
		}
	}
	return nil
}

// validateTLSConfig checks TLS certificate file paths exist and are readable
// before attempting a connection. This catches misconfigurations early with
// clear error messages rather than cryptic connection failures.
func validateTLSConfig(cfg *TLSConfig) error {
	if cfg == nil {
		return nil
	}
	if !cfg.Enable {
		if cfg.CertFile != "" || cfg.KeyFile != "" || cfg.CAFile != "" || cfg.InsecureSkipVerify {
			return &StreamlineError{
				Code:    ErrConfiguration,
				Message: "TLS settings were provided but TLS is disabled",
				Hint:    "Set TLS.Enable to true, or remove the TLS certificate and verification settings",
			}
		}
		return nil
	}

	// Mutual TLS: cert and key must both be present or both absent
	if (cfg.CertFile != "") != (cfg.KeyFile != "") {
		return &StreamlineError{
			Code:    ErrConfiguration,
			Message: "TLS cert and key must both be provided for mutual TLS, or both omitted",
			Hint:    "Set both CertFile and KeyFile, or leave both empty for server-only TLS",
		}
	}

	if cfg.CertFile != "" {
		if err := validateFileReadable(cfg.CertFile, "TLS client certificate"); err != nil {
			return err
		}
	}
	if cfg.KeyFile != "" {
		if err := validateFileReadable(cfg.KeyFile, "TLS client key"); err != nil {
			return err
		}
	}
	if cfg.CAFile != "" {
		if err := validateFileReadable(cfg.CAFile, "TLS CA certificate"); err != nil {
			return err
		}
	}

	return nil
}

func validateFileReadable(path, label string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return &StreamlineError{
			Code:    ErrConfiguration,
			Message: fmt.Sprintf("%s file not found: %s", label, path),
			Hint:    fmt.Sprintf("Verify the %s file path is correct and the file exists", label),
		}
	}
	if err != nil {
		return &StreamlineError{
			Code:    ErrConfiguration,
			Message: fmt.Sprintf("cannot access %s file %s: %v", label, path, err),
		}
	}
	if info.IsDir() {
		return &StreamlineError{
			Code:    ErrConfiguration,
			Message: fmt.Sprintf("%s path is a directory, expected a file: %s", label, path),
		}
	}
	return nil
}
