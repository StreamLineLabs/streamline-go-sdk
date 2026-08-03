package streamline

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeTestKeyPair generates a short-lived self-signed certificate and writes
// the PEM-encoded certificate and private key into dir. The certificate is a CA
// so the same material can stand in for both a trust anchor and a client
// certificate.
func writeTestKeyPair(t *testing.T, dir, name string) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	certPath = filepath.Join(dir, name+".pem")
	keyPath = filepath.Join(dir, name+"-key.pem")

	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write certificate: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	return certPath, keyPath
}

func TestBuildTLSConfigLoadsCAFile(t *testing.T) {
	dir := t.TempDir()
	caPath, _ := writeTestKeyPair(t, dir, "ca")

	tlsConfig, err := buildTLSConfig(&TLSConfig{Enable: true, CAFile: caPath})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsConfig.RootCAs == nil {
		t.Error("RootCAs should be populated when a CA file is configured")
	}
	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %d, want %d (TLS 1.2)", tlsConfig.MinVersion, tls.VersionTLS12)
	}
	if tlsConfig.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should stay false unless explicitly requested")
	}
	if len(tlsConfig.Certificates) != 0 {
		t.Errorf("Certificates = %d, want 0 without a client certificate", len(tlsConfig.Certificates))
	}
}

func TestBuildTLSConfigRejectsInvalidCAPEM(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "not-a-cert.pem")
	if err := os.WriteFile(caPath, []byte("this is not PEM"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	_, err := buildTLSConfig(&TLSConfig{Enable: true, CAFile: caPath})
	if err == nil {
		t.Fatal("expected an error for a CA file that is not valid PEM")
	}
	var sErr *StreamlineError
	if !errors.As(err, &sErr) {
		t.Fatalf("expected a *StreamlineError, got %T", err)
	}
	if sErr.Code != ErrConfiguration {
		t.Errorf("error code = %v, want %v", sErr.Code, ErrConfiguration)
	}
}

func TestBuildTLSConfigLoadsClientCertificate(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeTestKeyPair(t, dir, "client")

	tlsConfig, err := buildTLSConfig(&TLSConfig{Enable: true, CertFile: certPath, KeyFile: keyPath})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if len(tlsConfig.Certificates) != 1 {
		t.Fatalf("Certificates = %d, want 1", len(tlsConfig.Certificates))
	}
}

func TestBuildTLSConfigRejectsMissingCAFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.pem")

	if _, err := buildTLSConfig(&TLSConfig{Enable: true, CAFile: missing}); err == nil {
		t.Fatal("expected an error for a CA file that does not exist")
	}
}

// TestBuildSaramaConfigWiresTLSMaterial covers the full path from Config to the
// Sarama network settings, which is what actually reaches the broker.
func TestBuildSaramaConfigWiresTLSMaterial(t *testing.T) {
	dir := t.TempDir()
	caPath, _ := writeTestKeyPair(t, dir, "ca")
	certPath, keyPath := writeTestKeyPair(t, dir, "client")

	cfg := Config{
		Brokers: []string{"localhost:9093"},
		TLS: &TLSConfig{
			Enable:   true,
			CAFile:   caPath,
			CertFile: certPath,
			KeyFile:  keyPath,
		},
	}.withDefaults()

	saramaConfig, err := buildSaramaConfig(cfg)
	if err != nil {
		t.Fatalf("buildSaramaConfig: %v", err)
	}
	if err := saramaConfig.Validate(); err != nil {
		t.Fatalf("sarama config invalid: %v", err)
	}
	if !saramaConfig.Net.TLS.Enable {
		t.Fatal("Net.TLS.Enable = false, want true")
	}
	if saramaConfig.Net.TLS.Config == nil {
		t.Fatal("Net.TLS.Config is nil")
	}
	if saramaConfig.Net.TLS.Config.RootCAs == nil {
		t.Error("RootCAs should be populated from CAFile")
	}
	if len(saramaConfig.Net.TLS.Config.Certificates) != 1 {
		t.Errorf("Certificates = %d, want 1 from CertFile/KeyFile", len(saramaConfig.Net.TLS.Config.Certificates))
	}
}

func TestBuildSaramaConfigRejectsPopulatedDisabledTLS(t *testing.T) {
	cfg := Config{
		Brokers: []string{"localhost:9092"},
		TLS: &TLSConfig{
			Enable: false,
			CAFile: filepath.Join(t.TempDir(), "does-not-exist.pem"),
		},
	}.withDefaults()

	_, err := buildSaramaConfig(cfg)
	if err == nil {
		t.Fatal("expected populated disabled TLS configuration to fail")
	}
}

// TestBuildSaramaConfigRejectsHalfConfiguredMutualTLS keeps the mutual-TLS
// validation wired into client construction rather than only into validation.go.
func TestBuildSaramaConfigRejectsHalfConfiguredMutualTLS(t *testing.T) {
	dir := t.TempDir()
	certPath, _ := writeTestKeyPair(t, dir, "client")

	cfg := Config{
		Brokers: []string{"localhost:9093"},
		TLS:     &TLSConfig{Enable: true, CertFile: certPath},
	}.withDefaults()

	_, err := buildSaramaConfig(cfg)
	if err == nil {
		t.Fatal("expected an error when CertFile is set without KeyFile")
	}
	var sErr *StreamlineError
	if !errors.As(err, &sErr) {
		t.Fatalf("expected a *StreamlineError, got %T", err)
	}
	if sErr.Code != ErrConfiguration {
		t.Errorf("error code = %v, want %v", sErr.Code, ErrConfiguration)
	}
}
