package streamline

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

const attestHeader = "streamline-attest"

// VerificationResult holds the outcome of an attestation verification.
type VerificationResult struct {
	// Verified is true when the Ed25519 signature is valid.
	Verified bool
	// ProducerID is the key_id from the attestation envelope.
	ProducerID string
	// SchemaID is the schema id (zero means absent).
	SchemaID int
	// ContractID is an optional contract identifier.
	ContractID string
	// TimestampMs is the attestation timestamp in epoch milliseconds.
	TimestampMs int64
}

// attestationEnvelope is the JSON structure inside the streamline-attest header.
type attestationEnvelope struct {
	PayloadSHA256 string `json:"payload_sha256"`
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	Offset        int64  `json:"offset"`
	SchemaID      int    `json:"schema_id"`
	TimestampMs   int64  `json:"timestamp_ms"`
	KeyID         string `json:"key_id"`
	Signature     string `json:"signature"`
	ContractID    string `json:"contract_id,omitempty"`
}

// Verifier verifies streamline-attest headers on consumed messages using
// a local Ed25519 public key. No network calls are made.
type Verifier struct {
	publicKey ed25519.PublicKey
}

// NewVerifier creates a Verifier backed by the given Ed25519 public key.
// The key must be exactly 32 bytes.
func NewVerifier(publicKey ed25519.PublicKey) *Verifier {
	return &Verifier{publicKey: publicKey}
}

// Verify checks the attestation header on a ConsumerMessage.
// It extracts the streamline-attest header, parses the base64-encoded JSON,
// reconstructs the canonical bytes, and verifies the Ed25519 signature.
func (v *Verifier) Verify(msg *ConsumerMessage) (VerificationResult, error) {
	if msg == nil {
		return VerificationResult{}, fmt.Errorf("message is nil")
	}

	raw, ok := msg.Headers[attestHeader]
	if !ok || len(raw) == 0 {
		return VerificationResult{Verified: false}, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(string(raw))
	if err != nil {
		return VerificationResult{Verified: false}, nil
	}

	var env attestationEnvelope
	if err := json.Unmarshal(decoded, &env); err != nil {
		return VerificationResult{Verified: false}, nil
	}

	canonical := fmt.Sprintf("%s|%d|%d|%s|%d|%d|%s",
		env.Topic, env.Partition, env.Offset, env.PayloadSHA256,
		env.SchemaID, env.TimestampMs, env.KeyID,
	)

	sig, err := base64.StdEncoding.DecodeString(env.Signature)
	if err != nil {
		return VerificationResult{Verified: false}, nil
	}

	verified := ed25519.Verify(v.publicKey, []byte(canonical), sig)

	return VerificationResult{
		Verified:    verified,
		ProducerID:  env.KeyID,
		SchemaID:    env.SchemaID,
		ContractID:  env.ContractID,
		TimestampMs: env.TimestampMs,
	}, nil
}
