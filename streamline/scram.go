package streamline

import (
	"crypto/sha256"
	"crypto/sha512"

	"github.com/xdg-go/scram"
)

// SHA256 returns SHA256 hash generator.
var SHA256 scram.HashGeneratorFcn = sha256.New

// SHA512 returns SHA512 hash generator.
var SHA512 scram.HashGeneratorFcn = sha512.New

// XDGSCRAMClient implements sarama.SCRAMClient.
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	HashGeneratorFcn scram.HashGeneratorFcn
}

// Begin starts the SCRAM authentication.
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) error {
	client, err := x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.Client = client
	x.ClientConversation = client.NewConversation()
	return nil
}

// Step performs an authentication step.
func (x *XDGSCRAMClient) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

// Done returns whether the conversation is complete.
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
