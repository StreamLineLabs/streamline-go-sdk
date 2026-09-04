//go:build !embedded || !cgo

package embedded

import "time"

// Instance is a placeholder for an embedded Streamline server. The package was
// built without the "embedded" build tag or with CGO disabled, so every method
// returns ErrNotEnabled. See the package documentation for build instructions.
type Instance struct{}

// New always returns ErrNotEnabled in builds without the "embedded" tag.
func New(config Config) (*Instance, error) {
	return nil, ErrNotEnabled
}

// Close is a no-op in builds without the "embedded" tag.
func (i *Instance) Close() {}

// Produce always returns ErrNotEnabled in builds without the "embedded" tag.
func (i *Instance) Produce(topic string, value []byte) error {
	return ErrNotEnabled
}

// ProduceWithKey always returns ErrNotEnabled in builds without the "embedded" tag.
func (i *Instance) ProduceWithKey(topic string, value, key []byte) error {
	return ErrNotEnabled
}

// Consume always returns ErrNotEnabled in builds without the "embedded" tag.
func (i *Instance) Consume(topic string, timeout time.Duration) (*Message, error) {
	return nil, ErrNotEnabled
}

// CreateTopic always returns ErrNotEnabled in builds without the "embedded" tag.
func (i *Instance) CreateTopic(name string, partitions int) error {
	return ErrNotEnabled
}

// Query always returns ErrNotEnabled in builds without the "embedded" tag.
func (i *Instance) Query(sql string) (string, error) {
	return "", ErrNotEnabled
}

// Version returns an empty string in builds without the "embedded" tag.
func Version() string {
	return ""
}
