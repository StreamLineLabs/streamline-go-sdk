// Package embedded provides an in-process Streamline instance using CGO.
//
// This allows Go applications to embed a Streamline server directly,
// eliminating the need for a separate server process.
//
// # Build requirements
//
// The bindings are compiled only when the "embedded" build tag is set and CGO
// is enabled, because they link against the native Streamline library:
//
//	CGO_ENABLED=1 go build -tags embedded ./...
//
// The linker needs libstreamline (see the streamline core repository for build
// instructions) on its search path, for example:
//
//	CGO_ENABLED=1 \
//	CGO_CFLAGS="-I/path/to/streamline/include" \
//	CGO_LDFLAGS="-L/path/to/streamline/lib" \
//	go build -tags embedded ./...
//
// Without the build tag (the default) the package still compiles and exposes
// the same API, but every operation returns ErrNotEnabled. This keeps
// "go build ./..." and "go test ./..." self-contained for users that only need
// the network client.
//
// # Usage
//
//	instance, err := embedded.New(embedded.Config{InMemory: true})
//	if err != nil { log.Fatal(err) }
//	defer instance.Close()
//
//	err = instance.Produce("my-topic", []byte("hello"))
//	msg, err := instance.Consume("my-topic", 5*time.Second)
package embedded

import "errors"

// ErrNotEnabled is returned by every operation when the package was built
// without the "embedded" build tag or with CGO disabled.
var ErrNotEnabled = errors.New("streamline embedded: not available; rebuild with CGO_ENABLED=1 go build -tags embedded and link libstreamline")

// ErrClosed is returned when an operation is attempted on a closed instance.
var ErrClosed = errors.New("streamline embedded: instance is closed")

// ErrQueryUnsupported is returned by Instance.Query because the Streamline C
// ABI (streamline.h) exposes no SQL entry point. Use the HTTP query API
// (streamline.NewQueryClient) against a running server instead.
var ErrQueryUnsupported = errors.New("streamline embedded: SQL queries are not exposed by the Streamline C ABI")

// Config for the embedded Streamline instance.
type Config struct {
	DataDir    string `json:"data_dir,omitempty"`
	InMemory   bool   `json:"in_memory,omitempty"`
	Partitions int    `json:"partitions,omitempty"`
}

// Message received from a topic.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp int64
}
