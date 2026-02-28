// Package embedded provides an in-process Streamline instance using CGO.
//
// This allows Go applications to embed a Streamline server directly,
// eliminating the need for a separate server process.
//
// Usage:
//
//	instance, err := embedded.New(embedded.Config{InMemory: true})
//	if err != nil { log.Fatal(err) }
//	defer instance.Close()
//
//	err = instance.Produce("my-topic", []byte("hello"))
//	msg, err := instance.Consume("my-topic", 5*time.Second)
package embedded

/*
#cgo LDFLAGS: -lstreamline
#include "streamline.h"
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"time"
	"unsafe"
)

// Config for the embedded Streamline instance.
type Config struct {
	DataDir    string `json:"data_dir,omitempty"`
	InMemory   bool   `json:"in_memory,omitempty"`
	Partitions int    `json:"partitions,omitempty"`
}

// Instance is an embedded Streamline server.
type Instance struct {
	handle *C.StreamlineHandle
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

// New creates a new embedded Streamline instance.
func New(config Config) (*Instance, error) {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}

	cConfig := C.CString(string(configJSON))
	defer C.free(unsafe.Pointer(cConfig))

	handle := C.streamline_create(cConfig)
	if handle == nil {
		errMsg := C.GoString(C.streamline_last_error())
		return nil, fmt.Errorf("create instance: %s", errMsg)
	}

	return &Instance{handle: handle}, nil
}

// Close destroys the instance and frees resources.
func (i *Instance) Close() {
	if i.handle != nil {
		C.streamline_destroy(i.handle)
		i.handle = nil
	}
}

// Produce sends a message to a topic.
func (i *Instance) Produce(topic string, value []byte) error {
	return i.ProduceWithKey(topic, value, nil)
}

// ProduceWithKey sends a keyed message to a topic.
func (i *Instance) ProduceWithKey(topic string, value, key []byte) error {
	cTopic := C.CString(topic)
	defer C.free(unsafe.Pointer(cTopic))

	var keyPtr *C.uint8_t
	var keyLen C.size_t
	if key != nil {
		keyPtr = (*C.uint8_t)(unsafe.Pointer(&key[0]))
		keyLen = C.size_t(len(key))
	}

	result := C.streamline_produce(
		i.handle,
		cTopic,
		(*C.uint8_t)(unsafe.Pointer(&value[0])),
		C.size_t(len(value)),
		keyPtr,
		keyLen,
	)

	if result != C.STREAMLINE_OK {
		return fmt.Errorf("produce failed (code %d)", result)
	}
	return nil
}

// Consume reads a single message from a topic.
func (i *Instance) Consume(topic string, timeout time.Duration) (*Message, error) {
	cTopic := C.CString(topic)
	defer C.free(unsafe.Pointer(cTopic))

	msg := C.streamline_consume(i.handle, cTopic, C.int64_t(timeout.Milliseconds()))
	if msg == nil {
		return nil, nil // timeout, no message
	}
	defer C.streamline_message_free(msg)

	return &Message{
		Topic:     C.GoString(msg.topic),
		Partition: int32(msg.partition),
		Offset:    int64(msg.offset),
		Key:       C.GoBytes(unsafe.Pointer(msg.key), C.int(msg.key_len)),
		Value:     C.GoBytes(unsafe.Pointer(msg.value), C.int(msg.value_len)),
		Timestamp: int64(msg.timestamp),
	}, nil
}

// CreateTopic creates a new topic.
func (i *Instance) CreateTopic(name string, partitions int) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.streamline_create_topic(i.handle, cName, C.int32_t(partitions))
	if result != C.STREAMLINE_OK {
		return fmt.Errorf("create topic failed (code %d)", result)
	}
	return nil
}

// Query executes a SQL query on stream data.
func (i *Instance) Query(sql string) (string, error) {
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))

	result := C.streamline_query(i.handle, cSQL)
	if result == nil {
		return "", fmt.Errorf("query returned nil")
	}
	defer C.streamline_query_result_free(result)

	if result.error_code != 0 {
		return "", fmt.Errorf("query error: %s", C.GoString(result.error_message))
	}

	return C.GoStringN(result.json_data, C.int(result.json_len)), nil
}

// Version returns the Streamline version.
func Version() string {
	return C.GoString(C.streamline_version())
}
