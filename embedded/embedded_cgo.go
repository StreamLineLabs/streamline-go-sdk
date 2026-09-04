//go:build embedded && cgo

package embedded

/*
#cgo LDFLAGS: -lstreamline
#include "streamline.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

const (
	// defaultPartition is the partition used by the single-partition helpers.
	defaultPartition = 0

	// defaultPartitions is used when Config.Partitions is unset.
	defaultPartitions = 1

	// consumePollInterval is how often Consume re-checks a topic while waiting.
	consumePollInterval = 10 * time.Millisecond
)

// Instance is an embedded Streamline server.
type Instance struct {
	mu     sync.Mutex
	handle *C.StreamlineHandle

	// offsets tracks the next offset to read per topic for Consume.
	offsets map[string]int64
}

// New creates a new embedded Streamline instance.
func New(config Config) (*Instance, error) {
	partitions := config.Partitions
	if partitions <= 0 {
		partitions = defaultPartitions
	}

	cConfig := C.StreamlineConfig{
		default_partitions: C.int32_t(partitions),
	}

	if config.InMemory || config.DataDir == "" {
		cConfig.in_memory = 1
	} else {
		cDataDir := C.CString(config.DataDir)
		defer C.free(unsafe.Pointer(cDataDir))
		cConfig.data_dir = cDataDir
	}

	runtime.LockOSThread()
	handle := C.streamline_open(&cConfig)
	if handle == nil {
		err := cError("open instance", 0)
		runtime.UnlockOSThread()
		return nil, err
	}
	runtime.UnlockOSThread()

	return &Instance{handle: handle, offsets: make(map[string]int64)}, nil
}

// Close destroys the instance and frees resources.
func (i *Instance) Close() {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.handle != nil {
		C.streamline_close(i.handle)
		i.handle = nil
	}
}

// Produce sends a message to a topic.
func (i *Instance) Produce(topic string, value []byte) error {
	return i.ProduceWithKey(topic, value, nil)
}

// ProduceWithKey sends a keyed message to a topic.
func (i *Instance) ProduceWithKey(topic string, value, key []byte) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.handle == nil {
		return ErrClosed
	}

	cTopic := C.CString(topic)
	defer C.free(unsafe.Pointer(cTopic))

	keyPtr, keyLen := byteSlice(key)
	valuePtr, valueLen := byteSlice(value)

	runtime.LockOSThread()
	offset := C.streamline_produce(
		i.handle,
		cTopic,
		C.int32_t(defaultPartition),
		keyPtr, keyLen,
		valuePtr, valueLen,
	)
	if offset < 0 {
		err := cError("produce", int(offset))
		runtime.UnlockOSThread()
		return err
	}
	runtime.UnlockOSThread()
	return nil
}

// Consume reads a single message from a topic, waiting up to timeout for one to
// arrive. It returns (nil, nil) when the timeout expires without a message.
//
// Reads start at the beginning of the topic and advance per instance, so
// repeated calls walk the log in order.
func (i *Instance) Consume(topic string, timeout time.Duration) (*Message, error) {
	deadline := time.Now().Add(timeout)

	for {
		msg, err := i.consumeOnce(topic)
		if err != nil || msg != nil {
			return msg, err
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil // timeout, no message
		}
		if remaining > consumePollInterval {
			remaining = consumePollInterval
		}
		time.Sleep(remaining)
	}
}

func (i *Instance) consumeOnce(topic string) (*Message, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.handle == nil {
		return nil, ErrClosed
	}

	cTopic := C.CString(topic)
	defer C.free(unsafe.Pointer(cTopic))

	var batch C.StreamlineRecordBatch
	runtime.LockOSThread()
	result := C.streamline_consume(
		i.handle,
		cTopic,
		C.int32_t(defaultPartition),
		C.int64_t(i.offsets[topic]),
		1,
		&batch,
	)
	if result < 0 {
		err := cError("consume", int(result))
		runtime.UnlockOSThread()
		return nil, err
	}
	runtime.UnlockOSThread()
	defer C.streamline_free_record_batch(&batch)

	if batch.count == 0 || batch.records == nil {
		return nil, nil
	}

	record := *batch.records
	msg := &Message{
		Topic:     topic,
		Partition: defaultPartition,
		Offset:    int64(record.offset),
		Key:       cBytes(record.key_ptr, record.key_len),
		Value:     cBytes(record.value_ptr, record.value_len),
		Timestamp: int64(record.timestamp),
	}
	i.offsets[topic] = msg.Offset + 1

	return msg, nil
}

// CreateTopic creates a new topic.
func (i *Instance) CreateTopic(name string, partitions int) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.handle == nil {
		return ErrClosed
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	runtime.LockOSThread()
	result := C.streamline_create_topic(i.handle, cName, C.int32_t(partitions))
	if result != C.STREAMLINE_OK {
		err := cError("create topic", int(result))
		runtime.UnlockOSThread()
		return err
	}
	runtime.UnlockOSThread()
	return nil
}

// Query always returns ErrQueryUnsupported: the Streamline C ABI does not
// expose a SQL entry point. Use streamline.NewQueryClient against the HTTP API
// of a running server instead.
func (i *Instance) Query(sql string) (string, error) {
	return "", ErrQueryUnsupported
}

// Version returns the Streamline version.
func Version() string {
	return C.GoString(C.streamline_version())
}

// byteSlice returns a C pointer/length pair for a Go byte slice.
func byteSlice(b []byte) (*C.uint8_t, C.uint32_t) {
	if len(b) == 0 {
		return nil, 0
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0])), C.uint32_t(len(b))
}

// cBytes copies a C buffer into a Go byte slice.
func cBytes(ptr *C.uint8_t, length C.uint32_t) []byte {
	if ptr == nil || length == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(ptr), C.int(length))
}

// cError builds an error from the thread-local last error message.
func cError(op string, code int) error {
	if msg := C.streamline_last_error(); msg != nil {
		return fmt.Errorf("streamline embedded: %s failed (code %d): %s", op, code, C.GoString(msg))
	}
	return fmt.Errorf("streamline embedded: %s failed (code %d)", op, code)
}
