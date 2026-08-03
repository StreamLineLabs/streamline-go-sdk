package streamline

import (
	"errors"
	"fmt"
	"testing"
)

type stubCloser struct {
	err    error
	closed int
}

func (s *stubCloser) Close() error {
	s.closed++
	return s.err
}

func TestJoinCloseSurfacesCleanupFailure(t *testing.T) {
	closeErr := errors.New("boom")
	closer := &stubCloser{err: closeErr}

	err := joinClose(nil, closer, "close thing")
	if err == nil {
		t.Fatal("expected the close failure to be returned when the operation succeeded")
	}
	if !errors.Is(err, closeErr) {
		t.Errorf("error = %v, want it to wrap %v", err, closeErr)
	}
	if closer.closed != 1 {
		t.Errorf("Close called %d times, want 1", closer.closed)
	}
}

func TestJoinCloseKeepsOperationError(t *testing.T) {
	opErr := errors.New("operation failed")
	closer := &stubCloser{err: errors.New("close failed")}

	err := joinClose(opErr, closer, "close thing")
	if !errors.Is(err, opErr) {
		t.Fatalf("error = %v, want the operation error %v to win", err, opErr)
	}
	if closer.closed != 1 {
		t.Errorf("Close called %d times, want 1 (cleanup must still run)", closer.closed)
	}
}

func TestJoinCloseNoErrors(t *testing.T) {
	closer := &stubCloser{}

	if err := joinClose(nil, closer, "close thing"); err != nil {
		t.Fatalf("joinClose = %v, want nil", err)
	}
	if closer.closed != 1 {
		t.Errorf("Close called %d times, want 1", closer.closed)
	}
}

// TestJoinCloseInDeferredResult mirrors the call-site pattern used across the
// package: a named error result folded with the close failure.
func TestJoinCloseInDeferredResult(t *testing.T) {
	closeErr := errors.New("close failed")

	run := func(closer *stubCloser, opErr error) (err error) {
		defer func() { err = joinClose(err, closer, "close thing") }()
		return opErr
	}

	if err := run(&stubCloser{err: closeErr}, nil); !errors.Is(err, closeErr) {
		t.Errorf("error = %v, want it to wrap the close failure", err)
	}

	opErr := fmt.Errorf("operation failed")
	if err := run(&stubCloser{err: closeErr}, opErr); !errors.Is(err, opErr) {
		t.Errorf("error = %v, want the operation error to win", err)
	}
}
