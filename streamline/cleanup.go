package streamline

import (
	"fmt"
	"io"
)

// joinClose closes c and returns the error the caller should propagate.
//
// Operation errors always win over cleanup errors: err is returned untouched
// when it is non-nil, so a close failure can never mask the real problem, yet
// it is still surfaced when the operation itself succeeded.
//
// It is meant to be deferred from a function with a named error result:
//
//	func f() (err error) {
//	    ...
//	    defer func() { err = joinClose(err, resp.Body, "close response body") }()
//	}
func joinClose(err error, c io.Closer, what string) error {
	if cerr := c.Close(); cerr != nil && err == nil {
		return fmt.Errorf("streamline: %s: %w", what, cerr)
	}
	return err
}
