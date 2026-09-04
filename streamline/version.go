package streamline

import "fmt"

// Version is the semantic version of this SDK.
const Version = "0.4.0"

func validateReleaseTag(tag string) error {
	want := "v" + Version
	if tag != want {
		return fmt.Errorf("release tag %q does not match SDK version %q", tag, want)
	}
	return nil
}
