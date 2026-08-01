package localfirst_test

import (
	"testing"

	localfirst "github.com/dhiazfathra/local-first-architecture/localfirst-go"
)

func TestVersion(t *testing.T) {
	if localfirst.Version == "" {
		t.Fatal("Version must not be empty")
	}
}
