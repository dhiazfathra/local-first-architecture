package demo

import (
	"context"
	"testing"
)

func TestDefaultExec(t *testing.T) {
	ctx := context.Background()
	if err := defaultExec(ctx, "go", "version"); err != nil {
		t.Fatalf("defaultExec(go version) = %v, want nil", err)
	}
	if err := defaultExec(ctx, "definitely-not-a-real-binary-9f3a"); err == nil {
		t.Fatal("defaultExec must report a failure to run")
	}
	// A command that runs and exits non-zero must also be an error, with its
	// output attached — a silent `docker network disconnect` failure would make
	// the demo lie.
	if err := defaultExec(ctx, "go", "definitely-not-a-subcommand"); err == nil {
		t.Fatal("defaultExec must report a non-zero exit")
	}
}
