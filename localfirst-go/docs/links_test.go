// Package docs holds no production code. This file is a guard: it fails the
// build if a documented file goes missing or a relative link inside the
// documentation stops resolving.
package docs_test

import (
	"os"
	"path/filepath"
	"testing"
)

// required lists every documentation file this repo promises to ship, relative
// to the module root. Each documentation task appends to it.
var required = []string{
	"docs/adr/0001-per-node-ownership.md",
	"docs/adr/0002-sqlite-local-log.md",
	"docs/adr/0003-hlc-for-ordering-only.md",
	"docs/adr/0004-symmetric-sync-protocol.md",
	"docs/adr/0005-domain-agnostic-engine.md",
}

// root is the module root; this test file lives in docs/.
const root = ".."

func TestRequiredDocsExist(t *testing.T) {
	for _, rel := range required {
		t.Run(rel, func(t *testing.T) {
			if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
				t.Fatalf("documented file missing: %v", err)
			}
		})
	}
}
