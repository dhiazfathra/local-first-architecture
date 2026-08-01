// Package docs holds no production code. This file is a guard: it fails the
// build if a documented file goes missing or a relative link inside the
// documentation stops resolving.
package docs_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// required lists every documentation file this repo promises to ship, relative
// to the module root. Each documentation task appends to it.
var required = []string{
	"README.md",
	"docs/adr/0001-per-node-ownership.md",
	"docs/adr/0002-sqlite-local-log.md",
	"docs/adr/0003-hlc-for-ordering-only.md",
	"docs/adr/0004-symmetric-sync-protocol.md",
	"docs/adr/0005-domain-agnostic-engine.md",
	"docs/architecture.md",
	"docs/limitations.md",
	"docs/swapping-the-domain.md",
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

// link matches an inline markdown link: [text](target).
var link = regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)

// TestRelativeLinksResolve walks every markdown file in the module and checks
// that each relative link points at something that exists. External (http) and
// in-page (#anchor) links are out of scope: this test guards our own tree.
func TestRelativeLinksResolve(t *testing.T) {
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(body)
		lines := strings.Split(text, "\n")
		fenced := make([]bool, len(lines))
		inFence := false
		for i, line := range lines {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				inFence = !inFence
			}
			fenced[i] = inFence
		}
		// ponytail: fence-aware — Go generic syntax like Fold[S any](ctx, ...)
		// inside a code block reads identically to a markdown link.
		for _, m := range link.FindAllStringSubmatchIndex(text, -1) {
			if fenced[strings.Count(text[:m[0]], "\n")] {
				continue
			}
			raw := text[m[2]:m[3]]
			target := strings.SplitN(raw, "#", 2)[0]
			if target == "" || strings.Contains(target, "://") {
				continue
			}
			// Sibling reference architectures live outside this module and are
			// not checked out in every environment, so their presence is not
			// this repo's invariant.
			if strings.HasPrefix(target, "../../../") {
				continue
			}
			if _, statErr := os.Stat(filepath.Join(filepath.Dir(path), target)); statErr != nil {
				t.Errorf("%s: broken link %q", path, raw)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}
