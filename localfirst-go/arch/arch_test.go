// Package arch holds the architecture guard. It has no production code: its only
// job is to fail the build if the domain leaks into the engine.
package arch_test

import (
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const (
	module = "github.com/dhiazfathra/local-first-architecture/localfirst-go"
	domain = module + "/domain"
)

// engine lists the packages that must remain domain-agnostic. If this list ever
// shrinks, the repo's central claim has been weakened — argue it in an ADR
// first.
var engine = []string{
	module + "/eventlog",
	module + "/clock",
	module + "/sync",
	module + "/projection",
}

// TestEngineDoesNotDependOnTheDomain parses the real import graph — direct and
// transitive — and fails naming the exact path if any engine package can reach
// the domain.
func TestEngineDoesNotDependOnTheDomain(t *testing.T) {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps}
	pkgs, err := packages.Load(cfg, engine...)
	if err != nil {
		t.Fatalf("load packages: %v", err)
	}
	if n := packages.PrintErrors(pkgs); n > 0 {
		t.Fatalf("%d packages failed to load; fix the build first", n)
	}
	if len(pkgs) != len(engine) {
		t.Fatalf("loaded %d packages, want %d — did a package get renamed?", len(pkgs), len(engine))
	}

	for _, p := range pkgs {
		if path := reaches(p, domain, map[string]bool{}); path != nil {
			t.Errorf("%s must not depend on the domain:\n  %s", p.PkgPath, strings.Join(path, "\n    -> "))
		}
	}
}

// reaches returns the import chain from p to target, or nil if none exists.
func reaches(p *packages.Package, target string, seen map[string]bool) []string {
	if seen[p.PkgPath] {
		return nil
	}
	seen[p.PkgPath] = true

	// Deterministic order, so a failure message is stable across runs.
	names := make([]string, 0, len(p.Imports))
	for path := range p.Imports {
		names = append(names, path)
	}
	sort.Strings(names)

	for _, path := range names {
		if path == target {
			return []string{p.PkgPath, target}
		}
		if !strings.HasPrefix(path, module) {
			continue // third-party and stdlib cannot reach our domain
		}
		if rest := reaches(p.Imports[path], target, seen); rest != nil {
			return append([]string{p.PkgPath}, rest...)
		}
	}
	return nil
}

// TestGuardActuallyDetectsALeak proves the guard is not vacuously green: run it
// against a package that DOES legitimately import the domain and expect a hit.
func TestGuardActuallyDetectsALeak(t *testing.T) {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps}
	pkgs, err := packages.Load(cfg, module+"/transport/grpc")
	if err != nil {
		t.Fatalf("load packages: %v", err)
	}
	if len(pkgs) != 1 {
		t.Fatalf("loaded %d packages, want 1", len(pkgs))
	}
	path := reaches(pkgs[0], domain, map[string]bool{})
	if path == nil {
		t.Fatal("transport/grpc imports domain, so the guard must find a path; it found none — the guard is broken")
	}
	if path[len(path)-1] != domain {
		t.Fatalf("path %v must end at the domain", path)
	}
}
