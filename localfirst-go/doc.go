// Package localfirst is a readable reference architecture for local-first
// event sourcing.
//
// The engine packages (clock, eventlog, projection, sync) are domain-agnostic:
// they move opaque event bytes and know nothing about inventory. The domain
// package supplies interpretation through two seams, projection.Reducer and
// eventlog.Validator. See docs/architecture.md.
package localfirst

// Version is the reference-architecture revision, reported by both binaries.
const Version = "0.1.0"
