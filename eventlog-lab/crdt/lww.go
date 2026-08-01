// Package crdt holds the merge semantics for a StockItem. Every replica --
// including the central store -- runs exactly this code, so there is only one
// definition of "merged".
package crdt

import "github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"

// LWW is a last-writer-wins register. Because clock.HLC has a strict total
// order, two distinct writes can never tie, so every replica independently
// reaches the same verdict and no tiebreak policy is needed.
type LWW[T any] struct {
	Value T         `json:"value"`
	At    clock.HLC `json:"at"`
}

// Set accepts v only if at strictly follows the stored stamp. It reports
// whether the write was accepted. Re-delivering the same write is a no-op,
// which is what makes LWW fields safe under replay.
func (r *LWW[T]) Set(v T, at clock.HLC) bool {
	if !r.At.Before(at) {
		return false
	}
	r.Value, r.At = v, at
	return true
}
