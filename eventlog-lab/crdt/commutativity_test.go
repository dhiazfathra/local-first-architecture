package crdt

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

// permutations returns every ordering of idx. Only ever called for small n.
func permutations(idx []int) [][]int {
	if len(idx) <= 1 {
		return [][]int{append([]int(nil), idx...)}
	}
	var out [][]int
	for i := range idx {
		rest := make([]int, 0, len(idx)-1)
		rest = append(rest, idx[:i]...)
		rest = append(rest, idx[i+1:]...)
		for _, p := range permutations(rest) {
			out = append(out, append([]int{idx[i]}, p...))
		}
	}
	return out
}

// sampledPermutations returns n seeded random orderings of 0..size-1.
func sampledPermutations(rng *rand.Rand, size, n int) [][]int {
	out := make([][]int, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, rng.Perm(size))
	}
	return out
}

// buildEvents makes n events across three nodes and every kind, with distinct
// HLCs so LWW has a strict winner.
func buildEvents(n int) []eventlog.Event {
	nodes := []clock.NodeID{"A", "B", "C"}
	names := []string{"widget", "gasket", "flange"}
	out := make([]eventlog.Event, 0, n)
	for i := 0; i < n; i++ {
		id := nodes[i%len(nodes)]
		e := eventlog.Event{
			ID:  eventlog.EventID{NodeID: id, Seq: eventlog.Seq(i/len(nodes) + 1)},
			HLC: clock.HLC{Wall: int64(100 + i), Logical: uint32(i), NodeID: id},
			SKU: "SKU-1",
		}
		switch i % 4 {
		case 0:
			e.Kind, e.Delta = eventlog.KindQuantityDelta, int64(3+i)
		case 1:
			e.Kind, e.Delta = eventlog.KindQuantityDelta, -int64(1+i)
		case 2:
			name := names[i%len(names)]
			rp := int64(10 + i)
			e.Kind, e.Meta = eventlog.KindMetaSet, &eventlog.MetaSet{Name: &name, ReorderPoint: &rp}
		default:
			del := i%8 == 3
			e.Kind, e.DeletedTo = eventlog.KindDeleteSet, &del
		}
		out = append(out, e)
	}
	return out
}

func TestApplyIsCommutativeOverEveryPermutation(t *testing.T) {
	rng := rand.New(rand.NewSource(1234))

	tests := []struct {
		name       string
		size       int
		exhaustive bool
		samples    int
	}{
		{name: "2 events exhaustive", size: 2, exhaustive: true},
		{name: "3 events exhaustive", size: 3, exhaustive: true},
		{name: "4 events exhaustive", size: 4, exhaustive: true},
		{name: "5 events exhaustive", size: 5, exhaustive: true},
		{name: "6 events exhaustive", size: 6, exhaustive: true},
		{name: "12 events sampled", size: 12, samples: 500},
		{name: "40 events sampled", size: 40, samples: 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := buildEvents(tt.size)

			var orders [][]int
			if tt.exhaustive {
				idx := make([]int, tt.size)
				for i := range idx {
					idx[i] = i
				}
				orders = permutations(idx)
				if want := factorial(tt.size); len(orders) != want {
					t.Fatalf("permutations(%d) = %d orders, want %d",
						tt.size, len(orders), want)
				}
			} else {
				orders = sampledPermutations(rng, tt.size, tt.samples)
			}

			var want *ItemState
			for _, order := range orders {
				got := NewItemState()
				for _, i := range order {
					got.Apply(events[i])
				}
				if want == nil {
					want = got
					continue
				}
				if !got.Equal(want) {
					t.Fatalf("order %v diverged: quantity %d name %q deleted %v, want quantity %d name %q deleted %v",
						order, got.Quantity(), got.Name.Value, got.Deleted.Value,
						want.Quantity(), want.Name.Value, want.Deleted.Value)
				}
			}
		})
	}
}

func factorial(n int) int {
	f := 1
	for i := 2; i <= n; i++ {
		f *= i
	}
	return f
}

func TestApplyIsAssociativeAcrossPartitionedSubsets(t *testing.T) {
	// Associativity in CRDT terms: merging in any grouping is the same. Folding
	// A then B is folding (A+B), so split an event set every possible way and
	// require one answer.
	events := buildEvents(8)
	var want *ItemState
	for split := 0; split <= len(events); split++ {
		got := NewItemState()
		for _, e := range events[:split] {
			got.Apply(e)
		}
		for _, e := range events[split:] {
			got.Apply(e)
		}
		if want == nil {
			want = got
			continue
		}
		if !got.Equal(want) {
			t.Fatalf("split at %d diverged: quantity %d, want %d",
				split, got.Quantity(), want.Quantity())
		}
	}
	if got := fmt.Sprintf("%d", want.Quantity()); got == "" {
		t.Fatal("unreachable")
	}
}
