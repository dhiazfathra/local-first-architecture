package jepsenlite

import (
	"fmt"
	"math/rand"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// OpKind is one of the four in-process node operations the simulator drives.
type OpKind int

// The four op kinds. These mirror node.Node's op API exactly.
const (
	OpReceive OpKind = iota
	OpPick
	OpSetMeta
	OpDelete
)

// String names the op kind for reports.
func (k OpKind) String() string {
	switch k {
	case OpReceive:
		return "receive"
	case OpPick:
		return "pick"
	case OpSetMeta:
		return "setmeta"
	case OpDelete:
		return "delete"
	default:
		return fmt.Sprintf("OpKind(%d)", int(k))
	}
}

// Op is a single scheduled operation against one node.
type Op struct {
	Node         clock.NodeID
	Kind         OpKind
	SKU          string
	Qty          int64 // OpReceive, OpPick: always > 0
	Name         string
	ReorderPoint int64
	Deleted      bool
}

// FaultEvent schedules a fault to fire immediately before op index At.
// Kind is "partition", "asym", "heal", or "crash"; A and B name the nodes
// involved ("crash" and "heal" use A only, "heal" uses neither).
type FaultEvent struct {
	At   int
	Kind string
	A, B clock.NodeID
}

// Schedule is the complete, replayable description of one simulator run.
// GenSchedule is a pure function of its arguments, so a seed is a full
// reproduction recipe.
type Schedule struct {
	Seed      int64
	Nodes     []clock.NodeID
	SKUs      []string
	Ops       []Op
	Faults    []FaultEvent
	Slow      clock.NodeID           // "" unless Faults.SlowPeer
	Skews     map[clock.NodeID]int64 // empty unless Faults.ClockSkew
	Jumps     map[clock.NodeID]int   // backwards jump period, 0 = never
	SyncEvery int                    // run a full sync round every N ops
}

// GenSchedule builds a deterministic schedule. Same (seed, nodes, ops, faults)
// always yields a byte-identical Schedule.
func GenSchedule(seed int64, nodes, ops int, f Faults) Schedule {
	if nodes < 1 {
		nodes = 1
	}
	if ops < 0 {
		ops = 0
	}
	rng := rand.New(rand.NewSource(seed))

	s := Schedule{
		Seed:      seed,
		Skews:     map[clock.NodeID]int64{},
		Jumps:     map[clock.NodeID]int{},
		SyncEvery: 5,
	}
	for i := 0; i < nodes; i++ {
		s.Nodes = append(s.Nodes, clock.NodeID(fmt.Sprintf("N%d", i)))
	}
	// Few SKUs on purpose: contention finds bugs, breadth does not.
	for i := 0; i <= max(1, nodes); i++ {
		s.SKUs = append(s.SKUs, fmt.Sprintf("SKU-%d", i))
	}

	names := []string{"widget", "gasket", "flange", "bolt"}
	for i := 0; i < ops; i++ {
		op := Op{
			Node: s.Nodes[rng.Intn(len(s.Nodes))],
			SKU:  s.SKUs[rng.Intn(len(s.SKUs))],
		}
		switch rng.Intn(10) {
		case 0, 1, 2, 3:
			op.Kind, op.Qty = OpReceive, int64(1+rng.Intn(20))
		case 4, 5, 6, 7:
			op.Kind, op.Qty = OpPick, int64(1+rng.Intn(20))
		case 8:
			op.Kind = OpSetMeta
			op.Name = names[rng.Intn(len(names))]
			op.ReorderPoint = int64(rng.Intn(50))
		default:
			op.Kind = OpDelete
			op.Deleted = rng.Intn(2) == 0
		}
		s.Ops = append(s.Ops, op)
	}

	if f.ClockSkew {
		for i, id := range s.Nodes {
			// Alternate ahead and behind: a clock running behind is the one
			// that loses every LWW conflict unless Observe is wired right.
			mag := int64(1+rng.Intn(60)) * 1000
			if i%2 == 1 {
				mag = -mag
			}
			s.Skews[id] = mag
			if rng.Intn(2) == 0 {
				s.Jumps[id] = 3 + rng.Intn(5)
			}
		}
		// Guarantee both interesting cases exist regardless of the draw.
		s.Skews[s.Nodes[0]] = -30000
		s.Jumps[s.Nodes[0]] = 4
	}
	if f.SlowPeer {
		s.Slow = s.Nodes[rng.Intn(len(s.Nodes))]
	}
	s.Faults = genFaultEvents(rng, s.Nodes, ops, f)
	return s
}

// genFaultEvents emits fault events in nondecreasing At order. Every partition
// gets a matching heal so recovery is exercised.
func genFaultEvents(rng *rand.Rand, ids []clock.NodeID, ops int, f Faults) []FaultEvent {
	if ops == 0 {
		return nil
	}
	var out []FaultEvent
	pairFaults := (f.Partition || f.AsymPartition) && len(ids) > 1

	// Windows of length ops/8, at most four of them.
	window := max(1, ops/8)
	for start := window; start+window < ops && len(out) < 12; start += 3 * window {
		if pairFaults {
			a, b := pickPair(rng, ids)
			kind := "partition"
			if f.AsymPartition && (!f.Partition || rng.Intn(2) == 0) {
				kind = "asym"
			}
			out = append(out,
				FaultEvent{At: start, Kind: kind, A: a, B: b},
				FaultEvent{At: start + window, Kind: "heal"},
			)
		}
		if f.CrashMidAppend {
			out = append(out, FaultEvent{
				At:   start + window,
				Kind: "crash",
				A:    ids[rng.Intn(len(ids))],
			})
		}
	}
	sortFaults(out)
	return out
}

// pickPair returns two distinct node IDs.
func pickPair(rng *rand.Rand, ids []clock.NodeID) (clock.NodeID, clock.NodeID) {
	i := rng.Intn(len(ids))
	j := rng.Intn(len(ids) - 1)
	if j >= i {
		j++
	}
	return ids[i], ids[j]
}

// sortFaults stable-sorts by At using insertion sort -- the slice is tiny and
// this keeps the generator free of any nondeterministic comparator.
func sortFaults(fs []FaultEvent) {
	for i := 1; i < len(fs); i++ {
		for j := i; j > 0 && fs[j].At < fs[j-1].At; j-- {
			fs[j], fs[j-1] = fs[j-1], fs[j]
		}
	}
}
