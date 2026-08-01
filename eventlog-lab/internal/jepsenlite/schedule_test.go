package jepsenlite

import (
	"reflect"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

func TestGenScheduleIsDeterministic(t *testing.T) {
	f := Faults{Partition: true, ClockSkew: true, Duplicate: true, SlowPeer: true}
	a := GenSchedule(42, 3, 60, f)
	b := GenSchedule(42, 3, 60, f)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("GenSchedule is not deterministic for the same seed")
	}
	c := GenSchedule(43, 3, 60, f)
	if reflect.DeepEqual(a, c) {
		t.Fatalf("GenSchedule(42) == GenSchedule(43); the seed is not being used")
	}
}

func TestGenScheduleShape(t *testing.T) {
	tests := []struct {
		name      string
		nodes     int
		ops       int
		faults    Faults
		wantNodes int
		wantOps   int
		wantSkew  bool
		wantSlow  bool
		wantFault bool
	}{
		{
			name:  "no faults yields ops only",
			nodes: 3, ops: 30, faults: Faults{},
			wantNodes: 3, wantOps: 30,
		},
		{
			name:  "skew populates offsets per node",
			nodes: 3, ops: 30, faults: Faults{ClockSkew: true},
			wantNodes: 3, wantOps: 30, wantSkew: true,
		},
		{
			name:  "slow peer names one node",
			nodes: 3, ops: 30, faults: Faults{SlowPeer: true},
			wantNodes: 3, wantOps: 30, wantSlow: true,
		},
		{
			name:  "partition emits fault events",
			nodes: 3, ops: 40, faults: Faults{Partition: true},
			wantNodes: 3, wantOps: 40, wantFault: true,
		},
		{
			name:  "asymmetric partition emits fault events",
			nodes: 3, ops: 40, faults: Faults{AsymPartition: true},
			wantNodes: 3, wantOps: 40, wantFault: true,
		},
		{
			name:  "crash emits fault events",
			nodes: 2, ops: 40, faults: Faults{CrashMidAppend: true},
			wantNodes: 2, wantOps: 40, wantFault: true,
		},
		{
			name:  "single node clamps to one and needs no pair faults",
			nodes: 1, ops: 5, faults: Faults{Partition: true},
			wantNodes: 1, wantOps: 5,
		},
		{
			name:  "zero nodes clamps to one",
			nodes: 0, ops: 5, faults: Faults{},
			wantNodes: 1, wantOps: 5,
		},
		{
			name:  "zero ops is legal",
			nodes: 2, ops: 0, faults: Faults{},
			wantNodes: 2, wantOps: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := GenSchedule(7, tt.nodes, tt.ops, tt.faults)
			if len(s.Nodes) != tt.wantNodes {
				t.Errorf("len(Nodes) = %d, want %d", len(s.Nodes), tt.wantNodes)
			}
			if len(s.Ops) != tt.wantOps {
				t.Errorf("len(Ops) = %d, want %d", len(s.Ops), tt.wantOps)
			}
			if got := len(s.Skews) > 0; got != tt.wantSkew {
				t.Errorf("Skews populated = %v, want %v", got, tt.wantSkew)
			}
			if got := s.Slow != ""; got != tt.wantSlow {
				t.Errorf("Slow set = %v (%q), want %v", got, s.Slow, tt.wantSlow)
			}
			if got := len(s.Faults) > 0; got != tt.wantFault {
				t.Errorf("Faults emitted = %v, want %v", got, tt.wantFault)
			}
			if s.SyncEvery < 1 {
				t.Errorf("SyncEvery = %d, want >= 1", s.SyncEvery)
			}
			if len(s.SKUs) < 2 {
				t.Errorf("len(SKUs) = %d, want >= 2 (contention needs sharing)", len(s.SKUs))
			}
		})
	}
}

func TestGenScheduleOpsAreWellFormed(t *testing.T) {
	s := GenSchedule(11, 3, 200, Faults{})
	nodes := map[clock.NodeID]bool{}
	for _, id := range s.Nodes {
		nodes[id] = true
	}
	skus := map[string]bool{}
	for _, sku := range s.SKUs {
		skus[sku] = true
	}
	kinds := map[OpKind]int{}
	for i, op := range s.Ops {
		if !nodes[op.Node] {
			t.Fatalf("op %d node %q not in Nodes", i, op.Node)
		}
		if !skus[op.SKU] {
			t.Fatalf("op %d sku %q not in SKUs", i, op.SKU)
		}
		if (op.Kind == OpReceive || op.Kind == OpPick) && op.Qty <= 0 {
			t.Fatalf("op %d kind %v qty = %d, want > 0 (node rejects non-positive)",
				i, op.Kind, op.Qty)
		}
		kinds[op.Kind]++
	}
	for _, k := range []OpKind{OpReceive, OpPick, OpSetMeta, OpDelete} {
		if kinds[k] == 0 {
			t.Errorf("kind %v never generated in 200 ops", k)
		}
	}
}

func TestGenScheduleFaultEventsAreOrderedAndHealed(t *testing.T) {
	s := GenSchedule(13, 3, 120, Faults{Partition: true, AsymPartition: true, CrashMidAppend: true})
	last := -1
	opens := 0
	for i, fe := range s.Faults {
		if fe.At < last {
			t.Fatalf("fault %d At = %d, out of order after %d", i, fe.At, last)
		}
		last = fe.At
		if fe.At < 0 || fe.At >= len(s.Ops) {
			t.Fatalf("fault %d At = %d out of range [0,%d)", i, fe.At, len(s.Ops))
		}
		switch fe.Kind {
		case "partition", "asym":
			if fe.A == fe.B {
				t.Fatalf("fault %d partitions node %q from itself", i, fe.A)
			}
			opens++
		case "heal":
			opens--
		case "crash":
			if fe.A == "" {
				t.Fatalf("fault %d crash has no node", i)
			}
		default:
			t.Fatalf("fault %d unknown kind %q", i, fe.Kind)
		}
	}
	if opens < 0 {
		t.Fatalf("more heals than partitions")
	}
	if opens == len(s.Faults) {
		t.Fatalf("no heal emitted; recovery is never exercised")
	}
}

func TestGenScheduleSkewsIncludeNegativeAndBackwardsJumps(t *testing.T) {
	s := GenSchedule(17, 4, 40, Faults{ClockSkew: true})
	sawNegative, sawJump := false, false
	for _, id := range s.Nodes {
		if s.Skews[id] < 0 {
			sawNegative = true
		}
		if s.Jumps[id] > 0 {
			sawJump = true
		}
	}
	if !sawNegative {
		t.Errorf("no node given a negative skew; the losing-clock case is untested")
	}
	if !sawJump {
		t.Errorf("no node given a backwards jump; HLC clamping is untested")
	}
}

func TestOpKindString(t *testing.T) {
	tests := []struct {
		k    OpKind
		want string
	}{
		{OpReceive, "receive"},
		{OpPick, "pick"},
		{OpSetMeta, "setmeta"},
		{OpDelete, "delete"},
		{OpKind(99), "OpKind(99)"},
	}
	for _, tt := range tests {
		if got := tt.k.String(); got != tt.want {
			t.Errorf("OpKind(%d).String() = %q, want %q", int(tt.k), got, tt.want)
		}
	}
}
