package jepsenlite

import (
	"context"
	"math/rand"
	"strings"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
)

func TestRunHoldsAllThreePropertiesUnderEveryFaultCombination(t *testing.T) {
	tests := []struct {
		name   string
		faults Faults
	}{
		{name: "no faults", faults: Faults{}},
		{name: "partition", faults: Faults{Partition: true}},
		{name: "asymmetric partition", faults: Faults{AsymPartition: true}},
		{name: "clock skew", faults: Faults{ClockSkew: true}},
		{name: "duplicate delivery", faults: Faults{Duplicate: true}},
		{name: "reorder", faults: Faults{Reorder: true}},
		{name: "crash mid-append", faults: Faults{CrashMidAppend: true}},
		{name: "slow peer", faults: Faults{SlowPeer: true}},
		{
			name:   "spec CLI example: partition, skew, dup",
			faults: Faults{Partition: true, ClockSkew: true, Duplicate: true},
		},
		{
			name: "all seven composed",
			faults: Faults{
				Partition: true, AsymPartition: true, ClockSkew: true,
				Duplicate: true, Reorder: true, CrashMidAppend: true, SlowPeer: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, seed := range []int64{1, 2, 3} {
				sch := GenSchedule(seed, 3, 120, tt.faults)
				res, err := Run(context.Background(), t.TempDir(), sch, tt.faults)
				if err != nil {
					t.Fatalf("Run(seed=%d) error = %v\nreproduce with: go run ./cmd/lab sim --seed %d --nodes 3 --ops 120 --faults %s",
						seed, err, seed, strings.Join(tt.faults.Names(), ","))
				}
				if res.Seed != seed {
					t.Errorf("Result.Seed = %d, want %d", res.Seed, seed)
				}
				if res.Acked+res.Failed != res.Ops {
					t.Errorf("Acked+Failed = %d, want Ops = %d",
						res.Acked+res.Failed, res.Ops)
				}
				if res.Acked == 0 {
					t.Errorf("no op was acknowledged; the run tested nothing")
				}
			}
		})
	}
}

func TestCheckConvergenceDetectsDivergenceAndNamesTheSKU(t *testing.T) {
	// Convergence is checked against a deliberately corrupted node: inject an
	// event into one node's log only, after quiescence, and check by hand.
	ctx := context.Background()
	sch := GenSchedule(99, 2, 10, Faults{})
	c, err := NewCluster(t.TempDir(), sch, Faults{})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.ApplyOp(ctx, Op{Node: "N0", Kind: OpReceive, SKU: "SKU-0", Qty: 5}); err != nil {
		t.Fatalf("ApplyOp() error = %v", err)
	}
	if err := c.Quiesce(ctx, 8); err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	// Now diverge N0 only.
	rogue := eventlog.Event{
		ID:    eventlog.EventID{NodeID: "ROGUE", Seq: 1},
		HLC:   clock.HLC{Wall: 1 << 40, NodeID: "ROGUE"},
		SKU:   "SKU-0",
		Kind:  eventlog.KindQuantityDelta,
		Delta: 1000,
	}
	if err := c.Logs["N0"].Append(ctx, rogue); err != nil {
		t.Fatalf("Append(rogue) error = %v", err)
	}
	err = c.CheckConvergence(ctx, []string{"SKU-0"})
	if err == nil {
		t.Fatalf("CheckConvergence() = nil, want divergence error")
	}
	if !strings.Contains(err.Error(), "SKU-0") {
		t.Errorf("CheckConvergence() error = %q, want it to name the SKU", err)
	}
}

func TestCheckNoLostEventDetectsAMissingEvent(t *testing.T) {
	ctx := context.Background()
	sch := GenSchedule(5, 2, 4, Faults{})
	c, err := NewCluster(t.TempDir(), sch, Faults{})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	id, err := c.ApplyOp(ctx, Op{Node: "N0", Kind: OpReceive, SKU: "SKU-0", Qty: 3})
	if err != nil {
		t.Fatalf("ApplyOp() error = %v", err)
	}
	// Before syncing, N1 does not hold it: the property must fail.
	if err := c.CheckNoLostEvent(ctx, []eventlog.EventID{id}); err == nil {
		t.Fatalf("CheckNoLostEvent() before sync = nil, want error")
	}
	if err := c.Quiesce(ctx, 8); err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	if err := c.CheckNoLostEvent(ctx, []eventlog.EventID{id}); err != nil {
		t.Fatalf("CheckNoLostEvent() after quiescence = %v, want nil", err)
	}
}

func TestCheckOrderIndependenceOnAContendedSKU(t *testing.T) {
	ctx := context.Background()
	sch := GenSchedule(21, 3, 60, Faults{})
	cl, err := NewCluster(t.TempDir(), sch, Faults{})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer func() { _ = cl.Close() }()

	for _, op := range sch.Ops {
		if _, err := cl.ApplyOp(ctx, op); err != nil {
			t.Fatalf("ApplyOp(%+v) error = %v", op, err)
		}
	}
	if err := cl.Quiesce(ctx, 16); err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	if err := cl.CheckOrderIndependence(ctx, rand.New(rand.NewSource(21)), sch.SKUs); err != nil {
		t.Fatalf("CheckOrderIndependence() error = %v", err)
	}
}

func TestQuiesceReportsFailureRatherThanGivingUpSilently(t *testing.T) {
	ctx := context.Background()
	sch := GenSchedule(31, 2, 4, Faults{})
	c, err := NewCluster(t.TempDir(), sch, Faults{})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.ApplyOp(ctx, Op{Node: "N0", Kind: OpReceive, SKU: "SKU-0", Qty: 1}); err != nil {
		t.Fatalf("ApplyOp() error = %v", err)
	}
	if err := c.Quiesce(ctx, 0); err == nil {
		t.Fatalf("Quiesce(maxRounds=0) = nil, want error")
	}
}

func TestResultString(t *testing.T) {
	r := Result{
		Seed:   42,
		Faults: []string{"dup", "partition"},
		Ops:    10, Acked: 9, Failed: 1, Rounds: 3, Crashes: 1,
		Stats:      Stats{Dropped: 4, Duplicated: 2, Reordered: 1},
		Quantities: map[string]int64{"SKU-0": -6},
		Anomalies:  []string{"SKU-0 quantity -6"},
	}
	s := r.String()
	for _, want := range []string{"seed 42", "partition", "acked 9", "SKU-0", "-6"} {
		if !strings.Contains(s, want) {
			t.Errorf("Result.String() = %q, missing %q", s, want)
		}
	}
}

func TestApplyOpRejectsAnUnknownOpKind(t *testing.T) {
	ctx := context.Background()
	sch := GenSchedule(41, 1, 1, Faults{})
	c, err := NewCluster(t.TempDir(), sch, Faults{})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.ApplyOp(ctx, Op{Node: "N0", Kind: OpKind(99), SKU: "SKU-0"}); err == nil {
		t.Fatalf("ApplyOp(unknown kind) = nil, want error")
	}
}

func TestApplyOpRejectsAnUnknownNode(t *testing.T) {
	ctx := context.Background()
	sch := GenSchedule(43, 1, 1, Faults{})
	c, err := NewCluster(t.TempDir(), sch, Faults{})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.ApplyOp(ctx, Op{Node: "nope", Kind: OpReceive, SKU: "SKU-0", Qty: 1}); err == nil {
		t.Fatalf("ApplyOp(unknown node) = nil, want error")
	}
}

// TestCompactionSkipsEventsAStalePeerHasNotAcked asserts what AckedFloor
// actually guarantees when a peer is stale: the floor is empty (N2 has acked
// nothing), so Compact must delete NOTHING. Earlier versions of this test
// only asserted convergence after N2 later synced, which passes trivially
// whether or not Compact deleted anything -- it never proved deletion safety.
// TestCompactionDeletesEventsOnceAllPeersHaveAcked below is the counterpart
// that actually exercises deletion.
func TestCompactionSkipsEventsAStalePeerHasNotAcked(t *testing.T) {
	ctx := context.Background()
	sch := GenSchedule(77, 3, 0, Faults{})
	c, err := NewCluster(t.TempDir(), sch, Faults{})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	// N0 and N1 do work and sync with each other. N2 stays stale.
	for i := 0; i < 12; i++ {
		if _, err := c.ApplyOp(ctx, Op{Node: "N0", Kind: OpReceive, SKU: "SKU-0", Qty: 2}); err != nil {
			t.Fatalf("ApplyOp() error = %v", err)
		}
		if _, err := c.ApplyOp(ctx, Op{Node: "N1", Kind: OpPick, SKU: "SKU-0", Qty: 1}); err != nil {
			t.Fatalf("ApplyOp() error = %v", err)
		}
	}
	if err := c.SyncPair(ctx, "N0", "N1"); err != nil {
		t.Fatalf("SyncPair(N0,N1) error = %v", err)
	}

	before, err := c.Logs["N0"].CountForSKU(ctx, "SKU-0")
	if err != nil {
		t.Fatalf("CountForSKU() before error = %v", err)
	}

	// Force a snapshot on N0 covering everything it holds, then compact using
	// the acked floor across ALL of N0's peers -- N2 has acked nothing, so
	// the floor is empty and Compact must delete nothing N2 might still lack.
	if err := c.Snapshot(ctx, "N0", "SKU-0"); err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	acked, err := c.AckedFloor(ctx, "N0")
	if err != nil {
		t.Fatalf("AckedFloor() error = %v", err)
	}
	if len(acked) != 0 {
		t.Fatalf("AckedFloor() = %v, want empty -- N2 has acked nothing", acked)
	}
	if err := c.Logs["N0"].Compact(ctx, acked); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	after, err := c.Logs["N0"].CountForSKU(ctx, "SKU-0")
	if err != nil {
		t.Fatalf("CountForSKU() after error = %v", err)
	}
	if after != before {
		t.Fatalf("Compact() against an empty floor deleted events: before %d, after %d, want unchanged", before, after)
	}

	// Now the stale peer syncs and everyone must still converge, from the
	// raw (undeleted) events -- this test never exercised deletion safety.
	if err := c.Quiesce(ctx, 16); err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	if err := c.CheckConvergence(ctx, []string{"SKU-0"}); err != nil {
		t.Fatalf("CheckConvergence() after aggressive compaction = %v", err)
	}
	st, err := c.Project(ctx, "N2", "SKU-0")
	if err != nil {
		t.Fatalf("Project(N2) error = %v", err)
	}
	if got := st.Quantity(); got != 12 {
		t.Errorf("stale peer quantity = %d, want 12 (12*+2 and 12*-1)", got)
	}
}

// TestCompactionDeletesEventsOnceAllPeersHaveAcked is the deletion-safety
// counterpart: once every peer (including N2) has synced and acked, the
// floor is no longer empty, Compact must actually delete the covered events,
// and the stale-snapshot-plus-remainder read path must still converge with
// every other replica even though the raw event rows are gone on N0.
func TestCompactionDeletesEventsOnceAllPeersHaveAcked(t *testing.T) {
	ctx := context.Background()
	sch := GenSchedule(78, 3, 0, Faults{})
	c, err := NewCluster(t.TempDir(), sch, Faults{})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	for i := 0; i < 12; i++ {
		if _, err := c.ApplyOp(ctx, Op{Node: "N0", Kind: OpReceive, SKU: "SKU-0", Qty: 2}); err != nil {
			t.Fatalf("ApplyOp() error = %v", err)
		}
		if _, err := c.ApplyOp(ctx, Op{Node: "N1", Kind: OpPick, SKU: "SKU-0", Qty: 1}); err != nil {
			t.Fatalf("ApplyOp() error = %v", err)
		}
	}
	// Every peer syncs with N0 before compaction, so nobody is stale.
	if err := c.SyncPair(ctx, "N0", "N1"); err != nil {
		t.Fatalf("SyncPair(N0,N1) error = %v", err)
	}
	if err := c.SyncPair(ctx, "N0", "N2"); err != nil {
		t.Fatalf("SyncPair(N0,N2) error = %v", err)
	}

	if err := c.Snapshot(ctx, "N0", "SKU-0"); err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	acked, err := c.AckedFloor(ctx, "N0")
	if err != nil {
		t.Fatalf("AckedFloor() error = %v", err)
	}
	if len(acked) == 0 {
		t.Fatalf("AckedFloor() = empty, want non-empty -- every peer has acked")
	}
	if err := c.Logs["N0"].Compact(ctx, acked); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	after, err := c.Logs["N0"].CountForSKU(ctx, "SKU-0")
	if err != nil {
		t.Fatalf("CountForSKU() after error = %v", err)
	}
	if after != 0 {
		t.Fatalf("CountForSKU() after Compact = %d, want 0 -- fully-acked, snapshotted events must be deleted", after)
	}

	if err := c.Quiesce(ctx, 16); err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	if err := c.CheckConvergence(ctx, []string{"SKU-0"}); err != nil {
		t.Fatalf("CheckConvergence() after full compaction = %v", err)
	}
	st, err := c.Project(ctx, "N0", "SKU-0")
	if err != nil {
		t.Fatalf("Project(N0) error = %v", err)
	}
	if got := st.Quantity(); got != 12 {
		t.Errorf("N0 quantity after compaction = %d, want 12 -- read path must use the snapshot, since the raw events are gone", got)
	}
}
