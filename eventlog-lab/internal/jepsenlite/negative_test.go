package jepsenlite

import (
	"context"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
)

// TestConcurrentPickBelowZeroConvergesToNegativeSix is the spec's headline
// example, and it asserts the negative as the CORRECT result.
//
// An item holds 10 units. Two nodes are partitioned and each picks 8. Both
// writes are valid locally -- neither node can see the other. After healing,
// every replica converges on 10 - 8 - 8 = -6.
//
// This is correct CRDT behavior and this project accepts it. A commutative
// counter cannot enforce "quantity >= 0" without exactly the coordination that
// local-first architecture exists to avoid. Negative stock is a reportable
// anomaly at central, never a rejected write. If a future change makes this
// test fail by "fixing" the negative, that change is the regression.
func TestConcurrentPickBelowZeroConvergesToNegativeSix(t *testing.T) {
	ctx := context.Background()
	sch := GenSchedule(2026, 3, 0, Faults{Partition: true})
	c, err := NewCluster(t.TempDir(), sch, Faults{Partition: true})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	// Ten units, known to everyone.
	if _, err := c.ApplyOp(ctx, Op{Node: "N0", Kind: OpReceive, SKU: "SKU-0", Qty: 10}); err != nil {
		t.Fatalf("Receive(10) error = %v", err)
	}
	if _, err := c.Quiesce(ctx, 8); err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	for _, id := range c.IDs {
		st, err := c.Project(ctx, id, "SKU-0")
		if err != nil {
			t.Fatalf("Project(%q) error = %v", id, err)
		}
		if got := st.Quantity(); got != 10 {
			t.Fatalf("pre-partition quantity at %q = %d, want 10", id, got)
		}
	}

	// Partition N0 from N1 in both directions, then each picks 8.
	c.Inj.Partition("N0", "N1")
	if _, err := c.ApplyOp(ctx, Op{Node: "N0", Kind: OpPick, SKU: "SKU-0", Qty: 8}); err != nil {
		t.Fatalf("N0 Pick(8) error = %v", err)
	}
	if _, err := c.ApplyOp(ctx, Op{Node: "N1", Kind: OpPick, SKU: "SKU-0", Qty: 8}); err != nil {
		t.Fatalf("N1 Pick(8) error = %v", err)
	}

	// Each node sees only its own pick while partitioned.
	for _, tc := range []struct {
		id   clock.NodeID
		want int64
	}{{"N0", 2}, {"N1", 2}} {
		st, err := c.Project(ctx, tc.id, "SKU-0")
		if err != nil {
			t.Fatalf("Project(%q) error = %v", tc.id, err)
		}
		if got := st.Quantity(); got != tc.want {
			t.Errorf("during partition, %q quantity = %d, want %d", tc.id, got, tc.want)
		}
	}

	// Heal and converge.
	if _, err := c.Quiesce(ctx, 16); err != nil {
		t.Fatalf("Quiesce() after heal error = %v", err)
	}
	for _, id := range c.IDs {
		st, err := c.Project(ctx, id, "SKU-0")
		if err != nil {
			t.Fatalf("Project(%q) error = %v", id, err)
		}
		if got := st.Quantity(); got != -6 {
			t.Errorf("converged quantity at %q = %d, want -6 "+
				"(10 - 8 - 8; negative stock is accepted, not rejected)", id, got)
		}
	}
	if err := c.CheckConvergence(ctx, []string{"SKU-0"}); err != nil {
		t.Errorf("CheckConvergence() = %v, want nil", err)
	}
}
