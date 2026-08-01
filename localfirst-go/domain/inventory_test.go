package domain_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
)

func newInv(t *testing.T, owned ...string) (*domain.Inventory, *eventlog.Store) {
	t.Helper()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	inv, err := domain.NewInventory(context.Background(), store, clock.New("n1", nil), owned)
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	return inv, store
}

func TestCommands(t *testing.T) {
	tests := []struct {
		name    string
		run     func(inv *domain.Inventory) error
		wantErr error
		want    map[domain.Key]int64
	}{
		{
			name: "receive then issue",
			run: func(inv *domain.Inventory) error {
				if err := inv.Receive(context.Background(), "S", "A", 10); err != nil {
					return err
				}
				return inv.Issue(context.Background(), "S", "A", 4)
			},
			want: map[domain.Key]int64{{SKU: "S", Location: "A"}: 6},
		},
		{
			name: "issue below zero is rejected and leaves no trace",
			run: func(inv *domain.Inventory) error {
				if err := inv.Receive(context.Background(), "S", "A", 2); err != nil {
					return err
				}
				return inv.Issue(context.Background(), "S", "A", 3)
			},
			wantErr: domain.ErrNegativeBalance,
			want:    map[domain.Key]int64{{SKU: "S", Location: "A"}: 2},
		},
		{
			name: "move to another node's location decrements only locally",
			run: func(inv *domain.Inventory) error {
				if err := inv.Receive(context.Background(), "S", "A", 5); err != nil {
					return err
				}
				return inv.Move(context.Background(), "S", "A", "REMOTE-Z", 5)
			},
			want: map[domain.Key]int64{
				{SKU: "S", Location: "A"}:        0,
				{SKU: "S", Location: "REMOTE-Z"}: 5,
			},
		},
		{
			name: "move more than held is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Move(context.Background(), "S", "A", "REMOTE-Z", 1)
			},
			wantErr: domain.ErrNegativeBalance,
		},
		{
			name: "receiving into a location this node does not own is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Receive(context.Background(), "S", "NOT-MINE", 1)
			},
			wantErr: domain.ErrNotOwned,
		},
		{
			name: "issuing from a location this node does not own is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Issue(context.Background(), "S", "NOT-MINE", 1)
			},
			wantErr: domain.ErrNotOwned,
		},
		{
			name: "moving out of a location this node does not own is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Move(context.Background(), "S", "NOT-MINE", "A", 1)
			},
			wantErr: domain.ErrNotOwned,
		},
		{
			name: "zero quantity is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Receive(context.Background(), "S", "A", 0)
			},
			wantErr: domain.ErrBadQty,
		},
		{
			name: "negative quantity is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Issue(context.Background(), "S", "A", -1)
			},
			wantErr: domain.ErrBadQty,
		},
		{
			name: "negative move quantity is rejected",
			run: func(inv *domain.Inventory) error {
				return inv.Move(context.Background(), "S", "A", "B", -1)
			},
			wantErr: domain.ErrBadQty,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv, _ := newInv(t, "A", "B")
			err := tc.run(inv)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for k, want := range tc.want {
				if got := inv.Balance(k.SKU, k.Location); got != want {
					t.Fatalf("Balance(%s,%s) = %d, want %d", k.SKU, k.Location, got, want)
				}
			}
		})
	}
}

func TestRejectedCommandAppendsNothing(t *testing.T) {
	ctx := context.Background()
	inv, store := newInv(t, "A")
	if err := inv.Receive(ctx, "S", "A", 1); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := inv.Issue(ctx, "S", "A", 99); !errors.Is(err, domain.ErrNegativeBalance) {
		t.Fatalf("Issue: %v, want ErrNegativeBalance", err)
	}
	recs, err := store.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("log holds %d records, want 1", len(recs))
	}
}

func TestNewInventoryReplaysExistingLog(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "n1.db")
	store, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inv, err := domain.NewInventory(ctx, store, clock.New("n1", nil), []string{"A"})
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	if err := inv.Receive(ctx, "S", "A", 7); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := eventlog.Open(path, "n1")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	restored, err := domain.NewInventory(ctx, reopened, clock.New("n1", nil), []string{"A"})
	if err != nil {
		t.Fatalf("NewInventory after restart: %v", err)
	}
	if got := restored.Balance("S", "A"); got != 7 {
		t.Fatalf("replayed balance = %d, want 7 (state must be rebuilt from the log)", got)
	}
}

func TestNewInventoryFailsOnCorruptLog(t *testing.T) {
	ctx := context.Background()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()
	if _, err := store.Merge(ctx, []eventlog.Record{
		{NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n2"}, Type: "inventory.Teleported"},
	}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if _, err := domain.NewInventory(ctx, store, clock.New("n1", nil), []string{"A"}); !errors.Is(err, eventlog.ErrUnknownType) {
		t.Fatalf("err = %v, want ErrUnknownType", err)
	}
}

func TestBalancesSnapshotIsACopy(t *testing.T) {
	inv, _ := newInv(t, "A")
	if err := inv.Receive(context.Background(), "S", "A", 3); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	snap := inv.Balances()
	snap[domain.Key{SKU: "S", Location: "A"}] = 999
	if inv.Balance("S", "A") != 3 {
		t.Fatal("Balances() must return a copy")
	}
}

// TestProjectAppliesRemoteRecords covers the Projector half of the seam, which
// central and any global view rely on.
func TestProjectAppliesRemoteRecords(t *testing.T) {
	inv, store := newInv(t, "A")
	remote := eventlog.Record{
		NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n2"},
		Type: domain.TypeReceived, Payload: mustJSON(t, domain.Received{SKU: "S", Location: "Q", Qty: 4}),
	}
	if _, err := store.Merge(context.Background(), []eventlog.Record{remote}, inv); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if got := inv.Balance("S", "Q"); got != 4 {
		t.Fatalf("Balance = %d, want 4", got)
	}
	if _, err := inv.Project(eventlog.Record{NodeID: "n2", Seq: 2, Type: "inventory.Teleported"}); !errors.Is(err, eventlog.ErrUnknownType) {
		t.Fatalf("Project(unknown) = %v, want ErrUnknownType", err)
	}
}

// TestMergeObservesPeerClockBeforeNextLocalAppend proves ADR 0003's promise:
// after this node merges a record with a future timestamp, its own next
// record sorts after that timestamp, not before it.
func TestMergeObservesPeerClockBeforeNextLocalAppend(t *testing.T) {
	ctx := context.Background()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	clk := clock.New("n1", nil)
	inv, err := domain.NewInventory(ctx, store, clk, nil)
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}

	future := eventlog.Record{
		NodeID:  "n2",
		Seq:     1,
		Clock:   clock.HLC{Wall: clk.Now().Wall + 1_000_000, NodeID: "n2"},
		Type:    domain.TypeReceived,
		Payload: mustJSON(t, domain.Received{SKU: "S", Location: "Q", Qty: 1}),
	}
	if _, err := store.Merge(ctx, []eventlog.Record{future}, inv); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if err := inv.Receive(ctx, "S", "n1", 1); err != nil {
		t.Fatalf("Receive: %v", err)
	}

	recs, err := store.Since(ctx, nil)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	var local eventlog.Record
	for _, r := range recs {
		if r.NodeID == "n1" {
			local = r
		}
	}
	if c := future.Clock.Compare(local.Clock); c >= 0 {
		t.Fatalf("local record's clock did not advance past the merged future one: future=%+v local=%+v", future.Clock, local.Clock)
	}
}

func TestCheckRejectsUnknownType(t *testing.T) {
	inv, _ := newInv(t, "A")
	if err := inv.Check(eventlog.Record{NodeID: "n1", Seq: 1, Type: "inventory.Teleported"}); !errors.Is(err, eventlog.ErrUnknownType) {
		t.Fatalf("Check = %v, want ErrUnknownType", err)
	}
}

// TestCheckIgnoresUnrelatedNegativeShadowBalance proves Check only validates
// the keys the candidate record touches. A synced Moved authored elsewhere
// legitimately leaves this node with a negative shadow balance at a location
// it does not own (docs/limitations.md); that must never block an unrelated
// local command against a key this node does own.
func TestCheckIgnoresUnrelatedNegativeShadowBalance(t *testing.T) {
	inv, store := newInv(t, "A")
	// n2 authored a move crediting "A" (owned by this node) and debiting "Q"
	// (owned by n2). Once merged, this node's projection carries Q: -4, a
	// shadow entry for a location it does not own.
	remote := eventlog.Record{
		NodeID: "n2", Seq: 1, Clock: clock.HLC{Wall: 1, NodeID: "n2"},
		Type: domain.TypeMoved, Payload: mustJSON(t, domain.Moved{SKU: "S", From: "Q", To: "A", Qty: 4}),
	}
	if _, err := store.Merge(context.Background(), []eventlog.Record{remote}, inv); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if got := inv.Balance("S", "Q"); got != -4 {
		t.Fatalf("Balance(Q) = %d, want -4 (shadow entry)", got)
	}
	// A local command touching only "A" must succeed despite the unrelated
	// negative shadow at "Q".
	if err := inv.Receive(context.Background(), "S", "A", 1); err != nil {
		t.Fatalf("Receive on unrelated key rejected because of shadow balance: %v", err)
	}
}

func TestOwnsEverythingWhenNoLocationsConfigured(t *testing.T) {
	// Central and tests pass no owned list: ownership checks are then off.
	inv, _ := newInv(t)
	if err := inv.Receive(context.Background(), "S", "anywhere", 1); err != nil {
		t.Fatalf("Receive: %v", err)
	}
}
