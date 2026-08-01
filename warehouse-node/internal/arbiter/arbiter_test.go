package arbiter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

var at = time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)

// env seals a payload into an envelope with the given identity, the way a node's log
// would before pushing it upstream.
func env(t *testing.T, node domain.NodeID, seq uint64, typ, agg string, payload any) domain.Envelope {
	t.Helper()
	e, err := domain.NewEnvelope(
		domain.EventID{NodeID: node, Seq: seq},
		domain.HLC{Wall: at.UnixMilli() + int64(seq), Node: node},
		at.Add(time.Duration(seq)*time.Second), nil,
		domain.Event{Type: typ, AggregateID: agg, Payload: payload})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	return e
}

func goodsReceived(receipt, note, po, sku, lot string, qty float64, to domain.LocationCode) domain.GoodsReceived {
	return domain.GoodsReceived{ReceiptID: receipt, DeliveryNote: note, PORef: po,
		Move: domain.Movement{SKU: sku, LotID: lot, From: domain.External, To: to, Qty: qty}}
}

// newArbiter returns an arbiter over a memory store seeded with the widget item
// master, a 100-unit purchase order and two known nodes.
func newArbiter(t *testing.T) (*Arbiter, central.Store) {
	t.Helper()
	ctx := context.Background()
	store := central.NewMemory()
	if err := store.UpsertItem(ctx, domain.Item{SKU: "WIDGET", Description: "Blue widget",
		BaseUoM: "EA", LotTracked: true, ShelfLifeDays: 30}); err != nil {
		t.Fatalf("UpsertItem: %v", err)
	}
	if err := store.UpsertPurchaseOrder(ctx, "PO-1", "WIDGET", 100); err != nil {
		t.Fatalf("UpsertPurchaseOrder: %v", err)
	}
	for _, node := range []domain.NodeID{"wh-a", "wh-b"} {
		if err := store.RegisterNode(ctx, node, nil); err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
	}
	return New(store, func() time.Time { return at }), store
}

// decodeAdjusted pulls the StockAdjusted payload out of a compensating envelope.
func decodeAdjusted(t *testing.T, e domain.Envelope) domain.StockAdjusted {
	t.Helper()
	payload, err := domain.DecodePayload(e)
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	a, ok := payload.(domain.StockAdjusted)
	if !ok {
		t.Fatalf("payload is %T, want domain.StockAdjusted", payload)
	}
	return a
}

func TestArbitrateAcceptsAValidReceipt(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	e := env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
		goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 60, "RECV-01"))

	decision, comps, err := a.Arbitrate(ctx, e)
	if err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if decision.Verdict != central.VerdictAccepted || decision.Reason != "" {
		t.Fatalf("decision = %+v, want an unqualified acceptance", decision)
	}
	if len(comps) != 0 {
		t.Fatalf("comps = %+v, want none", comps)
	}
	total, err := store.ReceivedAgainstPO(ctx, "PO-1", "WIDGET")
	if err != nil || total != 60 {
		t.Fatalf("ReceivedAgainstPO = %v, %v; want 60, nil", total, err)
	}
	recorded, ok, err := store.Decision(ctx, e.ID)
	if err != nil || !ok || recorded.Verdict != central.VerdictAccepted {
		t.Errorf("stored decision = %+v, ok %v, err %v; want an accepted verdict", recorded, ok, err)
	}
}

// TestArbitrateIsIdempotentOnRetry proves that arbitrating the same event twice —
// simulating a caller retrying after a crash between EmitCentral, Enqueue, and
// RecordDecision — returns the original decision and compensations without
// re-running validators or re-emitting a second compensating event.
func TestArbitrateIsIdempotentOnRetry(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	e := env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
		goodsReceived("R1", "DN-1", "PO-1", "GHOST", "", 5, "RECV-01"))

	first, firstComps, err := a.Arbitrate(ctx, e)
	if err != nil {
		t.Fatalf("first Arbitrate: %v", err)
	}
	if first.Verdict != central.VerdictRejected || len(firstComps) == 0 {
		t.Fatalf("first decision = %+v, comps %+v; want a rejection with a compensation", first, firstComps)
	}

	second, secondComps, err := a.Arbitrate(ctx, e)
	if err != nil {
		t.Fatalf("second Arbitrate: %v", err)
	}
	if second.Verdict != first.Verdict || second.Reason != first.Reason {
		t.Fatalf("second decision = %+v, want it to match the first %+v", second, first)
	}
	if len(secondComps) != len(firstComps) || secondComps[0].ID != firstComps[0].ID {
		t.Fatalf("second comps = %+v, want exactly the first compensation replayed, not a new one", secondComps)
	}
	all, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	compCount := 0
	for _, ev := range all {
		if ev.CausationID != nil && *ev.CausationID == e.ID {
			compCount++
		}
	}
	if compCount != 1 {
		t.Fatalf("compensating events for %s = %d, want exactly 1 (no duplicate on retry)", e.ID, compCount)
	}
}

// TestRejectionRules is one case per central-enforced invariant, asserting the exact
// compensating event emitted: its type, reason, movement and CausationID.
func TestRejectionRules(t *testing.T) {
	type wantEvent struct {
		typ    string
		reason string
		move   domain.Movement
	}
	tests := []struct {
		name       string
		seed       func(t *testing.T, a *Arbiter, s central.Store)
		event      func(t *testing.T) domain.Envelope
		wantReason string
		wantEvents []wantEvent
		wantFailed string
	}{
		{
			name: "unknown sku",
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
					goodsReceived("R1", "DN-1", "PO-1", "GHOST", "", 5, "RECV-01"))
			},
			wantReason: domain.ReasonUnknownSKU,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonUnknownSKU,
				move:   domain.Movement{SKU: "GHOST", From: "RECV-01", To: domain.External, Qty: 5},
			}},
		},
		{
			name: "deleted sku is treated as unknown",
			seed: func(t *testing.T, _ *Arbiter, s central.Store) {
				if err := s.UpsertItem(context.Background(), domain.Item{SKU: "WIDGET",
					BaseUoM: "EA", LotTracked: true, Deleted: true}); err != nil {
					t.Fatalf("UpsertItem: %v", err)
				}
			},
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
					goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 5, "RECV-01"))
			},
			wantReason: domain.ReasonUnknownSKU,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonUnknownSKU,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 5},
			}},
		},
		{
			name: "duplicate delivery note",
			seed: func(t *testing.T, a *Arbiter, _ central.Store) {
				first := env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
					goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 10, "RECV-01"))
				if _, _, err := a.Arbitrate(context.Background(), first); err != nil {
					t.Fatalf("seed Arbitrate: %v", err)
				}
			},
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-b", 1, domain.TypeGoodsReceived, "R2",
					goodsReceived("R2", "DN-1", "PO-1", "WIDGET", "L1", 10, "RECV-09"))
			},
			wantReason: domain.ReasonDuplicateReceipt,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonDuplicateReceipt,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-09", To: domain.External, Qty: 10},
			}},
		},
		{
			name: "receipt exceeds the open purchase order",
			seed: func(t *testing.T, a *Arbiter, _ central.Store) {
				first := env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
					goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 90, "RECV-01"))
				if _, _, err := a.Arbitrate(context.Background(), first); err != nil {
					t.Fatalf("seed Arbitrate: %v", err)
				}
			},
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-b", 1, domain.TypeGoodsReceived, "R2",
					goodsReceived("R2", "DN-2", "PO-1", "WIDGET", "L1", 30, "RECV-09"))
			},
			wantReason: domain.ReasonPOOverReceipt,
			wantEvents: []wantEvent{
				{
					typ:    domain.TypeStockAdjusted,
					reason: domain.ReasonPOOverReceipt,
					move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-09", To: domain.External, Qty: 20},
				},
				{typ: domain.TypeReceiptLineRecorded},
			},
		},
		{
			name: "transfer to an unknown node",
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1", domain.TransferDispatched{
					TransferID: "T1", FromNode: "wh-a", ToNode: "wh-z",
					Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})
			},
			wantReason: domain.ReasonTransferRejected,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonTransferRejected,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "PICK-01", Qty: 6},
			}},
			wantFailed: "T1",
		},
		{
			name: "transfer to a node that refuses the item",
			seed: func(t *testing.T, _ *Arbiter, s central.Store) {
				if err := s.RegisterNode(context.Background(), "wh-b", []string{"WIDGET"}); err != nil {
					t.Fatalf("RegisterNode: %v", err)
				}
			},
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1", domain.TransferDispatched{
					TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
					Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})
			},
			wantReason: domain.ReasonTransferRejected,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonTransferRejected,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "PICK-01", Qty: 6},
			}},
			wantFailed: "T1",
		},
		{
			name: "transfer received exceeds what was dispatched",
			seed: func(t *testing.T, a *Arbiter, _ central.Store) {
				dispatch := env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1", domain.TransferDispatched{
					TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
					Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})
				if _, _, err := a.Arbitrate(context.Background(), dispatch); err != nil {
					t.Fatalf("seed Arbitrate: %v", err)
				}
			},
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-b", 1, domain.TypeTransferReceived, "T1", domain.TransferReceived{
					TransferID: "T1",
					Lines:      []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-09", Qty: 10}}})
			},
			wantReason: domain.ReasonTransferOverReceipt,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonTransferOverReceipt,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-09", To: domain.External, Qty: 4},
			}},
		},
		{
			name: "transfer received with no dispatch at all",
			event: func(t *testing.T) domain.Envelope {
				return env(t, "wh-b", 1, domain.TypeTransferReceived, "T9", domain.TransferReceived{
					TransferID: "T9",
					Lines:      []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-09", Qty: 3}}})
			},
			wantReason: domain.ReasonTransferOverReceipt,
			wantEvents: []wantEvent{{
				typ:    domain.TypeStockAdjusted,
				reason: domain.ReasonTransferOverReceipt,
				move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-09", To: domain.External, Qty: 3},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, store := newArbiter(t)
			ctx := context.Background()
			if tt.seed != nil {
				tt.seed(t, a, store)
			}
			e := tt.event(t)

			decision, comps, err := a.Arbitrate(ctx, e)
			if err != nil {
				t.Fatalf("Arbitrate: %v", err)
			}
			if decision.Verdict != central.VerdictRejected {
				t.Fatalf("verdict = %q, want %q", decision.Verdict, central.VerdictRejected)
			}
			if decision.Reason != tt.wantReason {
				t.Fatalf("reason = %q, want %q", decision.Reason, tt.wantReason)
			}
			if len(comps) != len(tt.wantEvents) {
				t.Fatalf("emitted %d compensating events, want %d: %+v", len(comps), len(tt.wantEvents), comps)
			}
			if decision.CompensatingID == nil || *decision.CompensatingID != comps[0].ID {
				t.Errorf("decision.CompensatingID = %v, want %v", decision.CompensatingID, comps[0].ID)
			}
			for i, want := range tt.wantEvents {
				got := comps[i]
				if got.Type != want.typ {
					t.Errorf("comps[%d].Type = %q, want %q", i, got.Type, want.typ)
				}
				if got.ID.NodeID != central.CentralNode {
					t.Errorf("comps[%d] came from %q, want it attributed to central", i, got.ID.NodeID)
				}
				if got.CausationID == nil || *got.CausationID != e.ID {
					t.Errorf("comps[%d].CausationID = %v, want %v", i, got.CausationID, e.ID)
				}
				if want.typ != domain.TypeStockAdjusted {
					continue
				}
				adjusted := decodeAdjusted(t, got)
				if adjusted.Reason != want.reason {
					t.Errorf("comps[%d].Reason = %q, want %q", i, adjusted.Reason, want.reason)
				}
				if adjusted.Move != want.move {
					t.Errorf("comps[%d].Move = %+v, want %+v", i, adjusted.Move, want.move)
				}
			}
			// The original event is still stored: history is immutable, mistakes
			// included.
			all, err := store.Events(ctx)
			if err != nil {
				t.Fatalf("Events: %v", err)
			}
			var found bool
			for _, stored := range all {
				found = found || stored.ID == e.ID
			}
			if !found {
				t.Error("the rejected event is not in the log; rejection must never delete history")
			}
			if tt.wantFailed != "" {
				row, ok, err := store.InTransit(ctx, tt.wantFailed, domain.StockKey{SKU: "WIDGET", LotID: "L1"})
				if err != nil || !ok || !row.Failed {
					t.Errorf("InTransit(%s) = %+v, ok %v, err %v; want it marked failed",
						tt.wantFailed, row, ok, err)
				}
			}
			// The compensation is queued for the node that produced the event, so it
			// arrives on the next sync session. A node cannot refuse it.
			queued, err := store.Outbound(ctx, e.ID.NodeID, 0, 10)
			if err != nil {
				t.Fatalf("Outbound: %v", err)
			}
			if len(queued) < len(comps) {
				t.Errorf("queued %d events for %s, want at least the %d compensations",
					len(queued), e.ID.NodeID, len(comps))
			}
		})
	}
}

// TestOverReceivedTransferStillForwardsToSource proves that a TransferReceived
// rejected as an over-receipt is still enqueued for the source node. Central
// records the accepted portion and compensates only the excess at the
// destination; if the receipt were never forwarded, the source's own transfer
// projection would show the goods as in-transit forever even though they arrived.
func TestOverReceivedTransferStillForwardsToSource(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()

	dispatch := env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1", domain.TransferDispatched{
		TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
		Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})
	if _, _, err := a.Arbitrate(ctx, dispatch); err != nil {
		t.Fatalf("seed dispatch: %v", err)
	}

	receipt := env(t, "wh-b", 1, domain.TypeTransferReceived, "T1", domain.TransferReceived{
		TransferID: "T1",
		Lines:      []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-09", Qty: 10}}})
	decision, comps, err := a.Arbitrate(ctx, receipt)
	if err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if decision.Verdict != central.VerdictRejected || decision.Reason != domain.ReasonTransferOverReceipt {
		t.Fatalf("decision = %+v, want a transfer_overreceipt rejection", decision)
	}
	if len(comps) != 1 {
		t.Fatalf("comps = %+v, want exactly one (the destination's excess reversal)", comps)
	}

	// The receipt event itself — not just its compensation — must reach wh-a, the
	// transfer's source, or its transfer projection never learns the goods arrived.
	queued, err := store.Outbound(ctx, "wh-a", 0, 10)
	if err != nil {
		t.Fatalf("Outbound: %v", err)
	}
	var forwarded bool
	for _, q := range queued {
		if q.Env.ID == receipt.ID {
			forwarded = true
		}
	}
	if !forwarded {
		t.Fatalf("wh-a's outbound queue = %+v, want the original receipt %s forwarded to it", queued, receipt.ID)
	}
}

func TestPOOverReceiptRecordsOnlyTheAcceptedPortion(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	if _, _, err := a.Arbitrate(ctx, env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
		goodsReceived("R1", "DN-1", "PO-1", "WIDGET", "L1", 90, "RECV-01"))); err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if _, _, err := a.Arbitrate(ctx, env(t, "wh-b", 1, domain.TypeGoodsReceived, "R2",
		goodsReceived("R2", "DN-2", "PO-1", "WIDGET", "L1", 30, "RECV-09"))); err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	total, err := store.ReceivedAgainstPO(ctx, "PO-1", "WIDGET")
	if err != nil {
		t.Fatalf("ReceivedAgainstPO: %v", err)
	}
	if total != 100 {
		t.Fatalf("ReceivedAgainstPO = %v, want 100: the excess is compensated, not counted", total)
	}
	// A third receipt against a now-full purchase order over-receives by its whole
	// quantity, and produces exactly one compensation — not one per earlier receipt.
	_, comps, err := a.Arbitrate(ctx, env(t, "wh-a", 2, domain.TypeGoodsReceived, "R3",
		goodsReceived("R3", "DN-3", "PO-1", "WIDGET", "L1", 5, "RECV-01")))
	if err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if len(comps) != 2 {
		t.Fatalf("emitted %d events, want 2 (the adjustment and the line reversal)", len(comps))
	}
	if got := decodeAdjusted(t, comps[0]).Move.Qty; got != 5 {
		t.Errorf("compensated quantity = %v, want the whole 5", got)
	}
}

func TestUnknownPurchaseOrderIsAccepted(t *testing.T) {
	a, _ := newArbiter(t)
	decision, comps, err := a.Arbitrate(context.Background(),
		env(t, "wh-a", 1, domain.TypeGoodsReceived, "R1",
			goodsReceived("R1", "DN-1", "PO-UNKNOWN", "WIDGET", "L1", 5, "RECV-01")))
	if err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	if decision.Verdict != central.VerdictAccepted || len(comps) != 0 {
		t.Fatalf("decision = %+v, comps = %+v; want acceptance: a stub po has no ordered quantity to exceed",
			decision, comps)
	}
}

func TestAcceptedTransferHalvesMoveInTransitToZero(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}

	if _, _, err := a.Arbitrate(ctx, env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1",
		domain.TransferDispatched{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})); err != nil {
		t.Fatalf("Arbitrate dispatch: %v", err)
	}
	row, ok, err := store.InTransit(ctx, "T1", key)
	if err != nil || !ok || row.Dispatched-row.Received != 6 {
		t.Fatalf("in transit after dispatch = %+v, ok %v, err %v; want 6", row, ok, err)
	}
	if _, _, err := a.Arbitrate(ctx, env(t, "wh-b", 1, domain.TypeTransferReceived, "T1",
		domain.TransferReceived{TransferID: "T1",
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-09", Qty: 6}}})); err != nil {
		t.Fatalf("Arbitrate receipt: %v", err)
	}
	if row, _, err = store.InTransit(ctx, "T1", key); err != nil || row.Dispatched-row.Received != 0 {
		t.Fatalf("in transit after receipt = %+v, %v; want 0", row, err)
	}
	// The dispatch was forwarded to the destination, and the receipt back to the
	// source, so both nodes can close their view of the transfer.
	toDest, err := store.Outbound(ctx, "wh-b", 0, 10)
	if err != nil {
		t.Fatalf("Outbound(wh-b): %v", err)
	}
	if len(toDest) != 1 || toDest[0].Env.Type != domain.TypeTransferDispatched {
		t.Errorf("queued for wh-b = %+v, want the dispatch", toDest)
	}
	toSource, err := store.Outbound(ctx, "wh-a", 0, 10)
	if err != nil {
		t.Fatalf("Outbound(wh-a): %v", err)
	}
	if len(toSource) != 1 || toSource[0].Env.Type != domain.TypeTransferReceived {
		t.Errorf("queued for wh-a = %+v, want the receipt", toSource)
	}
}

func TestDiscrepanciesReportUnmatchedDispatchesWithoutCompensating(t *testing.T) {
	a, store := newArbiter(t)
	ctx := context.Background()
	if _, _, err := a.Arbitrate(ctx, env(t, "wh-a", 1, domain.TypeTransferDispatched, "T1",
		domain.TransferDispatched{TransferID: "T1", FromNode: "wh-a", ToNode: "wh-b",
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: 6}}})); err != nil {
		t.Fatalf("Arbitrate: %v", err)
	}
	before, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	if rows, err := Discrepancies(ctx, store, at); err != nil || len(rows) != 0 {
		t.Fatalf("Discrepancies inside the window = %+v, %v; want none", rows, err)
	}
	rows, err := Discrepancies(ctx, store, at.Add(72*time.Hour))
	if err != nil {
		t.Fatalf("Discrepancies: %v", err)
	}
	if len(rows) != 1 || rows[0].TransferID != "T1" {
		t.Fatalf("Discrepancies = %+v, want T1 reported", rows)
	}

	after, err := store.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(after) != len(before) {
		t.Error("reporting a discrepancy emitted events; a lost truck is a human problem, never auto-compensated")
	}
}

func TestArbitrateRejectsAnUndecodableEvent(t *testing.T) {
	a, _ := newArbiter(t)
	bad := domain.Envelope{
		ID:      domain.EventID{NodeID: "wh-a", Seq: 1},
		Type:    "NotAnEventType",
		HLC:     domain.HLC{Wall: 1, Node: "wh-a"},
		Payload: json.RawMessage(`{}`),
	}
	if _, _, err := a.Arbitrate(context.Background(), bad); err == nil {
		t.Fatal("Arbitrate on an unknown event type: expected an error, never a silent skip")
	}
}

func TestValidatorsAreNamedAndOrdered(t *testing.T) {
	a, _ := newArbiter(t)
	want := []string{"unknown_sku", "duplicate_delivery_note", "po_over_receipt",
		"transfer_destination", "transfer_over_receipt"}
	got := a.Validators()
	if len(got) != len(want) {
		t.Fatalf("%d validators, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Name() != want[i] {
			t.Errorf("validator %d is %q, want %q", i, got[i].Name(), want[i])
		}
	}
}
