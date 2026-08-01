package central

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

// runStoreContract is the behaviour every Store implementation must satisfy. It is
// run once against Memory and once against Postgres, which is what makes having two
// implementations affordable.
func runStoreContract(t *testing.T, open func(t *testing.T) Store) {
	t.Helper()

	at := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)

	nodeEnv := func(node domain.NodeID, seq uint64, typ string) domain.Envelope {
		move := domain.Movement{SKU: "WIDGET", From: "RECV-01", To: "PICK-01", Qty: float64(seq)}
		var payload any = domain.PutAway{Move: move}
		if typ == domain.TypePicked {
			payload = domain.Picked{Move: move}
		}
		env, err := domain.NewEnvelope(
			domain.EventID{NodeID: node, Seq: seq},
			domain.HLC{Wall: at.UnixMilli() + int64(seq), Node: node},
			at.Add(time.Duration(seq)*time.Second), nil,
			domain.Event{Type: typ, AggregateID: "WIDGET", Payload: payload})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		return env
	}

	t.Run("append is idempotent and returns only what was new", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		first := []domain.Envelope{nodeEnv("wh-a", 1, domain.TypePutAway), nodeEnv("wh-a", 2, domain.TypePutAway)}

		got, err := s.Append(ctx, first)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("first Append returned %d new, want 2", len(got))
		}
		got, err = s.Append(ctx, append(first, nodeEnv("wh-a", 3, domain.TypePicked)))
		if err != nil {
			t.Fatalf("re-Append: %v", err)
		}
		if len(got) != 1 || got[0].ID.Seq != 3 {
			t.Fatalf("overlapping Append returned %+v, want only seq 3", got)
		}
		all, err := s.Events(ctx)
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if len(all) != 3 {
			t.Fatalf("Events returned %d, want 3", len(all))
		}
		for i := range all {
			if all[i].ID.Seq != uint64(i+1) {
				t.Errorf("Events[%d].Seq = %d, want %d: reads must come back in HLC order",
					i, all[i].ID.Seq, i+1)
			}
		}
	})

	t.Run("events are partitioned by node", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		if _, err := s.Append(ctx, []domain.Envelope{
			nodeEnv("wh-a", 1, domain.TypePutAway), nodeEnv("wh-b", 1, domain.TypePutAway),
		}); err != nil {
			t.Fatalf("Append: %v", err)
		}
		// The same sequence number from two nodes is two distinct events, because
		// identity is the (node, seq) pair.
		all, err := s.Events(ctx)
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("Events returned %d, want 2", len(all))
		}
	})

	t.Run("central emits its own events with sequence and hlc assigned", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		envs, err := s.EmitCentral(ctx, []domain.Event{
			{Type: domain.TypeStockAdjusted, AggregateID: "WIDGET", Payload: domain.StockAdjusted{
				Move:   domain.Movement{SKU: "WIDGET", From: "RECV-01", To: domain.External, Qty: 4},
				Reason: domain.ReasonPOOverReceipt}},
		}, &domain.EventID{NodeID: "wh-a", Seq: 7}, at)
		if err != nil {
			t.Fatalf("EmitCentral: %v", err)
		}
		if len(envs) != 1 {
			t.Fatalf("EmitCentral returned %d envelopes, want 1", len(envs))
		}
		e := envs[0]
		if e.ID.NodeID != CentralNode || e.ID.Seq != 1 {
			t.Errorf("ID = %v, want central/1", e.ID)
		}
		if e.HLC.Node != CentralNode || e.HLC.Wall == 0 {
			t.Errorf("HLC = %+v, want central's clock reading", e.HLC)
		}
		if e.CausationID == nil || e.CausationID.Seq != 7 {
			t.Errorf("CausationID = %v, want wh-a/7", e.CausationID)
		}

		next, err := s.EmitCentral(ctx, []domain.Event{
			{Type: domain.TypeItemUpserted, AggregateID: "WIDGET", Payload: domain.ItemUpserted{
				Item: domain.Item{SKU: "WIDGET", BaseUoM: "EA"}}},
		}, nil, at.Add(time.Second))
		if err != nil {
			t.Fatalf("second EmitCentral: %v", err)
		}
		if next[0].ID.Seq != 2 {
			t.Errorf("second Seq = %d, want 2: central's sequence must be monotonic", next[0].ID.Seq)
		}
		if next[0].CausationID != nil {
			t.Errorf("CausationID = %v, want nil for a non-compensating event", next[0].CausationID)
		}
	})

	t.Run("cursors are per node and default to zero", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		for _, node := range []domain.NodeID{"wh-a", "wh-b"} {
			if got, err := s.PushedSeq(ctx, node); err != nil || got != 0 {
				t.Fatalf("PushedSeq(%s) = %d, %v; want 0, nil", node, got, err)
			}
			if got, err := s.DeliveredOrd(ctx, node); err != nil || got != 0 {
				t.Fatalf("DeliveredOrd(%s) = %d, %v; want 0, nil", node, got, err)
			}
		}
		if err := s.SetPushedSeq(ctx, "wh-a", 42); err != nil {
			t.Fatalf("SetPushedSeq: %v", err)
		}
		if err := s.SetDeliveredOrd(ctx, "wh-a", 7); err != nil {
			t.Fatalf("SetDeliveredOrd: %v", err)
		}
		if err := s.SetPushedSeq(ctx, "wh-a", 43); err != nil {
			t.Fatalf("SetPushedSeq again: %v", err)
		}
		if got, err := s.PushedSeq(ctx, "wh-a"); err != nil || got != 43 {
			t.Errorf("PushedSeq = %d, %v; want 43, nil", got, err)
		}
		if got, err := s.DeliveredOrd(ctx, "wh-a"); err != nil || got != 7 {
			t.Errorf("DeliveredOrd = %d, %v; want 7, nil", got, err)
		}
		if got, err := s.PushedSeq(ctx, "wh-b"); err != nil || got != 0 {
			t.Errorf("PushedSeq(wh-b) = %d, %v; want 0, nil: cursors are per node", got, err)
		}
	})

	t.Run("outbound queue is ordered, per target and resumable", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		envs := []domain.Envelope{nodeEnv("wh-a", 1, domain.TypePutAway), nodeEnv("wh-a", 2, domain.TypePicked)}
		if _, err := s.Append(ctx, envs); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := s.Enqueue(ctx, "wh-b", envs); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		out, err := s.Outbound(ctx, "wh-b", 0, 10)
		if err != nil {
			t.Fatalf("Outbound: %v", err)
		}
		if len(out) != 2 || out[0].Ord >= out[1].Ord {
			t.Fatalf("Outbound = %+v, want two rows in increasing ord order", out)
		}
		if out[0].Env.ID != envs[0].ID {
			t.Errorf("Outbound[0] = %v, want %v", out[0].Env.ID, envs[0].ID)
		}
		resumed, err := s.Outbound(ctx, "wh-b", out[0].Ord, 10)
		if err != nil {
			t.Fatalf("resumed Outbound: %v", err)
		}
		if len(resumed) != 1 || resumed[0].Env.ID != envs[1].ID {
			t.Fatalf("resumed = %+v, want only the second event", resumed)
		}
		limited, err := s.Outbound(ctx, "wh-b", 0, 1)
		if err != nil {
			t.Fatalf("limited Outbound: %v", err)
		}
		if len(limited) != 1 {
			t.Fatalf("limited = %+v, want the limit respected", limited)
		}
		// A non-positive limit returns nothing, never the unbounded queue.
		if zero, err := s.Outbound(ctx, "wh-b", 0, 0); err != nil || len(zero) != 0 {
			t.Fatalf("Outbound with limit 0 = %+v, %v; want empty", zero, err)
		}
		if neg, err := s.Outbound(ctx, "wh-b", 0, -1); err != nil || len(neg) != 0 {
			t.Fatalf("Outbound with limit -1 = %+v, %v; want empty", neg, err)
		}

		other, err := s.Outbound(ctx, "wh-a", 0, 10)
		if err != nil {
			t.Fatalf("Outbound(wh-a): %v", err)
		}
		if len(other) != 0 {
			t.Errorf("Outbound(wh-a) = %+v, want empty: the queue is per target", other)
		}

		// Enqueue is idempotent per (target, event): retrying a crash-interrupted
		// Arbitrate must not duplicate the outbound entry.
		if err := s.Enqueue(ctx, "wh-b", envs); err != nil {
			t.Fatalf("re-Enqueue: %v", err)
		}
		if out, err = s.Outbound(ctx, "wh-b", 0, 10); err != nil || len(out) != 2 {
			t.Fatalf("Outbound after re-Enqueue = %+v, %v; want still exactly 2 rows", out, err)
		}
	})

	t.Run("item master round-trips including deletion", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		if _, ok, err := s.Item(ctx, "GHOST"); err != nil || ok {
			t.Fatalf("Item(GHOST) = ok %v, err %v; want false, nil", ok, err)
		}
		item := domain.Item{SKU: "WIDGET", Description: "Blue widget", BaseUoM: "EA",
			AltUoM: map[domain.UoM]float64{"CASE": 12}, LotTracked: true, ShelfLifeDays: 30}
		if err := s.UpsertItem(ctx, item); err != nil {
			t.Fatalf("UpsertItem: %v", err)
		}
		got, ok, err := s.Item(ctx, "WIDGET")
		if err != nil || !ok {
			t.Fatalf("Item = ok %v, err %v; want true, nil", ok, err)
		}
		if got.BaseUoM != "EA" || got.AltUoM["CASE"] != 12 || !got.LotTracked || got.ShelfLifeDays != 30 {
			t.Errorf("Item = %+v, want it to round-trip %+v", got, item)
		}
		item.Deleted = true
		if err := s.UpsertItem(ctx, item); err != nil {
			t.Fatalf("UpsertItem deleted: %v", err)
		}
		got, ok, err = s.Item(ctx, "WIDGET")
		if err != nil || !ok || !got.Deleted {
			t.Errorf("deleted Item = %+v, ok %v, err %v; want the row present and flagged deleted", got, ok, err)
		}
	})

	t.Run("purchase orders and receipts against them accumulate across nodes", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		if _, ok, err := s.PurchaseOrder(ctx, "PO-1", "WIDGET"); err != nil || ok {
			t.Fatalf("PurchaseOrder before upsert = ok %v, err %v; want false, nil", ok, err)
		}
		if err := s.UpsertPurchaseOrder(ctx, "PO-1", "WIDGET", 100); err != nil {
			t.Fatalf("UpsertPurchaseOrder: %v", err)
		}
		qty, ok, err := s.PurchaseOrder(ctx, "PO-1", "WIDGET")
		if err != nil || !ok || qty != 100 {
			t.Fatalf("PurchaseOrder = %v, ok %v, err %v; want 100, true, nil", qty, ok, err)
		}
		facts := []ReceiptFact{
			{EventID: domain.EventID{NodeID: "wh-a", Seq: 1}, Node: "wh-a", PORef: "PO-1",
				DeliveryNote: "DN-1", SKU: "WIDGET", QtyBase: 60},
			{EventID: domain.EventID{NodeID: "wh-b", Seq: 1}, Node: "wh-b", PORef: "PO-1",
				DeliveryNote: "DN-2", SKU: "WIDGET", QtyBase: 30},
		}
		for _, f := range facts {
			if err := s.RecordReceipt(ctx, f); err != nil {
				t.Fatalf("RecordReceipt: %v", err)
			}
		}
		total, err := s.ReceivedAgainstPO(ctx, "PO-1", "WIDGET")
		if err != nil || total != 90 {
			t.Fatalf("ReceivedAgainstPO = %v, %v; want 90, nil: receipts from both nodes count", total, err)
		}
		// Recording the same event twice must not double-count: retried sync
		// batches deliver the same event again.
		if err := s.RecordReceipt(ctx, facts[0]); err != nil {
			t.Fatalf("re-RecordReceipt: %v", err)
		}
		if total, err = s.ReceivedAgainstPO(ctx, "PO-1", "WIDGET"); err != nil || total != 90 {
			t.Fatalf("ReceivedAgainstPO after replay = %v, %v; want 90, nil", total, err)
		}
		if total, err = s.ReceivedAgainstPO(ctx, "PO-9", "WIDGET"); err != nil || total != 0 {
			t.Fatalf("ReceivedAgainstPO for an unknown po = %v, %v; want 0, nil", total, err)
		}
		// Receipt reads one event's own contribution back, which is what a validator
		// re-run by a crash-retry subtracts so it does not reject itself.
		got, ok, err := s.Receipt(ctx, facts[0].EventID)
		if err != nil || !ok || got != facts[0] {
			t.Fatalf("Receipt = %+v, ok %v, err %v; want %+v, true, nil", got, ok, err, facts[0])
		}
		if _, ok, err = s.Receipt(ctx, domain.EventID{NodeID: "wh-z", Seq: 9}); err != nil || ok {
			t.Fatalf("Receipt of an unrecorded event = ok %v, err %v; want false, nil", ok, err)
		}
	})

	t.Run("delivery notes remember who keyed them first", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		if _, ok, err := s.DeliveryNoteFirstSeen(ctx, "DN-1", "WIDGET"); err != nil || ok {
			t.Fatalf("first seen before any receipt = ok %v, err %v; want false, nil", ok, err)
		}
		first := ReceiptFact{EventID: domain.EventID{NodeID: "wh-a", Seq: 1}, Node: "wh-a",
			PORef: "PO-1", DeliveryNote: "DN-1", SKU: "WIDGET", QtyBase: 10}
		if err := s.RecordReceipt(ctx, first); err != nil {
			t.Fatalf("RecordReceipt: %v", err)
		}
		if err := s.RecordReceipt(ctx, ReceiptFact{EventID: domain.EventID{NodeID: "wh-b", Seq: 4},
			Node: "wh-b", PORef: "PO-1", DeliveryNote: "DN-1", SKU: "WIDGET", QtyBase: 10}); err != nil {
			t.Fatalf("duplicate RecordReceipt: %v", err)
		}
		got, ok, err := s.DeliveryNoteFirstSeen(ctx, "DN-1", "WIDGET")
		if err != nil || !ok {
			t.Fatalf("first seen = ok %v, err %v; want true, nil", ok, err)
		}
		if got != first.EventID {
			t.Errorf("first seen = %v, want %v: the earliest receipt owns the note", got, first.EventID)
		}
	})

	t.Run("node configuration records existence and refusals", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		if _, known, err := s.NodeConfig(ctx, "wh-z"); err != nil || known {
			t.Fatalf("NodeConfig(wh-z) = known %v, err %v; want false, nil", known, err)
		}
		if err := s.RegisterNode(ctx, "wh-b", []string{"HAZMAT"}); err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
		rejects, known, err := s.NodeConfig(ctx, "wh-b")
		if err != nil || !known {
			t.Fatalf("NodeConfig = known %v, err %v; want true, nil", known, err)
		}
		if !rejects["HAZMAT"] || rejects["WIDGET"] {
			t.Errorf("rejects = %+v, want HAZMAT only", rejects)
		}
		if err := s.RegisterNode(ctx, "wh-b", nil); err != nil {
			t.Fatalf("re-RegisterNode: %v", err)
		}
		if rejects, _, err = s.NodeConfig(ctx, "wh-b"); err != nil || len(rejects) != 0 {
			t.Errorf("rejects after reconfiguration = %+v, %v; want empty", rejects, err)
		}
	})

	t.Run("in-transit is non-zero only between dispatch and receipt", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
		if _, ok, err := s.InTransit(ctx, "T1", key); err != nil || ok {
			t.Fatalf("InTransit before dispatch = ok %v, err %v; want false, nil", ok, err)
		}
		row := InTransitRow{TransferID: "T1", Key: key, FromNode: "wh-a", ToNode: "wh-b",
			Dispatched: 6, DispatchedAt: at}
		if err := s.RecordDispatch(ctx, row); err != nil {
			t.Fatalf("RecordDispatch: %v", err)
		}
		got, ok, err := s.InTransit(ctx, "T1", key)
		if err != nil || !ok {
			t.Fatalf("InTransit = ok %v, err %v; want true, nil", ok, err)
		}
		if got.Dispatched-got.Received != 6 {
			t.Errorf("in transit = %v, want 6 after dispatch", got.Dispatched-got.Received)
		}
		if err := s.AddReceived(ctx, domain.EventID{NodeID: "wh-b", Seq: 1}, "T1", key, 4); err != nil {
			t.Fatalf("AddReceived: %v", err)
		}
		if got, _, err = s.InTransit(ctx, "T1", key); err != nil || got.Dispatched-got.Received != 2 {
			t.Fatalf("in transit after partial receipt = %+v, %v; want 2", got, err)
		}
		if err := s.AddReceived(ctx, domain.EventID{NodeID: "wh-b", Seq: 2}, "T1", key, 2); err != nil {
			t.Fatalf("AddReceived: %v", err)
		}
		if got, _, err = s.InTransit(ctx, "T1", key); err != nil || got.Dispatched-got.Received != 0 {
			t.Fatalf("in transit after full receipt = %+v, %v; want 0", got, err)
		}
		// Retrying the same event is a no-op, not a double-add.
		if err := s.AddReceived(ctx, domain.EventID{NodeID: "wh-b", Seq: 2}, "T1", key, 2); err != nil {
			t.Fatalf("AddReceived retry: %v", err)
		}
		if got, _, err = s.InTransit(ctx, "T1", key); err != nil || got.Dispatched-got.Received != 0 {
			t.Fatalf("in transit after retried receipt = %+v, %v; want unchanged at 0", got, err)
		}
		if err := s.AddReceived(ctx, domain.EventID{NodeID: "wh-b", Seq: 3}, "T9", key, 1); err == nil {
			t.Error("AddReceived on an unknown transfer: expected an error")
		}
		// Each event's own folded quantity is readable back, so a crash-retried
		// validator can exclude it from the balance instead of rejecting itself.
		for _, tc := range []struct {
			id   domain.EventID
			want float64
		}{
			{domain.EventID{NodeID: "wh-b", Seq: 1}, 4},
			{domain.EventID{NodeID: "wh-b", Seq: 2}, 2},
			{domain.EventID{NodeID: "wh-b", Seq: 3}, 0},
		} {
			qty, err := s.ReceivedFromEvent(ctx, tc.id, "T1", key)
			if err != nil || qty != tc.want {
				t.Errorf("ReceivedFromEvent(%v) = %v, %v; want %v, nil", tc.id, qty, err, tc.want)
			}
		}
	})

	t.Run("open transfers past a window are reportable and failure is recorded", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		key := domain.StockKey{SKU: "WIDGET", LotID: "L1"}
		if err := s.RecordDispatch(ctx, InTransitRow{TransferID: "T1", Key: key, FromNode: "wh-a",
			ToNode: "wh-b", Dispatched: 6, DispatchedAt: at}); err != nil {
			t.Fatalf("RecordDispatch: %v", err)
		}
		if err := s.RecordDispatch(ctx, InTransitRow{TransferID: "T2", Key: key, FromNode: "wh-a",
			ToNode: "wh-b", Dispatched: 1, DispatchedAt: at.Add(48 * time.Hour)}); err != nil {
			t.Fatalf("RecordDispatch: %v", err)
		}
		open1, err := s.OpenTransfers(ctx, at.Add(24*time.Hour))
		if err != nil {
			t.Fatalf("OpenTransfers: %v", err)
		}
		if len(open1) != 1 || open1[0].TransferID != "T1" {
			t.Fatalf("OpenTransfers = %+v, want only T1: T2 is inside the window", open1)
		}
		if err := s.AddReceived(ctx, domain.EventID{NodeID: "wh-b", Seq: 1}, "T1", key, 6); err != nil {
			t.Fatalf("AddReceived: %v", err)
		}
		if open1, err = s.OpenTransfers(ctx, at.Add(24*time.Hour)); err != nil || len(open1) != 0 {
			t.Fatalf("OpenTransfers after receipt = %+v, %v; want empty", open1, err)
		}
		if err := s.FailTransfer(ctx, "T2"); err != nil {
			t.Fatalf("FailTransfer: %v", err)
		}
		got, _, err := s.InTransit(ctx, "T2", key)
		if err != nil || !got.Failed {
			t.Errorf("InTransit(T2) = %+v, %v; want Failed", got, err)
		}
		if open2, err := s.OpenTransfers(ctx, at.Add(1000*time.Hour)); err != nil || len(open2) != 0 {
			t.Errorf("OpenTransfers = %+v, %v; want empty: a failed transfer is not a lost truck", open2, err)
		}
	})

	t.Run("decisions are recorded per event and readable back", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		id := domain.EventID{NodeID: "wh-a", Seq: 3}
		if _, ok, err := s.Decision(ctx, id); err != nil || ok {
			t.Fatalf("Decision before recording = ok %v, err %v; want false, nil", ok, err)
		}
		comp := domain.EventID{NodeID: CentralNode, Seq: 1}
		want := Decision{EventID: id, Verdict: VerdictRejected,
			Reason: domain.ReasonPOOverReceipt, CompensatingID: &comp}
		if err := s.RecordDecision(ctx, want); err != nil {
			t.Fatalf("RecordDecision: %v", err)
		}
		got, ok, err := s.Decision(ctx, id)
		if err != nil || !ok {
			t.Fatalf("Decision = ok %v, err %v; want true, nil", ok, err)
		}
		if got.Verdict != want.Verdict || got.Reason != want.Reason ||
			got.CompensatingID == nil || *got.CompensatingID != comp {
			t.Errorf("Decision = %+v, want %+v", got, want)
		}
		accepted := Decision{EventID: domain.EventID{NodeID: "wh-a", Seq: 4}, Verdict: VerdictAccepted}
		if err := s.RecordDecision(ctx, accepted); err != nil {
			t.Fatalf("RecordDecision accepted: %v", err)
		}
		if got, _, err = s.Decision(ctx, accepted.EventID); err != nil || got.CompensatingID != nil {
			t.Errorf("accepted Decision = %+v, %v; want no compensating event", got, err)
		}
	})

	t.Run("payloads survive as raw json", func(t *testing.T) {
		s, ctx := open(t), context.Background()
		env := nodeEnv("wh-a", 1, domain.TypePutAway)
		if _, err := s.Append(ctx, []domain.Envelope{env}); err != nil {
			t.Fatalf("Append: %v", err)
		}
		all, err := s.Events(ctx)
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		var want, got map[string]any
		if err := json.Unmarshal(env.Payload, &want); err != nil {
			t.Fatalf("unmarshal want: %v", err)
		}
		if err := json.Unmarshal(all[0].Payload, &got); err != nil {
			t.Fatalf("unmarshal got: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("payload = %v, want %v", got, want)
		}
		for k, v := range want {
			if !reflect.DeepEqual(got[k], v) {
				t.Errorf("payload[%q] = %v, want %v", k, got[k], v)
			}
		}
	})
}
