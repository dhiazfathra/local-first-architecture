package projection

import (
	"testing"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
)

func dispatched(id string, from, to domain.NodeID, qty float64) domain.Event {
	return domain.Event{Type: domain.TypeTransferDispatched, AggregateID: id,
		Payload: domain.TransferDispatched{TransferID: id, FromNode: from, ToNode: to,
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-01", To: domain.External, Qty: qty}}}}
}

func arrived(id string, qty float64) domain.Event {
	return domain.Event{Type: domain.TypeTransferReceived, AggregateID: id,
		Payload: domain.TransferReceived{TransferID: id,
			Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "RECV-01", Qty: qty}}}}
}

func TestTransfersProjection(t *testing.T) {
	tests := []struct {
		name           string
		events         []domain.Event
		wantDispatched float64
		wantReceived   float64
		wantStatus     domain.TransferStatus
	}{
		{
			name:           "dispatch only is in flight",
			events:         []domain.Event{dispatched("T1", "wh-a", "wh-b", 6)},
			wantDispatched: 6,
			wantStatus:     domain.TransferInFlight,
		},
		{
			name:           "partial receipt stays in flight",
			events:         []domain.Event{dispatched("T1", "wh-a", "wh-b", 6), arrived("T1", 4)},
			wantDispatched: 6,
			wantReceived:   4,
			wantStatus:     domain.TransferInFlight,
		},
		{
			name:           "full receipt completes",
			events:         []domain.Event{dispatched("T1", "wh-a", "wh-b", 6), arrived("T1", 6)},
			wantDispatched: 6,
			wantReceived:   6,
			wantStatus:     domain.TransferComplete,
		},
		{
			name:           "receipt arriving before the forwarded dispatch still completes",
			events:         []domain.Event{arrived("T1", 6), dispatched("T1", "wh-a", "wh-b", 6)},
			wantDispatched: 6,
			wantReceived:   6,
			wantStatus:     domain.TransferComplete,
		},
		{
			name: "central rejection marks the transfer failed",
			events: []domain.Event{
				dispatched("T1", "wh-a", "wh-b", 6),
				{Type: domain.TypeStockAdjusted, AggregateID: "T1", Payload: domain.StockAdjusted{
					Move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "PICK-01", Qty: 6},
					Reason: domain.ReasonTransferRejected}},
			},
			wantDispatched: 6,
			wantStatus:     domain.TransferFailed,
		},
		{
			name: "an unrelated adjustment leaves the status alone",
			events: []domain.Event{
				dispatched("T1", "wh-a", "wh-b", 6),
				adjust("WIDGET", "L1", domain.External, "PICK-01", 1, domain.ReasonCountVariance),
			},
			wantDispatched: 6,
			wantStatus:     domain.TransferInFlight,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, set := openSet(t)
			emit(t, l, set, tt.events...)

			rows, err := set.Transfers()
			if err != nil {
				t.Fatalf("Transfers: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("len(rows) = %d, want 1: %+v", len(rows), rows)
			}
			got := rows[0]
			if got.ID != "T1" {
				t.Errorf("ID = %q, want T1", got.ID)
			}
			if got.Dispatched != tt.wantDispatched {
				t.Errorf("Dispatched = %v, want %v", got.Dispatched, tt.wantDispatched)
			}
			if got.Received != tt.wantReceived {
				t.Errorf("Received = %v, want %v", got.Received, tt.wantReceived)
			}
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", got.Status, tt.wantStatus)
			}
		})
	}
}

func TestTransferQueriesFailAfterClose(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, dispatched("T1", "wh-a", "wh-b", 6))
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := set.Transfers(); err == nil {
		t.Error("Transfers: expected an error")
	}
	if _, err := set.Exceptions(); err == nil {
		t.Error("Exceptions: expected an error")
	}
}

// TestApplyPropagatesATransferProjectionFailure proves every event applyTransfer
// touches surfaces a write failure through Apply rather than swallowing it.
func TestApplyPropagatesATransferProjectionFailure(t *testing.T) {
	tests := []struct {
		name  string
		event domain.Event
	}{
		{"dispatch", dispatched("T1", "wh-a", "wh-b", 6)},
		{"receipt", arrived("T1", 6)},
		{"rejection", domain.Event{Type: domain.TypeStockAdjusted, AggregateID: "T1", Payload: domain.StockAdjusted{
			Move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: domain.External, To: "PICK-01", Qty: 6},
			Reason: domain.ReasonTransferRejected}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, set := openSet(t)
			if _, err := l.DB().Exec(`DROP TABLE transfers`); err != nil {
				t.Fatalf("drop transfers: %v", err)
			}
			envs, err := l.Emit([]domain.Event{tt.event}, nil)
			if err != nil {
				t.Fatalf("Emit: %v", err)
			}
			if err := set.Apply(envs[0]); err == nil {
				t.Fatal("Apply: expected an error with the transfers table missing")
			}
		})
	}
}

// TestTransfersFailsToScanAMalformedRow mirrors StockOnHand's and Reservations'
// malformed-row coverage: a non-numeric column makes Scan itself fail.
func TestTransfersFailsToScanAMalformedRow(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, dispatched("T1", "wh-a", "wh-b", 6))
	if _, err := l.DB().Exec(`UPDATE transfers SET dispatched = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt dispatched: %v", err)
	}
	if _, err := set.Transfers(); err == nil {
		t.Fatal("Transfers: expected a scan error for a non-numeric dispatched")
	}
}

func TestExceptionsFailsToScanAMalformedRow(t *testing.T) {
	l, set := openSet(t)
	origin := emit(t, l, set, received("WIDGET", "L1", "RECV-01", 5))[0]
	emitCaused(t, l, set, origin.ID,
		adjust("WIDGET", "L1", "RECV-01", domain.External, 5, domain.ReasonPOOverReceipt))
	if _, err := l.DB().Exec(`UPDATE exceptions SET qty = 'not-a-number'`); err != nil {
		t.Fatalf("corrupt qty: %v", err)
	}
	if _, err := set.Exceptions(); err == nil {
		t.Fatal("Exceptions: expected a scan error for a non-numeric qty")
	}
}

func TestExceptionsRejectAnUnparseableTimestamp(t *testing.T) {
	l, set := openSet(t)
	emit(t, l, set, received("WIDGET", "L1", "RECV-01", 1))
	if _, err := l.DB().Exec(`INSERT INTO exceptions
		(id, kind, reason, caused_by, sku, location, lot_id, qty, recorded_at, resolved)
		VALUES ('x', 'compensation', 'r', 'c', 'WIDGET', 'RECV-01', 'L1', 1, 'not-a-time', 0)`); err != nil {
		t.Fatalf("seed bad row: %v", err)
	}
	if _, err := set.Exceptions(); err == nil {
		t.Error("Exceptions: expected a parse error, got nil")
	}
	if _, err := l.DB().Exec(`INSERT INTO transfers
		(id, from_node, to_node, dispatched, received, status, dispatched_at)
		VALUES ('y', 'a', 'b', 1, 0, 'in_flight', 'not-a-time')`); err != nil {
		t.Fatalf("seed bad row: %v", err)
	}
	if _, err := set.Transfers(); err == nil {
		t.Error("Transfers: expected a parse error, got nil")
	}
}
