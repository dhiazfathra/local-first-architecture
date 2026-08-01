package grpctransport

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/projection"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

func widget() domain.Item {
	return domain.Item{SKU: "WIDGET", Description: "Blue widget", BaseUoM: "EA",
		AltUoM: map[domain.UoM]float64{"CASE": 12}, LotTracked: true, ShelfLifeDays: 3650}
}

// newAPI starts a node with two locations and the widget item master, and wraps it
// in the gRPC service with a fixed clock.
func newAPI(t *testing.T) *NodeAPI {
	t.Helper()
	at := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	n := 0
	svc, err := node.Open(filepath.Join(t.TempDir(), "node.db"), "wh-a", func() time.Time {
		n++
		return at.Add(time.Duration(n) * time.Second)
	})
	if err != nil {
		t.Fatalf("node.Open: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	for code, typ := range map[domain.LocationCode]domain.LocationType{
		"RECV-01": domain.LocReceiving, "PICK-01": domain.LocPick, "STAGE-01": domain.LocStaging,
	} {
		if err := svc.RegisterLocation(code, typ); err != nil {
			t.Fatalf("RegisterLocation(%s): %v", code, err)
		}
	}
	if _, err := svc.Execute(func(*domain.State) ([]domain.Event, error) {
		return []domain.Event{{Type: domain.TypeItemUpserted, AggregateID: "WIDGET",
			Payload: domain.ItemUpserted{Item: widget()}}}, nil
	}); err != nil {
		t.Fatalf("seed item master: %v", err)
	}
	return NewNodeAPI(svc, func() time.Time { return at })
}

func line(qty float64, uom string) *nodeapi.Line {
	return &nodeapi.Line{Sku: "WIDGET", LotId: "L1", Qty: qty, Uom: uom}
}

// TestCommandsSucceedFromLocalStateOnly walks the whole command surface in the order
// an operator would: receive, put away, reserve, release, pick, count, transfer.
// Nothing in this test is connected to central, which is the property being asserted.
func TestCommandsSucceedFromLocalStateOnly(t *testing.T) {
	api := newAPI(t)
	ctx := context.Background()

	steps := []struct {
		name      string
		call      func() (*nodeapi.CommandResponse, error)
		wantCount int
	}{
		{
			name: "receive one case into the dock",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R1", DeliveryNote: "DN-1",
					PoRef: "PO-1", Line: line(1, "CASE"), To: "RECV-01"})
			},
			wantCount: 3,
		},
		{
			name: "put six away to the pick face",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.PutAway(ctx, &nodeapi.PutAwayRequest{Line: line(6, "EA"), From: "RECV-01", To: "PICK-01"})
			},
			wantCount: 1,
		},
		{
			name: "reserve two",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.Reserve(ctx, &nodeapi.ReserveRequest{ReservationId: "RS1", Line: line(2, "EA"), Location: "PICK-01"})
			},
			wantCount: 1,
		},
		{
			name: "release the reservation",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.ReleaseReservation(ctx, &nodeapi.ReleaseReservationRequest{ReservationId: "RS1"})
			},
			wantCount: 1,
		},
		{
			name: "pick one for a customer",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.Pick(ctx, &nodeapi.PickRequest{Line: line(1, "EA"), From: "PICK-01", OrderRef: "SO-9"})
			},
			wantCount: 1,
		},
		{
			name: "start a count of the pick face",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.StartCount(ctx, &nodeapi.StartCountRequest{CountId: "C1", Location: "PICK-01"})
			},
			wantCount: 1,
		},
		{
			name: "count four where the book says five",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.CountLine(ctx, &nodeapi.CountLineRequest{CountId: "C1", Line: line(4, "EA")})
			},
			wantCount: 1,
		},
		{
			name: "close the count, booking the variance",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.CloseCount(ctx, &nodeapi.CloseCountRequest{CountId: "C1"})
			},
			wantCount: 2,
		},
		{
			name: "dispatch two to the other warehouse",
			call: func() (*nodeapi.CommandResponse, error) {
				return api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1",
					ToNode: "wh-b", From: "PICK-01", Lines: []*nodeapi.Line{line(2, "EA")}})
			},
			wantCount: 1,
		},
	}
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			resp, err := s.call()
			if err != nil {
				t.Fatalf("%s: %v", s.name, err)
			}
			if len(resp.GetEventIds()) != s.wantCount {
				t.Fatalf("event ids = %v, want %d", resp.GetEventIds(), s.wantCount)
			}
		})
	}

	stock, err := api.StockOnHand(ctx, &nodeapi.StockOnHandRequest{Sku: "WIDGET", Location: "PICK-01"})
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(stock.GetRows()) != 1 || stock.GetRows()[0].GetQty() != 2 {
		t.Fatalf("rows = %+v, want a single row of 2", stock.GetRows())
	}

	transfers, err := api.Transfers(ctx, &nodeapi.TransfersRequest{})
	if err != nil {
		t.Fatalf("Transfers: %v", err)
	}
	if len(transfers.GetRows()) != 1 || transfers.GetRows()[0].GetStatus() != string(domain.TransferInFlight) {
		t.Fatalf("transfers = %+v, want one in-flight row", transfers.GetRows())
	}

	exceptions, err := api.Exceptions(ctx, &nodeapi.ExceptionsRequest{})
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions.GetRows()) != 0 {
		t.Fatalf("exceptions = %+v, want none: nothing has been compensated", exceptions.GetRows())
	}
}

// TestCommandsRejectViolationsAsFailedPrecondition asserts every command surfaces a
// node-enforced invariant as FailedPrecondition naming the rule, and appends nothing.
func TestCommandsRejectViolationsAsFailedPrecondition(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name     string
		call     func(*NodeAPI) error
		wantRule string
	}{
		{
			name: "receive an unknown sku",
			call: func(a *NodeAPI) error {
				_, err := a.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R1", DeliveryNote: "DN-1", PoRef: "PO-1",
					Line: &nodeapi.Line{Sku: "GHOST", Qty: 1, Uom: "EA"}, To: "RECV-01"})
				return err
			},
			wantRule: domain.RuleSKUExists,
		},
		{
			name: "put away more than is on the dock",
			call: func(a *NodeAPI) error {
				_, err := a.PutAway(ctx, &nodeapi.PutAwayRequest{Line: line(1, "EA"), From: "RECV-01", To: "PICK-01"})
				return err
			},
			wantRule: domain.RuleStockNonNegative,
		},
		{
			name: "pick from an empty shelf",
			call: func(a *NodeAPI) error {
				_, err := a.Pick(ctx, &nodeapi.PickRequest{Line: line(1, "EA"), From: "PICK-01"})
				return err
			},
			wantRule: domain.RuleStockNonNegative,
		},
		{
			name: "reserve more than is available",
			call: func(a *NodeAPI) error {
				_, err := a.Reserve(ctx, &nodeapi.ReserveRequest{ReservationId: "RS1", Line: line(1, "EA"), Location: "PICK-01"})
				return err
			},
			wantRule: domain.RuleReservationAvailable,
		},
		{
			name: "release a reservation that does not exist",
			call: func(a *NodeAPI) error {
				_, err := a.ReleaseReservation(ctx, &nodeapi.ReleaseReservationRequest{ReservationId: "nope"})
				return err
			},
			wantRule: domain.RuleAggregateState,
		},
		{
			name: "start a count at an unknown location",
			call: func(a *NodeAPI) error {
				_, err := a.StartCount(ctx, &nodeapi.StartCountRequest{CountId: "C1", Location: "MARS-01"})
				return err
			},
			wantRule: domain.RuleLocationExists,
		},
		{
			name: "count a line on a count that was never started",
			call: func(a *NodeAPI) error {
				_, err := a.CountLine(ctx, &nodeapi.CountLineRequest{CountId: "C9", Line: line(1, "EA")})
				return err
			},
			wantRule: domain.RuleAggregateState,
		},
		{
			name: "close a count that was never started",
			call: func(a *NodeAPI) error {
				_, err := a.CloseCount(ctx, &nodeapi.CloseCountRequest{CountId: "C9"})
				return err
			},
			wantRule: domain.RuleAggregateState,
		},
		{
			name: "dispatch stock the node does not have",
			call: func(a *NodeAPI) error {
				_, err := a.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{TransferId: "T1", ToNode: "wh-b",
					From: "PICK-01", Lines: []*nodeapi.Line{line(1, "EA")}})
				return err
			},
			wantRule: domain.RuleStockNonNegative,
		},
		{
			name: "receive a transfer this node was never told about",
			call: func(a *NodeAPI) error {
				_, err := a.ReceiveTransfer(ctx, &nodeapi.ReceiveTransferRequest{TransferId: "T9", To: "RECV-01",
					Lines: []*nodeapi.Line{line(1, "EA")}})
				return err
			},
			wantRule: domain.RuleAggregateState,
		},
		{
			name: "bad uom for the item",
			call: func(a *NodeAPI) error {
				_, err := a.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R1", DeliveryNote: "DN-1", PoRef: "PO-1",
					Line: &nodeapi.Line{Sku: "WIDGET", LotId: "L1", Qty: 1, Uom: "PALLET"}, To: "RECV-01"})
				return err
			},
			wantRule: domain.RuleUoMValid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newAPI(t)
			err := tt.call(api)
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.FailedPrecondition {
				t.Fatalf("err = %v, want a FailedPrecondition status", err)
			}
			if !strings.Contains(st.Message(), tt.wantRule) {
				t.Fatalf("message = %q, want it to name rule %q", st.Message(), tt.wantRule)
			}
		})
	}
}

func TestReceiveTransferSucceedsAfterTheDispatchArrives(t *testing.T) {
	api := newAPI(t)
	ctx := context.Background()

	// Central forwards the source node's dispatch to this node.
	dispatch, err := domain.NewEnvelope(
		domain.EventID{NodeID: "wh-b", Seq: 1},
		domain.HLC{Wall: 1, Node: "wh-b"},
		time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC), nil,
		domain.Event{Type: domain.TypeTransferDispatched, AggregateID: "T1",
			Payload: domain.TransferDispatched{TransferID: "T1", FromNode: "wh-b", ToNode: "wh-a",
				Lines: []domain.Movement{{SKU: "WIDGET", LotID: "L1", From: "PICK-09", To: domain.External, Qty: 5}}}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if _, err := api.svc.Ingest([]domain.Envelope{dispatch}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if _, err := api.ReceiveTransfer(ctx, &nodeapi.ReceiveTransferRequest{TransferId: "T1", To: "RECV-01",
		Lines: []*nodeapi.Line{line(5, "EA")}}); err != nil {
		t.Fatalf("ReceiveTransfer: %v", err)
	}
	rows, err := api.StockOnHand(ctx, &nodeapi.StockOnHandRequest{})
	if err != nil {
		t.Fatalf("StockOnHand: %v", err)
	}
	if len(rows.GetRows()) != 1 || rows.GetRows()[0].GetQty() != 5 {
		t.Fatalf("rows = %+v, want a single row of 5", rows.GetRows())
	}
}

// TestExceptionsReturnsCompensationRows asserts a compensation central ingests shows
// up on the operator's Exceptions query, covering the row-mapping loop that the
// happy-path test (which books no compensations) never exercises.
func TestExceptionsReturnsCompensationRows(t *testing.T) {
	api := newAPI(t)
	ctx := context.Background()

	if _, err := api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R1", DeliveryNote: "DN-1", PoRef: "PO-1",
		Line: line(4, "EA"), To: "RECV-01"}); err != nil {
		t.Fatalf("Receive: %v", err)
	}

	cause := domain.EventID{NodeID: "wh-a", Seq: 1}
	compensation, err := domain.NewEnvelope(
		domain.EventID{NodeID: "wh-a", Seq: 100},
		domain.HLC{Wall: 2, Node: "wh-a"},
		time.Date(2026, 7, 30, 9, 0, 0, 0, time.UTC), &cause,
		domain.Event{Type: domain.TypeStockAdjusted, AggregateID: "WIDGET", Payload: domain.StockAdjusted{
			Move:   domain.Movement{SKU: "WIDGET", LotID: "L1", From: "RECV-01", To: domain.External, Qty: 4},
			Reason: domain.ReasonPOOverReceipt,
		}})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if _, err := api.svc.Ingest([]domain.Envelope{compensation}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	exceptions, err := api.Exceptions(ctx, &nodeapi.ExceptionsRequest{})
	if err != nil {
		t.Fatalf("Exceptions: %v", err)
	}
	if len(exceptions.GetRows()) != 1 {
		t.Fatalf("rows = %+v, want one compensation row", exceptions.GetRows())
	}
	row := exceptions.GetRows()[0]
	if row.GetKind() != string(projection.ExceptionCompensation) || row.GetReason() != domain.ReasonPOOverReceipt ||
		row.GetSku() != "WIDGET" || row.GetQty() != 4 {
		t.Fatalf("row = %+v, want a WIDGET compensation of 4", row)
	}
}

func TestNonRuleErrorsBecomeInternal(t *testing.T) {
	api := newAPI(t)
	ctx := context.Background()
	if err := api.svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	calls := map[string]func() error{
		"Receive": func() error {
			_, err := api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R2", DeliveryNote: "DN-2", PoRef: "PO-1",
				Line: line(1, "EA"), To: "RECV-01"})
			return err
		},
		"StockOnHand": func() error {
			_, err := api.StockOnHand(ctx, &nodeapi.StockOnHandRequest{})
			return err
		},
		"Exceptions": func() error {
			_, err := api.Exceptions(ctx, &nodeapi.ExceptionsRequest{})
			return err
		},
		"Transfers": func() error {
			_, err := api.Transfers(ctx, &nodeapi.TransfersRequest{})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			st, ok := status.FromError(call())
			if !ok || st.Code() != codes.Internal {
				t.Fatalf("%s: status = %v, want Internal", name, st)
			}
		})
	}
	if err := StatusError(nil); err != nil {
		t.Errorf("StatusError(nil) = %v, want nil", err)
	}
}
