package integration

import (
	"context"
	"testing"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

// TestEveryScenarioReplaysIdentically runs each scenario's command sequence, then
// replays the resulting log onto empty projections and asserts the read models come
// out identical, for both nodes and for central's log too.
//
// This is what licenses two mechanisms the rest of the design leans on: the
// projection_version rebuild, which empties the tables and replays everything, and
// idempotent sync, which can deliver the same event twice in a different order.
func TestEveryScenarioReplaysIdentically(t *testing.T) {
	scenarios := []struct {
		name    string
		ordered float64
		rejects map[domain.NodeID][]string
		run     func(t *testing.T, c *cluster)
	}{
		{
			name:    "shared purchase order over-received",
			ordered: 100,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				if _, err := c.node("wh-a").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RA",
					DeliveryNote: "DN-A", PoRef: "PO-1", Line: line(80), To: "RECV-wh-a"}); err != nil {
					t.Fatalf("Receive: %v", err)
				}
				if _, err := c.node("wh-b").api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RB",
					DeliveryNote: "DN-B", PoRef: "PO-1", Line: line(40), To: "RECV-wh-b"}); err != nil {
					t.Fatalf("Receive: %v", err)
				}
				c.syncNode("wh-a")
				c.syncNode("wh-b")
			},
		},
		{
			name:    "duplicate delivery note",
			ordered: 1000,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				for _, spec := range []struct {
					id      domain.NodeID
					receipt string
				}{{"wh-a", "RA"}, {"wh-b", "RB"}} {
					if _, err := c.node(spec.id).api.Receive(ctx, &nodeapi.ReceiveRequest{
						ReceiptId: spec.receipt, DeliveryNote: "DN-DUPE", PoRef: "PO-1",
						Line: line(12), To: "RECV-" + string(spec.id)}); err != nil {
						t.Fatalf("Receive: %v", err)
					}
				}
				c.syncNode("wh-a")
				c.syncNode("wh-b")
			},
		},
		{
			name:    "transfer happy path",
			ordered: 1000,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				stockAt(t, c, "wh-a", "R1", "DN-1", 10)
				c.syncAll()
				if _, err := c.node("wh-a").api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{
					TransferId: "T1", ToNode: "wh-b", From: "PICK-wh-a",
					Lines: []*nodeapi.Line{line(6)}}); err != nil {
					t.Fatalf("DispatchTransfer: %v", err)
				}
				c.syncAll()
				if _, err := c.node("wh-b").api.ReceiveTransfer(ctx, &nodeapi.ReceiveTransferRequest{
					TransferId: "T1", To: "RECV-wh-b", Lines: []*nodeapi.Line{line(6)}}); err != nil {
					t.Fatalf("ReceiveTransfer: %v", err)
				}
				c.syncAll()
			},
		},
		{
			name:    "transfer to a refusing destination",
			ordered: 1000,
			rejects: map[domain.NodeID][]string{"wh-b": {"WIDGET"}},
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				stockAt(t, c, "wh-a", "R1", "DN-1", 10)
				c.syncAll()
				if _, err := c.node("wh-a").api.DispatchTransfer(ctx, &nodeapi.DispatchTransferRequest{
					TransferId: "T1", ToNode: "wh-b", From: "PICK-wh-a",
					Lines: []*nodeapi.Line{line(6)}}); err != nil {
					t.Fatalf("DispatchTransfer: %v", err)
				}
				c.syncAll()
			},
		},
		{
			name:    "reservations taken and released",
			ordered: 1000,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				stockAt(t, c, "wh-a", "R1", "DN-1", 10)
				api := c.node("wh-a").api
				if _, err := api.Reserve(ctx, &nodeapi.ReserveRequest{ReservationId: "RS1",
					Line: line(4), Location: "PICK-wh-a"}); err != nil {
					t.Fatalf("Reserve: %v", err)
				}
				if _, err := api.Reserve(ctx, &nodeapi.ReserveRequest{ReservationId: "RS2",
					Line: line(2), Location: "PICK-wh-a"}); err != nil {
					t.Fatalf("Reserve: %v", err)
				}
				if _, err := api.ReleaseReservation(ctx,
					&nodeapi.ReleaseReservationRequest{ReservationId: "RS1"}); err != nil {
					t.Fatalf("ReleaseReservation: %v", err)
				}
				c.syncAll()
			},
		},
		{
			name:    "count variance booked",
			ordered: 1000,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				stockAt(t, c, "wh-a", "R1", "DN-1", 10)
				api := c.node("wh-a").api
				if _, err := api.StartCount(ctx, &nodeapi.StartCountRequest{CountId: "C1",
					Location: "PICK-wh-a"}); err != nil {
					t.Fatalf("StartCount: %v", err)
				}
				if _, err := api.CountLine(ctx, &nodeapi.CountLineRequest{CountId: "C1",
					Line: line(7)}); err != nil {
					t.Fatalf("CountLine: %v", err)
				}
				if _, err := api.CloseCount(ctx, &nodeapi.CloseCountRequest{CountId: "C1"}); err != nil {
					t.Fatalf("CloseCount: %v", err)
				}
				c.syncAll()
			},
		},
		{
			name:    "compensation drives a location negative",
			ordered: 6,
			run: func(t *testing.T, c *cluster) {
				ctx := context.Background()
				api := c.node("wh-a").api
				if _, err := api.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "RA",
					DeliveryNote: "DN-A", PoRef: "PO-1", Line: line(10), To: "RECV-wh-a"}); err != nil {
					t.Fatalf("Receive: %v", err)
				}
				if _, err := api.Pick(ctx, &nodeapi.PickRequest{Line: line(10), From: "RECV-wh-a",
					OrderRef: "SO-1"}); err != nil {
					t.Fatalf("Pick: %v", err)
				}
				c.syncAll()
			},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			c := newCluster(t, sc.ordered, sc.rejects)
			sc.run(t, c)
			for _, id := range []domain.NodeID{"wh-a", "wh-b"} {
				assertDeterministic(t, c.node(id))
			}
			assertCentralBalancesSumToZero(t, c)
		})
	}
}

// assertCentralBalancesSumToZero folds central's whole log through the same movement
// arithmetic the nodes use. Because every movement carries an explicit from and to,
// with external as the sentinel for the outside world, the sum of every balance must
// be exactly zero. A non-zero sum means stock was invented or destroyed somewhere.
func assertCentralBalancesSumToZero(t *testing.T, c *cluster) {
	t.Helper()
	envs, err := c.store.Events(context.Background())
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	balances := map[domain.StockKey]float64{}
	for _, env := range envs {
		payload, err := domain.DecodePayload(env)
		if err != nil {
			t.Fatalf("DecodePayload(%s): %v", env.ID, err)
		}
		for _, m := range movementsOfEnvelope(payload) {
			balances[m.FromKey()] -= m.Qty
			balances[m.ToKey()] += m.Qty
		}
	}
	var total float64
	for _, qty := range balances {
		total += qty
	}
	if total != 0 {
		t.Errorf("central balances sum to %v, want 0: every movement is a balanced pair", total)
	}
}

// movementsOfEnvelope mirrors projection.MovementsOf for the payload types central
// sees. It is duplicated here rather than exported because the projection package's
// version is an internal detail of the read models.
func movementsOfEnvelope(payload any) []domain.Movement {
	switch p := payload.(type) {
	case domain.GoodsReceived:
		return []domain.Movement{p.Move}
	case domain.PutAway:
		return []domain.Movement{p.Move}
	case domain.Picked:
		return []domain.Movement{p.Move}
	case domain.StockAdjusted:
		return []domain.Movement{p.Move}
	case domain.TransferDispatched:
		return p.Lines
	case domain.TransferReceived:
		return p.Lines
	default:
		return nil
	}
}
