// Package grpctransport exposes a warehouse node over gRPC to the operator's client.
// It is named grpctransport rather than grpc so it does not shadow
// google.golang.org/grpc at its call sites.
package grpctransport

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

// NodeAPI serves the operator-facing service from one node's local state. No method
// here waits on central: that is what keeps the warehouse working offline.
type NodeAPI struct {
	nodeapi.UnimplementedNodeAPIServer
	svc *node.Service
	now func() time.Time
}

// NewNodeAPI wraps a node service. now supplies the instant commands that care about
// dates (pick, dispatch — both check lot expiry) validate against; it is injected so
// the domain never reaches for a clock.
func NewNodeAPI(svc *node.Service, now func() time.Time) *NodeAPI {
	return &NodeAPI{svc: svc, now: now}
}

// StatusError maps a domain error onto a gRPC status. A violated node-enforced
// invariant is FailedPrecondition and names its rule, because the operator can act
// on it. Anything else is Internal.
func StatusError(err error) error {
	if err == nil {
		return nil
	}
	var rule domain.RuleError
	if errors.As(err, &rule) {
		return status.Error(codes.FailedPrecondition, rule.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// run executes a command and turns the resulting envelopes into their identities.
func (a *NodeAPI) run(cmd node.Command) (*nodeapi.CommandResponse, error) {
	envs, err := a.svc.Execute(cmd)
	if err != nil {
		return nil, StatusError(err)
	}
	ids := make([]string, 0, len(envs))
	for _, env := range envs {
		ids = append(ids, env.ID.String())
	}
	return &nodeapi.CommandResponse{EventIds: ids}, nil
}

// toLine converts one wire line into a domain line.
func toLine(l *nodeapi.Line) domain.Line {
	return domain.Line{SKU: l.GetSku(), LotID: l.GetLotId(), Qty: l.GetQty(), UoM: domain.UoM(l.GetUom())}
}

func toLines(ls []*nodeapi.Line) []domain.Line {
	out := make([]domain.Line, 0, len(ls))
	for _, l := range ls {
		out = append(out, toLine(l))
	}
	return out
}

// Receive records a supplier delivery line arriving at the dock.
func (a *NodeAPI) Receive(_ context.Context, r *nodeapi.ReceiveRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReceive(s, domain.ReceiveCmd{
			ReceiptID: r.GetReceiptId(), DeliveryNote: r.GetDeliveryNote(), PORef: r.GetPoRef(),
			Line: toLine(r.GetLine()), To: domain.LocationCode(r.GetTo())})
	})
}

// PutAway moves received goods into storage. Nothing enters or leaves the warehouse.
func (a *NodeAPI) PutAway(_ context.Context, r *nodeapi.PutAwayRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoPutAway(s, domain.PutAwayCmd{Line: toLine(r.GetLine()),
			From: domain.LocationCode(r.GetFrom()), To: domain.LocationCode(r.GetTo())})
	})
}

// Pick takes goods off a shelf for a customer order.
func (a *NodeAPI) Pick(_ context.Context, r *nodeapi.PickRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoPick(s, domain.PickCmd{Line: toLine(r.GetLine()),
			From: domain.LocationCode(r.GetFrom()), OrderRef: r.GetOrderRef(), At: a.now()})
	})
}

// Reserve places a soft hold so two orders cannot promise the same units.
func (a *NodeAPI) Reserve(_ context.Context, r *nodeapi.ReserveRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReserve(s, domain.ReserveCmd{ReservationID: r.GetReservationId(),
			Line: toLine(r.GetLine()), Location: domain.LocationCode(r.GetLocation())})
	})
}

// ReleaseReservation cancels a hold, returning its quantity to available.
func (a *NodeAPI) ReleaseReservation(_ context.Context, r *nodeapi.ReleaseReservationRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReleaseReservation(s, domain.ReleaseReservationCmd{ReservationID: r.GetReservationId()})
	})
}

// StartCount opens a physical recount of one location.
func (a *NodeAPI) StartCount(_ context.Context, r *nodeapi.StartCountRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoStartCount(s, domain.StartCountCmd{CountID: r.GetCountId(),
			Location: domain.LocationCode(r.GetLocation())})
	})
}

// CountLine records what the operator physically counted.
func (a *NodeAPI) CountLine(_ context.Context, r *nodeapi.CountLineRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoCountLine(s, domain.CountLineCmd{CountID: r.GetCountId(), Line: toLine(r.GetLine())})
	})
}

// CloseCount ends a count and books one adjustment per variance line.
func (a *NodeAPI) CloseCount(_ context.Context, r *nodeapi.CloseCountRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoCloseCount(s, domain.CloseCountCmd{CountID: r.GetCountId()})
	})
}

// DispatchTransfer is the first half of an inter-node transfer: this node's stock
// leaves now. Whether the destination exists and accepts the item is central's call,
// not this node's, so it is not checked here.
func (a *NodeAPI) DispatchTransfer(_ context.Context, r *nodeapi.DispatchTransferRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoDispatchTransfer(s, domain.DispatchTransferCmd{
			TransferID: r.GetTransferId(), FromNode: a.svc.NodeID(), ToNode: domain.NodeID(r.GetToNode()),
			From: domain.LocationCode(r.GetFrom()), Lines: toLines(r.GetLines()), At: a.now()})
	})
}

// ReceiveTransfer is the second half: the truck arrived. It requires that central has
// already forwarded the matching dispatch to this node.
func (a *NodeAPI) ReceiveTransfer(_ context.Context, r *nodeapi.ReceiveTransferRequest) (*nodeapi.CommandResponse, error) {
	return a.run(func(s *domain.State) ([]domain.Event, error) {
		return domain.DoReceiveTransfer(s, domain.ReceiveTransferCmd{TransferID: r.GetTransferId(),
			To: domain.LocationCode(r.GetTo()), Lines: toLines(r.GetLines())})
	})
}

// StockOnHand answers the balance query. Empty fields mean "no filter".
func (a *NodeAPI) StockOnHand(_ context.Context, r *nodeapi.StockOnHandRequest) (*nodeapi.StockOnHandResponse, error) {
	rows, err := a.svc.StockOnHand(r.GetSku(), domain.LocationCode(r.GetLocation()))
	if err != nil {
		return nil, StatusError(err)
	}
	out := make([]*nodeapi.StockRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, &nodeapi.StockRow{Sku: row.SKU, Location: string(row.Location), LotId: row.LotID,
			Qty: row.Qty, Available: row.Available})
	}
	return &nodeapi.StockOnHandResponse{Rows: out}, nil
}

// Exceptions answers "what did central reverse, and why". It is the operator's only
// window onto compensation, including balances a compensation drove negative.
func (a *NodeAPI) Exceptions(_ context.Context, _ *nodeapi.ExceptionsRequest) (*nodeapi.ExceptionsResponse, error) {
	rows, err := a.svc.Exceptions()
	if err != nil {
		return nil, StatusError(err)
	}
	out := make([]*nodeapi.ExceptionRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, &nodeapi.ExceptionRow{Id: row.ID, Kind: string(row.Kind), Reason: row.Reason,
			CausedBy: row.CausedBy, Sku: row.Key.SKU, Location: string(row.Key.Location), LotId: row.Key.LotID,
			Qty: row.Qty, RecordedAt: timestamppb.New(row.RecordedAt.UTC()), Resolved: row.Resolved})
	}
	return &nodeapi.ExceptionsResponse{Rows: out}, nil
}

// Transfers answers this node's view of every inter-node transfer.
func (a *NodeAPI) Transfers(_ context.Context, _ *nodeapi.TransfersRequest) (*nodeapi.TransfersResponse, error) {
	rows, err := a.svc.Transfers()
	if err != nil {
		return nil, StatusError(err)
	}
	out := make([]*nodeapi.TransferRow, 0, len(rows))
	for _, row := range rows {
		var dispatchedAt *timestamppb.Timestamp
		if !row.DispatchedAt.IsZero() {
			dispatchedAt = timestamppb.New(row.DispatchedAt.UTC())
		}
		out = append(out, &nodeapi.TransferRow{Id: row.ID, FromNode: string(row.FromNode), ToNode: string(row.ToNode),
			Dispatched: row.Dispatched, Received: row.Received, Status: string(row.Status), DispatchedAt: dispatchedAt})
	}
	return &nodeapi.TransfersResponse{Rows: out}, nil
}
