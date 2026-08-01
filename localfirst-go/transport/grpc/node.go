// Package grpc adapts the node's client API to gRPC. It is deliberately thin:
// decode the request, call one domain command, map the error. The domain has no
// dependency on this package — the arrow points one way only.
package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

// NodeService serves the client API from one node's local state.
type NodeService struct {
	nodepb.UnimplementedNodeServer
	inv *domain.Inventory
}

// NewNodeService wraps an inventory aggregate.
func NewNodeService(inv *domain.Inventory) *NodeService { return &NodeService{inv: inv} }

// Receive adds stock.
func (s *NodeService) Receive(ctx context.Context, req *nodepb.ReceiveRequest) (*nodepb.Empty, error) {
	return empty(s.inv.Receive(ctx, req.GetSku(), req.GetLocation(), req.GetQty()))
}

// Issue removes stock.
func (s *NodeService) Issue(ctx context.Context, req *nodepb.IssueRequest) (*nodepb.Empty, error) {
	return empty(s.inv.Issue(ctx, req.GetSku(), req.GetLocation(), req.GetQty()))
}

// Move transfers stock, possibly to a location owned by another node.
func (s *NodeService) Move(ctx context.Context, req *nodepb.MoveRequest) (*nodepb.Empty, error) {
	return empty(s.inv.Move(ctx, req.GetSku(), req.GetFrom(), req.GetTo(), req.GetQty()))
}

// Balance reports one projected balance.
func (s *NodeService) Balance(_ context.Context, req *nodepb.BalanceRequest) (*nodepb.BalanceResponse, error) {
	return &nodepb.BalanceResponse{Qty: s.inv.Balance(req.GetSku(), req.GetLocation())}, nil
}

// Balances reports every projected balance this node holds.
func (s *NodeService) Balances(context.Context, *nodepb.Empty) (*nodepb.BalancesResponse, error) {
	state := s.inv.Balances()
	out := &nodepb.BalancesResponse{Entries: make([]*nodepb.BalanceEntry, 0, len(state))}
	for k, qty := range state {
		out.Entries = append(out.Entries, &nodepb.BalanceEntry{Sku: k.SKU, Location: k.Location, Qty: qty})
	}
	return out, nil
}

func empty(err error) (*nodepb.Empty, error) {
	if err != nil {
		return nil, toStatus(err)
	}
	return &nodepb.Empty{}, nil
}

// toStatus maps domain errors to gRPC codes. Anything unrecognised is Internal:
// a transport must never turn an unknown failure into a success.
func toStatus(err error) error {
	switch {
	case errors.Is(err, domain.ErrNegativeBalance):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrNotOwned):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, domain.ErrBadQty):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
