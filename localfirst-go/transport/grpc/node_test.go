package grpc_test

import (
	"context"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	transport "github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

func service(t *testing.T) *transport.NodeService {
	t.Helper()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	inv, err := domain.NewInventory(context.Background(), store, clock.New("n1", nil), []string{"A"})
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	return transport.NewNodeService(inv)
}

func TestNodeServiceHappyPath(t *testing.T) {
	ctx, svc := context.Background(), service(t)
	if _, err := svc.Receive(ctx, &nodepb.ReceiveRequest{Sku: "S", Location: "A", Qty: 10}); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if _, err := svc.Issue(ctx, &nodepb.IssueRequest{Sku: "S", Location: "A", Qty: 3}); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := svc.Move(ctx, &nodepb.MoveRequest{Sku: "S", From: "A", To: "REMOTE", Qty: 2}); err != nil {
		t.Fatalf("Move: %v", err)
	}
	got, err := svc.Balance(ctx, &nodepb.BalanceRequest{Sku: "S", Location: "A"})
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if got.GetQty() != 5 {
		t.Fatalf("Balance = %d, want 5", got.GetQty())
	}
	all, err := svc.Balances(ctx, &nodepb.Empty{})
	if err != nil {
		t.Fatalf("Balances: %v", err)
	}
	if len(all.GetEntries()) != 2 {
		t.Fatalf("Balances returned %d entries, want 2", len(all.GetEntries()))
	}
}

func TestNodeServiceErrorMapping(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(svc *transport.NodeService) error
		want codes.Code
	}{
		{
			name: "negative balance is a failed precondition",
			call: func(s *transport.NodeService) error {
				_, err := s.Issue(ctx, &nodepb.IssueRequest{Sku: "S", Location: "A", Qty: 1})
				return err
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "unowned location is permission denied",
			call: func(s *transport.NodeService) error {
				_, err := s.Receive(ctx, &nodepb.ReceiveRequest{Sku: "S", Location: "X", Qty: 1})
				return err
			},
			want: codes.PermissionDenied,
		},
		{
			name: "bad quantity is invalid argument",
			call: func(s *transport.NodeService) error {
				_, err := s.Receive(ctx, &nodepb.ReceiveRequest{Sku: "S", Location: "A", Qty: 0})
				return err
			},
			want: codes.InvalidArgument,
		},
		{
			name: "unowned move source is permission denied",
			call: func(s *transport.NodeService) error {
				_, err := s.Move(ctx, &nodepb.MoveRequest{Sku: "S", From: "X", To: "A", Qty: 1})
				return err
			},
			want: codes.PermissionDenied,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(service(t))
			if got := status.Code(err); got != tc.want {
				t.Fatalf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}

func TestNodeServiceMapsUnexpectedErrorsToInternal(t *testing.T) {
	// A closed store makes Append fail for a reason the domain does not name.
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "n1.db"), "n1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inv, err := domain.NewInventory(context.Background(), store, clock.New("n1", nil), []string{"A"})
	if err != nil {
		t.Fatalf("NewInventory: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = transport.NewNodeService(inv).Receive(context.Background(),
		&nodepb.ReceiveRequest{Sku: "S", Location: "A", Qty: 1})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("code = %v (err %v), want Internal", got, err)
	}
}
