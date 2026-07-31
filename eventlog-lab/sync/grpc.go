package sync

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync/syncpb"
)

// Register attaches srv to a gRPC server.
func Register(s *grpc.Server, srv *Server) { syncpb.RegisterSyncServer(s, srv) }

// grpcDialer dials real gRPC peers.
type grpcDialer struct{ opts []grpc.DialOption }

// NewGRPCDialer returns a Dialer backed by gRPC.
func NewGRPCDialer(opts ...grpc.DialOption) Dialer { return &grpcDialer{opts: opts} }

func (d *grpcDialer) Dial(ctx context.Context, addr string) (Stream, error) {
	conn, err := grpc.NewClient(addr, d.opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %q: %w", addr, err)
	}
	st, err := syncpb.NewSyncClient(conn).Replicate(ctx)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("grpc replicate %q: %w", addr, err)
	}
	return &grpcStream{Sync_ReplicateClient: st, conn: conn}, nil
}

// grpcStream closes the connection when the client stops sending.
type grpcStream struct {
	syncpb.Sync_ReplicateClient
	conn *grpc.ClientConn
}

func (s *grpcStream) CloseSend() error {
	err := s.Sync_ReplicateClient.CloseSend()
	if cerr := s.conn.Close(); err == nil && cerr != nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("grpc close send: %w", err)
	}
	return nil
}
