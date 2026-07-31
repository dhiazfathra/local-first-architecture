package sync

import (
	"context"
	"errors"
	"fmt"
	"io"

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
	sendErr := s.Sync_ReplicateClient.CloseSend()
	// Half-closing the send side does not mean the server has received or
	// applied anything already in flight -- over a real connection those
	// frames can still be buffered. By protocol the server ends the session
	// right after our Ack, so one more Recv either confirms that (io.EOF) or
	// surfaces why it didn't. Only after that is it safe to tear down the
	// conn: closing it first can race the server's in-flight merge and
	// silently drop it.
	var drainErr error
	if _, recvErr := s.Recv(); recvErr != nil && !errors.Is(recvErr, io.EOF) {
		drainErr = recvErr
	}
	closeErr := s.conn.Close()
	if err := errors.Join(sendErr, drainErr, closeErr); err != nil {
		return fmt.Errorf("grpc close send: %w", err)
	}
	return nil
}
