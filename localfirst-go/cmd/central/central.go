package main

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/central"
	lfsync "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
)

// Config is everything central needs. Note what is missing: no owned locations
// and no domain commands. Central only ever receives records.
type Config struct {
	DSN    string
	Listen string
	NodeID string
}

// Run brings up central: Postgres-backed log plus the same Sync service a node
// serves. That is the entire difference between the two binaries.
func Run(ctx context.Context, cfg Config) error {
	store, err := central.OpenPG(ctx, cfg.DSN, cfg.NodeID)
	if err != nil {
		return err
	}
	defer store.Close()

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("central: listen %s: %w", cfg.Listen, err)
	}
	srv := grpc.NewServer()
	syncpb.RegisterSyncServer(srv, lfsync.NewServer(store, nil))

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	select {
	case <-ctx.Done():
		srv.GracefulStop()
		return nil
	case err := <-errc:
		return err
	}
}
