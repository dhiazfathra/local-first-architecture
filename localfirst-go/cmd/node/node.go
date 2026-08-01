package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/clock"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/domain"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/eventlog"
	lfsync "github.com/dhiazfathra/local-first-architecture/localfirst-go/sync"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/sync/syncpb"
	transport "github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

// Config is everything a node needs. Locations are the ones this node
// exclusively owns; nothing else may write to them.
type Config struct {
	NodeID      string
	DBPath      string
	Listen      string
	CentralAddr string // empty means "run standalone"
	Locations   []string
	SyncEvery   time.Duration
}

// Run brings up one node: local log, projected balances, the client API, the
// Sync service (so any peer can replicate from it) and a background sync client.
// It returns when ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	store, err := eventlog.Open(cfg.DBPath, cfg.NodeID)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	inv, err := domain.NewInventory(ctx, store, clock.New(cfg.NodeID, nil), cfg.Locations)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("node: listen %s: %w", cfg.Listen, err)
	}
	srv := grpc.NewServer()
	nodepb.RegisterNodeServer(srv, transport.NewNodeService(inv))
	// inv, NOT nil: records merged from peers must reach this node's in-memory
	// projection. A Moved authored by another node whose destination is one of
	// OUR locations increments our balance, and without a projector here that
	// increment stays invisible until the process restarts and replays the log.
	syncpb.RegisterSyncServer(srv, lfsync.NewServer(store, inv))

	if cfg.CentralAddr != "" {
		cc, err := grpc.NewClient(cfg.CentralAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("node: dial central: %w", err)
		}
		defer func() { _ = cc.Close() }()
		go lfsync.NewClient(cc, store, inv, "central").Run(ctx, cfg.SyncEvery)
	}

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
