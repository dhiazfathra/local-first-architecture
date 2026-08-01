// Command central runs the arbitration server. It serves the replication stream over
// Postgres, seeds the reference data only central owns, replicates the item master
// down to every node, and reports transfers stuck in transit.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/arbiter"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/central"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	syncrepl "github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/sync"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// Order is one purchase-order line: how much of a SKU was ordered. Purchase-order
// lifecycle is out of scope, so an order is only ever this stub.
type Order struct {
	PORef string  `json:"po_ref"`
	SKU   string  `json:"sku"`
	Qty   float64 `json:"qty"`
}

// NodeSpec declares a warehouse and the SKUs it refuses to stock. Both are things no
// node has authority over, which is why a transfer's destination is central's call.
type NodeSpec struct {
	ID      domain.NodeID `json:"id"`
	Rejects []string      `json:"rejects,omitempty"`
}

// Bootstrap is the reference data central is configured with.
type Bootstrap struct {
	Items  []domain.Item `json:"items"`
	Orders []Order       `json:"orders"`
	Nodes  []NodeSpec    `json:"nodes"`
}

// osExit is os.Exit, swappable in tests so main can run to completion in-process
// instead of terminating the test binary.
var osExit = os.Exit

func main() {
	if err := realMain(); err != nil {
		fmt.Fprintf(os.Stderr, "central: %v\n", err)
		osExit(1)
	}
}

// realMain parses flags, loads the bootstrap, opens Postgres, and calls run.
func realMain() error {
	fs := flag.NewFlagSet("central", flag.ExitOnError)
	dsn := fs.String("dsn", "", "Postgres connection string")
	listen := fs.String("listen", ":9090", "address to serve the sync stream on")
	bootstrap := fs.String("bootstrap", "", "path to a JSON file of items, orders and nodes")
	window := fs.Duration("transit-window", 48*time.Hour,
		"how long a transfer may be in transit before it is reported as a discrepancy")
	every := fs.Duration("report-every", time.Hour, "interval between discrepancy reports")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	ctx := context.Background()
	b, err := loadBootstrap(*bootstrap)
	if err != nil {
		return err
	}
	store, err := central.OpenPostgres(ctx, *dsn)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listen, err)
	}
	return run(ctx, store, b, *window, *every, time.Now, lis)
}

// loadBootstrap reads the configuration file. An empty path means no reference data,
// which is valid: a fresh central can be seeded later.
func loadBootstrap(path string) (Bootstrap, error) {
	if path == "" {
		return Bootstrap{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Bootstrap{}, fmt.Errorf("read bootstrap %s: %w", path, err)
	}
	var b Bootstrap
	if err := json.Unmarshal(raw, &b); err != nil {
		return Bootstrap{}, fmt.Errorf("parse bootstrap %s: %w", path, err)
	}
	return b, nil
}

// applyBootstrap upserts the reference data and queues one ItemUpserted per item to
// every declared node. That queue is how the item master gets replicated read-only
// down to the warehouses: nodes never emit Item events, they only apply them.
func applyBootstrap(ctx context.Context, s central.Store, b Bootstrap, now time.Time) error {
	for _, spec := range b.Nodes {
		if err := s.RegisterNode(ctx, spec.ID, spec.Rejects); err != nil {
			return err
		}
	}
	for _, order := range b.Orders {
		if err := s.UpsertPurchaseOrder(ctx, order.PORef, order.SKU, order.Qty); err != nil {
			return err
		}
	}
	for _, item := range b.Items {
		if err := s.UpsertItem(ctx, item); err != nil {
			return err
		}
		envs, err := s.EmitCentral(ctx, []domain.Event{{Type: domain.TypeItemUpserted,
			AggregateID: item.SKU, Payload: domain.ItemUpserted{Item: item}}}, nil, now)
		if err != nil {
			return err
		}
		for _, spec := range b.Nodes {
			if err := s.Enqueue(ctx, spec.ID, envs); err != nil {
				return err
			}
		}
	}
	return nil
}

// reportDiscrepancies returns transfers dispatched longer than window ago that are
// still carrying stock. It only ever reports: compensating a late truck would invent
// stock at the source that may be sitting in a lay-by. A human resolves it.
func reportDiscrepancies(ctx context.Context, s central.Store, window time.Duration,
	now func() time.Time) ([]central.InTransitRow, error) {
	return arbiter.Discrepancies(ctx, s, now().Add(-window))
}

// run seeds the store, starts the discrepancy report loop, and serves the sync stream
// until ctx is done.
func run(ctx context.Context, s central.Store, b Bootstrap, window, every time.Duration,
	now func() time.Time, lis net.Listener) error {
	if err := applyBootstrap(ctx, s, b, now()); err != nil {
		return err
	}

	go reportLoop(ctx, s, window, every, now)

	srv := grpc.NewServer()
	syncpb.RegisterSyncServer(srv, syncrepl.NewServer(s, arbiter.New(s, now)))
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve sync: %w", err)
	}
	return nil
}

// reportLoop logs stuck transfers on an interval. It is deliberately toothless.
func reportLoop(ctx context.Context, s central.Store, window, every time.Duration, now func() time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		rows, err := reportDiscrepancies(ctx, s, window, now)
		if err != nil {
			log.Printf("central: discrepancy report failed: %v", err)
			continue
		}
		for _, row := range rows {
			log.Printf("central: transfer %s from %s to %s has %v of %s in transit since %s",
				row.TransferID, row.FromNode, row.ToNode,
				row.Dispatched-row.Received, row.Key.SKU, row.DispatchedAt)
		}
	}
}
