// Command node runs one warehouse. It serves the operator gRPC API from a local
// SQLite event log and, when a central address is configured, replicates to central in
// the background. With no central address it is a complete working warehouse: no
// command ever waits on the network.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/node"
	syncrepl "github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/sync"
	grpctransport "github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/transport/grpc"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/syncpb"
)

// Config is everything this binary needs. Central may be empty, which means fully
// offline operation.
type Config struct {
	DB        string
	ID        domain.NodeID
	Central   string
	SyncEvery time.Duration
	Backoff   time.Duration
	Locations map[domain.LocationCode]domain.LocationType
}

// osExit is os.Exit, swappable in tests so main can run to completion
// in-process instead of terminating the test binary.
var osExit = os.Exit

func main() {
	if err := realMain(); err != nil {
		fmt.Fprintf(os.Stderr, "node: %v\n", err)
		osExit(1)
	}
}

// realMain parses flags, builds the listener, and calls run.
func realMain() error {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	var cfg Config
	var id, locations string
	fs.StringVar(&cfg.DB, "db", "node.db", "path to this node's SQLite event log")
	fs.StringVar(&id, "id", "", "this node's identity, e.g. wh-a")
	fs.StringVar(&cfg.Central, "central", "", "central's address; empty means run offline")
	fs.DurationVar(&cfg.SyncEvery, "sync-every", 5*time.Second, "interval between sync sessions")
	fs.DurationVar(&cfg.Backoff, "backoff", 30*time.Second, "wait after a failed sync session")
	fs.StringVar(&locations, "locations", "",
		"comma-separated code:type pairs to register on startup, e.g. RECV-01:receiving,PICK-01:pick")
	listen := fs.String("listen", ":8080", "address to serve the operator API on")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	if id == "" {
		return fmt.Errorf("-id is required")
	}
	cfg.ID = domain.NodeID(id)
	parsed, err := parseLocations(locations)
	if err != nil {
		return err
	}
	cfg.Locations = parsed

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listen, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, cfg, lis)
}

// parseLocations turns the -locations flag into the map run expects.
func parseLocations(spec string) (map[domain.LocationCode]domain.LocationType, error) {
	out := map[domain.LocationCode]domain.LocationType{}
	if spec == "" {
		return out, nil
	}
	for _, pair := range strings.Split(spec, ",") {
		code, typ, ok := strings.Cut(pair, ":")
		if !ok {
			return nil, fmt.Errorf("location %q is not code:type", pair)
		}
		out[domain.LocationCode(code)] = domain.LocationType(typ)
	}
	return out, nil
}

// run opens the node, registers its locations, starts the sync client if central is
// configured, and serves the operator API until ctx is done.
func run(ctx context.Context, cfg Config, lis net.Listener) error {
	svc, err := node.Open(cfg.DB, cfg.ID, time.Now)
	if err != nil {
		return err
	}
	defer func() { _ = svc.Close() }()

	for code, typ := range cfg.Locations {
		if err := svc.RegisterLocation(code, typ); err != nil {
			return err
		}
	}

	if cfg.Central != "" {
		conn, err := grpc.NewClient(cfg.Central, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial central at %s: %w", cfg.Central, err)
		}
		defer func() { _ = conn.Close() }()
		client := syncrepl.NewClient(svc, syncpb.NewSyncClient(conn))
		// The sync loop is deliberately fire-and-forget. Central being unreachable is
		// a normal state for a warehouse, not an error an operator should ever see.
		go func() { _ = client.Run(ctx, cfg.SyncEvery, cfg.Backoff) }()
	}

	srv := grpc.NewServer()
	nodeapi.RegisterNodeAPIServer(srv, grpctransport.NewNodeAPI(svc, time.Now))
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve operator api: %w", err)
	}
	return nil
}
