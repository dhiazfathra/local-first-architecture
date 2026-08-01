// Command node runs one local-first inventory node: it owns some locations,
// accepts commands with no network, and reconciles with central in the
// background.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	var cfg Config
	var locations string
	flag.StringVar(&cfg.NodeID, "id", "node-1", "this node's id; also authors every record")
	flag.StringVar(&cfg.DBPath, "db", "node.db", "path to this node's SQLite log")
	flag.StringVar(&cfg.Listen, "listen", ":8080", "client + sync listen address")
	flag.StringVar(&cfg.CentralAddr, "central", "", "central address; empty to run standalone")
	flag.StringVar(&locations, "locations", "", "comma-separated locations this node owns")
	flag.DurationVar(&cfg.SyncEvery, "sync-every", 2*time.Second, "sync interval")
	flag.Parse()
	if locations != "" {
		cfg.Locations = strings.Split(locations, ",")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, cfg); err != nil {
		slog.Error("node stopped", "err", err)
		os.Exit(1)
	}
}
