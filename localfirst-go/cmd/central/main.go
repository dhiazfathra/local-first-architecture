// Command central holds every node's records in Postgres and reports the global
// sum. It is a peer with a bigger disk, not a coordinator.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var cfg Config
	flag.StringVar(&cfg.DSN, "dsn", os.Getenv("LOCALFIRST_PG_DSN"), "Postgres DSN")
	flag.StringVar(&cfg.Listen, "listen", ":9090", "sync listen address")
	flag.StringVar(&cfg.NodeID, "id", "central", "central's peer id")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, cfg); err != nil {
		slog.Error("central stopped", "err", err)
		os.Exit(1)
	}
}
