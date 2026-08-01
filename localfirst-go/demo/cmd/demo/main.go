// Command demo runs the seven-step local-first demonstration against the
// containers from docker-compose.yml. Every step, and every number it prints,
// comes from package demo — the same code the integration test asserts on.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/demo"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "demo: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\ndone. read docs/limitations.md for what this deliberately does not do.")
}

func run() error {
	network := flag.String("network", "localfirst-go_lfnet", "compose network to partition")
	dsn := flag.String("dsn", "postgres://localfirst:localfirst@127.0.0.1:5433/localfirst?sslmode=disable", "central's Postgres DSN")
	settle := flag.Duration("settle", 4*time.Second, "how long to let background sync run; must exceed the nodes' -sync-every")
	ready := flag.Duration("ready-timeout", 90*time.Second, "how long to wait for the containers to boot")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	env, cleanup, err := demo.NewComposeEnv(ctx, demo.ComposeConfig{
		Network: *network,
		DSN:     *dsn,
		Settle:  *settle,
		Nodes:   demo.DefaultNodes(),
	})
	if err != nil {
		return err
	}
	defer cleanup()

	fmt.Println("waiting for the containers…")
	if err := env.WaitReady(ctx, *ready); err != nil {
		return err
	}

	if _, err := demo.Run(ctx, env, os.Stdout); err != nil {
		return fmt.Errorf("demo FAILED: %w", err)
	}
	return nil
}
