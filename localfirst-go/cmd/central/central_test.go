package main

import (
	"context"
	"net"
	"os"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("LOCALFIRST_PG_DSN")
	if v == "" {
		t.Fatal("LOCALFIRST_PG_DSN is unset — run `make test`")
	}
	return v
}

func TestRunStartsAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := Config{DSN: dsn(t), Listen: freeAddr(t), NodeID: "central"}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not shut down")
	}
}

func TestRunFailsOnABadDSN(t *testing.T) {
	if err := Run(context.Background(), Config{
		DSN: "postgres://nobody@127.0.0.1:1/none", Listen: freeAddr(t), NodeID: "central",
	}); err == nil {
		t.Fatal("Run must fail when Postgres is unreachable")
	}
}

func TestRunFailsOnABadListenAddress(t *testing.T) {
	if err := Run(context.Background(), Config{
		DSN: dsn(t), Listen: "256.256.256.256:1", NodeID: "central",
	}); err == nil {
		t.Fatal("Run must fail when it cannot listen")
	}
}
