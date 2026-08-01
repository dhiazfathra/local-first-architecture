package demo

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dhiazfathra/local-first-architecture/localfirst-go/central"
	"github.com/dhiazfathra/local-first-architecture/localfirst-go/transport/grpc/nodepb"
)

// Exec runs an external command. ComposeEnv uses it only for
// `docker network (dis)connect`, which is the one thing it cannot do over gRPC —
// and injecting it is what lets the whole environment be tested without Docker.
type Exec func(ctx context.Context, name string, args ...string) error

func defaultExec(ctx context.Context, name string, args ...string) error {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("demo: %s %v: %w: %s", name, args, err, out)
	}
	return nil
}

// NodeAddr is one node as docker-compose.yml declares it: its logical identity,
// the location it owns, its container name (for the network commands) and its
// published address (for gRPC).
type NodeAddr struct {
	ID        string
	Location  string
	Container string
	Addr      string
}

// DefaultNodes is the three nodes from docker-compose.yml. It is a function, not
// a var, so a caller cannot accidentally mutate the shared slice.
func DefaultNodes() []NodeAddr {
	return []NodeAddr{
		{ID: "node-1", Location: "A", Container: "lf-node-1", Addr: "127.0.0.1:8081"},
		{ID: "node-2", Location: "B", Container: "lf-node-2", Addr: "127.0.0.1:8082"},
		{ID: "node-3", Location: "C", Container: "lf-node-3", Addr: "127.0.0.1:8083"},
	}
}

// ComposeConfig configures a ComposeEnv. Settle is how long to wait for the
// nodes' background sync loops to run; it must be comfortably longer than
// cmd/node's -sync-every.
type ComposeConfig struct {
	Network string
	DSN     string
	Settle  time.Duration
	Exec    Exec
	Nodes   []NodeAddr
}

// ComposeEnv drives real containers: commands go over gRPC to the node API, the
// global sum is read straight out of central's Postgres, and the partition is a
// real `docker network disconnect`.
type ComposeEnv struct {
	nodes   []Node
	pg      *central.PGStore
	network string
	run     Exec
	byID    map[string]NodeAddr
}

// NewComposeEnv dials every node and central. The returned func releases both.
func NewComposeEnv(ctx context.Context, cfg ComposeConfig) (*ComposeEnv, func(), error) {
	if cfg.Exec == nil {
		cfg.Exec = defaultExec
	}
	pg, err := central.OpenPG(ctx, cfg.DSN, "demo-reader")
	if err != nil {
		return nil, nil, fmt.Errorf("demo: open central: %w", err)
	}

	env := &ComposeEnv{
		pg:      pg,
		network: cfg.Network,
		run:     cfg.Exec,
		byID:    make(map[string]NodeAddr, len(cfg.Nodes)),
	}
	conns := make([]*grpc.ClientConn, 0, len(cfg.Nodes))
	cleanup := func() {
		for _, cc := range conns {
			_ = cc.Close()
		}
		pg.Close()
	}

	for _, n := range cfg.Nodes {
		cc, err := grpc.NewClient(n.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("demo: dial %s at %s: %w", n.ID, n.Addr, err)
		}
		conns = append(conns, cc)
		env.byID[n.ID] = n
		env.nodes = append(env.nodes, &grpcNode{
			id: n.ID, loc: n.Location,
			client: nodepb.NewNodeClient(cc),
			settle: cfg.Settle,
		})
	}
	return env, cleanup, nil
}

// Nodes returns the nodes in declaration order, which is the order demo.Seed
// applies to.
func (e *ComposeEnv) Nodes() []Node { return e.nodes }

// Central reads the global sum directly from Postgres. Central exposes no query
// RPC, and does not need one: the sum is a property of the records it already
// holds.
func (e *ComposeEnv) Central() Central { return e.pg }

// Disconnect performs a genuine network partition. This is the line the whole
// demo exists for.
func (e *ComposeEnv) Disconnect(ctx context.Context, nodeID string) error {
	return e.networkCmd(ctx, "disconnect", nodeID)
}

// Reconnect undoes it.
func (e *ComposeEnv) Reconnect(ctx context.Context, nodeID string) error {
	return e.networkCmd(ctx, "connect", nodeID)
}

func (e *ComposeEnv) networkCmd(ctx context.Context, verb, nodeID string) error {
	n, ok := e.byID[nodeID]
	if !ok {
		return fmt.Errorf("demo: unknown node %q", nodeID)
	}
	return e.run(ctx, "docker", "network", verb, e.network, n.Container)
}

// WaitReady blocks until every node answers a Balances call, or timeout
// elapses. Containers take a moment to boot, and a demo that races them prints
// a confusing failure instead of a story.
func (e *ComposeEnv) WaitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, n := range e.nodes {
		gn, ok := n.(*grpcNode)
		if !ok {
			continue
		}
		for {
			attempt, cancel := context.WithDeadline(ctx, deadline)
			_, err := gn.client.Balances(attempt, &nodepb.Empty{})
			cancel()
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("demo: %s never became ready: %w", gn.id, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	return nil
}

// grpcNode is one containerised node, reached over its client API.
type grpcNode struct {
	id     string
	loc    string
	client nodepb.NodeClient
	settle time.Duration
}

func (n *grpcNode) ID() string       { return n.id }
func (n *grpcNode) Location() string { return n.loc }

func (n *grpcNode) Receive(ctx context.Context, sku, location string, qty int64) error {
	_, err := n.client.Receive(ctx, &nodepb.ReceiveRequest{Sku: sku, Location: location, Qty: qty})
	return err
}

func (n *grpcNode) Issue(ctx context.Context, sku, location string, qty int64) error {
	_, err := n.client.Issue(ctx, &nodepb.IssueRequest{Sku: sku, Location: location, Qty: qty})
	return err
}

func (n *grpcNode) Balance(ctx context.Context, sku, location string) (int64, error) {
	resp, err := n.client.Balance(ctx, &nodepb.BalanceRequest{Sku: sku, Location: location})
	if err != nil {
		return 0, err
	}
	return resp.GetQty(), nil
}

// Sync waits out the node's own background sync interval. A containerised node
// syncs on a timer with no RPC to trigger it, so "give replication a chance to
// run" is literally a wait — and for a partitioned node, waiting achieves
// nothing, which is exactly the behaviour the scenario needs.
func (n *grpcNode) Sync(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(n.settle):
		return nil
	}
}

// compile-time proof that ComposeEnv is a full Env.
var _ Env = (*ComposeEnv)(nil)
