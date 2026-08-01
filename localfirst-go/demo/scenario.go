// Package demo runs the seven-step local-first demonstration: three nodes, one
// central, one network partition, and a converged global sum at the end.
//
// The steps are written against three small interfaces so that the identical
// code drives two very different environments: `make demo`, which cuts a real
// container off a real Docker network, and scenario_test.go, which runs
// everything in one process. A demo that only exists as shell commands rots;
// this one is compiled and tested.
package demo

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// SKU is the single product the demo moves around. One SKU keeps the printed
// output readable; nothing in the architecture cares how many there are.
const SKU = "SKU-1"

// OfflineOps is how many writes the isolated node accepts while partitioned.
// Twenty is enough that "they all succeeded" is obviously not luck.
const OfflineOps = 20

// Seed is how much each node receives in step 2, in node order. The three
// numbers are distinct so a reader can tell whose stock is missing from a sum.
var Seed = []int64{100, 50, 30}

// Node is one participant, as the scenario needs to see it: it accepts local
// commands, answers from local state, and can be asked to reconcile.
type Node interface {
	ID() string
	// Location is the single location this node exclusively owns.
	Location() string
	Receive(ctx context.Context, sku, location string, qty int64) error
	Issue(ctx context.Context, sku, location string, qty int64) error
	Balance(ctx context.Context, sku, location string) (int64, error)
	// Sync gives replication a chance to run and reports whether it could.
	// In-process that is one exchange; against containers it is waiting out a
	// background sync interval.
	Sync(ctx context.Context) error
}

// Central is the only thing the scenario asks of central: the global sum, which
// is a sum of per-node balances and never a merge.
type Central interface {
	GlobalSum(ctx context.Context) (map[string]int64, error)
}

// Env is the world the scenario runs in. Disconnect and Reconnect are the whole
// reason this abstraction exists: one implementation cuts a Docker network, the
// other refuses to sync.
type Env interface {
	Nodes() []Node
	Central() Central
	Disconnect(ctx context.Context, nodeID string) error
	Reconnect(ctx context.Context, nodeID string) error
}

// Result is every number the demo printed, so a test can assert on them instead
// of parsing prose.
type Result struct {
	SeedTotal       int64 // what the nodes received in total
	SumAfterSeed    int64 // central's global sum once everyone has synced
	IsolatedLocal   int64 // the partitioned node's own balance after its offline writes
	SumWhileOffline int64 // central's global sum during the partition
	SumAfterRejoin  int64 // central's global sum after convergence
	OfflineOK       int   // offline writes that succeeded
	OfflineFailed   int   // offline writes that failed; must be zero
}

// Scenario failures. Each is a claim in the README that stopped being true.
var (
	ErrWantThreeNodes       = errors.New("demo: scenario needs exactly three nodes")
	ErrDidNotConverge       = errors.New("demo: central's global sum is wrong")
	ErrOfflineWriteRejected = errors.New("demo: a write failed while the node was offline")
	ErrLeakedWhileOffline   = errors.New("demo: central's sum moved while the node was partitioned")
)

// Run executes the seven steps, printing each with its actual numbers, and
// returns them. Any deviation from the expected numbers is an error: a demo of
// convergence that prints a converged-looking number without checking it would
// be worse than no demo.
func Run(ctx context.Context, env Env, out io.Writer) (Result, error) {
	var res Result

	nodes := env.Nodes()
	if len(nodes) != 3 || len(Seed) != len(nodes) {
		return res, fmt.Errorf("%w: got %d", ErrWantThreeNodes, len(nodes))
	}

	// Step 1 -------------------------------------------------------------
	_, _ = fmt.Fprintln(out, "step 1/7  three nodes and one central are up")
	for _, n := range nodes {
		_, _ = fmt.Fprintf(out, "          %s exclusively owns location %s\n", n.ID(), n.Location())
	}

	// Step 2 -------------------------------------------------------------
	_, _ = fmt.Fprintf(out, "\nstep 2/7  receiving stock at each node (local writes; no network needed)\n")
	for i, n := range nodes {
		if err := n.Receive(ctx, SKU, n.Location(), Seed[i]); err != nil {
			return res, fmt.Errorf("demo: seed %s: %w", n.ID(), err)
		}
		res.SeedTotal += Seed[i]
		_, _ = fmt.Fprintf(out, "          %s received %3d %s at %s\n", n.ID(), Seed[i], SKU, n.Location())
	}
	if err := syncAll(ctx, nodes); err != nil {
		return res, err
	}
	sum, err := globalSum(ctx, env)
	if err != nil {
		return res, err
	}
	res.SumAfterSeed = sum
	_, _ = fmt.Fprintf(out, "          central global sum for %s = %d (expected %d)\n", SKU, res.SumAfterSeed, res.SeedTotal)
	if res.SumAfterSeed != res.SeedTotal {
		return res, fmt.Errorf("%w: after seeding it is %d, want %d", ErrDidNotConverge, res.SumAfterSeed, res.SeedTotal)
	}

	// Step 3 -------------------------------------------------------------
	iso := nodes[len(nodes)-1]
	_, _ = fmt.Fprintf(out, "\nstep 3/7  cutting %s off the network\n", iso.ID())
	if err := env.Disconnect(ctx, iso.ID()); err != nil {
		return res, fmt.Errorf("demo: disconnect %s: %w", iso.ID(), err)
	}
	_, _ = fmt.Fprintf(out, "          %s can no longer reach central or its peers\n", iso.ID())

	// Step 4 -------------------------------------------------------------
	_, _ = fmt.Fprintf(out, "\nstep 4/7  issuing %d units, one at a time, against the isolated node\n", OfflineOps)
	for i := range OfflineOps {
		if err := iso.Issue(ctx, SKU, iso.Location(), 1); err != nil {
			res.OfflineFailed++
			_, _ = fmt.Fprintf(out, "          op %2d FAILED: %v\n", i+1, err)
			continue
		}
		res.OfflineOK++
	}
	_, _ = fmt.Fprintf(out, "          %d succeeded, %d failed — an offline node is fully operational\n", res.OfflineOK, res.OfflineFailed)
	if res.OfflineFailed != 0 {
		return res, fmt.Errorf("%w: %d of %d failed", ErrOfflineWriteRejected, res.OfflineFailed, OfflineOps)
	}
	res.IsolatedLocal, err = iso.Balance(ctx, SKU, iso.Location())
	if err != nil {
		return res, fmt.Errorf("demo: balance %s: %w", iso.ID(), err)
	}
	_, _ = fmt.Fprintf(out, "          %s reads its own balance at %s = %d, from its own log\n", iso.ID(), iso.Location(), res.IsolatedLocal)

	// Step 5 -------------------------------------------------------------
	_, _ = fmt.Fprintln(out, "\nstep 5/7  central has not heard about any of them")
	// The isolated node's attempt is expected to get nowhere, so its failure is
	// not the scenario's failure.
	_ = syncAll(ctx, nodes)
	if res.SumWhileOffline, err = globalSum(ctx, env); err != nil {
		return res, err
	}
	_, _ = fmt.Fprintf(out, "          central global sum = %d, still, while %d units have already been issued\n", res.SumWhileOffline, res.OfflineOK)
	_, _ = fmt.Fprintln(out, "          this gap is not a bug: it is the price of accepting writes without coordination")
	if res.SumWhileOffline != res.SumAfterSeed {
		return res, fmt.Errorf("%w: %d, want %d", ErrLeakedWhileOffline, res.SumWhileOffline, res.SumAfterSeed)
	}

	// Step 6 -------------------------------------------------------------
	_, _ = fmt.Fprintf(out, "\nstep 6/7  reconnecting %s\n", iso.ID())
	if err := env.Reconnect(ctx, iso.ID()); err != nil {
		return res, fmt.Errorf("demo: reconnect %s: %w", iso.ID(), err)
	}

	// Step 7 -------------------------------------------------------------
	_, _ = fmt.Fprintln(out, "\nstep 7/7  syncing, then reading central again")
	if err := syncAll(ctx, nodes); err != nil {
		return res, err
	}
	if res.SumAfterRejoin, err = globalSum(ctx, env); err != nil {
		return res, err
	}
	want := res.SumAfterSeed - int64(res.OfflineOK)
	_, _ = fmt.Fprintf(out, "          central global sum = %d (expected %d - %d = %d)\n", res.SumAfterRejoin, res.SumAfterSeed, res.OfflineOK, want)
	if res.SumAfterRejoin != want {
		return res, fmt.Errorf("%w: after rejoining it is %d, want %d", ErrDidNotConverge, res.SumAfterRejoin, want)
	}
	_, _ = fmt.Fprintln(out, "          converged: every offline write is now in the global sum, in order, exactly once")

	return res, nil
}

// syncAll asks every node to reconcile, returning the first failure.
func syncAll(ctx context.Context, nodes []Node) error {
	for _, n := range nodes {
		if err := n.Sync(ctx); err != nil {
			return fmt.Errorf("demo: sync %s: %w", n.ID(), err)
		}
	}
	return nil
}

// globalSum reads central's total for the demo's single SKU. A SKU that is
// absent from the map sums to zero, which is the right answer for "nobody has
// any".
func globalSum(ctx context.Context, env Env) (int64, error) {
	sums, err := env.Central().GlobalSum(ctx)
	if err != nil {
		return 0, fmt.Errorf("demo: global sum: %w", err)
	}
	return sums[SKU], nil
}
