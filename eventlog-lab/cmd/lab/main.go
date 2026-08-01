// Command lab drives an eventlog-lab node and the fault simulator.
//
//	lab node  --id A --db a.db --central localhost:9000
//	lab op    --id A --db a.db receive SKU-1 10
//	lab op    --id A --db a.db pick    SKU-1 3
//	lab state --id A --db a.db SKU-1
//	lab sim   --seed 42 --nodes 3 --ops 500 --faults partition,skew,dup
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/internal/jepsenlite"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/node"
	syncpkg "github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const usage = `usage:
  lab node  --id ID --db FILE --central ADDR [--listen ADDR] [--every DUR] [--once] [--timeout DUR]
  lab op    --id ID --db FILE (receive|pick) SKU QTY
  lab state --id ID --db FILE SKU
  lab sim   [--seed N] [--nodes N] [--ops N] [--faults LIST]

faults: partition, asym, skew, dup, reorder, crash, slow, all`

// osExit is os.Exit, swappable in tests so main can run to completion
// in-process instead of terminating the test binary.
var osExit = os.Exit

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "lab:", err)
		osExit(1)
	}
}

// run dispatches a subcommand. It takes args and writers so every branch is
// reachable from a test.
func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "node":
		return runNode(args[1:], stdout, stderr)
	case "op":
		return runOp(args[1:], stdout)
	case "state":
		return runState(args[1:], stdout)
	case "sim":
		return runSim(args[1:], stdout)
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
}

// openNode opens the on-disk log and wires a node around it.
func openNode(id, db string) (*node.Node, *eventlog.SQLiteLog, error) {
	if id == "" {
		return nil, nil, errors.New("--id is required")
	}
	if db == "" {
		return nil, nil, errors.New("--db is required")
	}
	l, err := eventlog.OpenSQLite(db)
	if err != nil {
		return nil, nil, err
	}
	// ID and Log are already validated above; New can still fail while
	// recovering the HLC from an unreadable log, so propagate.
	n, err := node.New(node.Config{
		ID:            clock.NodeID(id),
		Log:           l,
		SnapshotEvery: 64,
	})
	if err != nil {
		_ = l.Close()
		return nil, nil, err
	}
	return n, l, nil
}

// runOp performs one local receive or pick. It never syncs: a node is fully
// usable offline, which is the entire point of the architecture.
func runOp(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("op", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	id := fs.String("id", "", "node id")
	db := fs.String("db", "", "log file")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w\n%s", err, usage)
	}
	rest := fs.Args()
	if len(rest) != 3 {
		return errors.New(usage)
	}
	qty, err := strconv.ParseInt(rest[2], 10, 64)
	if err != nil {
		return fmt.Errorf("quantity %q is not a number", rest[2])
	}

	n, l, err := openNode(*id, *db)
	if err != nil {
		return err
	}
	defer func() { _ = l.Close() }()

	ctx := context.Background()
	var eid eventlog.EventID
	switch rest[0] {
	case "receive":
		eid, err = n.Receive(ctx, rest[1], qty)
	case "pick":
		eid, err = n.Pick(ctx, rest[1], qty)
	default:
		return fmt.Errorf("unknown op %q; want receive or pick", rest[0])
	}
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "acked %s/%d\n", eid.NodeID, eid.Seq)
	return nil
}

// runState prints the merged state of one SKU.
func runState(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("state", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	id := fs.String("id", "", "node id")
	db := fs.String("db", "", "log file")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w\n%s", err, usage)
	}
	if fs.NArg() != 1 {
		return errors.New(usage)
	}
	n, l, err := openNode(*id, *db)
	if err != nil {
		return err
	}
	defer func() { _ = l.Close() }()

	st, err := n.Get(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "%s quantity %d name %q reorder_point %d deleted %v\n",
		fs.Arg(0), st.Quantity(), st.Name.Value, st.ReorderPoint.Value, st.Deleted.Value)
	return nil
}

// runNode serves replication and periodically syncs with central. It blocks
// until interrupted, unless --once is given.
func runNode(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	id := fs.String("id", "", "node id")
	db := fs.String("db", "", "log file")
	central := fs.String("central", "", "central address")
	listen := fs.String("listen", "", "address to serve replication on")
	every := fs.Duration("every", 5*time.Second, "sync interval")
	once := fs.Bool("once", false, "sync once and exit")
	timeout := fs.Duration("timeout", 30*time.Second, "bound on a single dial+sync (--once only)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w\n%s", err, usage)
	}
	if *central == "" {
		return errors.New("--central is required")
	}
	// time.NewTicker panics on a non-positive duration; a malformed --every
	// must be a clean argument error, not a crash discovered only once the
	// ticker is reached.
	if *every <= 0 {
		return fmt.Errorf("--every must be positive, got %s", *every)
	}
	if *timeout <= 0 {
		return fmt.Errorf("--timeout must be positive, got %s", *timeout)
	}
	n, l, err := openNode(*id, *db)
	if err != nil {
		return err
	}
	defer func() { _ = l.Close() }()

	client := syncpkg.NewClient(n, syncpkg.NewGRPCDialer(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	), 64)

	if *once {
		// grpc.NewClient dials lazily and ignores WithBlock, so the context
		// deadline below is what actually bounds a dead central instead of
		// hanging this process forever.
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		rep, err := client.SyncOnce(ctx, *central)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "synced with %s: sent %d received %d\n",
			rep.PeerID, rep.Sent, rep.Received)
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *listen != "" {
		ln, err := net.Listen("tcp", *listen)
		if err != nil {
			return fmt.Errorf("listen %s: %w", *listen, err)
		}
		gs := grpc.NewServer()
		syncpkg.Register(gs, syncpkg.NewServer(n, 64))
		serveDone := make(chan struct{})
		go func() {
			defer close(serveDone)
			// ErrServerStopped is the expected outcome of our own gs.Stop()
			// below on shutdown, not a failure worth logging.
			if err := gs.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				_, _ = fmt.Fprintln(stderr, "serve:", err)
			}
		}()
		// Wait for the goroutine to finish before returning: otherwise it
		// can still be writing to stderr after this function -- and the
		// caller reading its output -- have moved on.
		defer func() { gs.GracefulStop(); <-serveDone }()
		_, _ = fmt.Fprintf(stdout, "node %s serving on %s\n", n.ID(), *listen)
	}

	syncOnce := func() {
		// Sync failures are expected offline; a node stays usable.
		rep, err := client.SyncWithBackoff(ctx, *central, 4, 200*time.Millisecond, nil)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "sync:", err)
			return
		}
		_, _ = fmt.Fprintf(stdout, "synced with %s: sent %d received %d\n",
			rep.PeerID, rep.Sent, rep.Received)
	}

	// Sync immediately so a freshly started node does not sit unsynchronized
	// for up to a full --every interval before its first attempt.
	syncOnce()

	ticker := time.NewTicker(*every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			syncOnce()
		}
	}
}

// runSim runs the harness from the command line and prints a convergence
// report. It exits non-zero on a property violation, with the seed in the
// message so CI output is a reproduction recipe.
func runSim(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("sim", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	seed := fs.Int64("seed", 1, "random seed")
	nodes := fs.Int("nodes", 3, "node count")
	ops := fs.Int("ops", 500, "operation count")
	faults := fs.String("faults", "", "comma-separated fault list")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w\n%s", err, usage)
	}
	f, err := jepsenlite.ParseFaults(*faults)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "lab-sim-")
	if err != nil {
		return fmt.Errorf("temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	sch := jepsenlite.GenSchedule(*seed, *nodes, *ops, f)
	res, err := jepsenlite.Run(context.Background(), dir, sch, f)
	if err != nil {
		return fmt.Errorf("property violated -- reproduce with "+
			"`lab sim --seed %d --nodes %d --ops %d --faults %s`: %w",
			*seed, *nodes, *ops, *faults, err)
	}
	_, _ = fmt.Fprint(stdout, res.String())
	return nil
}
