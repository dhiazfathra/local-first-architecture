package main

import (
	"bytes"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/clock"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/eventlog"
	"github.com/dhiazfathra/local-first-architecture/eventlog-lab/node"
	syncpkg "github.com/dhiazfathra/local-first-architecture/eventlog-lab/sync"
)

// exec runs the CLI and returns stdout, stderr, and the error.
func execCLI(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err := run(args, &out, &errOut)
	return out.String(), errOut.String(), err
}

func TestRunUsageErrors(t *testing.T) {
	// op unknown verb reaches openNode (its args parse cleanly before the
	// unknown-verb check), so it needs a real, isolated path rather than a
	// literal name that would create a stray file in the working directory.
	opUnknownVerbDB := filepath.Join(t.TempDir(), "op-unknown-verb.db")
	tests := []struct {
		name    string
		args    []string
		wantMsg string
	}{
		{name: "no args", args: nil, wantMsg: "usage"},
		{name: "unknown command", args: []string{"frobnicate"}, wantMsg: "unknown command"},
		{name: "op without id", args: []string{"op", "receive", "SKU-1", "10"}, wantMsg: "--id"},
		{name: "op without db", args: []string{"op", "--id", "A", "receive", "SKU-1", "10"}, wantMsg: "--db"},
		{
			name:    "op unknown verb",
			args:    []string{"op", "--id", "A", "--db", opUnknownVerbDB, "teleport", "SKU-1", "1"},
			wantMsg: "unknown op",
		},
		{
			name:    "op missing quantity",
			args:    []string{"op", "--id", "A", "--db", "x.db", "receive", "SKU-1"},
			wantMsg: "usage",
		},
		{
			name:    "op non-numeric quantity",
			args:    []string{"op", "--id", "A", "--db", "x.db", "receive", "SKU-1", "lots"},
			wantMsg: "quantity",
		},
		{name: "state without sku", args: []string{"state", "--id", "A", "--db", "x.db"}, wantMsg: "usage"},
		{name: "node without central", args: []string{"node", "--id", "A", "--db", "x.db"}, wantMsg: "--central"},
		{
			name:    "sim unknown fault",
			args:    []string{"sim", "--seed", "1", "--nodes", "2", "--ops", "5", "--faults", "gremlins"},
			wantMsg: "unknown fault",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := execCLI(t, tt.args...)
			if err == nil {
				t.Fatalf("run(%v) = nil, want error", tt.args)
			}
			if !strings.Contains(strings.ToLower(err.Error()), tt.wantMsg) {
				t.Errorf("run(%v) error = %q, want it to mention %q", tt.args, err, tt.wantMsg)
			}
		})
	}
}

func TestOpAndStateRoundTrip(t *testing.T) {
	db := filepath.Join(t.TempDir(), "a.db")

	if _, _, err := execCLI(t, "op", "--id", "A", "--db", db, "receive", "SKU-1", "10"); err != nil {
		t.Fatalf("receive error = %v", err)
	}
	if _, _, err := execCLI(t, "op", "--id", "A", "--db", db, "pick", "SKU-1", "3"); err != nil {
		t.Fatalf("pick error = %v", err)
	}
	out, _, err := execCLI(t, "state", "--id", "A", "--db", db, "SKU-1")
	if err != nil {
		t.Fatalf("state error = %v", err)
	}
	for _, want := range []string{"SKU-1", "7"} {
		if !strings.Contains(out, want) {
			t.Errorf("state output = %q, missing %q", out, want)
		}
	}
}

func TestOpPrintsTheAcknowledgedEventID(t *testing.T) {
	db := filepath.Join(t.TempDir(), "b.db")
	out, _, err := execCLI(t, "op", "--id", "B", "--db", db, "receive", "SKU-2", "4")
	if err != nil {
		t.Fatalf("receive error = %v", err)
	}
	if !strings.Contains(out, "B/1") {
		t.Errorf("op output = %q, want the event id B/1", out)
	}
}

func TestOpRejectsNonPositiveQuantity(t *testing.T) {
	db := filepath.Join(t.TempDir(), "c.db")
	for _, qty := range []string{"0", "-5"} {
		if _, _, err := execCLI(t, "op", "--id", "C", "--db", db, "receive", "SKU-1", qty); err == nil {
			t.Errorf("receive %s = nil, want error", qty)
		}
	}
}

func TestStateOnAnUnknownSKUReportsZero(t *testing.T) {
	db := filepath.Join(t.TempDir(), "d.db")
	out, _, err := execCLI(t, "state", "--id", "D", "--db", db, "SKU-nope")
	if err != nil {
		t.Fatalf("state error = %v", err)
	}
	if !strings.Contains(out, "0") {
		t.Errorf("state output = %q, want quantity 0 for an unknown SKU", out)
	}
}

func TestSimPrintsAConvergenceReport(t *testing.T) {
	tests := []struct {
		name   string
		faults string
	}{
		{name: "no faults", faults: ""},
		{name: "spec example", faults: "partition,skew,dup"},
		{name: "all faults", faults: "all"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []string{"sim", "--seed", "42", "--nodes", "3", "--ops", "60"}
			if tt.faults != "" {
				args = append(args, "--faults", tt.faults)
			}
			out, _, err := execCLI(t, args...)
			if err != nil {
				t.Fatalf("sim error = %v", err)
			}
			for _, want := range []string{"seed 42", "ops 60", "convergence OK"} {
				if !strings.Contains(out, want) {
					t.Errorf("sim output = %q, missing %q", out, want)
				}
			}
		})
	}
}

func TestNodeValidatesArgsBeforeDialing(t *testing.T) {
	// A bad address must fail fast rather than block: no server is listening.
	db := filepath.Join(t.TempDir(), "n.db")
	_, _, err := execCLI(t, "node", "--id", "N", "--db", db,
		"--central", "127.0.0.1:1", "--once")
	if err == nil {
		t.Fatalf("node --once against a dead address = nil, want error")
	}
}

func TestOpenNodeRejectsUnopenableDB(t *testing.T) {
	// A directory is not a valid sqlite file: OpenSQLite must fail.
	dir := t.TempDir()
	if _, _, err := execCLI(t, "op", "--id", "A", "--db", dir, "receive", "SKU-1", "1"); err == nil {
		t.Fatalf("op against a directory db = nil, want error")
	}
}

func TestBadFlagsAreRejected(t *testing.T) {
	db := filepath.Join(t.TempDir(), "x.db")
	for _, args := range [][]string{
		{"op", "--bogus", "--id", "A", "--db", db, "receive", "SKU-1", "1"},
		{"state", "--bogus", "--id", "A", "--db", db, "SKU-1"},
		{"node", "--bogus", "--id", "A", "--db", db, "--central", "x"},
		{"sim", "--bogus"},
	} {
		if _, _, err := execCLI(t, args...); err == nil {
			t.Errorf("run(%v) = nil, want a flag parse error", args)
		}
	}
}

func TestStateOnEmptySKUReportsAnError(t *testing.T) {
	db := filepath.Join(t.TempDir(), "e.db")
	if _, _, err := execCLI(t, "state", "--id", "A", "--db", db, ""); err == nil {
		t.Fatalf("state with an empty sku = nil, want error")
	}
}

func TestStatePropagatesOpenNodeError(t *testing.T) {
	// --db is present but --id is not: openNode's own error must surface.
	if _, _, err := execCLI(t, "state", "--db", filepath.Join(t.TempDir(), "k.db"), "SKU-1"); err == nil {
		t.Fatalf("state without --id = nil, want error")
	}
}

func TestNodePropagatesOpenNodeError(t *testing.T) {
	// --central is present but --id is not: openNode's own error must surface.
	if _, _, err := execCLI(t, "node", "--db", filepath.Join(t.TempDir(), "l.db"), "--central", "x"); err == nil {
		t.Fatalf("node without --id = nil, want error")
	}
}

func TestNodeRejectsNonPositiveEveryAndTimeout(t *testing.T) {
	db := filepath.Join(t.TempDir(), "f.db")
	if _, _, err := execCLI(t, "node", "--id", "A", "--db", db, "--central", "x", "--every", "-1s"); err == nil {
		t.Fatalf("--every -1s = nil, want error")
	}
	if _, _, err := execCLI(t, "node", "--id", "A", "--db", db, "--central", "x", "--timeout", "0s"); err == nil {
		t.Fatalf("--timeout 0s = nil, want error")
	}
}

func TestNodeListenAddressInUseFailsFast(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	defer func() { _ = ln.Close() }()

	db := filepath.Join(t.TempDir(), "g.db")
	_, _, err = execCLI(t, "node", "--id", "A", "--db", db,
		"--central", "127.0.0.1:1", "--listen", ln.Addr().String())
	if err == nil {
		t.Fatalf("--listen on an address already in use = nil, want error")
	}
}

// startPeer serves replication over a real gRPC listener and returns its
// address. The server and the backing log are closed by t.Cleanup.
func startPeer(t *testing.T) (addr string) {
	t.Helper()
	db := filepath.Join(t.TempDir(), "peer.db")
	l, err := eventlog.OpenSQLite(db)
	if err != nil {
		t.Fatalf("open peer log: %v", err)
	}
	n, err := node.New(node.Config{ID: clock.NodeID("PEER"), Log: l, SnapshotEvery: 64})
	if err != nil {
		t.Fatalf("build peer node: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	syncpkg.Register(gs, syncpkg.NewServer(n, 64))
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() {
		gs.Stop()
		_ = l.Close()
	})
	return ln.Addr().String()
}

func TestNodeOnceSyncsSuccessfully(t *testing.T) {
	addr := startPeer(t)
	db := filepath.Join(t.TempDir(), "h.db")
	out, _, err := execCLI(t, "node", "--id", "A", "--db", db, "--central", addr, "--once")
	if err != nil {
		t.Fatalf("node --once against a live peer: %v", err)
	}
	if !strings.Contains(out, "synced with") {
		t.Errorf("node --once output = %q, want a sync report", out)
	}
}

// runInterruptibly runs runNode in the background, delivers SIGINT after a
// short delay (as a real deployment would on Ctrl-C), and returns once run
// has returned. The delay is real wall-clock time because runNode's serve
// loop is driven by signal.NotifyContext, not an injectable context -- this
// is the "short context deadline" the brief allows for the one command a
// unit test cannot run to completion.
func runInterruptibly(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	// Keep a handler installed for the whole test so a SIGINT that races the
	// command's own handler cannot terminate the test binary.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)
	t.Cleanup(func() { signal.Stop(sigs) })
	var out, errOut bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- run(args, &out, &errOut) }()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-time.After(150 * time.Millisecond):
			_ = syscall.Kill(os.Getpid(), syscall.SIGINT)
		case <-stop:
		}
	}()
	select {
	case err := <-done:
		return out.String(), errOut.String(), err
	case <-time.After(10 * time.Second):
		t.Fatal("node did not stop after SIGINT")
		return "", "", nil
	}
}

func TestNodeServesAndSyncsUntilInterrupted(t *testing.T) {
	addr := startPeer(t)
	db := filepath.Join(t.TempDir(), "i.db")
	out, errOut, err := runInterruptibly(t, "node", "--id", "A", "--db", db,
		"--central", addr, "--listen", "127.0.0.1:0", "--every", "20ms")
	if err != nil {
		t.Fatalf("node = %v, stderr %q", err, errOut)
	}
	if !strings.Contains(out, "serving on") || !strings.Contains(out, "synced with") {
		t.Errorf("node output = %q, want serve + sync lines", out)
	}
}

func TestNodeSyncFailureIsLoggedAndRetried(t *testing.T) {
	db := filepath.Join(t.TempDir(), "j.db")
	_, errOut, err := runInterruptibly(t, "node", "--id", "A", "--db", db,
		"--central", "127.0.0.1:1", "--every", "5ms")
	if err != nil {
		t.Fatalf("node = %v", err)
	}
	if !strings.Contains(errOut, "sync:") {
		t.Errorf("node stderr = %q, want a logged sync failure", errOut)
	}
}

func TestSimTempDirFailure(t *testing.T) {
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "does-not-exist"))
	if _, _, err := execCLI(t, "sim", "--seed", "1", "--nodes", "2", "--ops", "5"); err == nil {
		t.Fatalf("sim with an unusable TMPDIR = nil, want error")
	}
}

func TestMainReportsErrorsAndExitsNonzero(t *testing.T) {
	oldArgs, oldExit, oldStderr := os.Args, osExit, os.Stderr
	defer func() { os.Args, osExit, os.Stderr = oldArgs, oldExit, oldStderr }()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	os.Args = []string{"lab", "bogus"}
	var gotCode int
	osExit = func(code int) { gotCode = code }

	main()

	_ = w.Close()
	stderr, _ := io.ReadAll(r)
	if gotCode != 1 {
		t.Errorf("exit code = %d, want 1", gotCode)
	}
	if !strings.Contains(string(stderr), "lab:") {
		t.Errorf("stderr = %q, want it prefixed with %q", stderr, "lab:")
	}
}
