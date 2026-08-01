package main

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dhiazfathra/local-first-architecture/warehouse-node/internal/domain"
	"github.com/dhiazfathra/local-first-architecture/warehouse-node/proto/nodeapi"
)

// startNode runs the binary's run function over an in-process listener and returns an
// operator client for it.
func startNode(t *testing.T, cfg Config) nodeapi.NodeAPIClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close conn: %v", err)
		}
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run: %v", err)
		}
	})
	return nodeapi.NewNodeAPIClient(conn)
}

// TestNodeServesWithNoCentralConfigured is the offline-capability assertion: with no
// central address at all, every command still works.
func TestNodeServesWithNoCentralConfigured(t *testing.T) {
	client := startNode(t, Config{
		DB: filepath.Join(t.TempDir(), "node.db"),
		ID: "wh-a",
		Locations: map[domain.LocationCode]domain.LocationType{
			"RECV-01": domain.LocReceiving,
			"PICK-01": domain.LocPick,
		},
	})
	ctx := context.Background()

	// The item master normally arrives from central. With no central, an unknown SKU
	// is correctly refused — locally, immediately, naming the rule.
	_, err := client.Receive(ctx, &nodeapi.ReceiveRequest{ReceiptId: "R1", DeliveryNote: "DN-1",
		PoRef: "PO-1", Line: &nodeapi.Line{Sku: "WIDGET", LotId: "L1", Qty: 1, Uom: "EA"}, To: "RECV-01"})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("Receive = %v, want FailedPrecondition: no item master has replicated yet", err)
	}

	// Queries work regardless, which is what an operator needs when the line is down.
	if _, err := client.StockOnHand(ctx, &nodeapi.StockOnHandRequest{}); err != nil {
		t.Errorf("StockOnHand: %v", err)
	}
	if _, err := client.Exceptions(ctx, &nodeapi.ExceptionsRequest{}); err != nil {
		t.Errorf("Exceptions: %v", err)
	}
	if _, err := client.Transfers(ctx, &nodeapi.TransfersRequest{}); err != nil {
		t.Errorf("Transfers: %v", err)
	}
}

func TestNodeKeepsServingWhenCentralIsUnreachable(t *testing.T) {
	client := startNode(t, Config{
		DB:        filepath.Join(t.TempDir(), "node.db"),
		ID:        "wh-a",
		Central:   "127.0.0.1:1", // nothing listens there, ever
		SyncEvery: 5 * time.Millisecond,
		Backoff:   5 * time.Millisecond,
		Locations: map[domain.LocationCode]domain.LocationType{"RECV-01": domain.LocReceiving},
	})
	// Give the sync loop several failed attempts, then assert the API is unaffected.
	time.Sleep(30 * time.Millisecond)
	if _, err := client.StockOnHand(context.Background(), &nodeapi.StockOnHandRequest{}); err != nil {
		t.Fatalf("StockOnHand while central is unreachable: %v", err)
	}
}

func TestRunRejectsBadConfiguration(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "unopenable database",
			cfg:  Config{DB: filepath.Join(t.TempDir(), "missing", "dir", "node.db"), ID: "wh-a"},
		},
		{
			name: "invalid location type",
			cfg: Config{DB: filepath.Join(t.TempDir(), "node.db"), ID: "wh-a",
				Locations: map[domain.LocationCode]domain.LocationType{"X-01": "mezzanine"}},
		},
		{
			name: "unparseable central address",
			cfg: Config{DB: filepath.Join(t.TempDir(), "node.db"), ID: "wh-a",
				Central: "\x00 not a target"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lis := bufconn.Listen(1 << 10)
			defer func() { _ = lis.Close() }()
			if err := run(context.Background(), tt.cfg, lis); err == nil {
				t.Fatal("run: expected an error")
			}
		})
	}
}

func TestParseLocations(t *testing.T) {
	tests := []struct {
		name    string
		spec    string
		want    map[domain.LocationCode]domain.LocationType
		wantErr bool
	}{
		{name: "empty", spec: "", want: map[domain.LocationCode]domain.LocationType{}},
		{name: "one pair", spec: "RECV-01:receiving",
			want: map[domain.LocationCode]domain.LocationType{"RECV-01": domain.LocReceiving}},
		{name: "two pairs", spec: "RECV-01:receiving,PICK-01:pick",
			want: map[domain.LocationCode]domain.LocationType{
				"RECV-01": domain.LocReceiving, "PICK-01": domain.LocPick}},
		{name: "missing colon", spec: "RECV-01", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLocations(tt.spec)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
			for code, typ := range tt.want {
				if got[code] != typ {
					t.Errorf("got[%s] = %q, want %q", code, got[code], typ)
				}
			}
		})
	}
}
