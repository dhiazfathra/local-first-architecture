package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
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
	if cfg.Creds == nil {
		cfg.Creds = insecure.NewCredentials()
	}
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

func TestRunFailsWhenListenerIsClosed(t *testing.T) {
	lis := bufconn.Listen(1 << 10)
	_ = lis.Close()
	cfg := Config{DB: filepath.Join(t.TempDir(), "node.db"), ID: "wh-a"}
	if err := run(context.Background(), cfg, lis); err == nil {
		t.Fatal("run with closed listener: expected an error")
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

func TestMainReportsErrorsAndExitsNonzero(t *testing.T) {
	oldArgs, oldExit, oldStderr := os.Args, osExit, os.Stderr
	defer func() { os.Args, osExit, os.Stderr = oldArgs, oldExit, oldStderr }()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	os.Args = []string{"node", "--db", "missing/dir/node.db", "--id", "wh-a"}
	var gotCode int
	osExit = func(code int) { gotCode = code }

	main()

	_ = w.Close()
	stderr, _ := io.ReadAll(r)
	if gotCode != 1 {
		t.Errorf("exit code = %d, want 1", gotCode)
	}
	if !strings.Contains(string(stderr), "node:") {
		t.Errorf("stderr = %q, want it prefixed with %q", stderr, "node:")
	}
}

func TestMainReportsParseLocationsErrors(t *testing.T) {
	oldArgs, oldExit := os.Args, osExit
	defer func() { os.Args, osExit = oldArgs, oldExit }()

	os.Args = []string{"node", "--db", filepath.Join(t.TempDir(), "node.db"),
		"--id", "wh-a", "--locations", "INVALID"}
	var gotCode int
	osExit = func(code int) { gotCode = code }

	main()

	if gotCode != 1 {
		t.Errorf("exit code = %d, want 1", gotCode)
	}
}

// selfSignedCert writes a throwaway self-signed certificate and key PEM pair to dir,
// suitable as both a client certificate and its own CA in tests.
func selfSignedCert(t *testing.T, dir, name string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	certOut, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create %s: %v", certPath, err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("encode cert: %v", err)
	}
	_ = certOut.Close()
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyOut, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create %s: %v", keyPath, err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}); err != nil {
		t.Fatalf("encode key: %v", err)
	}
	_ = keyOut.Close()
	return certPath, keyPath
}

func TestClientCredentialsInsecureReturnsInsecureCredentials(t *testing.T) {
	creds, err := clientCredentials("", "", "", true)
	if err != nil {
		t.Fatalf("clientCredentials(insecure): %v", err)
	}
	if creds == nil {
		t.Fatal("clientCredentials(insecure): want non-nil credentials")
	}
}

func TestClientCredentialsRequiresAllTLSFlagsUnlessInsecure(t *testing.T) {
	if _, err := clientCredentials("", "", "", false); err == nil {
		t.Fatal("clientCredentials with no TLS flags: expected an error")
	}
}

func TestClientCredentialsRejectsAnUnreadableCertificate(t *testing.T) {
	dir := t.TempDir()
	if _, err := clientCredentials(filepath.Join(dir, "missing.crt"), filepath.Join(dir, "missing.key"),
		filepath.Join(dir, "ca.crt"), false); err == nil {
		t.Fatal("clientCredentials with a missing certificate: expected an error")
	}
}

func TestClientCredentialsRejectsAnUnreadableServerCA(t *testing.T) {
	dir := t.TempDir()
	cert, key := selfSignedCert(t, dir, "wh-a")
	if _, err := clientCredentials(cert, key, filepath.Join(dir, "missing-ca.crt"), false); err == nil {
		t.Fatal("clientCredentials with a missing server CA: expected an error")
	}
}

func TestClientCredentialsRejectsAnUnusableServerCA(t *testing.T) {
	dir := t.TempDir()
	cert, key := selfSignedCert(t, dir, "wh-a")
	badCA := filepath.Join(dir, "bad-ca.crt")
	if err := os.WriteFile(badCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := clientCredentials(cert, key, badCA, false); err == nil {
		t.Fatal("clientCredentials with an unusable server CA: expected an error")
	}
}

func TestClientCredentialsSucceedsWithAValidCertificateAndCA(t *testing.T) {
	dir := t.TempDir()
	cert, key := selfSignedCert(t, dir, "wh-a")
	ca, _ := selfSignedCert(t, dir, "central-ca")
	creds, err := clientCredentials(cert, key, ca, false)
	if err != nil {
		t.Fatalf("clientCredentials: %v", err)
	}
	if creds == nil {
		t.Fatal("clientCredentials: want non-nil credentials")
	}
}
