# warehouse-node

An offline-first warehouse service. Each warehouse node's source of truth is its own
append-only event log in SQLite, so operators keep receiving, putting away, picking,
counting and transferring stock with no network at all. A central server on Postgres
reconciles asynchronously, enforces the invariants no single node can check, and pushes
compensating events back down for what it rejects.

## The design decision everything follows from

Invariants split into two kinds:

- **Node-enforced**, checked synchronously before anything is appended, because the node
  exclusively owns the thing being checked: stock never negative, reservations within
  available, lot not expired at pick time, unit-of-measure valid for the item.
- **Central-enforced**, checked after the fact, because they need state that lives on
  another node or only at central: a receipt not exceeding an open purchase order, no
  duplicate supplier delivery note, a transfer destination that exists and accepts the
  item, a transfer receipt not exceeding what was dispatched.

A node rejection is immediate and the operator sees the rule name. A central rejection
arrives later as a compensating event, which the node cannot refuse, and surfaces in an
exceptions projection so the operator can see what was reversed and why.

Compensation is not rollback. Downstream events that already consumed the bad stock are
left alone, so a compensation can drive a location negative. That is accepted, flagged
in exceptions, and resolved by a human doing a stock count.

## Running

The replication stream between a node and central requires mutual TLS by default: the
node's client certificate is how central authenticates which node a stream belongs to,
rather than trusting the client-supplied `Hello.node_id` alone.

```sh
# central, on Postgres
go run ./cmd/central -dsn "postgres://user:pass@localhost:5432/warehouse?sslmode=disable" \
  -listen :9090 -bootstrap bootstrap.json -transit-window 48h \
  -tls-cert central.crt -tls-key central.key -tls-client-ca node-ca.crt

# a node, offline-capable; drop -central to run with no network at all
go run ./cmd/node -db wh-a.db -id wh-a -listen :8080 -central localhost:9090 \
  -locations RECV-01:receiving,PICK-01:pick,STAGE-01:staging \
  -tls-cert wh-a.crt -tls-key wh-a.key -tls-server-ca central-ca.crt
```

A node's client certificate Common Name must match its `-id`: central rejects a
`Hello` whose `node_id` does not match the authenticated peer identity. For local
development without certificates, pass `-insecure` to both binaries to fall back to
a plaintext stream with no peer authentication.

`bootstrap.json` seeds the reference data only central owns:

```json
{
  "items": [{"sku": "WIDGET", "description": "Blue widget", "base_uom": "EA",
             "alt_uom": {"CASE": 12}, "lot_tracked": true, "shelf_life_days": 30}],
  "orders": [{"po_ref": "PO-1", "sku": "WIDGET", "qty": 100}],
  "nodes": [{"id": "wh-a"}, {"id": "wh-b", "rejects": ["HAZMAT"]}]
}
```

## Testing

```sh
make test   # go test ./... -cover
make lint   # golangci-lint run
make proto  # regenerate proto/nodeapi and proto/syncpb
```

The central store's tests run against a real Postgres started by
`embedded-postgres`, so no Docker and no local database setup is needed. The
integration suite runs two nodes and central in one process over a real gRPC transport
on an in-process listener. 100% coverage is a repository standard.
