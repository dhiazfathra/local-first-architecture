# Architecture

Three diagrams and the prose to read them by. If you only have five minutes,
read the package diagram and the event lifecycle; the sync sequence is the
detail underneath.

## Packages

The dividing line runs down the middle of this diagram: four packages know
nothing about inventory, one package is the entire domain, and two thin layers
join them.

```mermaid
flowchart TB
    subgraph engine["engine — domain-agnostic (must never import domain)"]
        clock["clock<br/>hybrid logical clock"]
        eventlog["eventlog<br/>Record, Validator, Projector<br/>SQLite append-only store"]
        projection["projection<br/>Reducer[S], Fold"]
        sync["sync<br/>symmetric gRPC replication"]
    end

    subgraph app["domain-aware"]
        domain["domain<br/>THE ONLY inventory-aware package<br/>Received, Issued, Moved<br/>BalanceReducer, Inventory"]
        transport["transport/grpc<br/>node client API"]
        central["central<br/>Postgres log + GlobalSum"]
    end

    subgraph bins["binaries"]
        cmdnode["cmd/node"]
        cmdcentral["cmd/central"]
    end

    eventlog --> clock
    projection --> eventlog
    sync --> eventlog
    domain --> eventlog
    domain --> projection
    domain --> clock
    transport --> domain
    central --> eventlog
    central --> projection
    central --> domain
    central --> sync
    cmdnode --> transport
    cmdnode --> sync
    cmdnode --> domain
    cmdcentral --> central
    cmdcentral --> sync

    arch["arch/arch_test.go<br/>parses the real import graph<br/>fails if engine reaches domain"]
    arch -.guards.-> engine
```

Read the arrows: everything in `engine` points only at other `engine` packages
or the standard library. Nothing points right-to-left. `arch/arch_test.go`
enforces that with `golang.org/x/tools/go/packages`, so the claim is verified on
every `go test` rather than maintained by good intentions. See
[ADR 0005](adr/0005-domain-agnostic-engine.md).

`central` is allowed to import `domain` because it must interpret payloads to
compute a global sum — it is an application of the engine, not part of it.

## Event lifecycle

One command, start to finish. The important part is the box: validate, append
and project happen inside a single SQLite transaction, so a rejected command
leaves nothing behind and derived state can never disagree with the log.

```mermaid
flowchart TD
    A["client calls Node.Receive via gRPC"] --> B["transport/grpc NodeService<br/>decode request"]
    B --> C["domain.Inventory.Receive<br/>ownership check: is this location mine?"]
    C -->|not owned| X1["ErrNotOwned — nothing appended"]
    C -->|qty <= 0| X2["ErrBadQty — nothing appended"]
    C -->|ok| D["encode domain.Received to JSON payload<br/>type = domain.TypeReceived"]
    D --> E["clock.Clock.Now → clock.HLC"]
    E --> F["eventlog.Store.Append"]

    subgraph tx["ONE SQLite transaction"]
        F --> G["Validator.Check(record)<br/>domain.Inventory: would the reducer accept this?<br/>balance must not go negative"]
        G -->|error| H["ROLLBACK<br/>ErrNegativeBalance — no record on disk"]
        G -->|ok| I["INSERT OR IGNORE INTO records<br/>PRIMARY KEY (node_id, seq)"]
        I --> J["Projector.Project(record)<br/>returns commit func()"]
        J --> K["COMMIT"]
    end

    K --> L["run commit func(): swap in new domain.State"]
    L --> M["record is durable and locally visible<br/>no network was involved"]
    M --> N["background sync.Client picks it up on its next pass"]
```

Two things a reader should take from this:

- The command path never touches the network. A node with no connectivity, or no
  central configured at all, serves every call in this diagram.
- `Validator.Check` is the seam that lets the engine enforce an invariant it
  cannot understand. `eventlog` knows only that a `Check` returned an error.

Rebuilding state on startup is the same machinery run backwards:
`projection.Fold(ctx, store, domain.BalanceReducer{})` replays every record
through the reducer. An unrecognised `Type` is `eventlog.ErrUnknownType` and
stops the process — never a skipped record.

## Sync sequence

`sync.Exchange` is one function used by both sides. The only difference between
the two participants is the `initiator bool` argument: who speaks first.

```mermaid
sequenceDiagram
    autonumber
    participant N as node-3 (initiator)
    participant C as central (responder)

    Note over N,C: same Frame type in both directions — central is a peer with a bigger disk

    N->>C: Frame{Hello{node_id:"node-3", version:{node-3:41, node-1:12}}}
    C->>N: Frame{Hello{node_id:"central", version:{node-1:12, node-2:7, node-3:20}}}

    Note over C: eventlog Since(node-3's version)<br/>→ records node-3 lacks
    C->>N: Frame{Batch{records:[node-2/1 … node-2/7]}}
    C->>N: Frame{Ack{version:{node-1:12, node-2:7, node-3:20}}}

    Note over N: Merge → INSERT OR IGNORE<br/>duplicates cost nothing<br/>clock.Observe pulls local HLC forward

    N->>C: Frame{Batch{records:[node-3/21 … node-3/41]}}
    N->>C: Frame{Ack{version:{node-1:12, node-2:7, node-3:41}}}
    N--)C: close send side (EOF)

    Note over C: Merge, then recompute GlobalSum<br/>sum of per-node balances, never a merge
```

If that stream dies anywhere — step 3, step 7, mid-batch — nothing needs
repairing. Both sides keep whatever they had already committed, and the next
attempt starts again from `Hello` with current version vectors. Re-delivered
records hit the `(node_id, seq)` primary key and vanish. That is the entire
resumption story, and it is a consequence of
[ADR 0002](adr/0002-sqlite-local-log.md) rather than code anyone had to write.

A version vector that has gone *backwards* (a peer restored from an older
backup) is equally harmless: it asks for records it already has, and gets them
again for free.

## Where your domain plugs in

Two interfaces, and a command encoder:

```go
// reading
type Reducer[S any] interface {
	Zero() S
	Apply(state S, r eventlog.Record) (S, error)
}

// writing, called inside the append transaction
type Validator interface {
	Check(r Record) error
}
```

[swapping-the-domain](swapping-the-domain.md) does it end to end with a second
toy domain, in compiling, tested code. [limitations](limitations.md) lists what
this architecture deliberately does not do.
