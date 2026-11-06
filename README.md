# Clearinghouse

A Go/PostgreSQL transfer ledger whose API keeps accepting valid writes while its event consumer or message broker is down, then reconciles the recovered results.

This is a local engineering demonstration with fictional account balances. It adds real PostgreSQL contention, independently running services, JetStream delivery, tenant isolation and failure experiments beyond an embedded-database workflow engine. It is not a payment processor or evidence of production banking experience.

## Run

Requirements: Go 1.26+, Docker Engine, Python 3.10+. From this directory:

```sh
./scripts/services.sh up
export DATABASE_URL='postgres://postgres:localdemo@127.0.0.1:55432/clearinghouse?sslmode=disable'
export NATS_URL='nats://127.0.0.1:54222'
make test
make demo
```

`make demo` builds the binary, creates a unique PostgreSQL schema and NATS stream, provisions fictional accounts, starts two API processes and independent publisher/consumer processes, runs load and failure drills, records `evidence/demo.json`, and stops its processes. The task-owned PostgreSQL/NATS containers remain available; `./scripts/services.sh down` stops them without deleting their data. Repeated demos retain their unique schemas/streams for inspection. The Docker daemon must be running.

To include a real broker-stop/restart drill against the two task-owned containers:

```sh
DEMO_BROKER_OUTAGE=1 make demo
```

The historical local run used Colima and an isolated empty Docker configuration. Most Docker installations need no override. If you intentionally use that setup, create the ignored local directory and set the socket explicitly:

```sh
mkdir -p .runtime/docker-config
export DOCKER_CONFIG="$PWD/.runtime/docker-config"
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
```

On a normal Docker installation, those overrides are unnecessary. The demo passwords and tokens are public local examples, not production credentials. Ports are published on loopback.

For an interactive API, initialize a fresh schema, seed it, then start each mode in a terminal:

```sh
make build
export PGSCHEMA=interactive_demo
export NATS_STREAM=INTERACTIVE_DEMO
export API_TOKENS='{"local-demo-alpha-token":"demo"}'
.runtime/clearinghouse -mode init
.runtime/clearinghouse -mode seed
.runtime/clearinghouse -mode api
# Other terminals with the same environment:
.runtime/clearinghouse -mode publisher
.runtime/clearinghouse -mode consumer
```

```sh
curl --fail -H 'Authorization: Bearer local-demo-alpha-token' \
  -H 'Idempotency-Key: example-1' -H 'Content-Type: application/json' \
  -d '{"from":"a0","to":"a1","amount":25}' http://127.0.0.1:8089/v1/transfers
curl --fail -H 'Authorization: Bearer local-demo-alpha-token' http://127.0.0.1:8089/v1/reconcile
curl --fail -H 'Authorization: Bearer local-demo-alpha-token' http://127.0.0.1:8089/v1/summary
curl --fail http://127.0.0.1:8089/metrics
```

First success returns 201; an identical key/payload retry returns the original transfer with 200; a changed payload returns 409. Amounts are positive integer minor units, capped at 10^12. A tenant has exactly one currency, fixed by its first account and enforced through a PostgreSQL foreign key. Each account balance stays in [0, 10^12]. Summary and reconciliation output label the currency. There are no FX rates or cross-tenant transfers.

## Architecture and invariants

```mermaid
flowchart LR
  Client --> A[API process 1]
  Client --> B[API process 2]
  A --> PG[(PostgreSQL)]
  B --> PG
  PG --> P[Outbox publisher]
  P --> JS[(NATS JetStream)]
  JS --> C[Materializer]
  C --> PG
```

The API authenticates bearer tokens mapped to server-side tenant IDs; request JSON cannot choose a tenant. All account and idempotency queries are tenant scoped. Tokens are hashed before lookup, and never logged. The CLI performs provisioning; HTTP has no account-credit endpoint.

Transfers use READ COMMITTED transactions with explicitly locked account rows in ID order. Same-key operations serialize through a transaction-scoped advisory lock. The transaction updates both balances, inserts one immutable transfer and inserts its outbox event. Any failure rolls back all effects. Opposed transfers acquire locks in the same order. A canonical payload hash detects conflicting idempotency reuse. PostgreSQL retries for deadlock/serialization errors are bounded; a client receiving 503 must retry the same key. A commit acknowledgement lost in transit can be resolved by that retry.

`entries` derives one negative and one positive journal entry from each transfer, so a transfer cannot contain an unbalanced pair. Update/delete triggers reject journal rewrites. Reconciliation independently compares each account against opening balance plus journal deltas and verifies the aggregate journal net is zero. Database owner credentials remain trusted: this is not a defense against a malicious database administrator.

The publisher selects pending outbox rows with `FOR UPDATE SKIP LOCKED`, publishes each to JetStream, then marks it published. Publish acknowledgement before database commit is a deliberate duplicate-delivery window. JetStream's short deduplication window is an optimization; the permanent inbox primary key prevents duplicate materialization after that window. Consumer commit precedes broker acknowledgement. Counter/volume effects are commutative, so delivery order is not a correctness requirement. This is at-least-once delivery with idempotent effects, not a cross-system exactly-once transaction.

Broker publication and acknowledgement each have an explicit two-second deadline; worker batches have a six-second deadline. The independent reviewer found that NATS's default wait is bypassed when a deadline-free context is supplied. A real no-reply broker test reproduced the stall before this fix. PostgreSQL connections, statements, lock waits and HTTP requests also have deadlines.

Malformed/forged broker events are persisted in `dead_letters` before acknowledgement, including stream position, SHA-256 and a bounded payload prefix. Transient database failures remain unacknowledged and retry. Operators can inspect quarantine rows directly; no automated repair or replay UI is claimed.

## Evidence

[demo.json](evidence/demo.json) is the authoritative latest end-to-end run. It records environment, 600 hot-account and 600 spread-account requests at 24-client concurrency across two API processes, latency including bounded retries, and all assertions. The experiment deliberately shows contention differences; a single short laptop run is not a throughput ceiling or SLA.

The failure drill retries a committed command after SIGKILL of an API process, kills/restarts the consumer, and optionally stops/restarts the real broker while 50 additional transfers are accepted. It then reconciles every account, journal and materialized transfer count. The first development load run encountered a five-second hot-account statement timeout. That failed run led to explicit client retry accounting and a lock-timeout rollback test; errors were not reclassified as successful requests.

`go test -race -count=1 ./...` requires actual PostgreSQL and NATS. It exercises concurrent duplicate keys, overdraw prevention, tenant isolation, mixed-currency rejection, immutable journal writes, outbox-insert rollback, API validation, forged event rejection, deadline rollback, real broker reply loss, redelivery after consumer commit, and poison-message quarantine. Missing dependency variables fail the suite rather than silently skipping integration tests. Logs retain expected initial regression failures and final passing results.

## Limits and design walkthrough

The database and broker are single-node local services; there is no consensus implementation, geographic failover, cloud deployment, hosted CI execution, tenant-level quotas, managed key rotation, TLS termination or production incident history. Authentication is a local demonstration. PostgreSQL is the source of truth; the materializer checks events against its canonical journal. The 256 MiB JetStream cap rejects new publication when full, preserving pending outbox rows; a long-running deployment needs explicit retention and archival operations. Aggregated reporting uses signed 64-bit values and would require a broader decimal API before extreme lifetime volume.

Explain the lock order and hot-account bottleneck; walk through every publish/commit/ack boundary; kill the consumer while querying the pending count; show a conflicting idempotency key; then reconcile the recovered state. The recorded reviews are automated artifact checks, not an employer endorsement or evidence of personal authorship.

Implementation references: [PostgreSQL row locking](https://www.postgresql.org/docs/current/explicit-locking.html), [Go transactions](https://go.dev/doc/database/execute-transactions), [NATS pull consumers](https://docs.nats.io/learn/jetstream/pull-consumers).

## Public release verification

The accepted PostgreSQL/NATS and Docker runs above are historical local evidence. Publication checks are recorded separately in [release-verification.json](evidence/release-verification.json); they do not imply a new integration run or hosted CI execution. The optional GitHub Actions workflow runs only when manually dispatched. See [publication provenance](docs/PUBLICATION.md) for export changes and limits.
