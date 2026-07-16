# M4 network-free gate evidence

This record covers the local M4-7 gate only. It does not authorize the M4
completion state: isolated real-R2 evidence, a separate read-only verification,
and the 24-hour MT5 soak remain M4-8 requirements. The Race result below is a
local Linux-equivalent result; durable external retention is still audited in M4-9.

## Repository gate

Run on 2026-07-16 (Asia/Tokyo), from the current worktree. Because the
managed home cache is read-only in this environment, the successful gate used
`GOCACHE=/tmp/tick-go-build-cache UV_CACHE_DIR=/tmp/tick-uv-cache`; the TCP
Gateway tests also require a non-sandboxed loopback socket.

- `GOCACHE=/tmp/tick-go-build-cache UV_CACHE_DIR=/tmp/tick-uv-cache mise run check`: pass
  - Go repository tests: pass, including `internal/ingest`, `internal/delivery`,
    `internal/httpapi`, `internal/operations`, `internal/r2`, and `internal/retention`
  - Python: 35 passed
  - independent fixture verifier: 24 verified
  - `gofmt -l cmd internal`: empty
  - Ruff format/lint and `git diff --check`: pass
- `mise exec -- go vet ./...`: pass
- `git diff --check` and `git diff --cached --check`: pass

The network-free fault coverage is distributed across the package ownership
boundaries and is run by the repository gate:

- ingest: WAL-sync crash, journal rebuild, ACK-loss retry, partial frame,
  session replacement, dense-boundary hard cap, source-error durability, and
  disk high/critical/emergency fail-closed behavior
- retention: checkpoint/trash/unlink fault matrix, recovery, non-WAL rejection,
  proof and clock/disk blocking
- replay R2: action-barrier restart, observation discard, receipt crash,
  stale-observation mutation, timeout, budget, graph, and conditional-write faults
- delivery/API: remote read failure, bounded inventory/metadata, selector
  classification, HTTP timeout/cancellation, request/response limits, and
  read-only fetch-plan boundary

## Explicit 10x load gate

Command:

```text
TICKDATA_ENABLE_LOAD=1 \
TICKDATA_LOAD_OUTPUT=/tmp/m4-network-free-load-v3.json \
mise exec -- go test ./internal/ingest -run TestTenXIngestRate -count=1 -v
```

The opt-in test paces the fake Protocol V1 producer at 10x for the full
requested duration over `net.Pipe`, uses a real local Gateway/WAL/SQLite
journal, and writes a mode-0600 JSON verification record.
The run used baseline 10 records/s, target 100 records/s, and 200 records.
Observed record (`m4-network-free-load-v1`, saved during the v3 rerun at
`/tmp/m4-network-free-load-v3.json`):

```json
{
  "active_duration_ms": 1990,
  "actual_records_per_second": 100.46316477601947,
  "actual_duration_ms": 2001,
  "ack_ready": true,
  "max_goroutines": 5,
  "max_heap_bytes": 3802088,
  "p95_ack_latency_us": 986,
  "wal_bytes": 73845,
  "wal_entries": 200,
  "pass": true,
  "go_version": "go1.24.13",
  "goos": "linux",
  "goarch": "amd64"
}
```

## Race gate

The requested race gate was first attempted inside the managed sandbox, but
loopback Gateway tests were denied by the sandbox. The compiler itself then
compiled successfully, and the same command was rerun with loopback permitted:

```text
CGO_ENABLED=1 CC="<pinned user-space GCC 15.2.0 with glibc sysroot>" \
go test -race ./internal/ingest ./internal/wal ./internal/archive ./internal/r2 ./internal/delivery ./internal/retention ./internal/operations ./internal/httpapi -count=1 -json
```

Observed result on 2026-07-16 (Asia/Tokyo):

- exit code: `0`
- Go: `go1.24.13`, Linux amd64, `CGO_ENABLED=1`
- C compiler: GCC `15.2.0`
- package results: `internal/ingest`, `internal/wal`, `internal/archive`,
  `internal/r2`, `internal/delivery`, `internal/retention`,
  `internal/operations`, `internal/httpapi`: all pass
- JSON checks: no `DATA RACE` marker and no `Action: fail`
- raw JSON: 1,373 lines, 299,355 bytes, SHA-256
  `b427d385ea1eb39bfb2c4f3ec488c954b626c3731bdc55f6b7ac966793641c94`

The raw JSON remains outside the repository; the digest is recorded here for
the M4-9 artifact-retention audit. The Linux and Windows workflows also upload
durable Race artifacts when run externally.
