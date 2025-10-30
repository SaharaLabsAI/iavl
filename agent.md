## Agent Guide: IAVL v2

This document orients an AI/code agent (and humans) to work productively and safely in this repository.

### TL;DR
- **Language/Version**: Go 1.23
- **Module path**: `github.com/SaharaLabsAI/iavl/v2`
- **Build**: `go build ./...`
- **Test**: `go test ./...` (add `-race` locally)
- **Style**: Clear, defensive code; small, testable units; preserve determinism
- **High-risk areas**: hashing, node encoding, proof logic, on-disk formats, SQLite pragmas

## Project at a glance
IAVL v2 is a performance-minded rewrite of IAVL (immutable AVL) with:
- **Sharding** of branch nodes at large heights
- **SQLite** B-Tree storage for persistence
- A pluggable **DB interface** for alternate backends

Key packages:
- `tree/`: immutable/mutable tree algorithms (set/get/remove, snapshots, proofs)
- `node/`: node representation, hashing, encoding/decoding
- `db/`: storage interfaces and the `sqlite` implementation
- `common/`: cross-cutting utilities (compression, encoding, logging, metrics, pools)
- `tools/`: CLI utilities, migration helpers
- `tests/`: integration tests covering tree behavior and DB

## Build, test, and run
- Build all:
```bash
go build ./...
```
- Run tests (recommended locally):
```bash
go test ./... -race -count=1
```
- Run a single package or test:
```bash
go test ./tree -run TestName -v
```
- Basic vet/static checks:
```bash
go vet ./...
```

Notes:
- SQLite default path is `"/tmp/iavl2"` for persistent tests/runs; unit tests may use in-memory or temp paths.
- Some CLI subcommands in `tools/cmd` are scaffolded; use `go run ./tools/cmd --help` to inspect what is currently wired.

## Repository map (what lives where)
- `tree/`
  - `options.go`: runtime options (e.g., state storage, eviction)
  - `immutable.go`, `set.go`, `get.go`, `remove.go`: core operations
  - `snapshot.go`, `proof.go`, `version.go`: versions, snapshots, ICS23 proofs
- `node/`
  - `node.go`: node structure, child links
  - `hash.go`: SHA-256 based node hashing and value hashing
  - `codec.go`, `nodekey.go`: encoding, keys
- `db/`
  - `db.go`: storage interfaces (`DB`, `ReadonlyDB`, connections, write paths)
  - `sqlite/`: concrete SQLite backend (pooling, statements, options, write loop)
- `common/`
  - `compress/`: `s2` and `zstd` compression helpers
  - `encoding/`: shared encode/decode helpers
  - `logger/`, `metrics/`: logging and OpenTelemetry metrics abstractions
  - `pool/`: buffers, hash pools, node pools
- `tools/`
  - `cmd/`: CLI entrypoints
  - `migrate/`: migration tool(s)
- `tests/`: black-box and scenario tests for tree and DB

## Architectural invariants and don't-break rules
Changes to the following MUST preserve backward compatibility and determinism:
- **Hashing** (`node/hash.go`, `tree/hash.go`): uses SHA-256; any change will alter roots and proofs
- **Node encoding/decoding** (`node/*.go`, `common/encoding`): affects storage compatibility
- **Proofs/ICS23** (`tree/proof.go`): required for light client verification
- **On-disk format and schema** (`db/sqlite/*`): changing table shapes, PRAGMAs, or write ordering can corrupt or slow the DB; coordinate via migrations
- **Versioning/snapshots** (`tree/version.go`, `tree/snapshot.go`): must be monotonic and deterministic

If you must change any of the above, add migration/story in `CHANGELOG.md`, extend tests to cover compatibility, and provide a measured rollout plan.

## Safer places to contribute
- Adding or improving **tests** in `tests/`
- Instrumentation via `common/metrics` and `common/logger`
- Performance improvements that don't change hashes/encodings (e.g., pooling, allocation reductions, iterator behavior)
- Developer tooling in `tools/` and docs

## Extension points
- Implement a new storage backend by satisfying `db.DB`/`db.ReadonlyDB` from `db/db.go`.
- Augment compression or encoding through `common/compress` and `common/encoding` without altering public binary compatibility.
- Add CLI tooling under `tools/cmd` (Cobra-based).

## Runtime and options
- `tree.Options` (`tree/options.go`):
  - `StateStorage` (bool), `HeightFilter` (int8), `EvictionDepth` (int8), `MetricsProxy`
- `db/sqlite.Options` (`db/sqlite/options.go`):
  - Path, cache sizes, WAL/page sizes, busy timeouts, thread counts, statement cache, `ShardTrees`
  - Defaults tuned for performance; CI auto-disables OS thread locking

## Performance guidance
- Use buffer/hash pools from `common/pool` to limit allocations in hot paths
- Prefer batch operations where possible; use write batches and avoid per-node commits
- Keep iterators and snapshots streaming and memory-bounded
- Be mindful of WAL and page size interactions in SQLite options when changing defaults

## Testing guidance
- Add unit tests near the code and integration tests under `tests/`
- Cover: root determinism, proof verification, version/snapshot semantics, revert/pruning paths, DB reopen/persistency
- For storage changes, add reopen tests and large-set scenarios; measure with `-race`

## Observability
- Use `common/logger` for structured logs; avoid global loggers
- Expose counters/histograms via `common/metrics` (OpenTelemetry proxies provided)

## Local dev environment (optional)
- Nix users: repository provides `devenv.nix`/`devenv.yaml`. Typical flows:
```bash
# if you use devenv
devenv shell

# or standard
nix develop
```

## Release and changelog
- Maintain `CHANGELOG.md` entries for any user-visible behavior or compatibility change
- Tag releases as appropriate; follow semver pre-release tags already used in this repo

## Review checklist for agents
- Correctness: preserves hashes, proofs, and on-disk compatibility
- Determinism: same inputs -> same roots across platforms
- Performance: no regressions in hot paths; allocation changes justified
- API: public changes documented and tested
- Tests: new/changed behavior has unit + integration coverage
- Docs: update `README.md`/`CHANGELOG.md`/this file when relevant

## Pointers to critical files
- Hashing: `node/hash.go`
- Tree ops: `tree/{set,get,remove}.go`, `tree/immutable.go`
- Proofs: `tree/proof.go`
- Versions/Snapshots: `tree/version.go`, `tree/snapshot.go`
- Storage API: `db/db.go`
- SQLite backend: `db/sqlite/*.go`
- Encoding: `common/encoding/*.go`
- Compression: `common/compress/*`
- Metrics/Logging: `common/metrics/*`, `common/logger/*`
