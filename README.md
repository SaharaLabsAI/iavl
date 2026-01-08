## IAVL v2

Forked from v2.0.0-alpha.4 (21fac0bd)

IAVL v2 is performance minded rewrite of IAVL v1. Benchmarks show a 10-20x improvement in
throughput depending on the operation. The primary changes are:

- Sharding: branch nodes every 500_000 height.
- BTree on disk: SQLite (a mature BTree implementation) is used for storage.
- DB interface: Allow adding other DB backends.

### Install
Use Go modules:

```bash
go get github.com/SaharaLabsAI/iavl/v2
```

### Development with Nix
This repository provides a Nix flake development shell (see `flake.nix`).

- Enter the dev shell from the repo root:
  - `nix develop`
- Run commands inside the shell:
  - `go test ./...`
  - `go build ./...`
- Or run a command without entering an interactive shell:
  - `nix develop -c go test ./...`

### Documentation (in this repo)
- CHANGELOG: see `CHANGELOG.md`
- Contributing guide: see `CONTRIBUTING.md`
- Security policy: see `SECURITY.md`

### Contributing
Contributions are welcome! Please read `CONTRIBUTING.md` before opening a PR. If you're unsure where to start, feel free to propose ideas or small fixes-issues and discussions are appreciated.

### License
Licensed under Apache 2.0. See `LICENSE` for details. Attribution notices are listed in `NOTICE`.
