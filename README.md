## IAVL v2

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

### Development with Nix (devenv)
This repository includes a Nix-based development environment via [devenv](https://devenv.sh/).

- Enter the dev shell from the repo root:
  - `devenv shell`
- Inside the shell you have Go and Git available (from `devenv.nix`). Typical commands:
  - `go test ./...`
  - `go build ./...`

### Documentation (in this repo)
- CHANGELOG: see `CHANGELOG.md`
- Contributing guide: see `CONTRIBUTING.md`
- Security policy: see `SECURITY.md`

### Contributing
Contributions are welcome! Please read `CONTRIBUTING.md` before opening a PR. If you're unsure where to start, feel free to propose ideas or small fixes-issues and discussions are appreciated.

### License
Licensed under Apache 2.0. See `LICENSE` for details. Attribution notices are listed in `NOTICE`.
