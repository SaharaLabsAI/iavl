{
  description = "Sahara chain Go development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # go.mod: `go 1.23.1` + `toolchain go1.23.5`
        #
        # Some nixpkgs revisions keep `go_1_23` around as an EOL stub that throws
        # on evaluation; prefer it when available, otherwise fall back to `go`.
        go =
          if pkgs ? go_1_23 then
            let r = builtins.tryEval pkgs.go_1_23;
            in if r.success then r.value else pkgs.go
          else
            pkgs.go;
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            go
            git
            gnumake
            gcc
            pkg-config

            # Common tooling used by Cosmos SDK chains.
            protobuf
            buf

            # CGO dependencies often needed by indirect deps (e.g. RocksDB backend).
            rocksdb
            snappy
            lz4
            zstd

            jq
            file
          ];

          shellHook = ''
            # Some environments export GOROOT; if it doesn't match the `go`
            # binary on PATH you'll get "compile: version ... does not match".
            unset GOROOT

            # Ensure Go does not try to auto-download toolchains.
            export GOTOOLCHAIN=local

            # Keep build caches inside the repo for convenience.
            mkdir -p .cache/go-build .cache/gomod
            export GOCACHE="$PWD/.cache/go-build"
            export GOMODCACHE="$PWD/.cache/gomod"
          '';
        };

        # Optional shell for fully static builds (CGO + -static) using musl.
        # Keeping this separate avoids accidentally linking test binaries against
        # musl while still using a glibc dynamic loader, which can segfault at
        # process startup.
        devShells.static = pkgs.mkShell {
          packages = with pkgs; [
            go
            git
            gnumake
            gcc
            pkg-config
            musl

            protobuf
            buf

            rocksdb
            snappy
            lz4
            zstd

            jq
            file
          ];

          shellHook = ''
            unset GOROOT
            export GOTOOLCHAIN=local

            mkdir -p .cache/go-build .cache/gomod
            export GOCACHE="$PWD/.cache/go-build"
            export GOMODCACHE="$PWD/.cache/gomod"
          '';
        };
      });
}

