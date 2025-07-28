module github.com/cosmos/iavl/v2/migrate

go 1.23.0

toolchain go1.23.6

replace (
	github.com/cosmos/iavl/v2 => ../../
	// github.com/sahara/iavl/v2 => ../../../iavl2
	github.com/sahara/iavl/v2 => ../../../iavl2
)

require (
	github.com/cosmos/iavl/v2 v2.0.0-alpha.5
	github.com/gogo/protobuf v1.3.2
	github.com/sahara/iavl/v2 v2.0.0-00010101000000-000000000000
	github.com/spf13/cobra v1.9.1
	modernc.org/sqlite v1.38.0
)

require (
	github.com/aybabtme/uniplot v0.0.0-20151203143629-039c559e5e7e // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cosmos/gogoproto v1.7.0 // indirect
	github.com/cosmos/iavl-bench/bench v0.0.4 // indirect
	github.com/cosmos/ics23/go v0.11.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/eatonphil/gosqlite v0.10.1-0.20250409163211-9c47979bc5b1 // indirect
	github.com/emicklei/dot v1.8.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/kocubinski/costor-api v1.1.2 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/prometheus/client_golang v1.22.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.65.0 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rs/zerolog v1.33.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel v1.37.0 // indirect
	go.opentelemetry.io/otel/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/trace v1.37.0 // indirect
	golang.org/x/crypto v0.38.0 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/sync v0.14.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
	lukechampine.com/blake3 v1.4.1 // indirect
	modernc.org/libc v1.65.10 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)
