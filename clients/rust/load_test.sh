#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MANIFEST="$ROOT/clients/rust/Cargo.toml"
SEEDS="${SEEDS:-127.0.0.1:6784,127.0.0.1:6786,127.0.0.1:6788}"
OPS="${OPS:-500}"
REFRESH_SECS="${REFRESH_SECS:-5}"

echo "Running Rust client load test (round robin)…"
SEED="${SEEDS}" cargo run --manifest-path "$MANIFEST" --example load --release

echo "Running Rust client load test (consistent hash + discovery)…"
MODE=ch SEED="${SEEDS%%,*}" REFRESH_SECS="$REFRESH_SECS" OPS="$OPS" \
  cargo run --manifest-path "$MANIFEST" --example load --release

echo "Rust client load tests completed."
