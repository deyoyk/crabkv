#!/usr/bin/env bash
set -euo pipefail

# Build script for CrabKV with native CPU optimizations
# Run this before building for maximum performance

export RUSTFLAGS="-C target-cpu=native"
export CARGO_PROFILE_RELEASE_LTO=fat
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1

echo "Building CrabKV with native CPU optimizations..."
echo "RUSTFLAGS=$RUSTFLAGS"

cargo build --release

echo "Build complete! Binary at target/release/crabkv"
