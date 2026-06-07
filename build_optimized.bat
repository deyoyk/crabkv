@echo off
REM Build script for CrabKV with native CPU optimizations
REM Run this before building for maximum performance

set RUSTFLAGS=-C target-cpu=native
set CARGO_PROFILE_RELEASE_LTO=fat
set CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1

echo Building CrabKV with native CPU optimizations...
echo RUSTFLAGS=%RUSTFLAGS%

cargo build --release

echo Build complete! Binary at target/release/crabkv.exe
