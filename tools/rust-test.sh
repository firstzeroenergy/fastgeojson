#!/usr/bin/env bash
# Runs the crate's own unit tests.
#
#   tools/rust-test.sh [cargo test args...]
#
# These cover the pure parts -- number formatting, escaping, date arithmetic,
# chunk planning -- with no R object anywhere, so they run in seconds instead
# of the minute a `cargo build` plus `R CMD INSTALL` plus an R script costs.
# Everything that touches a SEXP stays in the R-level suites under
# tests/testthat and tools/bench.
#
# Three things have to be arranged before `cargo test` will even link, none of
# which the package build needs:
#
#   * The pinned toolchain is msvc, but the test binary has to resolve R's
#     symbols and R ships only R.dll with no import library. mingw's linker
#     binds a DLL directly, so the tests build for x86_64-pc-windows-gnu, the
#     same target R CMD INSTALL uses.
#   * Rtools names its compiler `gcc`, not `x86_64-w64-mingw32-gcc`, so the
#     linker is given explicitly.
#   * Rust's gnu std references libgcc_eh, which Rtools' static-posix
#     toolchain does not ship under that name; an empty stub satisfies it,
#     exactly as src/Makevars.win does for the package build.
#
# R.dll must also be on PATH at run time, which is why R's bin directory is
# prepended rather than only passed to the linker.
set -euo pipefail
cd "$(dirname "$0")/.."

R_BIN_DEFAULT="C:/PROGRA~1/R/R-45~1.1/bin/x64"
RTOOLS_DEFAULT="C:/rtools45/x86_64-w64-mingw32.static.posix/bin"
R_BIN="${FASTGEOJSON_R_BIN:-$R_BIN_DEFAULT}"
RTOOLS_BIN="${FASTGEOJSON_RTOOLS_BIN:-$RTOOLS_DEFAULT}"

if [ ! -f "$R_BIN/R.dll" ]; then
  echo "R.dll not found under $R_BIN; set FASTGEOJSON_R_BIN" >&2
  exit 1
fi
if [ ! -x "$RTOOLS_BIN/gcc.exe" ]; then
  echo "gcc not found under $RTOOLS_BIN; set FASTGEOJSON_RTOOLS_BIN" >&2
  exit 1
fi

# rustc writes its intermediates to TMP, which can be unwritable once Rtools is
# on PATH; point it somewhere it certainly can write.
export TMP="${TMP:-/tmp}" TEMP="$TMP" TMPDIR="$TMP"

MOCK="$(cygpath -m "$(pwd)")/src/rust/target/libgcc_mock"
mkdir -p "$MOCK"
: > "$MOCK/libgcc_eh.a"

# PATH is colon-separated in this shell, so the Windows-style paths the
# linker needs have to be converted before they go on it or they split at the
# drive letter -- which manifests as collect2 being unable to find `ld`.
export PATH="$(cygpath -u "$RTOOLS_BIN"):$(cygpath -u "$R_BIN"):$PATH"
export LIBRARY_PATH="$MOCK"
export RUSTFLAGS="-Clinker=$RTOOLS_BIN/gcc.exe -Lnative=$R_BIN -ldylib=R -Lnative=$MOCK"

exec cargo test \
  --manifest-path src/rust/Cargo.toml \
  --lib --offline --target x86_64-pc-windows-gnu "$@"
