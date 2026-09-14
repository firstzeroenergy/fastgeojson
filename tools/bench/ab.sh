#!/usr/bin/env bash
# A/B one benchmark script against the committed Rust sources.
#
#   tools/bench/ab.sh <bench-script.R>
#
# Builds and installs HEAD's src/rust/src, runs the benchmark, then restores
# the working copy, rebuilds and runs it again. Both halves use the same
# harness in the same session shape, which is the only way differences below
# about 10% mean anything on a desktop.
#
# Operates on the whole Rust source directory -- and ONLY that. R/ is not
# swapped, so both halves run the working copy's R code; an A/B of a change
# to R/fastgeojson.R through this script compares the change against itself.
# For an R-side change, `git stash` / install / measure / `git stash pop` /
# install / measure instead. An earlier version saved only lib.rs, which
# silently compared HEAD against HEAD once the crate was split into modules
# and reported a real gain as no change.
set -euo pipefail
cd "$(dirname "$0")/../.."
BENCH="${1:?usage: ab.sh <bench-script.R>}"
export PATH="/c/Program Files/R/R-4.5.1/bin/x64:$PATH"

SAVE="$(mktemp -d)"
cp -r src/rust/src/. "$SAVE/"
restore() { rm -rf src/rust/src; mkdir -p src/rust/src; cp -r "$SAVE/." src/rust/src/; rm -rf "$SAVE"; }
trap restore EXIT

build() {
  (cd src/rust && cargo build --release --offline 2>&1 | grep -E '^error' -A 6 || true)
  R CMD INSTALL --preclean --no-docs . >/dev/null 2>&1 || { echo "INSTALL FAILED"; exit 1; }
}

echo "===== A: HEAD ====="
rm -rf src/rust/src
mkdir -p src/rust/src
# Every tracked file under src/rust/src, at HEAD.
git ls-tree -r --name-only HEAD -- src/rust/src | while read -r f; do
  git show "HEAD:$f" > "$f"
done
build
Rscript "$BENCH"

echo
echo "===== B: working copy ====="
restore
trap - EXIT
build
Rscript "$BENCH"
rm -rf "$SAVE" 2>/dev/null || true
