## Update from 0.1.3

This is an update of the version on CRAN since 2026-01-23. It replaces the two exported functions of 0.1.3, `sf_geojson_str()` and `df_json_str()`, with a single `as_json()` that dispatches on its input; the old names are removed rather than deprecated. `tools::package_dependencies(reverse = TRUE)` lists no reverse dependencies, so nothing on CRAN is affected. The upgrade is described in `NEWS.md` and the README.

## Test environments

* Windows 11, R 4.5.1 (local), Rtools 4.5, rustc 1.75.0 — `devtools::check(cran = TRUE, remote = TRUE, incoming = TRUE)` and `R CMD check --as-cran`; the install, R test suite and Rust unit tests were repeated with rustc 1.92.0 (0 warnings)
* Ubuntu 24.04, R 4.5.1 (rocker/geospatial:4.5.1 in Docker), Rust 1.75.0 via rustup — `R CMD check --as-cran`

macOS has not been checked for this version; it will be exercised on GitHub Actions before submission. 0.1.3 passes on all CRAN macOS flavors.

## R CMD check results

0 errors | 0 warnings | 0 notes.

`checking CRAN incoming feasibility ... OK` with the remote checks enabled. The rocker image reports one NOTE, `Compilation used the following non-portable flag(s): -Wdate-time -Werror=format-security -Wformat`; those are the image's own `R CMD config CFLAGS`, which it does not list in `_R_CHECK_COMPILATION_FLAGS_KNOWN_`. The package sets no compiler flags, and 0.1.3, which compiles the same C file, shows no such NOTE on any CRAN flavor.

## Compiled code

The package builds a Rust static library (extendr 0.8.2) with all crates vendored in `src/rust/vendor.tar.xz` and built `--offline --locked`. The minimum Rust version, 1.68 (set by the vendored `syn`/`ryu`/`itoa`), is declared as `rust-version` in `Cargo.toml`, and the package was built and its test suite run with rustc 1.68.0, 1.75.0 and 1.92.0; the `rust-toolchain.toml` that 0.1.3 shipped is gone, so the builder's own toolchain is used. `SystemRequirements` declares Cargo and rustc. `checking Rust compilation` passes on both platforms. The build recipe was reworked since 0.1.3 (`configure` and `configure.win` now generate the Makevars through `tools/config.R`, the Makevars use portable make only, `cleanup` scripts were added, and the aarch64 Windows target triple is derived correctly); the details are under "Build" in `NEWS.md`.

One entry point outside the documented API is used deliberately: `ATTRIB`, in a single function (`read_coord_ptr`, `src/rust/src/geometry.rs`) that reads the `dim` attribute of a coordinate matrix from inside a worker thread. The documented substitute, `Rf_getAttrib`, calls `MARK_NOT_MUTABLE` on the attribute it returns — a write to a shared SEXP header — and this function runs concurrently across the geometries of one `sfc`, so several threads would write the same header at once. The alternative is to pre-read every geometry's `dim` serially on the R thread, which is the pass the parallel extraction exists to avoid. The read touches three pointers and writes nothing; it is documented in place. All other entry points used are in R's API or experimental API table (verified against `tools:::funAPI()` on R 4.5.1).

Worker threads never call the R API: they read vector data only, and every allocation or R call happens on the R thread. Interrupts are polled at phase boundaries through `R_ToplevelExec` so an interrupt cannot unwind through Rust frames.

## Dependencies

`Depends: R (>= 4.5)`, where 0.1.3 declared no floor — the package uses `VECTOR_PTR_RO`, `ANY_ATTRIB`, `Rf_charIsUTF8` and `Rf_charIsLatin1`, which arrived in R 4.5.0.

## Reverse dependencies

None.
