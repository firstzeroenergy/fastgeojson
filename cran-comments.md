## Resubmission

This is a resubmission. I have addressed the CRAN check failures as follows:

* Fixed a parallel build race in `src/Makevars` where Rust build artifacts (`rust/target`, `.cargo`, `rust/vendor`) could be removed while `cargo build` was still running under parallel `make`. Cleanup now runs only after `$(SHLIB)` finishes linking.
* Cargo is invoked with `-j 2` and uses `--offline` when `rust/vendor.tar.xz` is present, consistent with CRAN policy (no network access during installation).

## Test environments

* local Windows 11 install, R 4.5.2
* win-builder (devel and release)
* GitHub Actions (macos-latest, ubuntu-latest, windows-latest)
* R-hub: atlas (Fedora, R-devel) and macos-arm64

## R CMD check results

0 errors | 0 warnings | 0 notes

## Downstream dependencies

There are currently no downstream dependencies for this package.