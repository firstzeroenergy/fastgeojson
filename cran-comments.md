## Test environments
* local Windows 11 install, R 4.5.2
* R-universe (Ubuntu 22.04, Windows Server 2022, macOS Sequoia)
* win-builder (devel and release)
* R-hub (macOS-arm64, Ubuntu-latest)
* GitHub Actions (macos-latest, ubuntu-latest, windows-latest)

## R CMD check results

On Windows and older Linux (R-universe, win-builder, GHA oldrel):
0 errors | 0 warnings | 1 notes

On MacOS and modern Linux (R-hub, GHA devel/release):
0 errors | 1 warning | 0 notes

### Explanations

1.  **WARNING (MacOS/Linux):** Found ‘_abort’, possibly from ‘abort’ (C).
    * This is a known false positive for Rust-based packages on modern toolchains. The symbol arises from the Rust standard library's panic handler fallback (`compiler_builtins`) during linking. The package is configured with `panic = "unwind"` in Cargo.toml and does not call abort() directly. The symbol is dead code that the linker did not strip in these specific environments.

2.  **NOTE:** Possibly misspelled words in DESCRIPTION: 'GeoJSON'
    * 'GeoJSON' is the correct capitalization.

## Downstream dependencies
There are currently no downstream dependencies for this package.