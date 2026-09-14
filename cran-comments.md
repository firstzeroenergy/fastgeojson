## Update from 0.1.3

`sf_geojson_str()` and `df_json_str()` are replaced by a single `as_json()`; the old names are removed. No reverse dependencies.

`Depends: R (>= 4.5)` — the package uses `VECTOR_PTR_RO`, `ANY_ATTRIB`, `Rf_charIsUTF8` and `Rf_charIsLatin1`, added in R 4.5.0.

## Compiled code

Rust via extendr 0.8.2. All crates are vendored in `src/rust/vendor.tar.xz` and built `--offline --locked`; authorship and licences are in `inst/AUTHORS` and `LICENSE.note`. The minimum Rust version, 1.71, is declared as `rust-version` in `Cargo.toml`. No non-API entry points are used.

## Test environments

* Windows 11, R 4.5.1 and R 4.6.1, Rtools 4.5, rustc 1.71.0 / 1.75.0 / 1.92.0
* Ubuntu 24.04, R 4.5.1 (rocker/geospatial:4.5.1), rustc 1.75.0

## R CMD check results

0 errors | 0 warnings | 0 notes
