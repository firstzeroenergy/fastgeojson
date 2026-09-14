# fastgeojson 0.3.0

This release makes `as_json()` a genuine drop-in replacement for
`jsonlite::toJSON()`, and fixes several memory-safety defects found while
validating against jsonlite's own test suite.

## Breaking changes

* **`as_json()`'s signature now matches `jsonlite::toJSON()`** argument for
  argument, in the same order, and accepts `...`. Code that passed arguments
  positionally beyond `x` must be updated -- `as_json(df, "columns")` now means
  `dataframe = "columns"`, where previously position 2 was `auto_unbox`.
* Several defaults changed to match `toJSON()`:
  * `digits` is now `4` (was full `ryu` precision). Pass `digits = NA` for the
    shortest round-trippable representation.
  * `keep_vec_names` is now `FALSE` (was `TRUE`), so named atomic vectors
    become arrays rather than objects. Setting it to `TRUE` emits the same
    deprecation message jsonlite does.
  * `json_verbatim` is now `FALSE` (was `TRUE`).
  * `UTC` is now `FALSE` (was `TRUE`), so timestamps are no longer silently
    shifted to UTC.
* `POSIXt = "string"` (the default) now uses R's own `format()`, e.g.
  `"2013-06-17 22:33:44"`; `POSIXt = "ISO8601"` emits
  `"2013-06-17T22:33:44"` with no `Z` suffix. The two modes were previously
  swapped relative to jsonlite.
* `Date = "epoch"` now returns **days** since 1970-01-01, as jsonlite does. It
  previously returned milliseconds -- a factor of 86,400,000.

## Correctness

* **Character encoding.** Strings are now translated with
  `Rf_translateCharUTF8()` rather than read straight from `R_CHAR()`.
  latin1- and native-encoded values (including column names, factor levels and
  row names) previously emitted raw non-UTF-8 bytes, producing output that was
  neither valid UTF-8 nor parseable JSON. Strings marked `"bytes"` now raise the
  same error jsonlite raises instead of being passed through.
* **Escaping now matches jsonlite byte for byte:** short escapes for `\b`,
  `\t`, `\n`, `\f` and `\r`, lowercase `\u00xx` for other control bytes, and
  the solidus escaped only when it follows `<` (so an embedded `</script>`
  cannot terminate an enclosing script block).
* Whole doubles up to 2^53 no longer gain a spurious `.0` suffix.
* Nested data frames now emit `_row` for non-default row names.
* A data-frame-valued column (as produced by `tidyr::nest()`) is now encoded as
  one object per row. It was previously transposed, giving the first row the
  whole first column and later rows nothing.
* `raw` and `complex` vectors are now encoded rather than silently emitted as
  `{}`, with the `raw` and `complex` arguments honoured. `blob` vectors encode
  elementwise.
* Factor codes outside the declared levels no longer produce structurally
  invalid JSON.
* `difftime` and `integer64` are converted rather than reinterpreted;
  `integer64` values were previously emitted as garbage doubles.

## Memory safety

* **Fixed a segfault.** An `sf` object whose row names claimed more rows than
  the geometry column held caused `VECTOR_ELT` to dereference past the end of
  the list, terminating the R process. Such objects are now rejected with an
  error, and the geometry reader bounds-checks regardless.
* **Fixed an out-of-bounds heap read.** A data frame whose columns were shorter
  than its row count caused adjacent heap memory to be serialised into the JSON
  output. Columns now carry their length and every per-row read is bounds
  checked.
* **Removed undefined behaviour.** The output buffer was converted with
  `String::from_utf8_unchecked()` while it could contain non-UTF-8 bytes.
  Conversion is now checked whenever any non-ASCII input was seen.
* **Fixed several reachable panics**, including `NA` in character row names and
  `NA` in a `json`-classed column.
* **Bounded recursion.** Deeply nested lists terminated the R process with an
  uncatchable stack overflow; nesting beyond 5,000 levels now raises an
  ordinary R error.

## New features

* `fastgeojson_threads()` gets and sets the worker count. `n = 1` disables
  parallelism, which makes benchmarking reproducible. The default is the whole
  machine, honouring `FASTGEOJSON_NUM_THREADS`, `RAYON_NUM_THREADS`,
  `OMP_NUM_THREADS` and `OMP_THREAD_LIMIT`, and throttling to two threads when
  `R CMD check` is detected. Previously rayon's global pool took every core on
  the first call, even for a three-row data frame.
* `pretty` is supported, reproducing jsonlite's layout (scalar-only arrays stay
  on one line; objects expand).

## Build

* **Fixed the Windows aarch64 build.** `src/Makevars.win` derived the Rust
  target triple from make's `$(WIN)`, which is undefined on aarch64 and
  produced the malformed `--target=-pc-windows-gnu`. The triple now comes from
  `CARGO_BUILD_TARGET` when the environment supplies it, and otherwise from
  `R.version$arch`, via `tools/config.R` and a new `configure.win`.
* Bumped extendr to 0.8.2, which adds the aarch64-on-Windows support that
  0.8.1 lacked ("Cannot build extendr-ffi for unknown architecture").
* Removed `rust-toolchain.toml`. Its pin to 1.75.0 was inert (cargo runs from
  `src/`, so the override never applied) and 1.75.0 has no
  `aarch64-pc-windows-gnullvm` artifacts. The minimum version is now declared
  as `rust-version` in `Cargo.toml`, which cargo enforces without downloading a
  toolchain.
* `.Rbuildignore` now excludes `.RData*`, build artefacts and the generated
  `Makevars`. The source tarball was 41 MB because a stray `.RDataTmp` was
  being shipped; it is now 1.4 MB.
* `inst/AUTHORS` and `LICENSE.note` now list all 17 vendored crates with the
  versions recorded in `Cargo.lock`, and correctly note the `Unicode-3.0` term
  that `unicode-ident` carries.
* Added `cleanup` / `cleanup.win`, and made the post-link cleanup depend on
  `$(SHLIB)` so it cannot race under `make -j`.
* Vendored crates build offline with `--locked` and `-j 2`.

## Full jsonlite argument coverage

Every `toJSON()` argument is now implemented: `dataframe = "values"`,
`matrix = "columnmajor"`, `POSIXt = "mongo"`, `raw = "mongo"`,
`complex = "list"` (including inside a data frame, in both row and column
orientation), `always_decimal` and `pretty`.

The one deliberate deviation is the `sf` default. jsonlite 2.0.0 defaults sf
objects to `sf = "dataframe"` (a record array); `as_json()` defaults to
`sf = "geojson"`, because emitting a `FeatureCollection` for an sf object is
the purpose of this package and what existing callers depend on. All three
modes are byte-identical to jsonlite when requested explicitly, and
`options(fastgeojson.sf = "dataframe")` restores jsonlite's default globally.

Parity is verified by jsonlite's own toJSON test suite, ported into
`tests/testthat`: **907 expectations, no failures and no skips**.

# fastgeojson 0.2.2

* **Dataframe Orientation:** Added the `dataframe` argument to toggle between row-oriented output (default, `[{...}]`) and column-oriented output (`{...}`).
* **Context-Aware NA Handling:** Implemented "Smart" default logic for missing values to match standard R conventions:
    * **Default:** In column mode, numeric `NA`s are coerced to `"NA"` strings to maintain array type homogeneity, while other types default to `null`. In row mode, `NA` values are omitted to reduce payload size.
    * **Explicit:** Specifying `na = "null"` or `na = "string"` strictly enforces the requested format regardless of the data type or structure.
* **Null Value Control:** Added the `null` argument to control the serialization of `NULL` (empty) values in lists, supporting coercion to empty containers (`"list"`, default) or explicit JSON `null`.

# fastgeojson 0.2.1

* **Scalar Serialization Support:** Extended the `as_json()` interface to include the `auto_unbox` argument, enabling the serialization of length-one atomic vectors as native JSON scalars. This provides deterministic control over data typing, allowing specific distinction between singleton arrays and primitive scalar values in the output payload.

# fastgeojson 0.2.0

* **New Function:** Added `as_json()`, a high-performance, generic serializer that handles `sf` objects, data frames, lists, and atomic vectors. It serves as a parallelized, drop-in replacement for `jsonlite::toJSON()`.
* **Performance Engineering:**
    * **Geometry Arena:** Implemented a contiguous memory arena for spatial data, flattening nested R lists into a linear structure to eliminate allocation overhead during parallel processing.
    * **Direct-Heap Writing:** Switched to `ryu::raw` for floating-point formatting, writing bytes directly to the final memory buffer to bypass stack copies.
    * **LUT Escaping:** Implemented a static Look-Up Table (LUT) for string escaping, enabling O(1) scanning of characters.
    * **Loop Batching:** Optimized vector writes by batching JSON tokens (e.g., `",["`), to minimize capacity checks.

# fastgeojson 0.1.2

* Fixed `_abort` symbol warnings on macOS and Linux by implementing proper Rust build artifact cleanup.
* Achieved clean compilation (0 warnings) across Windows, macOS, and Linux.

# fastgeojson 0.1.1

* **Build Stability:** Pinned the compilation environment to **Rust 1.75** via `rust-toolchain.toml`. This ensures strictly reproducible builds and maximizes compatibility with deployment servers like `shinyapps.io` and Posit Connect.
* **Documentation:** Added a "Deploying to shinyapps.io" guide to the README.

# fastgeojson 0.1.0

* **Initial Release:** Introduced `fastgeojson`, a high-performance JSON and GeoJSON serializer backed by Rust.
* **Core Functions:**
    * Added `sf_geojson_str()`: Converts `sf` objects to GeoJSON FeatureCollections.
    * Added `df_json_str()`: Converts data frames to JSON arrays of objects.
* **Performance:** Implemented multi-threaded processing using the Rust `rayon` crate for massive datasets.
* **Integration:** Output strings are assigned the `json` and `geojson` classes to enable zero-copy transfer in Shiny and Plumber applications.
