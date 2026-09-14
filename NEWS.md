# fastgeojson 0.3.0

## Breaking changes

* `sf_geojson_str()` and `df_json_str()` are removed. `as_json()` replaces both and dispatches on its input.
* `as_json()` takes `jsonlite::toJSON()`'s arguments in the same order, so positional arguments after `x` change meaning, and follows jsonlite's output conventions. Two defaults differ: numbers are lossless (`digits = Inf`; `digits = 4` gives `toJSON()`'s output) and `sf` objects become GeoJSON (`sf = "geojson"`).
* Requires R 4.5 or later.

## Fixes

* Memory safety: worker threads read no attributes and hold no pointers into objects the call created; `gctorture` tests cover the cases that used to fail.
* `Date`, `POSIXt`, `factor`, `complex`, `raw`, matrices, nested and list-column frames, `integer64` and non-UTF-8 encodings now follow `toJSON()`; jsonlite's own test suite runs against `as_json()` with no failures.

## Performance

* 20–31% faster than 0.2.2 at identical output, and faster than yyjsonr single-threaded on every shape measured. Shortest-digit output uses Żmij.
* `as_bytes = TRUE` returns a raw vector, skipping R's string interning; `fastgeojson_threads()` sets the worker count.

## Build

* All Rust crates are vendored; minimum Rust 1.71; no non-API R entry points; Windows on aarch64 builds.

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

# fastgeojson 0.1.3

* Fixed a parallel build race during Rust compilation on some CRAN check platforms by ensuring Rust build artifact cleanup runs only after package linking completes.

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
