# fastgeojson 0.2.0

* **New Function:** Added `as_json()`, a high-performance, generic serializer that handles `sf` objects, data frames, lists, and atomic vectors. It serves as a parallelized, drop-in replacement for `jsonlite::toJSON()`.
* **Performance Engineering:**
    * **Geometry Arena:** Implemented a contiguous memory arena for spatial data, flattening nested R lists into a linear structure to eliminate allocation overhead during parallel processing.
    * **Direct-Heap Writing:** Switched to `ryu::raw` for floating-point formatting, writing bytes directly to the final memory buffer to bypass stack copies.
    * **LUT Escaping:** Implemented a static Look-Up Table (LUT) for string escaping, enabling O(1) scanning of characters.
    * **Loop Batching:** Optimized vector writes by batching JSON tokens (e.g., `",["`) to minimize capacity checks.

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