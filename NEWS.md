# fastgeojson 0.1.1

* **Build Stability:** Pinned the compilation environment to **Rust 1.75** via `rust-toolchain.toml`. This ensures strictly reproducible builds and maximizes compatibility with deployment servers like `shinyapps.io` and Posit Connect.
* **Documentation:** Added a "Deploying to shinyapps.io" guide to the README, detailing the necessary `.Rprofile` repository configuration.

# fastgeojson 0.1.0

* **Initial Release:** Introduced `fastgeojson`, a high-performance JSON and GeoJSON serializer backed by Rust.
* **Core Functions:**
    * Added `sf_geojson_str()`: Converts `sf` objects to GeoJSON FeatureCollections.
    * Added `df_json_str()`: Converts data frames to JSON arrays of objects.
* **Performance:** Implemented multi-threaded processing using the Rust `rayon` crate for massive datasets.
* **Integration:** Output strings are assigned the `json` and `geojson` classes to enable zero-copy transfer in Shiny and Plumber applications.