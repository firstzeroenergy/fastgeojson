
# fastgeojson <img src="man/figures/logo.png" align="right" height="138" />

**High-performance GeoJSON and JSON serialization for `sf` and tabular
objects**

`fastgeojson` provides extremely fast conversion of `sf` objects to
GeoJSON FeatureCollection strings and rectangular tabular objects
(`data.frame`, `data.table`, `tibble`) to JSON arrays of row objects.

Implemented in Rust via the **extendr** framework, it uses parallel
processing and low-level optimizations to deliver **2.4–16× speedups**
over existing R solutions on large datasets.

The resulting strings are ready for immediate use in web applications
(Shiny, Plumber), direct integration with `leaflet::addGeoJSON()`, and
other R packages that interface with JavaScript.

> **Status: v0.1.2** — Stable for production use and compatible with
> `shinyapps.io`. This is an early release of `fastgeojson`, so we are
> actively looking for edge cases; bug reports and feature requests are
> very welcome.

## Performance Benchmarks

Benchmarks conducted with `microbenchmark` (times in milliseconds; lower
is better).

### JSON serialization: 1 million rows × 4 mixed columns

| Package     | Median_ms | Speedup.vs.yyjsonr |
|:------------|----------:|:-------------------|
| jsonify     |      1573 | —                  |
| jsonlite    |      1281 | —                  |
| yyjsonr     |       235 | 1×                 |
| fastgeojson |        98 | 2.4×               |

### GeoJSON serialization: 1 million point features

| Package     | Median_ms | Speedup.vs.yyjsonr |
|:------------|----------:|:-------------------|
| geojsonsf   |      1920 | —                  |
| yyjsonr     |       586 | 1×                 |
| fastgeojson |       238 | 2.5×               |

## Installation

### From CRAN (Recommended)

Once available on CRAN, you can install the stable version directly:

``` r
install.packages("fastgeojson")
```

### Development Version (R-universe)

To install the latest development version or pre-compiled binaries for
Windows/macOS (no Rust required) before the CRAN release:

``` r
options(repos = c(
  firstzero = "https://firstzeroenergy.r-universe.dev",
  CRAN = "https://cloud.r-project.org"
))
install.packages("fastgeojson")
```

## Deploying to shinyapps.io

**Note:** If you are using the CRAN version, deployment works
automatically.

If you are using the **development version** from R-universe, you must
tell shinyapps.io where to find the package. **The Fix:** Add the
following lines to the very top of your `app.R` (or `global.R`) file.

``` r
options(repos = c(
  firstzero = "https://firstzeroenergy.r-universe.dev",
  CRAN = "https://cloud.r-project.org"
))
```

This ensures the build server can find and install `fastgeojson` from
the custom repository.

## Main Features of the Rust Implementation

The core performance advantages come from a carefully designed Rust
backend:

- **Parallel chunked processing** using `rayon`: large datasets are
  split into chunks (default ~2048 rows) and processed in parallel
  across available CPU cores.
- **Zero-allocation JSON writing** where possible: a custom `JsonWriter`
  builds output directly into a pre-allocated `Vec<u8>` using fast
  number formatting (`ryu` for floats, `itoa` for integers) and manual
  byte pushing.
- **Thread-safe column preparation**: properties are pre-processed into
  structures that can be safely shared across threads (e.g., string
  arenas for character columns, cached escaped factor levels).
- **Direct access to R vector data**: uses raw pointers to R’s internal
  vectors (INTEGER, REAL, etc.) to avoid copying.
- **Specialized geometry paths**: separate optimized writers for each
  geometry type (Point, MultiPoint, LineString, etc.) with unrolled
  coordinate serialization.
- **Compact output**: `NA` values in properties are omitted rather than
  written as `null`, reducing payload size.

These low-level optimizations eliminate the bottlenecks found in
general-purpose JSON libraries while preserving full compatibility with
R’s data model.

## Integration with Shiny

Because `fastgeojson` returns pre-classed `json` strings, you can bypass
R’s internal serialization when sending data to the browser.

``` r
# FAST: Direct handoff to Leaflet or deck.gl
observe({
  # fastgeojson serializes in Rust
  json_data <- sf_geojson_str(large_sf_object)
  
  # Sent directly to client without re-encoding
  session$sendCustomMessage("updateMap", json_data)
})
```

## Key Functions

- `sf_geojson_str(x)` → GeoJSON FeatureCollection string (class
  `"geojson" "json"`)
- `df_json_str(x)` → JSON array of row objects (class `"json"`)

Both return a single character string and include extensive input
validation.

## Usage Examples

### Converting `sf` to GeoJSON

``` r
library(sf)
library(fastgeojson)

nc <- st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)

geojson <- sf_geojson_str(nc)

class(geojson)
#> [1] "geojson" "json"

cat(substr(geojson, 1, 120))
```

Direct use in Leaflet:

``` r
library(leaflet)

leaflet() %>%
  addTiles() %>%
  addGeoJSON(geojson, fillOpacity = 0.6)
```

### Converting tabular data to JSON

``` r
library(fastgeojson)

df <- data.frame(
  id = 1:3,
  name = c("Alice", "Bob", "Charlie"),
  value = c(10.5, 20.1, NA),
  active = c(TRUE, FALSE, TRUE),
  stringsAsFactors = FALSE
)

json <- df_json_str(df)

class(json)
#> [1] "json"

cat(substr(json, 1, 120))
```

## Supported Features

- Column types: integer, double, logical, character, factor
- Dates / POSIXct: Serialized as seconds since Epoch (1970-01-01)
- Missing values: `NA` in properties omitted for compact output
- Geometries: POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON,
  MULTIPOLYGON
- Mixed geometry collections fully supported

## Known Limitations

To prioritize stability and speed, the following complex types are
currently not supported. Future updates may introduce support for these
types, provided they do not compromise encoding performance.

- List-Columns: Columns containing nested lists (e.g.,
  `c("tag1", "tag2")` inside a cell) are omitted from the output.
  - *Workaround:* Flatten list columns into strings before conversion.

  ``` r
  df$tags <- sapply(df$tags, paste, collapse = ", ")
  ```
- POSIXlt: The `POSIXlt` format is not supported due to its underlying
  list-based structure.
  - *Workaround:* Convert to standard `POSIXct` or Character.

  ``` r
  df$time_col <- as.POSIXct(df$time_col)
  ```

## Development

- Rust implementation: `src/rust/` (managed via `rust-toolchain.toml`
  for reproducible builds)
- R interface: `R/`
- Built with **extendr**

Bug reports, feature requests, and contributions are very welcome

## License

MIT © FirstZero Energy
