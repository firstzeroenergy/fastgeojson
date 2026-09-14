<!-- Generated from README.Rmd by tools/build_readme.R. Do not edit by hand. -->

# fastgeojson <img src="man/figures/logo.png" align="right" height="138" />

**High-performance GeoJSON and JSON serialization for R**

`fastgeojson` converts `sf` objects to GeoJSON FeatureCollections and generic R objects (`data.frame`, lists, vectors) to JSON strings.

Implemented in Rust via **extendr**, it writes lossless numbers, runs in parallel, and delivers **6–27× speedups** over `jsonlite` and `geojsonsf` on large datasets at equal output, and 2.7–3.2× over `yyjsonr`.

Results are ready for Shiny, Plumber, `leaflet::addGeoJSON()`, and any package that talks to JavaScript.

> **Status: v0.3.0** — `as_json()` takes `jsonlite::toJSON()`'s arguments, in the same order, and follows its output conventions; two defaults differ: numbers are lossless, and `sf` objects become GeoJSON. See [Upgrading](#upgrading-from-01x-or-02x).

## Performance

Fastest of 7 runs, every package writing lossless numbers (`jsonlite` at `digits = I(17)`, the others at their defaults) and the same JSON. Reproduce with `Rscript tools/bench/readme_bench.R`.

### 1 million rows × 4 mixed columns

|Package                | Time_ms| Output_MB|Speedup vs jsonlite |
|:----------------------|-------:|---------:|:-------------------|
|jsonlite               |    1945|      61.6|—                   |
|jsonify                |    1293|      61.1|1.5×                |
|yyjsonr                |     233|      61.1|8.3×                |
|fastgeojson (1 thread) |     148|      61.1|13.1×               |
|fastgeojson            |      73|      61.1|26.6×               |

### 1 million point features

|Package                | Time_ms| Output_MB|Speedup vs geojsonsf |
|:----------------------|-------:|---------:|:--------------------|
|geojsonsf              |    1858|     150.8|—                    |
|yyjsonr                |     573|     150.8|3.2×                 |
|fastgeojson (1 thread) |     327|     150.8|5.7×                 |
|fastgeojson            |     176|     150.8|10.6×                |

### 10,000 polygons × 200 vertices

|Package                | Time_ms| Output_MB|Speedup vs geojsonsf |
|:----------------------|-------:|---------:|:--------------------|
|geojsonsf              |     597|      76.4|—                    |
|yyjsonr                |     245|      76.4|2.4×                 |
|fastgeojson (1 thread) |     196|      76.4|3.0×                 |
|fastgeojson            |      92|      76.4|6.5×                 |

`jsonlite`'s own default rounds to 4 decimal places, which is faster for it (1.1 s here) and 20% smaller; `as_json(x, digits = 4)` reproduces that output. Single-threaded, `fastgeojson` is 1.25–1.8× faster than `yyjsonr`.

Most of each call is R interning the result as a string, at about a nanosecond per byte. `as_bytes = TRUE` returns the bytes without it:

| | full call | `as_bytes = TRUE` | R's share |
|---|---|---|---|
| 1M rows × 4 columns | 72.1 ms | 11.7 ms | 84% |
| 1M point features | 176.8 ms | 22.3 ms | 87% |
| 10k polygons × 200 vertices | 91.5 ms | 13.3 ms | 85% |

## Correctness

Output follows `jsonlite`'s conventions: jsonlite's own `toJSON` test suite (jsonlite 2.0.0, in `tests/testthat/` with `toJSON()` bound to `as_json()`) passes with no failures and no skips, including its `sf` tests against GDAL's GeoJSON writer.

Two defaults differ. `jsonlite` rounds numbers to 4 decimal places; `as_json()` writes the shortest decimal that reads back as the same double (`digits = 4` gives `toJSON()`'s output). And `jsonlite` writes `sf` objects as a record array; `as_json()` writes a `FeatureCollection`.

```r
as_json(nc)                      # FeatureCollection  (default here)
as_json(nc, sf = "features")     # array of Feature objects
as_json(nc, sf = "dataframe")    # record array       (jsonlite default)

options(fastgeojson.sf = "dataframe")   # or switch the default globally
```

## Upgrading from 0.1.x or 0.2.x

`sf_geojson_str()` and `df_json_str()` — the whole API of 0.1.x — are gone; `as_json()` does both and dispatches on its input. Replace either with `as_json(x, ...)`.

Arguments are positional in jsonlite's order, so `as_json(df, "columns")` now means `dataframe = "columns"`. Named arguments are unaffected.

Output at the defaults changes where 0.2.2 differed from `toJSON()` — measured by serializing the same inputs with both versions:

| | 0.2.2 | 0.3.0 |
| :--- | :--- | :--- |
| missing value in a row-oriented frame | `"d":null` | key omitted; `na = "null"` keeps it |
| bare numeric `NA`, `NaN`, `Inf` | `null` | `"NA"`, `"NaN"`, `"Inf"`; `na = "null"` keeps `null` |
| matrix | flattened, `[1,2,3,4]` | nested by row, `[[1,3],[2,4]]` |
| control character in a string | `\u000A` | `\n` |
| FeatureCollection | no `name` | `"name":"sfdata"`, as GDAL writes it |
| whole coordinate | `3.0` | `3` |

Numbers are otherwise unchanged: 0.2.2 had no `digits` argument and always wrote them losslessly, which is now the default `digits = Inf`. Named vectors, `"json"`-class strings and time zones behave as before — the `keep_vec_names`, `json_verbatim` and `UTC` arguments are new, and their defaults reproduce 0.2.2. So are `Date` and `POSIXt`, which follow `jsonlite`: `POSIXt = "string"` uses `format()`, `"ISO8601"` emits `"2013-06-17T22:33:44"`, and `Date = "epoch"` returns days. Full list in `NEWS.md`.

## Installation

Requires R 4.5 or later.

```r
install.packages("fastgeojson")           # CRAN, once available
```

Development version and pre-compiled Windows/macOS binaries (no Rust required):

```r
options(repos = c(
  firstzero = "https://firstzeroenergy.r-universe.dev",
  CRAN = "https://cloud.r-project.org"
))
install.packages("fastgeojson")
```

**Deploying to shinyapps.io:** the CRAN version works automatically. For the R-universe version, add those same `options(repos = ...)` lines to the top of `app.R` or `global.R` so the build server can find the package.

## API

```r
as_json(
  x,
  dataframe = c("rows", "columns", "values"),
  matrix    = c("rowmajor", "columnmajor"),
  Date      = c("ISO8601", "epoch"),
  POSIXt    = c("string", "ISO8601", "epoch", "mongo"),
  factor    = c("string", "integer"),
  complex   = c("string", "list"),
  raw       = c("base64", "hex", "mongo", "int", "js"),
  null      = c("list", "null"),
  na        = c("null", "string"),
  auto_unbox = FALSE,
  digits     = Inf,
  pretty     = FALSE,
  force      = FALSE,
  ...
)
```

`as_json()` is the only encoder. It detects the input type and returns a length-one character vector of class `"json"`, or `c("geojson", "json")` for `sf` input — or a raw vector with `as_bytes = TRUE`.

Two options go beyond `toJSON()`:

```r
as_json(x, digits = Inf)      # the default: shortest decimal that round-trips exactly
as_json(x, as_bytes = TRUE)   # a raw vector instead of a character vector
```

`digits`: `Inf` (the default) is lossless; a number is decimal places, as in `toJSON()`; `I(n)` is significant digits; `NA` is `toJSON()`'s 15 significant digits.

`fastgeojson_threads(n)` sets the worker count (`1` disables parallelism, `0` restores automatic); the default is the whole machine, honouring `FASTGEOJSON_NUM_THREADS`, `RAYON_NUM_THREADS`, `OMP_NUM_THREADS` and `OMP_THREAD_LIMIT`. Output is identical at any thread count.

### as_bytes

Returns the same bytes as a raw vector, skipping the interning that is 84–87% of a large call. Use it when the JSON is leaving R and never needs to be an R string:

```r
con <- file("out.json", "wb")                          # a file
writeBin(as_json(x, as_bytes = TRUE), con); close(con)

res$body <- as_json(x, as_bytes = TRUE)                # httpuv, plumber, shiny
httr2::req_body_raw(req, as_json(x, as_bytes = TRUE), "application/json")
writeBin(as_json(x, as_bytes = TRUE), gzfile("out.json.gz", "wb"))
```

Writing an 18 MB result to a file takes 9 ms this way, against 101 ms through `writeLines()` on the character result and 415 ms through `jsonlite::write_json()`. It cannot be combined with `pretty`.

For htmlwidgets (`leaflet::addGeoJSON()`, `deckgl`, `mapdeck`) use the default: they splice a `"json"`-classed string into the payload verbatim, but base64-encode a raw vector.

## Usage

```r
library(sf)
library(fastgeojson)

nc <- st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)

json_out <- as_json(nc)
class(json_out)
#> [1] "geojson" "json"

library(leaflet)
leaflet() |> addTiles() |> addGeoJSON(json_out)
```

```r
df <- data.frame(id = 1:2, name = c("Alice", "Bob"), score = c(98.5, NA))

as_json(df)
#> [{"id":1,"name":"Alice","score":98.5},{"id":2,"name":"Bob"}]

as_json(df, dataframe = "columns")
#> {"id":[1,2],"name":["Alice","Bob"],"score":[98.5,"NA"]}
```

Row-oriented output omits missing fields; column-oriented output keeps array lengths aligned, as `jsonlite` does.

```r
as_json(list(val = 5))
#> {"val":[5]}

as_json(list(val = 5), auto_unbox = TRUE)
#> {"val":5}

as_json(list(meta = list(version = "1.0"), payload = c(10, 20)), auto_unbox = TRUE)
#> {"meta":{"version":"1.0"},"payload":[10,20]}

as_json(list(a = 1:2, b = list(c = "x")), pretty = TRUE)
#> {
#>   "a": [1, 2],
#>   "b": {
#>     "c": ["x"]
#>   }
#> }
```

Because `as_json()` returns pre-classed `json` strings, Shiny can hand them to the browser without re-encoding:

```r
observe({
  session$sendCustomMessage("updateMap", as_json(large_sf_object))
})
```

## Supported types

- **Geometries:** POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON, GEOMETRYCOLLECTION, in XY, XYZ, XYM and XYZM.
- **Vectors:** integer, double, logical, character, factor, complex, raw.
- **Structures:** list columns, nested lists, matrices, arrays, nested data frames, tibbles.
- **Time:** `Date`, `POSIXt`, `bit64::integer64`; `difftime` as its numeric value.
- **Encodings:** latin1 and native inputs are translated to UTF-8; escaping and the handling of invalid bytes match `toJSON()`.

The full `jsonlite::toJSON()` argument surface is supported.

## Implementation

- Rows and geometries are split into chunks by estimated work and serialized in parallel; small inputs stay serial.
- Numbers: the fixed-decimal path ports `jsonlite`'s `modp_dtoa2`; `digits = Inf` uses Żmij.
- Strings are escaped straight from R's storage, one copy each, through a 256-entry table.
- Geometries are flattened into contiguous coordinate arrays and written in one pass.
- Dates and timestamps are computed, not formatted: tens of nanoseconds per value against `format()`'s microseconds.
- The result is sized exactly and written once into its destination.

## Development

Rust in `src/rust/`, R interface in `R/`, benchmarks and parity verifiers in `tools/bench/`. Built with **extendr**. `FASTGEOJSON_PROFILE=1` prints per-phase timings.

Bug reports, feature requests, and contributions are very welcome.

## License

MIT © FirstZero Energy

