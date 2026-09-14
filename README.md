<!-- Generated from README.Rmd by tools/build_readme.R. Do not edit by hand. -->

# fastgeojson <img src="man/figures/logo.png" align="right" height="138" />

**High-performance GeoJSON and JSON serialization for R**

`fastgeojson` converts `sf` objects to GeoJSON FeatureCollections and generic R objects (`data.frame`, lists, vectors) to JSON strings.

Implemented in Rust via **extendr**, it delivers **13–17× speedups** over existing R solutions on large datasets, with output byte-for-byte identical to `jsonlite::toJSON()`.

Results are ready for Shiny, Plumber, `leaflet::addGeoJSON()`, and any package that talks to JavaScript.

> **Status: v0.3.0** — `as_json()` takes `jsonlite::toJSON()`'s arguments, in the same order, with the same defaults. See [Upgrading](#upgrading-from-02x).

## Performance

Fastest of 7 runs, **default arguments for every package**. Reproduce with `Rscript tools/bench/readme_bench.R`.

### 1 million rows × 4 mixed columns

|Package                | Time_ms| Output_MB|Speedup vs jsonlite |
|:----------------------|-------:|---------:|:-------------------|
|jsonify                |    1361|      61.1|0.7×                |
|jsonlite               |    1014|      49.4|—                   |
|yyjsonr                |     233|      61.1|4.4×                |
|fastgeojson (1 thread) |     152|      49.4|6.7×                |
|fastgeojson            |      59|      49.4|17.2×               |

### 1 million point features

|Package                | Time_ms| Output_MB|Speedup vs geojsonsf |
|:----------------------|-------:|---------:|:--------------------|
|geojsonsf              |    2076|     150.8|—                    |
|yyjsonr                |     578|     150.8|3.6×                 |
|fastgeojson (1 thread) |     327|     119.8|6.3×                 |
|fastgeojson            |     144|     119.8|14.4×                |

### 10,000 polygons × 200 vertices

|Package     | Time_ms| Output_MB|Speedup vs geojsonsf |
|:-----------|-------:|---------:|:--------------------|
|geojsonsf   |     605|      76.4|—                    |
|yyjsonr     |     245|      76.4|2.5×                 |
|fastgeojson |      45|      37.6|13.4×                |

Output size is shown because packages that write shortest-round-trip numbers emit larger payloads for the same input; `fastgeojson` matches `jsonlite` exactly.

Most of what is left in those figures is not serialization. The same calls with `as_bytes = TRUE`, which returns the bytes without interning them as an R string, separate the two:

| | full call | `as_bytes = TRUE` | R's share |
|---|---|---|---|
| 1M rows × 4 columns | 54.0 ms | 11.9 ms | 78% |
| 1M point features | 123.6 ms | 15.6 ms | 87% |
| 10k polygons × 200 vertices | 44.7 ms | 7.0 ms | 84% |

R charges about a nanosecond per byte to build a character vector, because it scans and hashes every byte to intern it in the CHARSXP cache. No serializer can go under that: *R Internals* requires every CHARSXP to be made through `mkCharLenCE`. See [`as_bytes`](#as_bytes) for when you can skip it entirely.

## Correctness

jsonlite's own `toJSON` test suite runs against `as_json()` — the test files from jsonlite 2.0.0 live in `tests/testthat/` with `toJSON()` bound to `as_json()`. **907 expectations pass, no failures, no skips**, including jsonlite's `sf` tests, which validate against GDAL's GeoJSON writer.

One deliberate difference: `jsonlite` 2.0.0 defaults `sf` objects to a record array. `as_json()` defaults to a `FeatureCollection`.

```r
as_json(nc)                      # FeatureCollection  (default here)
as_json(nc, sf = "features")     # array of Feature objects
as_json(nc, sf = "dataframe")    # record array       (jsonlite default)

options(fastgeojson.sf = "dataframe")   # or switch the default globally
```

## Upgrading from 0.2.x

Arguments are positional in jsonlite's order, so `as_json(df, "columns")` now means `dataframe = "columns"`. Named arguments are unaffected.

Four defaults now match `toJSON()`, so output can differ from 0.2.x:

| Argument | 0.2.x | 0.3.0 |
| :--- | :--- | :--- |
| `digits` | `NULL` | `4` — pass `digits = NA` for full precision |
| `keep_vec_names` | `TRUE` | `FALSE` — named vectors become arrays |
| `json_verbatim` | `TRUE` | `FALSE` |
| `UTC` | `TRUE` | `FALSE` — timestamps keep their own time zone |

`Date` and `POSIXt` follow `jsonlite`: `POSIXt = "string"` uses `format()`, `"ISO8601"` emits `"2013-06-17T22:33:44"`, and `Date = "epoch"` returns days. Full list in `NEWS.md`.

`sf_geojson_str()` and `df_json_str()` are gone; `as_json()` does both and dispatches on its input. Replace either with `as_json(x, ...)`.

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
  digits     = 4,
  pretty     = FALSE,
  force      = FALSE,
  ...
)
```

`as_json()` is the only encoder. It detects the input type and returns a length-one character vector of class `"json"`, or `c("geojson", "json")` for `sf` input — or a raw vector with `as_bytes = TRUE`.

Two options go beyond `toJSON()`:

```r
as_json(x, digits = Inf)      # shortest decimal that round-trips exactly
as_json(x, as_bytes = TRUE)   # a raw vector instead of a character vector
```

`digits = Inf` is the only lossless setting; `digits = NA` matches `toJSON()`, which keeps 15 significant digits, so `pi` becomes `3.14159265358979`.

`fastgeojson_threads(n)` sets the worker count: `1` disables parallelism, `0` restores automatic. The default is the whole machine, honouring `FASTGEOJSON_NUM_THREADS`, `RAYON_NUM_THREADS`, `OMP_NUM_THREADS` and `OMP_THREAD_LIMIT`. Output is identical at any thread count.

### as_bytes

Returns the same bytes as a raw vector instead of a character vector, skipping the interning that is 78–87% of a large call. Use it when the JSON is leaving R and never needs to be an R string:

```r
con <- file("out.json", "wb")                          # a file
writeBin(as_json(x, as_bytes = TRUE), con); close(con)

res$body <- as_json(x, as_bytes = TRUE)                # httpuv, plumber, shiny
httr2::req_body_raw(req, as_json(x, as_bytes = TRUE), "application/json")
writeBin(as_json(x, as_bytes = TRUE), gzfile("out.json.gz", "wb"))
```

Writing a 17.7 MB result to a file takes 15 ms this way, against 117 ms through `writeLines()` on the character result and 488 ms through `jsonlite`. The character route pays the interning and then walks the string again.

Below about a megabyte of output it is not worth thinking about. It is not a route to a string, since `rawToChar()` pays the cost straight back, and `pretty` needs a string to indent, so that combination is an error rather than a silent fallback.

For **htmlwidgets — `leaflet::addGeoJSON()`, `deckgl`, `mapdeck` — use the default.** Its `"json"` class is what makes htmlwidgets splice the text into the widget payload verbatim; a raw vector is base64-encoded instead, which the browser cannot use and which is 37% larger. `as_bytes` fits the other pattern, where the map fetches the GeoJSON from a URL rather than carrying it inline.

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

Row-oriented output omits missing fields; column-oriented output keeps array lengths aligned. Both match `jsonlite`.

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

- **Geometries:** POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON, GEOMETRYCOLLECTION, in XY, XYZ, XYM and XYZM. Any `sfc` is encoded as geometry wherever it appears.
- **Vectors:** integer, double, logical, character, factor, complex, raw.
- **Structures:** list columns, nested lists, matrices, arrays, nested data frames, tibbles.
- **Time:** `Date`, `POSIXt`, `bit64::integer64` (exact digits, as `toJSON()` emits them). `difftime` is written as its numeric value, which `toJSON()` only does under `force = TRUE`.
- **Encodings:** latin1 and native inputs are translated to UTF-8 and escaping matches `jsonlite` byte for byte. Bytes that are not valid UTF-8 pass through unchanged, exactly as `toJSON()` passes them; a `"bytes"`-marked string raises the same error `toJSON()` raises.

The full `jsonlite::toJSON()` argument surface is supported.

## Implementation

- **Work-aware parallel chunking** — chunk size comes from estimated work (columns, sampled string lengths, sampled geometry cost), so wide frames and polygon layers parallelise as readily as tall numeric ones. Small inputs stay serial.
- **Number formatting matched to `jsonlite`** — the fixed-decimal path ports the same `modp_dtoa2` algorithm, so output agrees by construction; `%g` supplies the rest, taking its digits from `ryu` where rounding them provably gives the same answer, and `digits = Inf` is `ryu` outright.
- **Strings escaped in place** — where a column's bytes are already valid UTF-8, workers escape R's `CHARSXP` data directly, copying each string once, in parallel.
- **Table-driven escaping** — a 256-entry table probed eight bytes at a time, so a clean string costs one scan and one bulk copy.
- **Direct access to R vectors** — raw pointers into `INTEGER`, `REAL`, `LOGICAL`, `STRING_PTR_RO`, with lengths carried alongside.
- **Flat geometry arenas** — nested `sf` structures are flattened into contiguous coordinate arrays, letting specialised writers emit millions of coordinates in one pass.
- **Dates and timestamps computed, not formatted** — `Date` is calendar arithmetic and needs no time zone; `POSIXct` needs R only for its UTC offset. R's `format()` costs about 5 microseconds per value, against tens of nanoseconds here.
- **One pass over the output** — chunk offsets are known once the workers finish, so the result is sized exactly and written once, straight into its destination.

## Development

Rust in `src/rust/`, R interface in `R/`, benchmarks and parity verifiers in `tools/bench/`. Built with **extendr**. `FASTGEOJSON_PROFILE=1` prints per-phase timings.

Bug reports, feature requests, and contributions are very welcome.

## License

MIT © FirstZero Energy

