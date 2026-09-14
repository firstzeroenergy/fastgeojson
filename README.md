<!-- Generated from README.Rmd by tools/build_readme.R. Do not edit by hand. -->

# fastgeojson <img src="man/figures/logo.png" align="right" height="138" />

**High-performance GeoJSON and JSON serialization for R**

`fastgeojson` converts `sf` objects to GeoJSON FeatureCollections and generic R objects (`data.frame`, lists, vectors) to JSON strings.

Implemented in Rust via **extendr**, it delivers **6–27× speedups** over `jsonlite` and `geojsonsf` on large datasets at equal output, and 2.7–3.2× over `yyjsonr`, with output byte-for-byte identical to `jsonlite::toJSON()` under the same arguments.

Results are ready for Shiny, Plumber, `leaflet::addGeoJSON()`, and any package that talks to JavaScript.

> **Status: v0.3.0** — `as_json()` takes `jsonlite::toJSON()`'s arguments, in the same order. Two defaults differ, both deliberately: numbers are lossless, and `sf` objects become GeoJSON. See [Upgrading](#upgrading-from-01x-or-02x).

## Performance

Fastest of 7 runs, **every package writing lossless numbers**: `jsonlite` at `digits = I(17)`, its only exact setting; the others at their defaults, which already are. The outputs are the same JSON — `yyjsonr`'s is byte-identical to ours, `jsonify` and `geojsonsf` write 17 digits for the few values whose shortest form is 16, and our FeatureCollections carry the `name` member GDAL writes. Reproduce with `Rscript tools/bench/readme_bench.R`.

### 1 million rows × 4 mixed columns

|Package                | Time_ms| Output_MB|Speedup vs jsonlite |
|:----------------------|-------:|---------:|:-------------------|
|jsonlite               |    1965|      61.6|—                   |
|jsonify                |    1297|      61.1|1.5×                |
|yyjsonr                |     228|      61.1|8.6×                |
|fastgeojson (1 thread) |     206|      61.1|9.5×                |
|fastgeojson            |      72|      61.1|27.3×               |

### 1 million point features

|Package                | Time_ms| Output_MB|Speedup vs geojsonsf |
|:----------------------|-------:|---------:|:--------------------|
|geojsonsf              |    1848|     150.8|—                    |
|yyjsonr                |     571|     150.8|3.2×                 |
|fastgeojson (1 thread) |     460|     150.8|4.0×                 |
|fastgeojson            |     176|     150.8|10.5×                |

### 10,000 polygons × 200 vertices

|Package                | Time_ms| Output_MB|Speedup vs geojsonsf |
|:----------------------|-------:|---------:|:--------------------|
|geojsonsf              |     553|      76.4|—                    |
|fastgeojson (1 thread) |     288|      76.4|1.9×                 |
|yyjsonr                |     244|      76.4|2.3×                 |
|fastgeojson            |      90|      76.4|6.1×                 |

Output size is shown so that a difference in bytes is visible rather than assumed. At its default of 4 decimal places `jsonlite` takes 1.1 s on this table instead of 2.0 s and writes 20% fewer bytes; `as_json(x, digits = 4)` reproduces those bytes exactly. Single-threaded, `fastgeojson` and `yyjsonr` are level — both sit close to the floor described below — and the margin between them is the parallel serializer.

Most of what is left in those figures is not serialization. The same calls with `as_bytes = TRUE`, which returns the bytes without interning them as an R string, separate the two:

| | full call | `as_bytes = TRUE` | R's share |
|---|---|---|---|
| 1M rows × 4 columns | 72.6 ms | 12.1 ms | 83% |
| 1M point features | 179.6 ms | 26.3 ms | 85% |
| 10k polygons × 200 vertices | 92.5 ms | 11.9 ms | 87% |

R charges about a nanosecond per byte to build a character vector, because it scans and hashes every byte to intern it in the CHARSXP cache. No serializer can go under that: *R Internals* requires every CHARSXP to be made through `mkCharLenCE`. See [`as_bytes`](#as_bytes) for when you can skip it entirely.

## Correctness

jsonlite's own `toJSON` test suite runs against `as_json()` — the test files from jsonlite 2.0.0 live in `tests/testthat/` with `toJSON()` bound to `as_json()`. **over 900 expectations pass, no failures, no skips** (the exact count varies with the platform locale), including jsonlite's `sf` tests, which validate against GDAL's GeoJSON writer.

Two deliberate differences in defaults. `jsonlite` rounds numbers to 4 decimal places; `as_json()` writes the shortest decimal that reads back as the same double, so `pi` is `3.141592653589793` and a map projection's `0.000151481324748` is not `0.0002`. Pass `digits = 4` for `toJSON()`'s output. And `jsonlite` 2.0.0 defaults `sf` objects to a record array; `as_json()` defaults to a `FeatureCollection`.

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

`digits = Inf` is the only lossless setting. A number is decimal places, as in `toJSON()`, whose default `4` turns `pi` into `3.1416`; `I(n)` is significant digits; `NA` is `toJSON()`'s 15 significant digits, `3.14159265358979`.

`fastgeojson_threads(n)` sets the worker count: `1` disables parallelism, `0` restores automatic. The default is the whole machine, honouring `FASTGEOJSON_NUM_THREADS`, `RAYON_NUM_THREADS`, `OMP_NUM_THREADS` and `OMP_THREAD_LIMIT`. Output is identical at any thread count.

### as_bytes

Returns the same bytes as a raw vector instead of a character vector, skipping the interning that is 83–87% of a large call. Use it when the JSON is leaving R and never needs to be an R string:

```r
con <- file("out.json", "wb")                          # a file
writeBin(as_json(x, as_bytes = TRUE), con); close(con)

res$body <- as_json(x, as_bytes = TRUE)                # httpuv, plumber, shiny
httr2::req_body_raw(req, as_json(x, as_bytes = TRUE), "application/json")
writeBin(as_json(x, as_bytes = TRUE), gzfile("out.json.gz", "wb"))
```

Writing an 18 MB result to a file takes 9 ms this way, against 101 ms through `writeLines()` on the character result; `jsonlite::write_json()` takes 415 ms end to end. The character route pays the interning and then walks the string again.

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

