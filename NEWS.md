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
  * `digits` is now `4` (was full `ryu` precision). `digits = NA` matches
    `toJSON()`, which is 15 significant digits; pass `digits = Inf` for the
    shortest representation that round-trips exactly.
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

### Further parity fixes

* A logical matrix column emitted `null` for every cell, and so did a
  character matrix column: only integer and double were handled and everything
  else fell through to a null.
* A `NaN` inside a numeric matrix printed as `"NA"` rather than `"NaN"`.
* An array column with more than two dimensions emitted one number per row
  instead of the row's slice, because the detection required exactly two
  dimensions. It now nests over dimensions 2..N with the last innermost.
* A matrix column in a data frame ignored `digits` entirely, formatting
  straight through `ryu`.
* Converting a complex matrix column dropped its `dim`, so the result was
  rejected with "replacement has 6 rows, data has 2". `toJSON()` handles that
  input.
* `read_coord_ptr` called `REAL()` with no type check. `REAL()` raises an R
  error on anything else, which longjmps over Rust frames: a malformed
  geometry leaked about 1.8 KB per call and never reached a destructor. It is
  now guarded, and a coordinate object that is not a plain double vector goes
  through the recursive writer, which is what `toJSON()` does with it -- nine
  such shapes went from an error to matching byte for byte.
* A zero-length coordinate vector emitted a null geometry rather than
  `{"type":"Point","coordinates":[]}`. A real `POINT EMPTY` from `st_point()`
  is two NAs and never took that branch.

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
* `digits = Inf` writes the shortest decimal that reads back as the same
  double. It is the only lossless setting: `digits = NA` keeps 15 significant
  digits, so `pi` becomes `3.14159265358979`, and about 94% of doubles arising
  from real arithmetic need 16 or 17. `digits = 22` is exact but always writes
  17 digits, which is 2 to 63% more output than necessary depending on the
  data. `toJSON()` warns and falls back to `digits = NA` for a non-integer
  `digits`, so nothing that works against jsonlite changes meaning.
* `as_bytes = TRUE` returns a raw vector instead of a character vector.
  R creates a character vector at about 1 GB/s, because it hashes every byte
  to intern the string in its global CHARSXP cache; for a 49 MB result that is
  52 ms, which profiling put at three quarters of the total time for a
  million-row frame. A caller writing the JSON to a socket or a file never
  needed it interned. Incompatible with `pretty`, which needs a string.
* `FASTGEOJSON_PROFILE=1` makes each call print its phase timings to stderr.

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

## Performance

Serialization was profiled and reworked against a 21-shape benchmark corpus
(`tools/bench/`), measured at one thread and at full width, with every timed
call checked against a reference digest so a change that altered the output
is reported as a failure rather than a speedup.

Single-threaded throughput, which is the algorithmic number:

| shape | before | after | |
|---|---|---|---|
| nested list | 6.1 MB/s | 171.6 | **28x** |
| data frame with a list column | 20.9 | 251.3 | **12x** |
| plain double vector | 31.2 | 239.5 | 7.7x |
| `sf` polygons | 38.3 | 273.0 | 7.1x |
| `sf` linestrings | 40.7 | 275.4 | 6.8x |
| wide frame (200 columns) | 56.8 | 296.0 | 5.2x |
| tall narrow frame | 56.1 | 282.9 | 5.0x |
| numeric frame | 61.4 | 288.0 | 4.7x |

At full width, the wide frame reaches 759 MB/s (13x its old figure) and
polygon layers 721 MB/s (19x), both of which previously ran on a single core.

### Date and POSIXct

Both were formatted by R, at 4.7 and 5.3 microseconds per value against about
25 nanoseconds for every other type, which made a timestamp column two orders
of magnitude slower than the rest of the frame and no faster than jsonlite.

A `Date` needs no time zone, so nothing about it requires R: day numbers are
converted with `civil_from_days` and written directly. A `POSIXct` needs R
only for the UTC offset, which `as.POSIXlt()` supplies for 0.07 microseconds
per value; R now hands the writer local civil seconds and the rest is
arithmetic.

| 200k values, one column | before | after | vs jsonlite |
|---|---|---|---|
| `Date` | 952 ms | 4.97 ms | 1.0x to 200.6x |
| `POSIXct` | 1066 ms | 29.9 ms | 1.1x to 38.1x |

Both are verified against R across their whole admissible range:
`tools/bench/verify_date_parity.R` checks 325k values, including every day
from 1870 to 2070, the century and 400-year leap rules and the exact point at
which R's own `format()` gives up; `verify_time_parity.R` covers 13 time
zones, every hour of a year across four of them so both DST transitions are
straddled to the second, and instants 95 years either side of the epoch.

### Number formatting

`write_fixed_decimals`, the hottest function in the package, extracted decimal
digits one per division. It now takes two at a time from a 200-byte pair
table: 25% faster on integral values, 12.5% on large magnitudes, 9.6% on
coordinates.

`write_g_format`, which `digits = NA` and any value below 1e-5 or above 2^31
reach, called `format!()` to get scientific notation and again for fixed
notation -- two heap allocations and two runs of `core::fmt`'s float
conversion per value. The scientific form now goes into a stack buffer and the
fixed form is derived from that mantissa by moving the decimal point, which is
the same rounding. 393 to 136 ns per value.

### Strings

A character column's parallel prepass established only whether the workers
could read R's bytes directly, then each worker called `Rf_xlength` -- a
cross-DLL call per cell -- and scanned the bytes again for escapes. The
prepass now records length, NA and needs-escaping in four bytes per cell, so
the common case is a quote, one memcpy and a quote: 24% faster on short
strings, 20% on 40-character strings, 10% on URLs.

It also no longer abandons a column it cannot fully read. Cells needing
translation are translated on the R thread and only those cells, so one latin1
value among 200,000 ASCII ones costs what a clean column costs rather than
45% more.

### Assembly

Phase timing on a million-row frame at 32 workers showed the serializer taking
4 ms and scaling 16.6x from a single worker, while handing the result back to R
took 52 ms and assembling the chunks 11 ms. The scaling curve had plateaued at
2.7x, which fits Amdahl with a serial fraction of 0.37 -- it was never the
parallel region.

The chunk offsets are known once the workers finish, so the output is now
sized exactly and written once, straight into its destination; with
`as_bytes = TRUE` that destination is R's own vector. Chunk buffers are also
sized from the data rather than a flat 128 bytes per row or 2048 per feature,
which was 21x too large for a point feature and too small for a 200-vertex
polygon.

| 1M rows x 4 cols | 71.8 ms | `as_bytes` 17.0 ms | 4.2x |
|---|---|---|---|
| 200k rows x 50 cols | 210.6 | 47.9 | 4.4x |
| 1M point features | 176.2 | 67.7 | 2.6x |

### Geometry

Describing an sfc for the workers was the last serial phase of any size on the
`sf` path, and it did not scale: 30.15 ms on one worker and 32.98 ms on 32,
while serialization scaled 11.9x over the same range.

`Rf_getAttrib` walks the attribute pairlist and marks what it returns
NOT_MUTABLE, which is a write to a shared header; a direct pairlist walk is
three pointer reads per link and touches nothing. `VECTOR_ELT` per element
became one `VECTOR_PTR_RO` per list. A POINT never carries a `dim`, so for a
homogeneous point column the attribute lookup goes entirely. With those in
place the pass is pure reads and now runs in the pool, with anything it cannot
describe -- GEOMETRYCOLLECTION, an unrecognised class, coordinates that are not
plain doubles -- deferred to a second pass on the R thread.

| extract geometry | before | after |
|---|---|---|
| 1M points | 32.98 ms | 3.21 ms |
| 10k polygons | 3.08 ms | 0.55 ms |

Chunk boundaries also followed row counts, which assumes every feature costs
the same. Real layers hold Russia next to Monaco. Boundaries now follow
cumulative ordinates, so a large geometry gets a chunk of its own:

| 32 workers | before | after |
|---|---|---|
| uniform 10k x 200 vertices | 15.9x | 15.7x |
| 9990 small + 10 x 200k | 1.6x | 6.0x |
| 9900 small + 100 x 20k | 2.3x | 10.4x |

A single geometry is never split across workers, so a layer that is one huge
polygon still scales 1.0x.

### Matrix columns

A matrix column was rendered into an arena on the R thread, one cell at a
time, before the parallel phase began: about 30 ns per cell against 4 for a
plain column, scaling 1.2x where a plain frame scaled 6 to 12x. A numeric
matrix is now described for the workers, which read R's column-major storage
directly.

| 200k rows | before | after |
|---|---|---|
| 3-wide matrix | 18.5 ms | 3.1 ms |
| 10-wide | 60.5 ms | 7.8 ms |
| 50-wide | 288.5 ms | 26.7 ms |

Per cell and in thread scaling, a matrix column now behaves like a plain one.
Against `jsonlite` on the 10-wide case: 442.6 to 23.5 ms.

### Assembly

The chunk copies now run in parallel. Their destination ranges are a prefix
sum over the chunk lengths, so they are disjoint by construction.

| as_bytes | before | after |
|---|---|---|
| 1M rows x 4 cols | 13.98 ms | 11.90 ms |
| 1M point features | 43.71 ms | 31.65 ms |
| 200k rows x 50 cols | 35.57 ms | 29.54 ms |

### Measurement

`tools/bench/harness.R` reports the minimum of repeated timing blocks rather
than the median, because background load only ever makes a measurement slower.
Medians on the development machine drift by up to 18% run to run, which was
enough to make a change that does nothing look like a 24% regression.
`tools/bench/ab.sh` runs a benchmark against the committed sources and then the
working copy, so both halves are measured the same way.

Two candidate optimisations were implemented, measured and reverted because
they did nothing: specialised XY/XYZ/XYZM coordinate writers, since the loop
body is an entire float formatter at ~40 ns and the bookkeeping removed is
~1 ns; and unrolling the same loop over an array of cursors, which measured
slower than the code it replaced.

Against the fastest alternative for each shape (jsonlite, yyjsonr, jsonify,
geojsonsf), on **wall-clock time for the same input**: faster on all 21
shapes single-threaded, by 1.1x to 7.1x, and on all 21 at full width, by 1.1x
to 7.1x. Measured on throughput *per byte* instead, it wins 17 of 21
single-threaded and 20 of 21 at full width; the exceptions are all shapes
where the alternative emits close to twice the bytes for the same input,
because `as_json()` honours jsonlite's `digits = 4` while yyjsonr writes
shortest-round-trip. Both metrics are reported by `tools/bench/compare.R`.

The changes, in descending order of what they were worth:

* The class pre-encoding scan moved from interpreted R into Rust. It had been
  92% of the cost of serialising a list of 20000 small lists (68.5ms of
  74.3ms, against 4.2ms in the serializer itself).
* `R_IsNA`/`R_IsNaN` were cross-DLL calls per element in the hottest loops.
  `NA_real_` is a NaN whose low-order word is 1954, so a bit test replaces
  them.
* Chunk sizing now uses estimated work -- columns, string lengths, sampled
  geometry cost -- rather than row count. A 2000x200 frame and a
  1000-feature polygon layer had each been landing in a single chunk.
* Character columns whose bytes are already valid UTF-8 are escaped straight
  out of R's CHARSXP by the workers, removing an entire copy of every string
  and moving the escaping into the parallel region.
* The fixed-decimal formatter is a port of the `modp_dtoa2` that jsonlite
  itself uses, replacing `core::fmt`. Faster, and byte-identical to jsonlite
  by construction rather than by coincidence.
* extendr `Robj` construction removed from the recursive paths: it takes a
  global mutex twice per node, and had been running once per *cell* in one
  function.

`fastgeojson_threads(1)` also now genuinely runs on one worker. It previously
fell through to rayon's global pool, so it had never been a usable
single-threaded baseline.

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
