# fastgeojson 0.3.0

This release makes `as_json()` a genuine drop-in replacement for
`jsonlite::toJSON()`, and fixes several memory-safety defects found while
validating against jsonlite's own test suite.

## Breaking changes

* **`sf_geojson_str()` and `df_json_str()` are removed.** `as_json()` does
  everything they did and dispatches on the input, so they were a second way
  to say the same thing -- and a worse one: they skipped `as_json()`'s
  pre-encoding, so `Date`, `POSIXt`, `complex` and `raw` went down a different
  path with different output. Replace `df_json_str(x, ...)` with
  `as_json(x, ...)` and `sf_geojson_str(x, ...)` with `as_json(x, ...)`.
  The exported surface is now `as_json()` and `fastgeojson_threads()`.

* **`as_json()`'s signature now matches `jsonlite::toJSON()`** argument for
  argument, in the same order, and accepts `...`. Code that passed arguments
  positionally beyond `x` must be updated -- `as_json(df, "columns")` now means
  `dataframe = "columns"`, where previously position 2 was `auto_unbox`.
* Output at the defaults changes where 0.2.2 differed from `toJSON()`,
  measured by serializing the same inputs with both versions: a missing value
  in a row-oriented frame drops its key instead of writing `null`; bare
  numeric `NA`, `NaN` and `Inf` become the strings `"NA"`, `"NaN"`, `"Inf"`
  (`na = "null"` restores `null` in both cases); matrices nest by row instead
  of flattening; control characters take the short escapes (`\n`, not
  `\u000A`); a FeatureCollection carries `"name":"sfdata"`; a whole
  coordinate prints as `3`, not `3.0`; `-0` keeps its sign.
* Numbers are otherwise unchanged: 0.2.2 had no `digits` argument and always
  wrote them losslessly, which is now the default `digits = Inf`;
  `digits = 4` gives `toJSON()`'s output.
* Every other `toJSON()` argument is new -- `matrix`, `Date`, `POSIXt`,
  `factor`, `complex`, `raw`, `pretty`, `force`, `keep_vec_names`,
  `json_verbatim`, `UTC`, `rownames`, `always_decimal`, `use_signif` -- with
  jsonlite's defaults. For `keep_vec_names`, `json_verbatim` and `UTC` that
  default is what 0.2.2 did anyway: named vectors are arrays, `"json"`
  strings are escaped, timestamps keep their zone. `POSIXt = "string"` uses
  R's `format()`, `"ISO8601"` emits `"2013-06-17T22:33:44"` with no `Z`, and
  `Date = "epoch"` is days since 1970-01-01.

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
* Whole doubles below 1e16 print without a `.0` suffix. 0.2.2 wrote `3.0`
  for a coordinate of 3, and the shortest writer stopped short at 2^53.
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

### `digits = I(n)` was neither the right rounding nor the right notation

`I(n)` asks for *significant* digits, which `toJSON()` renders with
`sprintf("%.*g")`. This applied `signif()` to the whole object in R and then
formatted the rounded values at 15 digits, which was wrong in two ways:
`signif()` rounds the binary value half-to-even where `%g` rounds the decimal
expansion, and 15 digits never selects scientific notation. So `12345` at
`I(4)` came out as `12340` against `toJSON()`'s `1.234e+04`, and `0.12345` as
`0.1234` against `0.1235`.

The writer has had `%.*g` all along, for `digits = NA`; the argument simply
never reached it. Removing the R-side pass also made the mode 7x to 11x
faster, since it was an interpreted recursive walk that copied the object:

| shape | before | after | |
|---|---|---|---|
| 200000 doubles, `I(4)` | 20.0 ms | 1.8 | **11.3x** |
| 200000 doubles, `I(8)` | 21.1 | 1.8 | **11.5x** |
| 200000 x 4 frame, `I(4)` | 75.8 | 10.5 | **7.3x** |
| 20000 small lists, `I(4)` | 118.1 | 10.5 | **11.2x** |

Checked against `toJSON()` at every precision from `I(0)` to `I(17)`, over
values spanning 24 orders of magnitude, through data frames, nested frames,
lists and matrices, and under every `na` mode.

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

### A nested data.frame is now the same data.frame

`as_json()` had two implementations of a data frame. The top-level entry point
built one descriptor per column and walked rows in the worker pool; the
recursive serializer, which is what a frame reached through a list goes
through, had its own per-cell loop instead. That loop knew about far less than
a column can be, so nine things were wrong below the top level -- every one of
them reachable from `as_json(list(d = df))`:

* A `Date` column emitted its epoch day number: `18262` for `"2020-01-01"`.
* A `POSIXct` column emitted local civil seconds: `1577872800`.
* `na` left at its default emitted `"NA"` and `null` where `toJSON()` omits the
  key, so a row that should read `{}` came out as `{"a":"NA","b":null}`.
* A matrix column emitted only the row's first value: `1` for `[1,3]`.
* An `sfc` column lost its type: `[1,2]` for a Point.
* `dataframe = "columns"` was ignored -- the argument never reached the
  recursive entry point at all.
* `dataframe = "values"` likewise.
* A `data.frame`-valued column, as `tidyr::nest()` produces, repeated the Date
  and matrix defects, being rendered by the same per-cell writer.
* A list column holding a frame did too.

There is now one implementation. The recursive serializer hands a frame to the
column builder and the pooled row loop, which is also 3.8x to 4.5x faster for
being parallel, and a nested frame measures the same as the identical frame at
top level (9.7 ms against 9.7 ms for 100000 rows x 8 columns, against 38.5 ms
before).

`rownames = FALSE` still applies only to the outermost frame, which `toJSON()`
applies throughout; a nested frame with row names emits `_row` regardless.
That one is unchanged from previous releases.

### `rownames` has three states, and now reaches every depth

`toJSON()`'s `rownames` is not a two-state flag. Absent, it emits `_row` only
when the row names are informative -- character and not all digits. Given as
`TRUE`, it always emits `_row`, rendering `row.names(x)` by type, so an integer
comes out unquoted and the automatic `1..n` is materialised as numbers. Given
as `FALSE`, never.

We had two states, applied in R, which meant they reached only the outermost
frame: `as_json(list(d = df), rownames = FALSE)` still emitted `_row`. And the
`TRUE` state did not exist -- it was folded into the absent one, so asking for
row names a frame does not store produced nothing where `toJSON()` produces
`{"a":1,"_row":1}`.

The argument now travels to the writer as a three-state value, so a nested
frame, a `data.frame`-valued column and an `sf` object's properties all honour
it. Distinguishing the two questions took a second test: `is_default_rownames`
answers "does jsonlite omit this by default", which is true of `c("7", "8")`,
while the new `is_compact_rownames` answers "is there anything stored at all",
which is false for it -- so `rownames = TRUE` prints `"7"` rather than `1`.

Checked across 126 cases: seven kinds of row names by three states by three
orientations, at top level and nested.

### A `data.frame`-valued column inherits the orientation

The column `tidyr::nest()` produces was rendered row-oriented whatever
`dataframe` said. `toJSON()` renders it column-oriented under `"columns"` --
one object for the whole column rather than one per row -- and as bare arrays
of values under `"values"`.

| | `toJSON()` | before |
|---|---|---|
| `dataframe = "columns"` | `{"a":[1,2],"n":{"a":[1,2]}}` | `{"a":[1,2],"n":[{"a":1},{"a":2}]}` |
| `dataframe = "values"` | `[[1,[1]],[2,[2]]]` | `[[1,{"a":1}],[2,{"a":2}]]` |

The column-oriented form needed a column kind that renders once for the whole
column rather than cell by cell, since it has no `[...]` around it at all.

### An absent or repeated name produced an unusable key

An empty, NA or repeated name is now rewritten as R rewrites it: the element's
1-based index stands in for an absent name, then `make.unique` appends `.1`,
`.2` and so on.

```r
as_json(list(a = 1, 2))
#> {"a":[1],"2":[2]}        # was {"a":[1],"":[2]}
```

A data.frame with two columns called `a` emitted the key twice, which most
parsers reduce to one member. Applies at every depth, to named lists and column
names alike. Clean objects pay nothing for the check.

### A nested `json` value could rewrite the document around it

`json_verbatim = FALSE` is `toJSON()`'s default, and it is what makes a string
carrying the `json` class an ordinary string, escaped like one, rather than
text spliced into the output. We honoured it in R, which meant it reached only
the outermost object: a `json` value nested anywhere was spliced whatever the
setting.

That is not only a formatting difference. Given

```r
v <- structure('1,"injected":true', class = "json")
as_json(list(v = v))
```

`toJSON()` produces `{"v":["1,\"injected\":true"]}` and we produced
`{"v":1,"injected":true}` -- a second key, from a string. The argument exists
to prevent exactly that, so it now travels to the writer and applies at every
depth and in data.frame columns.

A `json` column under `dataframe = "columns"` is written whole rather than
bracketed, since `asJSON("json")` returns its text verbatim and never collapses
it into an array. `toJSON()` errors on such a column of more than one element,
so there is nothing to match past one.

Found by reading jsonlite's issue tracker, which is worth doing for the inputs
people report rather than for the bugs: we claim byte-identical parity, so a
jsonlite bug is ours to reproduce, not to fix.

### Bytes that are not valid UTF-8 now match `toJSON()`

R will hold a "string" whose bytes are not valid UTF-8 -- `rawToChar()` marks
nothing, and `Encoding<-` does not validate. `toJSON()` emits those bytes
unchanged; we raised an error. Now matched, in values, keys and factor levels.

```r
as_json(rawToChar(as.raw(0xe9)))
#> ["\xe9"]            as toJSON() does; was an error
```

A `"bytes"`-marked string still raises `toJSON()`'s own message, which is the
one input it does refuse. The result is built with `Rf_mkCharLenCE` rather
than through a Rust `String`, which is what makes carrying those bytes
possible and also drops a validation pass over the output.

### The two geometry writers are now checked against each other

An `sf` object's geometry is rendered by `write_geometry_parallel`, in a
worker, from a descriptor the extraction pass built. The same `sfc` reached
through a list is rendered by `render_geometry_to_bytes`, recursively, on the R
thread. Two implementations of one thing -- which is exactly the shape that
produced nine defects in the data.frame writer before the two were merged.

`verify_geometry_parity.R` checked the `sf` path against `toJSON()` but never
the two against each other. It now does: 30 shapes (every type, XYZ/XYM/XYZM,
every empty, geometry collections, and the >= 32-point matrices that arrive
ALTREP-wrapped) by nine argument sets that change a coordinate's text
(`digits` at 0, 8, NA, `I(4)` and `Inf`, both `na` modes, `always_decimal`),
compared three ways each, plus a 2000-feature mixed layer so the descriptor
path really chunks.

All 270 agree. The duplication has not produced a divergence here -- but it is
checked now rather than assumed.

### `integer64` is a number, not a string

`bit64::integer64` came out quoted -- `["1","1099511627776"]` where `toJSON()`
gives `[1,1099511627776]` -- and its NA as `null` where the numeric rule
applies. Now pre-rendered to the exact decimal digits (a value past 2^53 keeps
every digit, as jsonlite's own conversion does) and spliced unquoted, with NA
handled as for a double column in every `dataframe` and `na` mode.

The same fix stops the frame's "this is a column" flag leaking into the
elements of a list column, which had an `integer64` list column emitting bare
cells (`"l":3` for `"l":[3]`) and affected mongo timestamps in a list column
the same way.

### Two deliberate deviations, now written down

`difftime` is emitted as its numeric value; `toJSON()` errors on it unless
`force = TRUE`, in which case it gives the same numbers.

`POSIXt = "mongo"` with `dataframe = "columns"`: jsonlite emits
`{"t":[{"$date":[a,b]},NA]}` -- one `$date` holding an array, followed by a
bare `NA` token, which is not valid JSON. We emit one `{"$date":...}` per
cell, as in every other orientation.

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

### A timestamp column could emit another column's values

A data.frame with two or more `POSIXt` columns could emit the wrong timestamps
-- the first column holding the second's values -- when a garbage collection
landed mid-build. Reachable only through `df_json_str()`, which this release
removes. Fixed, and covered by 21 assertions under `gctorture(TRUE)` in
`tests/testthat/test-gctorture.R`.

## New features

* `fastgeojson_threads()` gets and sets the worker count. `n = 1` disables
  parallelism, which makes benchmarking reproducible. The default is the whole
  machine, honouring `FASTGEOJSON_NUM_THREADS`, `RAYON_NUM_THREADS`,
  `OMP_NUM_THREADS` and `OMP_THREAD_LIMIT`, and throttling to two threads when
  `R CMD check` is detected. Previously rayon's global pool took every core on
  the first call, even for a three-row data frame.
* `pretty` is supported, reproducing jsonlite's layout (scalar-only arrays stay
  on one line; objects expand).
* `digits = Inf`, the default, writes the shortest decimal that reads back as
  the same double. It is the only lossless setting: `digits = NA` keeps 15
  significant digits, so `pi` becomes `3.14159265358979`, and about 94% of
  doubles arising from real arithmetic need 16 or 17. `digits = 22` is exact
  but always writes 17 digits, which is 2 to 63% more output than necessary
  depending on the data. `toJSON()` warns and falls back to `digits = NA` for
  a non-integer `digits`, so nothing that works against jsonlite changes
  meaning. `complex = "string"` writes each part shortest in `prettyNum()`'s
  layout; jsonlite has no lossless setting for complex at all.
* `as_bytes = TRUE` returns a raw vector instead of a character vector.
  R creates a character vector at about 1 GB/s, because it hashes every byte
  to intern the string in its global CHARSXP cache; for a 49 MB result that is
  52 ms, which profiling put at three quarters of the total time for a
  million-row frame. A caller writing the JSON to a socket or a file never
  needed it interned. Incompatible with `pretty`, which needs a string.
* `FASTGEOJSON_PROFILE=1` makes each call print its phase timings to stderr.

### `as_bytes` was undocumented

`as_bytes = TRUE` returns the result as a raw vector instead of a character
vector, skipping the string interning that is 83% to 87% of a large call. It
has worked for some time but was missing from `?as_json`, whose `\value` also
claimed a character vector unconditionally. Now documented there and in the
README.

Writing an 18 MB result to a file: 9 ms via `writeBin()` on the raw vector,
101 ms via `writeLines()` on the character one, 415 ms via
`jsonlite::write_json()` end to end. Below
about a megabyte it makes no practical difference.

For htmlwidgets -- `leaflet::addGeoJSON()`, `deckgl`, `mapdeck` -- use the
default. A raw vector is base64-encoded into the widget payload, which the
browser cannot use and which is 37% larger.

## Performance

Against 0.2.2 on the README's three shapes, at identical output: 1M rows x 4
columns 90 -> 72 ms, 1M point features 231 -> 176 ms, 10k polygons 130 -> 93
ms (20-28% faster), on top of the parity work above.

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

### Free the scratch on a worker, not on the caller's thread

A finished serialization held its per-chunk output buffers and its geometry and
column descriptors until the result was built, then freed them -- several
megabytes across ~128 sub-megabyte heap blocks that serialise on the Windows
allocator lock, ~3-4 ms on the million-point path, all of it after the last
work was done and none of it visible to the caller until the call returned.
That free now runs on a pool worker; the R thread returns the result at once.

| 32 threads | before | after |
|---|---|---|
| 1M points, as_bytes | 21.26 ms | 17.15 ms |
| 1M x 4 frame, rows, as_bytes | 13.71 ms | 11.52 ms |
| 1M x 4 frame, columns, as_bytes | 21.49 ms | 16.45 ms |
| 1M points, character | 124.64 ms | 122.26 ms |

On the character path the frees run while this thread is inside
`mkCharLenCE`, so they hide behind it.

A `PENDING_DROPS` counter makes package unload and pool teardown wait for any
in-flight free, so a drop can never outlive the allocator; verified across
unload cycles and thread-count changes with drops in flight. It falls back to
an inline free at one worker (the `fastgeojson_threads(1)` baseline).

### Stream the as_bytes assembly copy past the cache

The parallel copy that assembles the per-chunk buffers into the result was an
ordinary `memcpy`, which reads each destination line for ownership before
overwriting it -- wasted traffic and cache pollution for bytes this core never
reads again. On the `as_bytes` path the raw vector goes straight back to R and
out to a file or socket, so the copy now uses `_mm_stream_si128`
(cache-bypassing, SSE2 baseline).

| 1M points, as_bytes, 32 threads | before | after |
|---|---|---|
| whole call | 17.21 ms | 15.22 ms |

The character path keeps the ordinary copy, because `mkCharLenCE` reads every
byte back immediately to hash it and wants it in cache. Byte-identical output,
verified across sizes straddling the parallel threshold.

### Point and line geometry: no scratch for rings there are none of

`extract_geometries_chunk` reserved a coordinate-matrix vector and a ring-count
vector per chunk, sized to the chunk's rows. Only polygons and multi-geometries
fill them; a POINT, MULTIPOINT or LINESTRING is one coordinate matrix carried in
the geometry descriptor itself. So on the point and line paths every chunk was
reserving hundreds of kilobytes it never wrote -- and freeing it at return --
across 128 sub-megabyte heap blocks that serialise on the allocator lock. They
are now empty for those types.

| as_bytes, 32 threads | before | after | |
|---|---|---|---|
| 20,000 LineStrings x 50 | 7.85 ms | 6.24 ms | 1.26x |
| 1M points | 22.17 ms | 21.43 ms | |
| 10k polygons (control) | 7.14 ms | 6.92 ms | -- |

Polygons are the control: they use both vectors, and are unchanged.

### One geometry is now split across the pool

A coordinate matrix above 131,072 ordinates is written in pieces across the
worker pool. Splitting only between features did nothing for a layer whose work
is one geometry -- a long track, a coastline, a detailed boundary.

| | before | after | |
|---|---|---|---|
| 1 LineString, 2M coordinates | 560.18 ms | 186.69 ms | **3.00x** |
| 1 Polygon, 1M coordinates | 280.31 ms | 93.22 ms | **3.01x** |
| 8 LineStrings, 250k each | 167.83 ms | 117.99 ms | 1.42x |
| 2000 LineStrings, 1k each | 102.33 ms | 103.23 ms | control |

A 200-vertex ring is 400 ordinates, so ordinary layers never reach the
threshold.

### The recursive path now uses the worker pool

Vectors, matrices and arrays handed straight to `as_json()` were serial however
large they were: only the data.frame path had ever entered the pool. Long runs
of doubles, integers and logicals are now split across it, measured with
`as_bytes = TRUE` so R's string interning is not in the way:

| shape (200,000 values) | before | after | |
|---|---|---|---|
| double vector | 21.1 ns/value | 3.9 | **5.4x** |
| 20000 x 10 double matrix | 22.8 | 3.9 | **5.8x** |
| 1000 x 20 x 10 double array | 23.2 | 4.3 | **5.4x** |
| 20000 x 10 integer matrix | 9.3 | 3.5 | 2.7x |
| 20000 x 10 logical matrix | 6.6 | 3.2 | 2.1x |

3.9 ns/value is what the same formatting costs through the data.frame path, so
the gap was parallelism and nothing else. Character vectors and lists stay
serial: a `CHARSXP` needs encoding handling that allocates on R's vmax stack,
and the list walk is the recursion itself. The split is on the outermost
dimension, so a `2 x 20000` matrix parallelises over its two rows and a
`20000 x 2` over its twenty thousand.

`tools/bench/verify_parallel_parity.R` checks every shape three ways -- against
`toJSON()`, and at one worker against many -- because the failure this could
hide is a doubled or missing separator at a chunk boundary, which no small test
would reach.

### `dataframe = "columns"` splits rows as well as columns

The column-oriented writer gave each column to one worker, which left most of
the machine idle whenever there were fewer columns than workers and a
one-column frame entirely serial: a 250000 x 1 numeric frame measured 5.31 ms
of `serialize (parallel)` on 32 workers, which is one worker's work.

Columns big enough on their own are now cut into row pieces too. The split is
decided by `rows_per_chunk` on that column's own work, so the thresholds that
keep a small frame serial still apply, and the piece count is capped so that
columns times pieces stays near the worker count rather than flooding the pool.
Each piece carries its own punctuation -- the first opens the array, the rest
begin with the separator, the last closes it -- so the pieces concatenate with
nothing between them.

| shape | before | after | |
|---|---|---|---|
| 250000 x 1, columns | 4.88 ms | 0.94 | **5.2x** |
| 250000 x 2, columns | 5.37 | 1.69 | **3.2x** |
| 125000 x 4, columns | 3.76 | 1.49 | **2.5x** |
| 250000 x 1 strings, columns | 2.87 | 2.24 | 1.3x |
| 20000 x 40, columns | 1.94 | 1.69 | 1.2x |
| 5 x 3, columns | 0.105 | 0.101 | unchanged |
| 250000 x 1, rows (control) | 1.01 | 0.95 | flat |

### Column descriptors cost a fifth of what they did

Building the descriptors asked extendr for an `Robj` per column and then asked
that `Robj` `inherits()` up to six times. The constructor takes a global
ownership mutex, inserts into a hash map and calls `Rf_protect`, with `Drop`
taking the mutex again; each `inherits` fetched and walked the class attribute.
Together they measured 0.62 us per column of pure setup, against about 30 ns to
write a three-row integer column. Reading the class attribute once into the
same bitset the recursive serializer already used, and reading the column
pointer from `VECTOR_PTR_RO` instead, brings it to 0.12 us. Column names are
escaped once into the descriptor's key rather than allocated as a name and then
as a key.

A frame small enough to need one chunk is now written straight into the output
buffer, skipping the chunk vector, the join and a full copy.

### Array columns of three dimensions or more

Only two-dimensional integer and double matrix columns were described for the
workers; three dimensions and up were rendered serially into an arena on the R
thread, though the cells are the same pointer reads. A 1000 x 20 x 10 double
array column cost 30.1 ns per value that way and now costs 4.5, which is what
the matrix column costs. Character arrays still go through the arena: their
cells need encoding handling that allocates on R's vmax stack.

### Logical matrix columns are written by the workers

Only integer and double matrix columns were described for the workers; a
logical one was rendered serially into an arena on the R thread, though it
needs no encoding at all. A 20000 x 10 logical matrix column went from 4.9 ms
to 1.8 ms.

`estimate_row_work` also counted a matrix column as one unit of work rather
than one per matrix column, so a 2000-row frame carrying a 200-column numeric
matrix scored 2000 -- below the threshold for using the pool at all. The shape
the worker-side matrix writer exists for was the one kept in a single chunk. It
now scales 2.9x where it scaled 1.0x.

### A latin1 column cost 93x what it needed to

Every cell of a latin1 column went through `Rf_translateCharUTF8` on the R
thread, one `iconv` call at a time. Bytes from 0xA0 up are their own code
points, so the workers now widen those themselves.

| 300,000 values, one column | before | after | |
|---|---|---|---|
| non-ASCII, marked latin1 | 815.43 ms | 8.76 ms | **93x** |

`0x80..0x9F` still goes through R, because what "latin1" means there is
platform-dependent -- CP1252 on Windows, the C1 controls through a strict
ISO-8859-1 `iconv`, and 27 of the 256 bytes disagree. All 255 byte values are
checked against `toJSON()` in `verify_string_parity.R`.

### Text read from a file took the slow path

Non-ASCII text from `readLines()` or `rawToChar()` is marked `CE_NATIVE` even
in a UTF-8 locale, and the test for which cells the workers could read asked
for the `CE_UTF8` mark specifically. All of it took the serial path. Now uses
`Rf_charIsUTF8`, which R 4.5.0 added for this.

| 300,000 values, one column | before | after |
|---|---|---|
| non-ASCII, marked unknown | 23.27 ms | 7.46 ms |

Byte-identical output, and unchanged for ASCII (6.99 to 6.92 ms) and for text
already marked UTF-8 (7.54 to 7.43).

### UTC timestamps no longer ask R for an offset that is always zero

A `POSIXct` column is pre-encoded in R because only R holds the time-zone
rules, and the one thing it needs from R is each instant's UTC offset via
`as.POSIXlt()$gmtoff` -- about 70 ms per million values. For UTC and its
aliases (`GMT`, `Etc/UTC`, `Zulu` and the rest) that offset is zero at every
instant, NA and the infinities included, so those zones skip the call.

| 1M POSIXct, as_bytes | before | after | |
|---|---|---|---|
| tz = "UTC" | 114.93 ms | 41.76 ms | **2.75x** |
| tz = "America/New_York" | 123.91 ms | 119.38 ms | -- |

Byte-identical across nine zones, every `POSIXt` mode, NA and Inf cells, the
all-midnight rule and a fractional-second midnight. A column in the machine's
own zone (`tz = ""`) keeps the full path, since that could be anything.

### Date and timestamp cells: no CRT floor, one store

`f64::floor` is a C-runtime call on the SSE2 baseline and was the largest item
in a Date cell; inside the admitted range an integer cast plus one correction
is the same floor with no call. And `"YYYY-MM-DD"` went out as twelve pushes,
each with a capacity check; it is now built from the digit-pair table and
written with a single 16-byte store.

| 1M column, 1 thread | before | after |
|---|---|---|
| Date, whole days | 81.89 ms | 76.38 ms |
| Date, fractional days | 85.53 ms | 76.87 ms |
| Date, integer-typed | 74.09 ms | 67.88 ms |
| POSIXct, `"string"` | 153.59 ms | 145.55 ms |
| POSIXct, `"ISO8601"` | 127.60 ms | 118.48 ms |

The timestamp writer gets the same two changes. Byte-identical on negative
fractional days and seconds, every year boundary from 0001 to 9999 and past
both, leap days, every layout, integer-typed dates, NA/NaN/Inf, and
`Date = "epoch"`.

One parity gap closed on the way: a timestamp `format()` cannot render (a year
past what `strftime` holds) is NA to R, and `toJSON()` drops the key in row
mode; we wrote `"t":null`. The Date column already handled this; the
timestamp column now does too.

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

### The fixed-decimal rounding no longer mispredicts

modp_dtoa2's rounding -- up past the half, to even on an exact half -- was an
`if / else if` on `diff > 0.5`, which on real data is a coin flip and
mispredicted half the time in the hottest function in the package. It is now
one boolean with the same truth table, guarded so that values that cannot
round (every whole number) skip it and keep their perfectly predicted branch.

| 1M x 4, 1 thread | before | after | |
|---|---|---|---|
| runif (0, 1) | 175.50 ms | 155.53 ms | -11% |
| rnorm | 178.12 ms | 158.70 ms | -11% |
| whole-number doubles (control) | 88.79 ms | 89.72 ms | -- |

Byte-identical: a new crate test walks exact binary halves at every precision
including the carries, and the full numeric parity grid passes.

### The fixed-decimal writer stops paying for casts it cannot need

`as` casts on floats saturate in Rust, and the clamping is not free. A
disassembly of the built library showed `value as i64` costing six
instructions past the conversion -- a compare against 9.22e18, a `movabs`, a
`cmova`, a NaN check and a second `cmov` -- and `tmp as u32` costing a `maxsd`
and a `minsd`, eight cycles of pure latency sitting directly on the floating
point dependency chain.

Neither clamp can ever fire. The single call site guarantees
`1e-5 < |v| < FIXED_MAX[d] <= 1e17`, which is finite, not NaN (both
comparisons reject it) and well inside `i64`; and `value - trunc(value)` is in
`[0, 1)`, so the scaled fraction is in `[0, 10^prec)` and well inside `u32`.
`to_int_unchecked` says so.

Two smaller things from the same disassembly: every digit-pair store into the
scratch array carried a bounds check and a panic branch, because `k`'s bound
comes from the digit counts and LLVM cannot see it; and modp_dtoa2's final
carry is unreachable in this port, since the trim leaves the fraction with
exactly the digits the emission loops then consume.

| | before | after | |
|---|---|---|---|
| integers (control) | 7.26 ns/value | 7.24 | flat |
| doubles in (0, 1) | 19.58 | 17.70 | **-9.6%** |
| doubles 1e6 to 1e9 | 29.27 | 24.39 | **-16.7%** |
| epoch milliseconds | 37.39 | 29.38 | **-21.4%** |
| coordinates, -125 to -66 | 25.86 | 21.50 | **-16.9%** |
| `digits = 9` | 27.90 | 23.92 | **-14.3%** |
| `digits = 0` | 17.89 | 16.14 | **-9.8%** |
| 10000 polygons x 200 vertices | 30.35 | 27.86 | **-8.2%** |

This is the default path, so it is every double in every column and every
ordinate of every geometry.

### `%g` reads its digits from ryu when that is provably the same answer

`core::fmt` runs the exact (Dragon4) algorithm whatever precision is asked of
it, so `write_g_format` cost about 100 ns per value at 5 significant digits and
at 15 alike. ryu's shortest form costs about 35.

Let `D` be that shortest form, with `nd` significant digits. `D` round-trips,
so `|D - v| < ulp(v)/2`, which in units of its last digit is under
`1.11e-16 * 10^nd` -- 0.11 units at 15 digits, 1.11 at 16, 11.1 at 17. So
rounding `D` to `p` digits gives the same answer as rounding `v` unless the two
straddle a half-way point, and that needs `D`'s discarded tail to be within
that many units of exactly one half. The tail is an integer, so the test is
exact; when it fails, `core::fmt` still runs.

Two bounds fall out. Above `p = 15` it is not correct at all: with `nd <= p`
there is no rounding, but `D` is only the answer if `v` rounded to `p` digits
is `D` padded with zeros, which needs `ulp(v) <= u_p` and so `p <= 15`. It is
why `%.17g` of 0.1 is `0.10000000000000001`. Above `p = 14` it stops paying:
full-entropy doubles have 16- or 17-digit shortest forms, so `p = 15` falls
back for 24% to 40% of values having already paid for ryu.

| shape | before | after | |
|---|---|---|---|
| `digits = 4`, values below 1e-5 | 102.5 ns/value | 82.9 | **1.24x** |
| `digits = 10` | 98.4 | 79.4 | **1.24x** |
| `digits = 4`, values in (0, 1) | 20.2 | 20.4 | control |
| `digits = NA` | 95.2 | 95.6 | unchanged by design |

Subnormals are excluded. The whole argument rests on
`ulp(v)/v <= 2.22e-16`, which is a property of normal doubles; for the smallest
subnormal `ulp` *is* the value, so ryu's `5e-324` says nothing about `%.2g`,
which is `4.9e-324`. That one value is what caught it.

### The fixed-decimal writer covers the magnitudes people actually have

A double is formatted either by a fixed-decimal writer or, outside its range,
by `%.*g`. That range was `|v| < 2^31` for every `digits`, which sent entirely
ordinary magnitudes down the `%g` path at five to six times the cost: epoch
milliseconds (1.6e12), and any count or identifier above two billion.

Fixed notation carries `int_digits + digits` significant digits while the `%g`
fallback caps its precision at 17, so the two agree exactly while
`|v| < 10^(17-digits)`. That is now the bound, per `digits` value:

| | before | after | vs jsonlite |
|---|---|---|---|
| doubles in (0, 1) | 20.3 ns/value | 20.2 | 3.4x |
| doubles 1e6 to 1e9 | 30.1 | 30.0 | 4.4x |
| doubles 3e9 to 1e10 | 107.2 | 30.5 | 5.2x -> **17.7x** |
| epoch milliseconds | 122.2 | 38.4 | 5.3x -> **16.6x** |

`digits = 0` keeps the old bound. It is the one precision where the notation
rule disagrees: `decimals` is `ceil(log10|v|)` there, which for an exact power
of ten equals the exponent rather than exceeding it, and `%g` turns scientific
as soon as the exponent reaches the precision -- so `%.10g` of 1e10 is `1e+10`
where fixed notation writes `10000000000`. Only exact powers of ten are
affected, and screening for them per value costs more than the mode is worth.

Values below 1e-5 are unchanged at about 105 ns: `%g` renders those in
scientific notation, which the fixed writer cannot express.

`verify_numeric_parity.R` now checks the whole grid -- ten `digits` values by
25 exponents, and a hundred groups sampled densely either side of every bound,
including exact powers of ten, exact halves, integers and the next
representable double.

### Doubles are reversed eight bytes at a time

`write_fixed_decimals` builds its digits least-significant-first and then
reverses them into the output a byte at a time -- nine or ten dependent stores
for a coordinate like `-120.1234`. A byte-swapped `u64` does eight at once, so
a typical value becomes one load, one bswap, one shift and one store. Measured
against an integer column as the untouched control:

| | before | after | |
|---|---|---|---|
| integers (control) | 7.17 ns/value | 7.22 | +0.7% |
| doubles in (0, 1) | 20.49 | 19.71 | -3.8% |
| doubles like -120.1234 | 25.93 | 25.23 | -2.7% |
| doubles 1e6 to 1e9 | 31.25 | 29.60 | -5.3% |
| 10000 polygons x 200 vertices | 32.29 | 31.18 | -3.4% |

### The default na mode stops dispatching four times per cell

`na = "smart"` is the default, and it is what makes row-oriented output drop
the key of a missing value rather than write null. Deciding that cost a
separate pass over the cell: `col_is_missing` calls `col_available`, then
matches on the column kind and loads the value; `write_col_value` then calls
`col_available` again, matches again and loads again. Four dispatches and two
loads to write one number.

For the three kinds that dominate -- integer, double, logical -- the test and
the write now come off one load. The fused tests are the exact negation of
`col_is_missing`'s (`is_na_int`, `!is_finite`), so the two cannot drift, and
the key is still written only after the decision is made: an earlier version
pushed the key first and could bail out of the Factor and Char arms, leaving a
dangling `"key":`, and that shape is not coming back.

| | before | after | |
|---|---|---|---|
| 1-column frame, rows | 27.94 ns/row | 27.05 | -3.2% |
| 2-column | 35.42 | 33.53 | -5.3% |
| 3-column | 49.65 | 47.45 | -4.4% |
| 4-column | 66.86 | 63.02 | -5.7% |
| envelope per column | 5.53 | 4.70 | **-15%** |

Bare vectors and the column- and value-oriented writers, none of which take
this path, are flat within 2%.

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

The chunk copies now run in parallel. Their destination ranges are a prefix
sum over the chunk lengths, so they are disjoint by construction.

| as_bytes | before | after |
|---|---|---|
| 1M rows x 4 cols | 13.98 ms | 11.90 ms |
| 1M point features | 43.71 ms | 31.65 ms |
| 200k rows x 50 cols | 35.57 ms | 29.54 ms |

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
  `aarch64-pc-windows-gnullvm` artifacts. The minimum version, 1.68, is now
  declared as `rust-version` in `Cargo.toml`, which cargo enforces without
  downloading a toolchain; the package builds and passes its tests on 1.68,
  1.75 and 1.92.
* `.Rbuildignore` now excludes `.RData*`, build artefacts and the generated
  `Makevars`. The source tarball was 41 MB because a stray `.RDataTmp` was
  being shipped; it is now 1.4 MB.
* `inst/AUTHORS` and `LICENSE.note` now list all 17 vendored crates with the
  versions recorded in `Cargo.lock`, and correctly note the `Unicode-3.0` term
  that `unicode-ident` carries.
* Added `cleanup` / `cleanup.win`, and made the post-link cleanup depend on
  `$(SHLIB)` so it cannot race under `make -j`.
* Vendored crates build offline with `--locked` and `-j 2`.

### Errors no longer print a Rust panic trace

extendr raises an R error by panicking with the message and catching it at the
C boundary, so Rust's default hook printed three lines of
`thread '<unnamed>' panicked at ...` ahead of every error, including ones
raised deliberately. It looked like a crash. The hook is now silent -- every
panic reaching it is already caught and surfaced as an R error -- and
`FASTGEOJSON_PANIC_TRACE=1` restores it.

`FASTGEOJSON_PROFILE=1` output goes through `REprintf` rather than the
process's stderr, so `capture.output(type = "message")` and R's sinks now see
it.

`.Call` entry points are registered-symbol only (`R_forceSymbols`), so a
mistyped one is a load-time error rather than a call-time one.

A long run now notices Ctrl-C at its phase boundaries. R-exts: "No part of R
can be interrupted whilst running long computations in compiled code", and
nothing here checked. The poll goes through `R_ToplevelExec` so the interrupt
cannot longjmp past Rust destructors. It does not reach inside a parallel
region -- only the R thread may ask R anything -- so a single large chunk
still runs to completion.

### Four things the manuals said we were doing wrong

None of these changes any output.

* **`as_bytes = TRUE` now serialises past 2 GB.** The limit belongs to R
  character strings, not raw vectors, but the guard sat above both.
* **Re-encoded strings no longer accumulate.** `Rf_translateCharUTF8` allocates
  on R's `R_alloc` stack, which is held until the call returns without
  `vmaxset`; a large column kept a second copy of itself.
* **An allocation failure raises an R condition** instead of aborting the
  session, which `catch_unwind` could not have caught.
* **`R_unload_fastgeojson`** stops the worker threads on `dyn.unload()`.

### Testing

The Rust crate has 61 unit tests of its own, over number formatting, escaping,
key padding, date and timestamp arithmetic, parallel assembly, the pretty
printer, the cell and coordinate writers, chunk planning and partitioning. They
run in six seconds; previously every change had to go through a `cargo build`,
an `R CMD INSTALL` and an R script to be judged at all. `tools/rust-test.sh`
runs them -- `cargo test` alone will not link, because R ships only `R.dll`
with no import library.

### Measurement

`tools/bench/harness.R` reports the minimum of repeated timing blocks rather
than the median, because background load only ever makes a measurement slower.

Two things on this platform distort benchmarks and are worth knowing if you run
them yourself. Above 8 MiB of output every measurement costs about twice as
much per byte -- 0.78 ns/byte below 2^23 and 1.64 above, because the heap
serves allocations that large from the OS and pays to have fresh pages zeroed.
And medians drift by up to 18% run to run, enough to invent a 24% regression.
`tools/bench/ab.sh` runs a benchmark against the committed sources and then the
working copy so both halves are measured the same way, and the harness reports
the minimum of repeated blocks rather than the median.

Several optimisations were implemented, measured and reverted for doing
nothing: specialised XY/XYZ/XYZM coordinate writers; unrolling that loop over
an array of cursors; emitting digits most-significant-first rather than
reversing a scratch array (1.2% to 3.9% *slower*); and `#[inline(never)]` on
the cold float-formatting branches. Two build-flag routes are unavailable
here at all -- profile-guided optimisation, because both GNU ld and LLD pad
the `$`-grouped PE sections LLVM's profile runtime uses to find its data, and
`-C target-cpu=native`, which takes the baseline from SSE2 to AVX2/FMA/BMI and
moves nothing. The hot loops are serial and branchy rather than
arithmetic-bound.

Against the fastest alternative for each shape (jsonlite, yyjsonr, jsonify,
geojsonsf), on **wall-clock time for the same input**: faster on all 21
shapes single-threaded, by 1.1x to 7.1x, and on all 21 at full width, by 1.1x
to 7.1x. Measured on throughput *per byte* instead, it wins 17 of 21
single-threaded and 20 of 21 at full width; the exceptions are all shapes
where the alternative emits close to twice the bytes for the same input:
those runs were made at `digits = 4`, jsonlite's default, while yyjsonr
writes shortest-round-trip. Both metrics are reported by
`tools/bench/compare.R`.

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

Two defaults deviate deliberately. `digits = Inf` writes numbers losslessly
where jsonlite rounds to 4 decimal places (see "New features"). And the `sf`
default: jsonlite 2.0.0 defaults sf
objects to `sf = "dataframe"` (a record array); `as_json()` defaults to
`sf = "geojson"`, because emitting a `FeatureCollection` for an sf object is
the purpose of this package and what existing callers depend on. All three
modes are byte-identical to jsonlite when requested explicitly, and
`options(fastgeojson.sf = "dataframe")` restores jsonlite's default globally.

Parity is verified by jsonlite's own toJSON test suite, ported into
`tests/testthat`: **over 900 expectations, no failures and no skips**.

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
