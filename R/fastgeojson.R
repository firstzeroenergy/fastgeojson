#' Fast Serialization of R Objects to JSON and GeoJSON
#'
#' @description
#' `as_json()` serializes R objects to JSON, and `sf` objects to GeoJSON, in
#' parallel and with lossless numbers. The encoders are implemented in Rust
#' via **extendr**.
#'
#' It takes the arguments of [jsonlite::toJSON()], in the same order, and
#' follows jsonlite's output conventions: jsonlite's own test suite runs
#' against it. Two defaults differ: numbers are written losslessly
#' (`digits = Inf`, where `toJSON()` rounds to 4 decimal places) and `sf`
#' objects become GeoJSON (`sf = "geojson"`, where `toJSON()` writes a record
#' array).
#'
#' @details
#' \strong{Coming from jsonlite.} Code that calls `toJSON()` with named
#' arguments can call `as_json()` with the same ones; pass `digits = 4` (and
#' `sf = "dataframe"` for `sf` input) to reproduce `toJSON()`'s defaults.
#' Unknown arguments are accepted through `...`, as `toJSON()` does.
#'
#' \strong{Encoding strategy.} `as_json()` inspects `x` and dispatches to the
#' appropriate Rust encoder:
#' \itemize{
#'   \item \strong{Simple features (`sf`):} a GeoJSON `FeatureCollection`.
#'   \item \strong{Data frames:} row- or column-oriented, processed in parallel
#'     in row chunks for large inputs.
#'   \item \strong{Atomic vectors and lists:} JSON arrays and objects,
#'     recursively.
#' }
#'
#' \strong{Type mapping.}
#' \itemize{
#'   \item \strong{Numeric:} JSON numbers, exact by default; see `digits`.
#'     `NA`, `NaN`, `Inf` and `-Inf` follow the `na` argument.
#'   \item \strong{Logical:} `true` / `false`.
#'   \item \strong{Character:} JSON strings. latin1- and native-encoded
#'     inputs are translated to UTF-8; bytes that are not valid UTF-8 pass
#'     through unchanged, as `toJSON()` passes them, and a `"bytes"`-marked
#'     string raises the error `toJSON()` raises.
#'   \item \strong{Factor:} labels, or integer codes when `factor = "integer"`.
#'   \item \strong{Date / POSIXt:} controlled by `Date` and `POSIXt`.
#'   \item \strong{complex / raw:} controlled by `complex` and `raw`.
#' }
#'
#' \strong{Threads.} See [fastgeojson_threads()] to control parallelism.
#'
#' @param x The object to serialize.
#' @param dataframe How to encode data frames: `"rows"` (default,
#'   `[{"a":1},{"a":2}]`), `"columns"` (`{"a":[1,2]}`) or `"values"`
#'   (`[[1],[2]]`, one array per row without names).
#' @param matrix How to encode matrices: `"rowmajor"` (default) or
#'   `"columnmajor"`.
#' @param Date How to encode `Date`: `"ISO8601"` (default, `"2015-01-01"`) or
#'   `"epoch"` (days since 1970-01-01).
#' @param POSIXt How to encode date-times: `"string"` (default, R's own
#'   `format()`), `"ISO8601"`, `"epoch"` (milliseconds) or `"mongo"`.
#' @param factor How to encode factors: `"string"` (default) or `"integer"`.
#' @param complex How to encode complex numbers: `"string"` (default,
#'   `"1+2i"`) or `"list"` (`{"real":[..],"imaginary":[..]}`).
#' @param raw How to encode raw vectors: `"base64"` (default), `"hex"`,
#'   `"mongo"`, `"int"` or `"js"`.
#' @param null How to encode `NULL` inside lists: `"list"` (default, `{}`) or
#'   `"null"`.
#' @param na How to encode missing values: `"null"` or `"string"`. When not
#'   supplied, type-specific defaults apply -- in particular, row-oriented data
#'   frame output omits the key entirely, matching `toJSON()`.
#' @param auto_unbox If `TRUE`, length-one atomic vectors become JSON scalars
#'   rather than length-one arrays. Defaults to `FALSE`.
#' @param digits Precision of numeric values. The default, `Inf`, writes the
#'   shortest decimal that reads back as the same double, so nothing is lost.
#'   A number is a count of decimal places (`toJSON()`'s default is `4`);
#'   wrap it in [base::I()] to count *significant* digits instead; `NA` is
#'   `toJSON()`'s 15 significant digits.
#' @param pretty If `TRUE`, indent the output by two spaces; a number sets the
#'   indent width.
#' @param force If `TRUE`, strip S3 classes that would otherwise raise an
#'   error. `sf` objects are exempt so geometry handling is preserved.
#' @param ... Further arguments passed to the encoder, mirroring
#'   `jsonlite::toJSON()`. Recognised here: `keep_vec_names`, `rownames`,
#'   `json_verbatim`, `UTC`, `time_format`, `always_decimal`, `use_signif`,
#'   `indent`, `sf` and `as_bytes`.
#'
#'   `as_bytes = TRUE`, which is not a `toJSON()` argument, returns the same
#'   bytes as a **raw vector**, skipping R's string interning -- most of a
#'   large call. Use it when the JSON is leaving R: `writeBin()` to a file or
#'   connection, an HTTP response body. It cannot be combined with `pretty`.
#'
#' @return A length-one character vector of class `"json"`, or
#'   `c("geojson", "json")` for `sf` input -- unless `as_bytes = TRUE`, which
#'   returns an unclassed raw vector holding the same bytes.
#'
#' @seealso [fastgeojson_threads()] to control parallelism.
#'
#' @examples
#' as_json(list(a = 1, b = "foo", c = NA))
#' as_json(list(val = 5), auto_unbox = TRUE)
#'
#' df <- data.frame(x = c(1.5, 2.5), y = c("a", "b"))
#' as_json(df)
#' as_json(df, dataframe = "columns")
#'
#' # Straight out to a file, without interning the result as an R string.
#' f <- tempfile()
#' con <- file(f, "wb")
#' writeBin(as_json(df, as_bytes = TRUE), con)
#' close(con)
#' unlink(f)
#'
#' if (requireNamespace("sf", quietly = TRUE)) {
#'   nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#'   geo <- as_json(nc[1:2, ])
#' }
#'
#' @name fastgeojson
#' @aliases fastgeojson as_json
#' @export
#' @useDynLib fastgeojson, .registration = TRUE
as_json <- function(
    x,
    dataframe = c("rows", "columns", "values"),
    matrix = c("rowmajor", "columnmajor"),
    Date = c("ISO8601", "epoch"),
    POSIXt = c("string", "ISO8601", "epoch", "mongo"),
    factor = c("string", "integer"),
    complex = c("string", "list"),
    raw = c("base64", "hex", "mongo", "int", "js"),
    null = c("list", "null"),
    na = c("null", "string"),
    auto_unbox = FALSE,
    digits = Inf,
    pretty = FALSE,
    force = FALSE,
    ...) {

  dataframe <- match.arg(dataframe)
  matrix    <- match.arg(matrix)
  Date      <- match.arg(Date)
  POSIXt    <- match.arg(POSIXt)
  factor    <- match.arg(factor)
  complex   <- match.arg(complex)
  raw       <- match.arg(raw)
  null      <- match.arg(null)

  # `toJSON()` keeps type-specific NA defaults unless `na` is given explicitly.
  # Our Rust "smart" mode is that behaviour: omit in row mode, null elsewhere.
  na <- if (missing(na)) "smart" else match.arg(na)

  dots            <- list(...)
  keep_vec_names  <- isTRUE(dots$keep_vec_names)
  json_verbatim   <- isTRUE(dots$json_verbatim)
  always_decimal  <- isTRUE(dots$always_decimal)
  UTC             <- isTRUE(dots$UTC)
  time_format     <- dots$time_format
  # Three states, as toJSON() has: absent emits `_row` only for row names that
  # are really there, TRUE emits it even for the automatic 1..n, FALSE never.
  # Passed down rather than applied here, so a nested frame honours it too.
  rownames        <- if (is.null(dots$rownames)) 1L else if (isTRUE(dots$rownames)) 2L else 0L
  use_signif      <- if (is.null(dots$use_signif)) inherits(digits, "AsIs") else isTRUE(dots$use_signif)
  strict_atomic   <- isTRUE(dots$strict_atomic)
  # as_bytes = TRUE returns a raw vector instead of a character vector.
  # R charges about 1 GB/s to make a character vector, because it hashes every
  # byte to intern the string in its CHARSXP cache; for a 49 MB result that is
  # 52 ms, measured at 74% of the total. Callers that write the JSON to a
  # socket or a file never needed it interned.
  as_bytes        <- isTRUE(dots$as_bytes)
  # jsonlite 2.0.0 defaults sf objects to "dataframe" (a record array).
  # fastgeojson defaults to "geojson", because emitting a FeatureCollection for
  # an sf object is the point of the package and what existing callers rely on.
  # Set options(fastgeojson.sf = "dataframe") for strict jsonlite parity.
  sf_mode <- if (is.null(dots$sf)) getOption("fastgeojson.sf", "geojson") else dots$sf

          

  # `sf` is consumed only by the sf method, so -- as in jsonlite -- it is
  # validated only when the object actually is an sf. Passing it alongside a
  # bare sfc, or any non-sf input, is silently ignored rather than an error.
  if (inherits(x, "sf")) {
    sf_mode <- match.arg(sf_mode, c("geojson", "features", "dataframe"))
  }

  # ---- digits ----------------------------------------------------------
  # digits = Inf, the default, asks for the shortest decimal that reads back
  # as the same double: lossless, and smaller and faster than either
  # alternative. It is
  # not a jsonlite mode -- toJSON() warns and falls back to digits = NA -- and
  # is the only exact one here, since digits = NA keeps 15 significant digits
  # and about 94% of doubles from real arithmetic need 16 or 17.
  digits_int <- NULL
  if (length(digits) == 1L && is.numeric(digits) && !is.na(digits) &&
      is.infinite(digits) && digits > 0) {
    digits_int <- .DIGITS_SHORTEST
  } else if (!is.null(digits) && !(length(digits) == 1L && is.na(digits))) {
    d <- suppressWarnings(as.integer(unclass(digits)))
    # Every count from 16 up renders the same way, 17 significant digits as
    # in toJSON(); the clamp keeps a user's 255 off the code for Inf.
    if (length(d) == 1L && !is.na(d)) digits_int <- min(d, 254L)
  }

  if (as_bytes && !identical(pretty, FALSE)) {
    stop("as_bytes = TRUE and pretty are incompatible: pretty-printing needs a string.",
         call. = FALSE)
  }

  # ---- NULL input ------------------------------------------------------
  if (is.null(x)) {
    out <- if (null == "list") "{}" else "null"
    return(if (as_bytes) charToRaw(out) else .as_json_class(out))
  }

  # ---- already-encoded JSON -------------------------------------------
  if (inherits(x, "json")) {
    if (json_verbatim) {
      out <- enc2utf8(as.character(x))
      return(if (as_bytes) charToRaw(out) else .as_json_class(out))
    }
    x <- as.character(unclass(x))
  }

  # ---- AsIs / strict atomic -------------------------------------------
  if ((strict_atomic || inherits(x, "AsIs")) && is.atomic(x)) auto_unbox <- FALSE

  # ---- data frame preparation -----------------------------------------
  if (inherits(x, "data.frame")) {
    has_rn <- .row_names_info(x) > 0
    if (force) {
      if (has_rn) x[["_row"]] <- row.names(x)
      row.names(x) <- NULL
    } else if (!has_rn) {
      row.names(x) <- NULL
    }
  } else if (force && !inherits(x, "sf")) {
    x <- unclass(x)
  }

  # complex = "list" in column mode needs the whole column as one object,
  # which the per-row column writer cannot express. Detect it before .prep()
  # converts the column, and tell .prep() to leave it alone.
  complex_cols <- inherits(x, "data.frame") && dataframe == "columns" &&
    identical(complex, "list") && any(vapply(x, is.complex, logical(1)))

  # ---- class-specific pre-encoding ------------------------------------
  # Runs before the named-vector handling below, because converting e.g. a
  # named POSIXlt to character is what turns it *into* a named atomic vector.
  # Date = "ISO8601" is formatted by the Rust writer straight from the day
  # numbers. R's format() costs ~4.7 us per value, which made Date about 200x
  # slower than every other column type; the writer does it in ~5 ns.
  skip_date <- identical(Date, "ISO8601")
  opts <- list(
    Date = Date, POSIXt = POSIXt, UTC = UTC, time_format = time_format,
    complex = complex, raw = raw, digits = digits_int, na = na,
    skip_complex = complex_cols, skip_date = skip_date,
    auto_unbox = auto_unbox, dataframe = dataframe
  )
  # The scan is done in Rust: the interpreted-R version of it was 92% of the
  # total cost for a list of many small lists.
  if (needs_prep_impl(x, !skip_date)) x <- .prep(x, opts)

  # ---- named atomic vectors -------------------------------------------
  # jsonlite drops vector names by default; keep_vec_names = TRUE reinstates
  # the old shiny behaviour and emits a deprecation message.
  if (is.atomic(x) && !is.null(names(x)) && !is.matrix(x)) {
    if (keep_vec_names) {
      .warn_keep_vec_names()
      x <- as.list(x)
      auto_unbox <- TRUE
    } else {
      names(x) <- NULL
    }
  }

  # digits = I(n) counts SIGNIFICANT digits, which toJSON() renders with
  # sprintf("%.*g"). Marking digits_int with the AsIs class carries that down
  # to the writer, which has %.*g already; this used to signif() the whole
  # object in R instead and then format the rounded values at 15 digits, which
  # was wrong twice -- signif() rounds the binary value half-to-even where %g
  # rounds the decimal expansion, and 15 digits never selects scientific
  # notation, so 12345 at I(4) came out as 12340 against toJSON()'s 1.234e+04.
  if (use_signif && !is.null(digits_int) && digits_int >= 0L && digits_int <= 17L) {
    digits_int <- I(digits_int)
  }

  # ---- dispatch --------------------------------------------------------
  res <- if (inherits(x, "sf") && sf_mode != "dataframe") {
    .as_json_class(
      sf_geojson_str_impl(x, auto_unbox, na, null, factor, digits_int, sf_mode, always_decimal, matrix == "columnmajor", rownames, json_verbatim, as_bytes),
      geo = identical(sf_mode, "geojson")
    )
  } else if (complex_cols) {
    # {"col":{"real":[..],"imaginary":[..]}} -- a whole-column object, which
    # the per-row column writer has no way to produce. Column-oriented output
    # is exactly a named-list serialisation, so build that instead.
    cols <- lapply(unclass(x), function(col) {
      if (is.complex(col)) list(real = Re(col), imaginary = Im(col)) else col
    })
    .as_json_class(obj_json_str_impl(cols, auto_unbox, dataframe, na, null, factor,
                                     digits_int, always_decimal, FALSE, rownames,
                                     json_verbatim, as_bytes))
  } else if (inherits(x, "data.frame")) {
    # sf = "dataframe" is jsonlite's callNextMethod(): serialise the frame
    # normally and let the geometry column render as typed geometry objects
    # in its own position. Dropping the class is what routes it here.
    if (inherits(x, "sf")) x <- .drop_sf_class(x)
    if (nrow(x) == 0L && dataframe == "rows") {
      if (as_bytes) charToRaw("[]") else .as_json_class("[]")
    } else {
      .as_json_class(df_json_str_impl(x, auto_unbox, dataframe, na, null, factor, digits_int, always_decimal, matrix == "columnmajor", rownames, json_verbatim, as_bytes))
    }
  } else {
    .as_json_class(obj_json_str_impl(x, auto_unbox, dataframe, na, null, factor, digits_int, always_decimal, matrix == "columnmajor", rownames, json_verbatim, as_bytes))
  }

  if (as_bytes) return(res)
  if (!identical(pretty, FALSE)) res <- .as_json_class(.pretty(res, pretty), geo = inherits(res, "geojson"))
  res
}

# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

.as_json_class <- function(s, geo = FALSE) {
  # as_bytes = TRUE returns a raw vector, which has no json class and must not
  # go through as.character() -- that would render it as hex pairs.
  if (is.raw(s)) return(s)
  structure(as.character(s), class = if (geo) c("geojson", "json") else "json")
}

# Strips only the `sf` class, keeping any tibble classes beneath it, so an sf
# tibble degrades to a tibble rather than to a bare data.frame.
.drop_sf_class <- function(x) {
  class(x) <- setdiff(class(x), "sf")
  x
}

.not_yet <- function(cond, what) {
  if (isTRUE(cond)) {
    stop(sprintf("%s is not yet implemented in fastgeojson; use jsonlite::toJSON() for this case.", what),
         call. = FALSE)
  }
}

.warn_keep_vec_names <- function() {
  message(
    "Input to asJSON(keep_vec_names=TRUE) is a named vector. ",
    "In a future version of jsonlite, this option will not be supported, ",
    "and named vectors will be translated into arrays instead of objects. ",
    "If you want JSON object output, please use a named list instead. See ?toJSON."
  )
}

# NA as opposed to NaN: both satisfy is.na(), but they format differently.
is_true_na <- function(v) {
  u <- unclass(v)
  if (is.double(u)) is.na(u) & !is.nan(u) else is.na(u)
}

# Classes that need converting in R before reaching the Rust encoder.
.PREP_CLASSES <- c("POSIXt", "Date", "difftime", "integer64", "blob")

.needs_prep_atomic <- function(v) {
  is.complex(v) || is.raw(v) || inherits(v, .PREP_CLASSES)
}

# Cheap guard so the common case (plain vector, plain data frame) pays almost
# nothing for this machinery.
.needs_prep <- function(x) {
  if (.needs_prep_atomic(x)) return(TRUE)
  if (is.list(x)) {
    if (inherits(x, "sfc")) return(FALSE)
    for (el in x) {
      if (.needs_prep_atomic(el)) return(TRUE)
      if (is.list(el) && !inherits(el, "sfc") && .needs_prep(el)) return(TRUE)
    }
  }
  FALSE
}

.prep <- function(x, opts) {
  if (inherits(x, "POSIXt")) return(.encode_posixt(x, opts))
  if (inherits(x, "Date"))   return(if (isTRUE(opts$skip_date)) x else .encode_date(x, opts))
  if (inherits(x, "difftime")) return(as.numeric(x))
  if (inherits(x, "integer64")) return(.encode_integer64(x, opts))
  # A blob is a list of raw vectors, encoded elementwise into one character
  # vector rather than a list of length-one vectors.
  if (inherits(x, "blob")) return(vapply(x, .base64, character(1), USE.NAMES = FALSE))
  if (is.complex(x))         return(.encode_complex(x, opts))
  if (is.raw(x))             return(.encode_raw(x, opts))
  if (is.list(x)) {
    if (inherits(x, "sfc")) return(x)
    # `in_df` is set for the columns of a frame and must not leak into the
    # elements of a list column, which are whole vectors rendered as arrays.
    # It did: an integer64 list column came out as bare cells, `"l":3` where
    # toJSON() gives `"l":[3]`, and its NA as null where the array rule gives
    # "NA".
    opts$in_df <- FALSE
    if (inherits(x, "data.frame")) {
      # jsonlite's data.frame method defaults to complex = "string" because a
      # {real, imaginary} object cannot occupy a single column; an explicit
      # complex = "list" makes it emit a nested object per row, which we do not
      # build yet.
      # A complex column only stays "string" by default; an explicit
      # complex = "list" is honoured, per row, by .encode_complex_rows().
      if (isTRUE(opts$skip_complex)) return(x)
      if (!identical(opts$complex, "list")) opts$complex <- "string"
      # asJSON.data.frame passes its own na default ("NA", i.e. omit) down to
      # the column methods, so a complex NA is dropped inside a frame but
      # rendered as the literal "NA" at top level.
      opts$in_df <- TRUE
    }
    for (i in seq_along(x)) {
      el <- x[[i]]
      if (.needs_prep_atomic(el) || (is.list(el) && !inherits(el, "sfc"))) {
        enc <- .prep(el, opts)
        # A matrix column keeps its dim. Without this, converting a complex
        # matrix column produced a bare vector of nrow * ncol values, which
        # `[[<-.data.frame` rejects: "replacement has 6 rows, data has 2".
        d <- dim(el)
        if (!is.null(d) && is.null(dim(enc)) && length(enc) == length(el)) {
          dim(enc) <- d
        }
        x[[i]] <- enc
      }
    }
    return(x)
  }
  x
}

# Mirrors jsonlite's asJSON("POSIXt") exactly, including UTC = FALSE default.
.encode_posixt <- function(v, opts) {
  if (identical(opts$POSIXt, "epoch")) {
    return(floor(as.numeric(as.POSIXct(v)) * 1000))
  }
  if (identical(opts$POSIXt, "mongo")) return(.encode_posixt_mongo(v, opts))

  fast <- .encode_posixt_local(v, opts)
  if (!is.null(fast)) return(fast)

  fmt <- opts$time_format
  if (is.null(fmt)) {
    fmt <- if (identical(opts$POSIXt, "string")) {
      ""
    } else if (isTRUE(opts$UTC)) {
      "%Y-%m-%dT%H:%M:%SZ"
    } else {
      "%Y-%m-%dT%H:%M:%S"
    }
  }
  out <- if (isTRUE(opts$UTC)) format(v, format = fmt, tz = "UTC") else format(v, format = fmt)
  out[is.na(v)] <- NA_character_
  # `format()` drops names for POSIXct/POSIXlt; keep_vec_names needs them.
  nm <- names(v)
  if (!is.null(nm)) names(out) <- nm
  out
}

# jsonlite: ISO8601 -> format(x); epoch -> unclass(x), i.e. DAYS not millis.
.encode_date <- function(v, opts) {
  if (identical(opts$Date, "epoch")) return(unclass(v))
  out <- format(v)
  # is.na() is true for NaN as well, but format() renders NaN as the literal
  # "NaN" and jsonlite emits that string. Only a true NA becomes NA_character_.
  out[is_true_na(v)] <- NA_character_
  out
}

# MongoDB extended JSON: {"$date": <millis>} per element.
#
# The shape depends on position, so the two cases are built separately:
# inside a data.frame each ROW carries a bare object, whereas at top level or
# in a list the whole vector is one array. Both are produced as pre-rendered
# `json` text, which the encoder splices verbatim.
# Hands the writer local civil seconds instead of formatted text.
#
# format() on a POSIXct costs ~5.3 us per value, which made a timestamp column
# about 200x slower than every other type. Only the UTC offset actually needs
# R, and as.POSIXlt() supplies it for ~0.07 us per value; adding it to the
# epoch seconds gives local civil time, which the writer turns into text with
# plain arithmetic.
#
# Returns NULL for the cases that still need R: a caller-supplied
# `time_format`, sub-second digits, a named vector (keep_vec_names would take
# it apart), or a time zone R cannot resolve to an offset.
# Time zones whose UTC offset is zero at every instant and that never observe
# daylight saving. Verified against as.POSIXlt()$gmtoff over instants spanning
# eighty years plus NA and the infinities: every one is 0. Not on the list:
# anything with a numeric offset ("Etc/GMT+5"), and "" (the machine's zone).
.zero_offset_zones <- c(
  "UTC", "GMT", "Etc/UTC", "Etc/GMT", "Etc/Zulu", "Zulu", "UCT", "Universal",
  "Greenwich", "GMT0", "GMT+0", "GMT-0", "Etc/UCT", "Etc/Universal",
  "Etc/Greenwich", "Etc/GMT0", "Etc/GMT+0", "Etc/GMT-0"
)

.encode_posixt_local <- function(v, opts) {
  if (!is.null(opts$time_format)) return(NULL)
  if (!is.null(names(v))) return(NULL)

  mode <- opts$POSIXt %||% "string"
  utc <- isTRUE(opts$UTC)

  # Only format = "" consults digits.secs; the ISO8601 layouts are explicit
  # formats with no %OS in them, so they are unaffected by the option.
  if (identical(mode, "string")) {
    ds <- getOption("digits.secs")
    if (!is.null(ds) && !isTRUE(ds < 1)) return(NULL)
  }

  ct <- as.POSIXct(v)
  # format.POSIXct() reads the tzone attribute only when tz is not supplied;
  # mirror that, because passing tz = "" explicitly would silently switch to
  # the machine's local zone.
  tzone <- attr(ct, "tzone")
  tz <- if (utc) "UTC" else if (!is.null(tzone)) tzone[1L] else ""
  secs <- as.numeric(ct)
  # A zone whose offset is identically zero -- UTC and its aliases -- needs
  # no as.POSIXlt(): gmtoff is 0 for every instant there, NA and the
  # infinities included. That call cost about 70 ms per million values, all
  # of it to compute a vector of zeros, and it was 5-9x the Rust work for the
  # same column. tz == "" is the machine's zone and could be anything, so it
  # keeps the full path.
  off <- if (nzchar(tz) && tz %in% .zero_offset_zones) {
    rep.int(0L, length(secs))
  } else {
    unclass(as.POSIXlt(ct, tz = tz))$gmtoff
  }
  if (is.null(off) || length(off) != length(secs)) return(NULL)

  # An offset R could not resolve is only a problem where there is a real
  # instant to place; NA and the infinities have no offset and do not need one.
  nonfin <- !is.finite(secs)
  anyn <- any(nonfin)
  if (anyNA(off) && (!anyn || anyNA(off[!nonfin]))) return(NULL)

  local <- floor(secs) + off
  # floor() and the offset would turn NaN and the infinities into NA; put the
  # originals back so the writer can emit "NaN", "Inf" and "-Inf" as jsonlite
  # does, and null only for a true NA.
  if (anyn) local[nonfin] <- secs[nonfin]

  code <- if (identical(mode, "string")) {
    # format.POSIXlt's format = "" rule: a date alone when every element of
    # the vector is exactly midnight. It is a whole-vector decision, which is
    # why it is taken here rather than per element.
    #
    # Testing seconds-of-day on the unfloored value is the same test as
    # format.POSIXlt's all(c(sec, min, hour) == 0) -- a fractional second
    # makes sec non-zero there and the remainder non-zero here -- but it reads
    # one vector instead of building a copy of three.
    tod <- (secs + off) %% 86400
    if (all(tod == 0, na.rm = TRUE)) 0L else 1L
  } else if (utc) {
    3L
  } else {
    2L
  }
  structure(local, class = "fgjtime", fgjfmt = code)
}

.encode_posixt_mongo <- function(v, opts) {
  ms <- floor(as.numeric(as.POSIXct(v)) * 1000)
  # paste0() recycles a zero-length argument to "", so guard explicitly or a
  # zero-length input yields the bogus '{"$date":}'.
  if (!length(ms)) {
    return(structure(if (isTRUE(opts$in_df)) character(0) else "[]", class = "fgjson"))
  }
  miss <- is.na(ms)
  txt <- paste0('{"$date":', format(ms, scientific = FALSE, trim = TRUE), "}")
  if (isTRUE(opts$in_df)) {
    # NA_character_ makes the column omit the key under na = "smart", which is
    # what jsonlite does; the explicit modes get literal null / "NA".
    txt[miss] <- switch(opts$na %||% "smart",
                        string = '"NA"',
                        null   = "null",
                        NA_character_)
    return(structure(txt, class = "fgjson"))
  }
  txt[miss] <- if (identical(opts$na, "string")) '"NA"' else "null"
  structure(paste0("[", paste(txt, collapse = ","), "]"), class = "fgjson")
}

# bit64::integer64, as jsonlite emits it: the exact decimal digits, unquoted.
#
# jsonlite converts through its own C routine rather than as.numeric(), so a
# value past 2^53 keeps every digit; bit64's as.character() is exact too. The
# digits are pre-rendered and spliced verbatim, because a double cannot carry
# them. This used to hand the writer a plain character vector, which came out
# quoted -- ["1","1099511627776"] where toJSON() gives [1,1099511627776].
#
# NA follows the numeric rule, not the string one: "NA" by default at top
# level (null only for na = "null"), and in a frame the key is dropped under
# the default, as for any numeric column.
.encode_integer64 <- function(v, opts) {
  if (!length(v)) {
    return(structure(if (isTRUE(opts$in_df)) character(0) else "[]", class = "fgjson"))
  }
  miss <- is.na(v)
  txt <- as.character(v)
  if (isTRUE(opts$in_df)) {
    # The numeric column rule under the default: the key is dropped in row
    # mode (NA_character_ is what makes the writer do that), and the cell is
    # the string "NA" in column and values mode, exactly as a double column.
    txt[miss] <- switch(opts$na %||% "smart",
                        string = '"NA"',
                        null   = "null",
                        if (identical(opts$dataframe, "rows")) NA_character_ else '"NA"')
    return(structure(txt, class = "fgjson"))
  }
  txt[miss] <- if (identical(opts$na, "null")) "null" else '"NA"'
  if (isTRUE(opts$auto_unbox) && length(txt) == 1L) {
    return(structure(txt, class = "fgjson"))
  }
  structure(paste0("[", paste(txt, collapse = ","), "]"), class = "fgjson")
}

# MongoDB extended JSON for a raw vector: {"$binary":..., "$type":...}
.encode_raw_mongo <- function(v) {
  ty <- attr(v, "type")
  if (!length(ty)) ty <- 5
  structure(
    paste0('{"$binary":"', .base64(v), '","$type":"', as.character(ty), '"}'),
    class = "fgjson"
  )
}

# complex = "list" for a data.frame column: one {"real":..,"imaginary":..}
# object per row, with a missing part omitted (or null / "NA") exactly as a
# row-oriented frame treats any other missing value.
.encode_complex_rows <- function(v, opts) {
  re <- Re(v)
  im <- Im(v)
  na_txt <- switch(opts$na %||% "smart", string = '"NA"', null = "null", NA_character_)
  part <- function(key, val) {
    s <- vapply(val, function(z) {
      if (is.na(z)) na_txt else as.character(as_json(z, auto_unbox = TRUE, digits = .digits_arg(opts$digits)))
    }, character(1))
    ifelse(is.na(s), NA_character_, paste0('"', key, '":', s))
  }
  a <- part("real", re)
  b <- part("imaginary", im)
  inner <- ifelse(is.na(a), ifelse(is.na(b), "", b),
                  ifelse(is.na(b), a, paste0(a, ",", b)))
  structure(paste0("{", inner, "}"), class = "fgjson")
}

`%||%` <- function(a, b) if (is.null(a)) b else a

# What `digits = Inf` becomes on the way to the writer: the code for "shortest
# decimal that reads back as the same double". R's own formatters reject it
# as a digit count, so every R-side encoder that renders a number must test
# for it rather than pass it on.
.DIGITS_SHORTEST <- 255L

.encode_complex <- function(v, opts) {
  if (identical(opts$complex, "list")) {
    if (isTRUE(opts$in_df)) return(.encode_complex_rows(v, opts))
    return(list(real = Re(v), imaginary = Im(v)))
  }
  d <- if (is.null(opts$digits)) 5L else opts$digits
  out <- if (identical(d, .DIGITS_SHORTEST)) .complex_shortest(v) else prettyNum(x = v, digits = d)
  # jsonlite's complex method defaults to na = "string", so a non-finite value
  # keeps prettyNum's literal "NA" text -- except inside a data.frame, whose
  # method passes na = "NA" (omit) down to its columns.
  literal_na <- if (identical(opts$na, "string")) {
    TRUE
  } else if (identical(opts$na, "null")) {
    FALSE
  } else {
    !isTRUE(opts$in_df)
  }
  if (!literal_na) out[!is.finite(v)] <- NA_character_
  if (length(v)) names(out) <- names(v)
  # prettyNum() and .complex_shortest() both return a plain vector; a complex
  # matrix at top level came out flat where toJSON() nests it by row. (Only
  # when there is a dim: `dim(out) <- NULL` would strip the names too.)
  if (!is.null(dim(v))) dim(out) <- dim(v)
  out
}

.encode_raw <- function(v, opts) {
  switch(
    opts$raw,
    base64 = .base64(v),
    mongo  = .encode_raw_mongo(v),
    hex    = as.character(as.hexmode(as.integer(v))),
    int    = as.integer(v),
    # Pre-rendered JavaScript, not a string: marked fgjson so it is spliced
    # whatever json_verbatim says, since it is ours and not the user's.
    js     = structure(paste0("(new Uint8Array(", .compact_ints(as.integer(v)), "))"),
                       class = "fgjson"),
    .base64(v)
  )
}

.compact_ints <- function(v) paste0("[", paste(v, collapse = ","), "]")

# The user-facing `digits` that an internal digits code stands for, for
# encoders that call as_json() again on a piece of their input.
.digits_arg <- function(d) if (identical(d, .DIGITS_SHORTEST)) Inf else d

# prettyNum()'s layout -- real, signed imaginary, "i" -- with each part the
# shortest decimal that reads back exactly. jsonlite has no lossless setting
# for complex at all: toJSON(z, digits = NA) errors inside prettyNum().
# Non-finite parts follow prettyNum(): "Inf+1i", "1-Infi", "NaN+1i", and the
# literal "NA" whenever either part is NA, for .encode_complex to treat as it
# treats prettyNum's.
.complex_shortest <- function(v) {
  if (!length(v)) return(character())
  parts <- function(p) {
    # c() drops dim and names: Re() of a matrix is a matrix, and the writer
    # would nest it. na = "string" renders NA, NaN and Inf as quoted words;
    # strip the quotes.
    s <- as.character(as_json(c(p), digits = Inf, na = "string"))
    gsub('"', "", strsplit(substr(s, 2L, nchar(s) - 1L), ",", fixed = TRUE)[[1L]], fixed = TRUE)
  }
  re <- Re(v)
  im <- Im(v)
  out <- paste0(parts(re), ifelse(!is.na(im) & im < 0, "-", "+"), parts(abs(im)), "i")
  out[(is.na(re) & !is.nan(re)) | (is.na(im) & !is.nan(im))] <- "NA"
  out
}

# Minimal base64 encoder so we do not depend on jsonlite at run time.
.base64 <- function(v) {
  alphabet <- c(LETTERS, letters, 0:9, "+", "/")
  n <- length(v)
  if (n == 0L) return("")
  pad <- (3L - n %% 3L) %% 3L
  v <- c(as.integer(v), rep(0L, pad))
  m <- matrix(v, nrow = 3L)
  b <- m[1L, ] * 65536L + m[2L, ] * 256L + m[3L, ]
  idx <- cbind(
    b %/% 262144L,
    (b %/% 4096L) %% 64L,
    (b %/% 64L) %% 64L,
    b %% 64L
  )
  out <- alphabet[t(idx) + 1L]
  if (pad > 0L) out[(length(out) - pad + 1L):length(out)] <- "="
  paste(out, collapse = "")
}


.pretty <- function(s, pretty) {
  indent <- if (isTRUE(pretty)) 2L else as.integer(pretty)
  if (is.na(indent)) indent <- 2L
  if (abs(indent) >= 20L) stop("`pretty` must be smaller than 20.", call. = FALSE)
  pretty_json_impl(as.character(s), indent)
}

# ------------------------------------------------------------------
# Thread control
# ------------------------------------------------------------------

#' Get or set the number of worker threads
#'
#' @description
#' Controls how many threads the Rust backend uses for parallel serialization.
#'
#' Normal use gets the whole machine. The count is resolved in this order: an
#' explicit value set here; then `FASTGEOJSON_NUM_THREADS`,
#' `RAYON_NUM_THREADS`, `OMP_NUM_THREADS` or `OMP_THREAD_LIMIT`; then two if
#' `R CMD check` is detected (CRAN's policy caps *checking* at two cores, since
#' the check farm is shared -- it places no limit on ordinary use); then the
#' number of available cores.
#'
#' @param n Integer. The number of threads to use. `n = 1` disables parallelism
#'   entirely, which is useful for reproducible benchmarking. `n <= 0` restores
#'   the automatic behaviour described above. `n = NULL` (default) changes
#'   nothing and reports the current value.
#'
#' @return An integer scalar, the number of worker threads in use after the
#'   call. Returned invisibly when `n` is supplied; visibly when it is not.
#'
#' @examples
#' fastgeojson_threads()
#' old <- fastgeojson_threads(2)
#' fastgeojson_threads(0)
#'
#' @export
fastgeojson_threads <- function(n = NULL) {
  if (!is.null(n)) {
    if (!is.numeric(n) || length(n) != 1L || is.na(n)) {
      stop("`n` must be a single non-NA number, or NULL.", call. = FALSE)
    }
    return(invisible(threads_impl(as.integer(n))))
  }
  threads_impl(NULL)
}


