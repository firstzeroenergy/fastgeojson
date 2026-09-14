#' Fast Serialization of R Objects to JSON and GeoJSON
#'
#' @description
#' `as_json()` is a high-performance, drop-in replacement for
#' [jsonlite::toJSON()]. It takes the same arguments, in the same order, with
#' the same defaults, and is intended to produce byte-identical output. The
#' encoders are implemented in Rust via **extendr** and parallelised with
#' `rayon`, and `sf` objects gain a dedicated GeoJSON path.
#'
#' @details
#' \strong{Drop-in use.} Because the signature matches `jsonlite::toJSON()`
#' argument-for-argument, existing code can usually be ported by changing only
#' the function name. Unknown arguments are accepted through `...` and passed
#' to the encoder, exactly as `toJSON()` does.
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
#'   \item \strong{Numeric:} JSON numbers, rounded to `digits` decimal places.
#'     `NA`, `NaN`, `Inf` and `-Inf` follow the `na` argument.
#'   \item \strong{Logical:} `true` / `false`.
#'   \item \strong{Character:} JSON strings. Output is always valid UTF-8;
#'     latin1- and native-encoded inputs are translated.
#'   \item \strong{Factor:} labels, or integer codes when `factor = "integer"`.
#'   \item \strong{Date / POSIXt:} controlled by `Date` and `POSIXt`.
#'   \item \strong{complex / raw:} controlled by `complex` and `raw`.
#' }
#'
#' \strong{Threads.} See [fastgeojson_threads()] to control parallelism.
#'
#' @param x The object to serialize.
#' @param dataframe How to encode data frames: `"rows"` (default,
#'   `[{"a":1},{"a":2}]`) or `"columns"` (`{"a":[1,2]}`).
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
#' @param digits Number of decimal places for numeric values, or `NA` for the
#'   shortest round-trippable representation. Wrap in [base::I()] to interpret
#'   as *significant* digits instead. Defaults to `4`, as in `toJSON()`.
#' @param pretty If `TRUE`, indent the output by two spaces; a number sets the
#'   indent width.
#' @param force If `TRUE`, strip S3 classes that would otherwise raise an
#'   error. `sf` objects are exempt so geometry handling is preserved.
#' @param ... Further arguments passed to the encoder, mirroring
#'   `jsonlite::toJSON()`. Recognised here: `keep_vec_names`, `rownames`,
#'   `json_verbatim`, `UTC`, `time_format`, `always_decimal`, `use_signif`,
#'   `indent` and `sf`.
#'
#' @return A length-one character vector of class `"json"`, or
#'   `c("geojson", "json")` for `sf` input.
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
    digits = 4,
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
  rownames        <- if (is.null(dots$rownames)) TRUE else isTRUE(dots$rownames)
  use_signif      <- if (is.null(dots$use_signif)) inherits(digits, "AsIs") else isTRUE(dots$use_signif)
  strict_atomic   <- isTRUE(dots$strict_atomic)
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
  digits_int <- NULL
  if (!is.null(digits) && !(length(digits) == 1L && is.na(digits))) {
    d <- suppressWarnings(as.integer(unclass(digits)))
    if (length(d) == 1L && !is.na(d)) digits_int <- d
  }

  # ---- NULL input ------------------------------------------------------
  if (is.null(x)) {
    return(.as_json_class(if (null == "list") "{}" else "null"))
  }

  # ---- already-encoded JSON -------------------------------------------
  if (inherits(x, "json")) {
    if (json_verbatim) {
      out <- enc2utf8(as.character(x))
      return(.as_json_class(out))
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
    } else if (!has_rn || !rownames) {
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
  opts <- list(
    Date = Date, POSIXt = POSIXt, UTC = UTC, time_format = time_format,
    complex = complex, raw = raw, digits = digits_int, na = na,
    skip_complex = complex_cols
  )
  if (.needs_prep(x)) x <- .prep(x, opts)

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

  if (use_signif && !is.null(digits_int) && digits_int < 16L) {
    x <- .apply_signif(x, digits_int)
    digits_int <- NULL
  }

  # ---- dispatch --------------------------------------------------------
  res <- if (inherits(x, "sf") && sf_mode != "dataframe") {
    .as_json_class(
      sf_geojson_str_impl(x, auto_unbox, na, null, factor, digits_int, sf_mode, always_decimal, matrix == "columnmajor"),
      geo = identical(sf_mode, "geojson")
    )
  } else if (complex_cols) {
    # {"col":{"real":[..],"imaginary":[..]}} -- a whole-column object, which
    # the per-row column writer has no way to produce. Column-oriented output
    # is exactly a named-list serialisation, so build that instead.
    cols <- lapply(unclass(x), function(col) {
      if (is.complex(col)) list(real = Re(col), imaginary = Im(col)) else col
    })
    .as_json_class(obj_json_str_impl(cols, auto_unbox, na, null, factor,
                                     digits_int, always_decimal, FALSE))
  } else if (inherits(x, "data.frame")) {
    # sf = "dataframe" is jsonlite's callNextMethod(): serialise the frame
    # normally and let the geometry column render as typed geometry objects
    # in its own position. Dropping the class is what routes it here.
    if (inherits(x, "sf")) x <- .drop_sf_class(x)
    if (nrow(x) == 0L && dataframe == "rows") {
      .as_json_class("[]")
    } else {
      .as_json_class(df_json_str_impl(x, auto_unbox, dataframe, na, null, factor, digits_int, always_decimal, matrix == "columnmajor"))
    }
  } else {
    .as_json_class(obj_json_str_impl(x, auto_unbox, na, null, factor, digits_int, always_decimal, matrix == "columnmajor"))
  }

  if (!identical(pretty, FALSE)) res <- .as_json_class(.pretty(res, pretty), geo = inherits(res, "geojson"))
  res
}

# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

.as_json_class <- function(s, geo = FALSE) {
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
  if (inherits(x, "Date"))   return(.encode_date(x, opts))
  if (inherits(x, "difftime")) return(as.numeric(x))
  if (inherits(x, "integer64")) return(as.character(x))
  # A blob is a list of raw vectors, encoded elementwise into one character
  # vector rather than a list of length-one vectors.
  if (inherits(x, "blob")) return(vapply(x, .base64, character(1), USE.NAMES = FALSE))
  if (is.complex(x))         return(.encode_complex(x, opts))
  if (is.raw(x))             return(.encode_raw(x, opts))
  if (is.list(x)) {
    if (inherits(x, "sfc")) return(x)
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
        x[[i]] <- .prep(el, opts)
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
  out[is.na(v)] <- NA_character_
  out
}

# MongoDB extended JSON: {"$date": <millis>} per element.
#
# The shape depends on position, so the two cases are built separately:
# inside a data.frame each ROW carries a bare object, whereas at top level or
# in a list the whole vector is one array. Both are produced as pre-rendered
# `json` text, which the encoder splices verbatim.
.encode_posixt_mongo <- function(v, opts) {
  ms <- floor(as.numeric(as.POSIXct(v)) * 1000)
  # paste0() recycles a zero-length argument to "", so guard explicitly or a
  # zero-length input yields the bogus '{"$date":}'.
  if (!length(ms)) {
    return(structure(if (isTRUE(opts$in_df)) character(0) else "[]", class = "json"))
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
    return(structure(txt, class = "json"))
  }
  txt[miss] <- if (identical(opts$na, "string")) '"NA"' else "null"
  structure(paste0("[", paste(txt, collapse = ","), "]"), class = "json")
}

# MongoDB extended JSON for a raw vector: {"$binary":..., "$type":...}
.encode_raw_mongo <- function(v) {
  ty <- attr(v, "type")
  if (!length(ty)) ty <- 5
  structure(
    paste0('{"$binary":"', .base64(v), '","$type":"', as.character(ty), '"}'),
    class = "json"
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
      if (is.na(z)) na_txt else as.character(as_json(z, auto_unbox = TRUE, digits = opts$digits))
    }, character(1))
    ifelse(is.na(s), NA_character_, paste0('"', key, '":', s))
  }
  a <- part("real", re)
  b <- part("imaginary", im)
  inner <- ifelse(is.na(a), ifelse(is.na(b), "", b),
                  ifelse(is.na(b), a, paste0(a, ",", b)))
  structure(paste0("{", inner, "}"), class = "json")
}

`%||%` <- function(a, b) if (is.null(a)) b else a

.encode_complex <- function(v, opts) {
  if (identical(opts$complex, "list")) {
    if (isTRUE(opts$in_df)) return(.encode_complex_rows(v, opts))
    return(list(real = Re(v), imaginary = Im(v)))
  }
  d <- if (is.null(opts$digits)) 5L else opts$digits
  out <- prettyNum(x = v, digits = d)
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
  out
}

.encode_raw <- function(v, opts) {
  switch(
    opts$raw,
    base64 = .base64(v),
    mongo  = .encode_raw_mongo(v),
    hex    = as.character(as.hexmode(as.integer(v))),
    int    = as.integer(v),
    js     = .as_json_class(paste0("(new Uint8Array(", .compact_ints(as.integer(v)), "))")),
    .base64(v)
  )
}

.compact_ints <- function(v) paste0("[", paste(v, collapse = ","), "]")

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

.apply_signif <- function(x, digits) {
  if (is.list(x)) {
    if (inherits(x, "sfc")) return(x)
    x[] <- lapply(x, function(col) {
      if (is.numeric(col) && !is.integer(col)) signif(col, digits)
      else if (is.list(col)) .apply_signif(col, digits)
      else col
    })
    return(x)
  }
  if (is.numeric(x) && !is.integer(x)) return(signif(x, digits))
  x
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
#' @return The number of threads that will be used; invisibly when setting.
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

# ------------------------------------------------------------------
# Legacy entry points
# ------------------------------------------------------------------

#' Direct encoders for `sf` objects and data frames
#'
#' @description
#' Thin wrappers over the Rust encoders. `as_json()` is preferred; these are
#' retained for backward compatibility and skip `as_json()`'s argument
#' handling, so they do not perform `Date`/`POSIXt`/`complex`/`raw`
#' pre-encoding.
#'
#' @param x An `sf` object (`sf_geojson_str`) or a data frame (`df_json_str`).
#' @param auto_unbox,na,null,factor,digits As in [as_json()].
#' @param dataframe `"rows"` or `"columns"`.
#'
#' @return A length-one character vector of class `"json"`, or
#'   `c("geojson", "json")` for `sf_geojson_str()`.
#'
#' @examples
#' df_json_str(data.frame(a = 1:2))
#'
#' @name legacy-encoders
#' @export
sf_geojson_str <- function(x, auto_unbox = FALSE, na = "smart", null = "list",
                           factor = "string", digits = NULL) {
  if (!inherits(x, "sf")) stop("Not an sf object", call. = FALSE)
  sf_geojson_str_impl(x, auto_unbox, na, null, factor, digits, "geojson", FALSE, FALSE)
}

#' @rdname legacy-encoders
#' @export
df_json_str <- function(x, auto_unbox = FALSE, dataframe = "rows", na = "smart",
                        null = "list", factor = "string", digits = NULL) {
  if (!inherits(x, "data.frame")) stop("Not a data.frame", call. = FALSE)
  df_json_str_impl(x, auto_unbox, dataframe, na, null, factor, digits, FALSE, FALSE)
}
