#!/usr/bin/env Rscript
# The recursive path: data frames reached through a list rather than as the
# top-level object, row-name handling, and encoding safety.
#
# Three defects lived here and each has a case below:
#
#  * `is_default_rownames` was called once per row inside the row loop, and
#    `Rf_getAttrib(R_RowNamesSymbol)` expands the compact c(NA, -n) form so its
#    O(1) test never fired. Theta(n^2): 392 ms for 32,000 rows, growing 4x per
#    doubling. The scaling case below fails if it comes back.
#
#  * `_row` was emitted for integer row names and for character row names that
#    all look like digits, where jsonlite emits nothing.
#
#  * The direct-read prepass trusted R's CE_UTF8 mark without validating, so a
#    string marked UTF-8 that is not UTF-8 reached `from_utf8_unchecked` --
#    undefined behaviour, from an ordinary R object.
#
#   Rscript tools/bench/verify_nested_parity.R

suppressMessages({library(fastgeojson); library(jsonlite)})

fails <- 0L
cmp <- function(x, label, ...) {
  # Both sides are allowed to fail, and a matching failure is a pass: toJSON()
  # refuses a "bytes"-marked string and so do we, with its message. Compared
  # as bytes, because some of the encoding cases below hold values R cannot
  # substr() in this locale.
  take <- function(e) tryCatch(charToRaw(as.character(e())),
                               error = function(c) charToRaw(paste("ERR:", conditionMessage(c))))
  want <- take(function() toJSON(x, ...))
  got  <- take(function() as_json(x, ...))
  if (identical(want, got)) {
    cat(sprintf("  PASS  %-48s\n", label))
  } else {
    fails <<- fails + 1L
    n <- min(length(want), length(got))
    i <- which(want[seq_len(n)] != got[seq_len(n)])[1]
    cat(sprintf("  FAIL  %-48s\n        first differing byte: %s\n        R   : %s\n        rust: %s\n",
                label,
                if (is.na(i)) sprintf("(length %d vs %d)", length(want), length(got)) else as.character(i),
                paste(format(utils::head(want, 50)), collapse = " "),
                paste(format(utils::head(got, 50)), collapse = " ")))
  }
}

cat("== row names, jsonlite's rule: character AND not all digits ==\n")
mk <- function(rn) { d <- data.frame(a = 1:3); if (!is.null(rn)) attr(d, "row.names") <- rn; d }
cases <- list(
  "automatic"              = NULL,
  "character r1,r2,r3"     = c("r1", "r2", "r3"),
  "character 1,2,3"        = c("1", "2", "3"),
  "character 5,6,7"        = c("5", "6", "7"),
  "integer 5,6,7"          = c(5L, 6L, 7L),
  "integer 1,2,3"          = c(1L, 2L, 3L),
  "character with empty"   = c("", "b", "c"),
  "character mixed"        = c("1", "b", "3"),
  "character with digits2" = c("01", "02", "03")
)
for (nm in names(cases)) {
  d <- mk(cases[[nm]])
  cmp(d, paste("top-level,", nm))
  cmp(list(d = d), paste("nested,", nm))
}
cmp(list(a = list(b = mk(c("r1", "r2", "r3")))), "twice nested, character row names")
cmp(mk(c("r1", "r2", "r3")), "rownames = FALSE", rownames = FALSE)
cmp(mk(c("r1", "r2", "r3")), "force = TRUE", force = TRUE)

cat("\n== a nested frame must be linear in its row count ==\n")
# Timing, but the signal is enormous: the defect grew 4x per doubling against
# 2x for linear, so even a loaded machine cannot blur them.
fastgeojson_threads(1)
ns <- c(4000, 8000, 16000, 32000)
el <- vapply(ns, function(n) {
  x <- list(d = data.frame(a = seq_len(n)))
  invisible(as_json(x))
  best <- Inf
  for (i in 1:3) {
    t0 <- Sys.time(); invisible(as_json(x))
    best <- min(best, as.numeric(Sys.time() - t0, units = "secs"))
  }
  best
}, 0)
fastgeojson_threads(0)
growth <- el[-1] / el[-length(el)]
cat(sprintf("  n=%6d  %8.2f ms%s\n", ns, el * 1000,
            c("", sprintf("   growth %.2fx", growth))))
# Linear is ~2x. Quadratic is ~4x. Fail above 3x, which neither can reach.
if (any(growth > 3)) {
  fails <- fails + 1L
  cat(sprintf("  FAIL  growth per doubling reached %.2fx -- superlinear\n", max(growth)))
} else {
  cat(sprintf("  PASS  growth per doubling stays at %.2fx, linear\n", max(growth)))
}

cat("\n== deep nesting: accepted up to MAX_DEPTH, refused past it ==\n")
# scan_needs_prep gave up at depth 64 and claimed prep was needed, which sent
# the whole structure through the interpreted-R .prep() recursion. That
# exhausts R's node stack near 1600 levels, so as_json() failed on input the
# Rust serializer accepts -- and cost 15.7 ms at depth 1000 doing it.
deepnest <- function(d) { x <- 1; for (i in seq_len(d)) x <- list(x); x }
for (d in c(100, 1000, 3000, 4999)) {
  r <- tryCatch({ invisible(as_json(deepnest(d))); TRUE },
                error = function(e) conditionMessage(e))
  if (isTRUE(r)) {
    cat(sprintf("  PASS  depth %d serialises\n", d))
  } else {
    fails <- fails + 1L
    cat(sprintf("  FAIL  depth %d: %s\n", d, substr(r, 1, 60)))
  }
}
r <- tryCatch({ invisible(as_json(deepnest(6000))); "returned" },
              error = function(e) conditionMessage(e))
if (grepl("nested more than", r)) {
  cat("  PASS  depth 6000 raises the depth error\n")
} else {
  fails <- fails + 1L
  cat(sprintf("  FAIL  depth 6000: %s\n", substr(r, 1, 60)))
}
# The deeper scan must still find what genuinely needs the R thread.
{
  x <- structure(19000, class = "Date"); for (i in 1:200) x <- list(x)
  if (grepl('"2022-01-08"', as.character(as_json(x)), fixed = TRUE)) {
    cat("  PASS  a Date 200 levels deep is still converted\n")
  } else {
    fails <- fails + 1L; cat("  FAIL  deep Date was not converted\n")
  }
  y <- as.POSIXct("2020-01-01 12:00:00", tz = "UTC"); for (i in 1:200) y <- list(y)
  if (grepl("2020-01-01 12:00:00", as.character(as_json(y)), fixed = TRUE)) {
    cat("  PASS  a POSIXct 200 levels deep is still converted\n")
  } else {
    fails <- fails + 1L; cat("  FAIL  deep POSIXct was not converted\n")
  }
}

cat("\n== a nested frame is the same frame: column kinds and arguments ==\n")
# The recursive serializer used to write nested frames with its own per-cell
# loop, which knew about far less than a column can be. Nine defects, every
# one of them reachable from as_json(list(d = df)):
#
#   Date -> 18262 instead of "2020-01-01"; POSIXct -> 1577872800; the default
#   `na` emitting "NA" and null where jsonlite omits the key; a matrix column
#   emitting only the row's first value; an sfc column losing its type;
#   dataframe = "columns" and "values" ignored outright; and the same Date and
#   matrix defects again in a data.frame-valued column and in a list column
#   holding a frame.
d_date <- data.frame(a = 1:2, d = as.Date(c("2020-01-01", "2021-06-15")))
d_time <- data.frame(a = 1:2, t = as.POSIXct(c("2020-01-01 10:00:00",
                                               "2021-06-15 23:59:59"), tz = "UTC"))
d_na <- data.frame(a = c(1, NA), b = c("p", NA), stringsAsFactors = FALSE)
d_mat <- data.frame(a = 1:2); d_mat$m <- matrix(1:4, nrow = 2)
d_lgl <- data.frame(l = c(TRUE, NA), s = c("a", NA), stringsAsFactors = FALSE)
cmp(list(d = d_date), "nested Date column")
cmp(list(d = d_time), "nested POSIXct column")
cmp(list(d = d_na), "nested NA, default na (the key is omitted)")
cmp(list(d = d_na), "nested NA, na = null", na = "null")
cmp(list(d = d_na), "nested NA, na = string", na = "string")
cmp(list(d = d_lgl), "nested logical and character NA")
cmp(list(d = d_mat), "nested matrix column")
cmp(list(d = d_date), "nested frame, dataframe = columns", dataframe = "columns")
cmp(list(d = d_date), "nested frame, dataframe = values", dataframe = "values")
cmp(list(d = data.frame(f = factor(c("x", "y")))), "nested factor column")
cmp(list(d = data.frame(f = factor(c("x", "y")))), "nested factor, factor = integer",
    factor = "integer")
cmp(list(a = list(b = d_date)), "twice-nested Date")
cmp(list(d = data.frame(a = c(1.23456789, 2.3456789))), "nested digits = 8", digits = 8)
cmp(list(d = data.frame(a = 1)), "nested one-row frame, auto_unbox", auto_unbox = TRUE)
cmp(list(d = data.frame()), "nested empty frame")
cmp(list(d = data.frame(a = integer(0))), "nested zero-row frame")
cmp(list(d = data.frame(t = as.POSIXct("2020-01-01", tz = "UTC"))),
    "nested POSIXt = epoch", POSIXt = "epoch")
cmp(list(d = d_date), "nested Date = epoch", Date = "epoch")
# A data.frame-valued column, as tidyr::nest() produces, and a list column
# holding a frame: both went through the same per-cell writer.
dn <- data.frame(a = 1:2)
dn$n <- data.frame(d = as.Date(c("2020-01-01", "2020-01-02")), k = c(1.5, 2.5))
cmp(dn, "data.frame-valued column with a Date")
dn2 <- data.frame(a = 1:2); dn2$n <- data.frame(m = I(matrix(1:4, nrow = 2)))
cmp(dn2, "data.frame-valued column with a matrix")
dl <- data.frame(a = 1:2)
dl$l <- list(data.frame(d = as.Date("2020-01-01")), 3)
cmp(dl, "list column holding a frame with a Date")
inner <- data.frame(k = c(1, 2)); row.names(inner) <- c("r1", "r2")
di <- data.frame(a = 1:2); di$n <- inner
cmp(di, "data.frame-valued column with row names")
if (requireNamespace("sf", quietly = TRUE)) {
  ds <- data.frame(a = 1:2)
  ds$g <- sf::st_sfc(sf::st_point(c(1, 2)), sf::st_point(c(3, 4)))
  cmp(list(d = ds), "nested frame with an sfc column")
}
# Frames joined by list columns recurse through the column builder, which is
# why the depth counter has to travel with it.
{
  mk <- function(n) {
    x <- data.frame(a = 1L)
    for (i in seq_len(n)) { y <- data.frame(a = 1L); y$l <- list(x); x <- y }
    x
  }
  ok <- TRUE
  for (n in c(10, 200, 1000)) {
    r <- tryCatch({ invisible(as_json(list(d = mk(n)))); TRUE },
                  error = function(e) conditionMessage(e))
    if (!isTRUE(r)) {
      ok <- FALSE
      fails <- fails + 1L
      cat(sprintf("  FAIL  chain of %d frames: %s\n", n, substr(r, 1, 60)))
    }
  }
  if (ok) cat("  PASS  chains of 10, 200 and 1000 frames via list columns\n")
}

cat("\n== bytes that are not valid UTF-8 ==\n")
# `Encoding<-` sets the mark without validating, and a string read with
# rawToChar() carries no mark at all, so R will happily hold a "string" whose
# bytes are not UTF-8. toJSON() emits those bytes unchanged and marks the
# result UTF-8 even though it is not -- output that is not valid JSON.
#
# That is what we reproduce. An earlier version of this file asserted the
# opposite, that such input must be refused, and we raised "serializer
# produced invalid UTF-8" where toJSON() succeeded. Refusing is not parity.
# The concern behind the old assertion was real but is now moot: it was that
# `String::from_utf8_unchecked` would be undefined behaviour on those bytes,
# and the result is no longer built through a Rust `String` at all --
# `Rf_mkCharLenCE` takes the bytes directly.
#
# The one input toJSON() does refuse is a "bytes"-marked string, and cmp()
# compares the error text, so that stays covered too.
enc_as <- function(v, e) { x <- v; Encoding(x) <- rep(e, length(x)); x }
bad_u <- enc_as(rawToChar(as.raw(0xe9)), "UTF-8")     # marked UTF-8, is not
bad_n <- rawToChar(as.raw(0xe9))                      # unmarked, is not
stopifnot(!validUTF8(bad_u), !validUTF8(bad_n))

cmp(bad_u, "marked UTF-8 but invalid, bare")
cmp(bad_n, "native and invalid, bare")
cmp(data.frame(a = bad_u, stringsAsFactors = FALSE), "marked UTF-8 but invalid, in a frame")
cmp(data.frame(a = bad_n, stringsAsFactors = FALSE), "native and invalid, in a frame")
cmp(list(a = bad_n), "native and invalid, in a list")
cmp(setNames(list(1), bad_n), "native and invalid, as a key")
cmp(data.frame(f = factor(bad_n)), "native and invalid, as a factor")
cmp(c("ok", bad_n, "fine"), "invalid among valid")
cmp(enc_as(rawToChar(as.raw(c(0x61, 0xff, 0x62))), "bytes"), "bytes-marked, which must error")

# as_bytes hands back the same bytes; a raw vector carries no encoding, so
# there was never a question there.
b <- tryCatch(as_json(data.frame(a = bad_n, stringsAsFactors = FALSE), as_bytes = TRUE),
              error = function(e) NULL)
if (is.raw(b) && any(b == as.raw(0xe9))) {
  cat(sprintf("  PASS  %-50s\n", "as_bytes returns the bytes unchanged"))
} else {
  fails <- fails + 1L
  cat(sprintf("  FAIL  %-50s\n", "as_bytes did not return the original bytes"))
}

# Valid non-ASCII and latin1 must be untouched by any of this.
cmp(data.frame(a = c("café", "中文"), stringsAsFactors = FALSE), "valid UTF-8 unaffected")
lat <- enc_as(rawToChar(as.raw(c(0x63, 0x61, 0x66, 0xe9))), "latin1")
cmp(data.frame(a = lat, stringsAsFactors = FALSE), "latin1 still translated")
cmp(data.frame(a = c(lat, "café", "ascii"), stringsAsFactors = FALSE), "latin1 + UTF-8 + ascii")

cat("\n== rownames has three states, and reaches every depth ==\n")
# toJSON()'s rule, established against it directly: absent emits `_row` only
# when the row names are informative (character and not all digits); TRUE
# always emits it, rendering row.names(x) by type -- an integer unquoted, a
# character quoted, so the automatic 1..n comes out as numbers; FALSE never.
#
# This used to be applied in R, which meant it reached only the outermost
# frame: as_json(list(d = df), rownames = FALSE) still emitted `_row`. And the
# TRUE state did not exist at all -- it was folded into the absent one, so
# asking for row names a frame does not store produced nothing.
rnmk <- function(rn) {
  d <- data.frame(a = 1:2)
  if (!is.null(rn)) attr(d, "row.names") <- rn
  d
}
rnkinds <- list(
  "automatic"   = NULL,
  "integer 5,6" = c(5L, 6L),
  "integer 1,2" = c(1L, 2L),
  "character"   = c("x", "y"),
  "all digits"  = c("7", "8"),
  "with empty"  = c("", "b"),
  "zero padded" = c("01", "02")
)
rnfail <- 0L
rntot <- 0L
for (arg in list(list(), list(rownames = TRUE), list(rownames = FALSE))) {
  state <- if (length(arg) == 0L) "absent" else paste0("rownames = ", arg$rownames)
  for (nm in names(rnkinds)) {
    d <- rnmk(rnkinds[[nm]])
    for (dfm in c("rows", "columns", "values")) {
      for (where in c("top", "nested")) {
        x <- if (where == "top") d else list(z = d)
        want <- do.call(toJSON, c(list(x, dataframe = dfm), arg))
        got <- do.call(as_json, c(list(x, dataframe = dfm), arg))
        rntot <- rntot + 1L
        if (!identical(as.character(want), as.character(got))) {
          rnfail <- rnfail + 1L
          cat(sprintf("  FAIL  %s, %s, %s, %s\n        R   : %s\n        rust: %s\n",
                      nm, state, dfm, where,
                      substr(as.character(want), 1, 110),
                      substr(as.character(got), 1, 110)))
        }
      }
    }
  }
}
if (rnfail == 0L) {
  cat(sprintf("  PASS  %d cases: %d row-name kinds x 3 states x 3 orientations, top and nested\n",
              rntot, length(rnkinds)))
} else {
  fails <- fails + rnfail
}

cat("\n== a data.frame-valued column inherits the orientation ==\n")
# tidyr::nest() produces these. The column used to be rendered row-oriented
# whatever `dataframe` said, so `columns` gave [{...},{...}] where toJSON()
# gives one column-oriented object for the whole column, and `values` gave
# objects where toJSON() gives bare arrays.
dvi <- data.frame(a = 1:2)
dvi$n <- data.frame(a = 1:2)
dvr <- data.frame(a = 1:2)
dvr$n <- local({
  i <- data.frame(k = c(1.5, 2.5), s = c("p", "q"), stringsAsFactors = FALSE)
  row.names(i) <- c("r1", "r2")
  i
})
dvs <- data.frame(a = 1:3)
dvs$n <- data.frame(k = 1:3)
for (dfm in c("rows", "columns", "values")) {
  cmp(dvi, sprintf("data.frame column, %s", dfm), dataframe = dfm)
  cmp(dvr, sprintf("data.frame column with row names, %s", dfm), dataframe = dfm)
  cmp(dvs, sprintf("three-row data.frame column, %s", dfm), dataframe = dfm)
  cmp(list(z = dvi), sprintf("nested data.frame column, %s", dfm), dataframe = dfm)
  for (rn in c(TRUE, FALSE)) {
    cmp(dvr, sprintf("data.frame column, %s, rownames = %s", dfm, rn),
        dataframe = dfm, rownames = rn)
  }
}

cat("\n== names: an empty, NA or repeated one is rewritten as R rewrites it ==\n")
# The rule is the one R itself applies when it builds a data.frame: the
# element's 1-based index stands in for a name that is not there, and then
# make.unique appends .1, .2 and so on. make.unique looks at the WHOLE set,
# not just the names already emitted, which is why c("a", "a", "a.1") comes
# out as a, a.2, a.1 -- the .1 is already spoken for further along.
#
# We used to emit an empty key for an absent name and the same key twice for
# a repeated one, so `list(a = 1, 2)` produced {"a":[1],"":[2]} and a frame
# with two columns called a produced an object with two "a" keys.
name_cases <- list(
  c("a", "a"), c("a", "a", "a"), c("a", "a.1", "a"), c("a", "a", "a.1"),
  c("a", "a", "a", "a.1", "a.2"), c("", "a", ""), c("2", "", "b"),
  c(NA, "a"), c("a", "", "a"), c("", ""), c(NA, NA), c("a", NA, "a", NA),
  c("a.1", "a", "a"), c("x", "x.1", "x.1"), c("a", "b", "c"),
  c("日", "日"), c("a\\b", "a\\b"), c("a", "A"),
  c("1", "1"), c("a.0", "a", "a")
)
lbl <- function(v) paste(ifelse(is.na(v), "<NA>", ifelse(nzchar(v), v, "<>")), collapse = ",")
for (v in name_cases) {
  l <- as.list(seq_along(v)); names(l) <- v
  cmp(l, paste0("list names ", lbl(v)))
  cmp(list(z = l), paste0("list names nested ", lbl(v)))
}
for (v in name_cases) {
  if (anyNA(v)) next            # names<- on a frame will not take an NA
  d <- as.data.frame(matrix(seq_len(2 * length(v)), nrow = 2)); names(d) <- v
  for (dfm in c("rows", "columns", "values")) {
    cmp(d, paste0("column names ", lbl(v), ", ", dfm), dataframe = dfm)
  }
  cmp(list(inner = d), paste0("column names nested ", lbl(v)))
}

cat("\n== names: past the cutover, and on the writers the builder declines ==\n")
# Up to NAME_SCAN_MAX names a repeat is found by comparing every pair as the
# keys are written; past it a set is built up front instead. Both sides of
# that cutover, and the zero-row and zero-column frames that fall through to
# the recursive writer rather than the row builder.
w <- as.data.frame(matrix(1:80, nrow = 2)); names(w) <- sprintf("c%d", 1:40)
cmp(w, "40 distinct columns")
w2 <- w; names(w2)[c(5, 30)] <- "c1"
cmp(w2, "40 columns, two repeats")
w3 <- w; names(w3)[c(2, 9, 33)] <- ""
cmp(w3, "40 columns, three blanks")
w4 <- w; names(w4)[c(3, 20)] <- "c1"; names(w4)[7] <- ""
cmp(list(k = w4), "40 columns nested, both faults")
z <- data.frame(a = integer(0), b = integer(0)); names(z) <- c("a", "a")
e <- data.frame(a = integer(0), b = integer(0)); names(e) <- c("", "a")
for (dfm in c("rows", "columns", "values")) {
  cmp(z, paste0("0 rows, repeated name, ", dfm), dataframe = dfm)
  cmp(e, paste0("0 rows, blank name, ", dfm), dataframe = dfm)
  cmp(data.frame(), paste0("no columns at all, ", dfm), dataframe = dfm)
  cmp(list(q = z), paste0("0 rows nested, ", dfm), dataframe = dfm)
}

cat(sprintf("\n%d failure(s)\n", fails))
if (fails > 0) quit(status = 1)
