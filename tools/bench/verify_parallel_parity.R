#!/usr/bin/env Rscript
# The recursive path now splits long numeric runs across the worker pool, so
# every shape here is checked three ways: against jsonlite, and at one worker
# against many.
#
# The failure this guards against is a chunk boundary. Each worker writes its
# own range with the separator test `i > s` rather than `i > 0`, and the ranges
# are joined with a comma between them; getting either wrong produces a
# doubled or a missing separator exactly at a boundary, which no small test
# reaches. Sizes therefore straddle rows_per_chunk's thresholds, and one size
# is a prime so no chunk divides it evenly.
#
#   Rscript tools/bench/verify_parallel_parity.R

suppressMessages({library(fastgeojson); library(jsonlite)})

fails <- 0L
cmp <- function(x, label, ...) {
  want <- as.character(toJSON(x, ...))
  fastgeojson_threads(1)
  one <- as.character(as_json(x, ...))
  fastgeojson_threads(0)
  many <- as.character(as_json(x, ...))
  ok <- identical(want, many) && identical(one, many)
  if (ok) {
    cat(sprintf("  PASS  %-52s\n", label))
  } else {
    fails <<- fails + 1L
    cat(sprintf("  FAIL  %-52s\n", label))
    if (!identical(one, many)) {
      d <- which(strsplit(one, "")[[1]] != strsplit(many, "")[[1]])[1]
      cat(sprintf("        1 worker vs many differ at byte %s\n",
                  if (is.na(d)) "(length only)" else d))
      cat(sprintf("        1   : ...%s...\n        many: ...%s...\n",
                  substr(one, max(1, d - 30), d + 30),
                  substr(many, max(1, d - 30), d + 30)))
    } else {
      cat(sprintf("        R   : %s\n        rust: %s\n",
                  substr(want, 1, 120), substr(many, 1, 120)))
    }
  }
}

# Straddling MIN_PARALLEL_WORK (32768 work units) from both sides, plus a
# prime so the last chunk is always short.
set.seed(1)
sizes <- c(1L, 2L, 1000L, 8191L, 32768L, 40009L, 200000L)

cat("== bare double vectors ==\n")
for (n in sizes) {
  cmp(runif(n), sprintf("%d doubles", n))
}
cat("\n== bare integer and logical vectors ==\n")
for (n in sizes) {
  cmp(sample(1000000L, n, TRUE), sprintf("%d integers", n))
  cmp(sample(c(TRUE, FALSE), n, TRUE), sprintf("%d logicals", n))
}

cat("\n== the non-finite values, spread across the chunks ==\n")
n <- 40009L
v <- runif(n)
v[seq(1, n, by = 97)] <- NA
v[seq(2, n, by = 101)] <- NaN
v[seq(3, n, by = 103)] <- Inf
v[seq(4, n, by = 107)] <- -Inf
cmp(v, "doubles with NA/NaN/Inf, na default")
cmp(v, "doubles with NA/NaN/Inf, na = null", na = "null")
cmp(v, "doubles with NA/NaN/Inf, na = string", na = "string")
iv <- sample(1000L, n, TRUE); iv[seq(1, n, by = 97)] <- NA_integer_
cmp(iv, "integers with NA, na default")
cmp(iv, "integers with NA, na = null", na = "null")
lv <- sample(c(TRUE, FALSE), n, TRUE); lv[seq(1, n, by = 97)] <- NA
cmp(lv, "logicals with NA, na default")
cmp(lv, "logicals with NA, na = string", na = "string")

cat("\n== digits, which changes every value's width ==\n")
for (d in list(0L, 2L, 4L, 8L, 15L, NA, I(4L))) {
  lbl <- if (inherits(d, "AsIs")) "I(4)" else as.character(d)
  cmp(runif(40009), sprintf("40009 doubles, digits = %s", lbl), digits = d)
}
cmp(runif(40009), "40009 doubles, always_decimal", always_decimal = TRUE)

cat("\n== matrices and arrays: the split is on the outer dimension ==\n")
cmp(matrix(runif(40000), nrow = 20000), "20000 x 2 double matrix")
cmp(matrix(runif(40000), nrow = 2), "2 x 20000 double matrix")
cmp(matrix(runif(40009 * 3), nrow = 40009), "40009 x 3 double matrix")
cmp(matrix(sample(1000L, 60000L, TRUE), nrow = 20000), "20000 x 3 integer matrix")
cmp(matrix(sample(c(TRUE, FALSE), 60000L, TRUE), nrow = 20000), "20000 x 3 logical matrix")
cmp(array(runif(60000), dim = c(3000, 10, 2)), "3000 x 10 x 2 double array")
cmp(array(runif(60000), dim = c(2, 10, 3000)), "2 x 10 x 3000 double array")
cmp(array(sample(1000L, 60000L, TRUE), dim = c(3000, 10, 2)), "3000 x 10 x 2 integer array")
cmp(matrix(runif(40000), nrow = 20000), "20000 x 2 matrix, columnmajor",
    matrix = "columnmajor")
cmp(array(runif(60000), dim = c(3000, 10, 2)), "3000 x 10 x 2 array, columnmajor",
    matrix = "columnmajor")
m <- matrix(runif(60000), nrow = 20000)
m[seq(1, 60000, by = 97)] <- NA
cmp(m, "20000 x 3 matrix with NA")

cat("\n== a long vector inside a list, so the outer walk stays serial ==\n")
cmp(list(a = runif(40009), b = sample(1000L, 40009L, TRUE)), "list of two long vectors")
cmp(list(list(runif(40009))), "long vector two levels down")

cat("\n== auto_unbox must not be defeated by the split ==\n")
cmp(1.5, "one double, auto_unbox", auto_unbox = TRUE)
cmp(runif(40009), "40009 doubles, auto_unbox", auto_unbox = TRUE)

cat("\n== data frames, whose columns take the same route ==\n")
df <- data.frame(a = runif(40009), b = sample(1000L, 40009L, TRUE),
                 c = sample(c(TRUE, FALSE), 40009L, TRUE))
cmp(df, "40009-row frame, rows")
cmp(df, "40009-row frame, columns", dataframe = "columns")
cmp(df, "40009-row frame, values", dataframe = "values")
cmp(list(d = df), "40009-row frame, nested")
cmp(list(d = df), "40009-row frame, nested columns", dataframe = "columns")

cat("\n== dataframe = columns splits rows too, so a narrow frame parallelises ==\n")
# One task per column left a 250000 x 1 frame entirely serial. Columns big
# enough on their own are now cut into row pieces as well, and the pieces carry
# their own separators: the first opens the array, the rest start with a comma,
# the last closes it. A misplaced separator would show up here as a doubled or
# missing comma at a piece boundary, which only a frame long enough to be split
# can reach.
set.seed(41)
for (nr in c(0L, 1L, 2L, 3L, 7L, 100L, 4999L, 60000L)) {
  for (nc in c(1L, 2L, 3L, 5L, 40L)) {
    d <- as.data.frame(setNames(lapply(seq_len(nc), function(i) {
      if (i %% 3L == 0L) sample(c(letters, NA), nr, TRUE)
      else if (i %% 3L == 1L) runif(nr)
      else sample(c(TRUE, FALSE, NA), nr, TRUE)
    }), paste0("c", seq_len(nc))))
    for (dfm in c("columns", "rows", "values")) {
      cmp(d, sprintf("%d x %d, dataframe = %s", nr, nc, dfm), dataframe = dfm)
    }
  }
}
# A frame wide enough that pieces and columns interleave, checked for
# thread-count invariance rather than only against toJSON().
{
  set.seed(9)
  d <- as.data.frame(setNames(lapply(1:3, function(i) runif(60000L)), c("a", "b", "c")))
  d$s <- sample(c(letters, NA), 60000L, TRUE)
  d$m <- matrix(runif(120000L), nrow = 60000L)
  outs <- vapply(c(1L, 2L, 3L, 8L, 17L, 0L), function(t) {
    fastgeojson_threads(t)
    as.character(as_json(d, dataframe = "columns"))
  }, "")
  fastgeojson_threads(0)
  if (length(unique(outs)) == 1L) {
    cat("  PASS  columns output identical at 1, 2, 3, 8, 17 and automatic workers\n")
  } else {
    fails <- fails + 1L
    cat("  FAIL  columns output depends on the worker count\n")
  }
}


# ------------------------------------------------------------------
# One coordinate matrix split across the pool
# ------------------------------------------------------------------
# A big enough ring or line is now written in pieces, because splitting only
# BETWEEN geometries leaves thirty-one workers idle on a layer whose work is
# one geometry. jsonlite has no opinion on GeoJSON, so these compare one
# worker against many: below MIN_SPLIT_ORDINATES nothing splits, which makes
# the single-worker run the reference for the piece writer.
#
# Each piece carries its own punctuation -- the first opens the array, the
# rest start with their separating comma, the last closes -- so the sizes
# below straddle the threshold and the piece boundaries, where a doubled or
# missing separator would hide.
if (requireNamespace("sf", quietly = TRUE)) {
  suppressMessages(library(sf))
  set.seed(7)
  mk <- function(n) cbind(runif(n) * 360 - 180, runif(n) * 180 - 90)
  try_sf <- function(expr) tryCatch(st_sf(id = 1, geometry = st_sfc(expr)),
                                    error = function(e) NULL)
  cmp_geom <- function(x, label, ...) {
    if (is.null(x)) return(invisible())
    fastgeojson_threads(1); one <- as.character(as_json(x, ...))
    fastgeojson_threads(0); many <- as.character(as_json(x, ...))
    if (identical(one, many)) {
      cat(sprintf("  PASS  %-52s\n", label))
    } else {
      fails <<- fails + 1L
      n <- min(nchar(one, type = "bytes"), nchar(many, type = "bytes"))
      d <- NA_integer_
      for (i in seq_len(n)) if (substr(one, i, i) != substr(many, i, i)) { d <- i; break }
      cat(sprintf("  FAIL  %-52s\n        differ at byte %s\n        1   : ...%s...\n        many: ...%s...\n",
                  label, if (is.na(d)) "(length only)" else d,
                  substr(one, max(1, d - 30), d + 30), substr(many, max(1, d - 30), d + 30)))
    }
  }

  cat("\n== one coordinate matrix, split across the pool ==\n")
  for (n in c(2, 65535, 65536, 65537, 65538, 100000, 131072, 200003)) {
    cmp_geom(try_sf(st_linestring(mk(n))), sprintf("LineString %d", n))
    cmp_geom(try_sf(st_multipoint(mk(n))), sprintf("MultiPoint %d", n))
    if (n >= 4) { m <- mk(n); m <- rbind(m, m[1, ])
                  cmp_geom(try_sf(st_polygon(list(m))), sprintf("Polygon %d", n)) }
  }
  # The split indexes a column-major matrix as i + j * nrow, so a third and a
  # fourth ordinate are where that arithmetic would show up wrong.
  for (nc in 3:4) {
    n <- 120000
    cmp_geom(try_sf(st_linestring(cbind(mk(n), matrix(runif(n * (nc - 2)), ncol = nc - 2)))),
             sprintf("LineString %d x %d ordinates", n, nc))
  }
  big <- try_sf(st_linestring(mk(150000)))
  for (d in list(0L, 4L, 8L, NA, Inf)) cmp_geom(big, sprintf("split, digits = %s", d), digits = d)
  cmp_geom(big, "split, always_decimal", always_decimal = TRUE)
  # sf refuses to build a geometry holding NA, so they go in afterwards.
  nf <- big
  nf$geometry[[1]][c(5, 70000, 149999), 1] <- c(NA, Inf, NaN)
  cmp_geom(nf, "split, non-finite ordinates")
  cmp_geom(nf, "split, non-finite, na = string", na = "string")
  # Big geometries inside a layer that is ALREADY being split between
  # features: the piece tasks are then nested inside a worker rather than
  # submitted from the R thread.
  cmp_geom(st_sf(id = 1:8, geometry = st_sfc(lapply(1:8, function(i) st_linestring(mk(90000))))),
           "8 x 90000, split inside a worker")
  rings <- lapply(1:3, function(k) { m <- mk(50000); rbind(m, m[1, ]) })
  cmp_geom(try_sf(st_polygon(rings)), "Polygon, 3 rings x 50000")
  cmp_geom(try_sf(st_multilinestring(lapply(1:3, function(k) mk(50000)))),
           "MultiLineString 3 x 50000")
  cmp_geom(try_sf(st_multipolygon(list(rings, rings))), "MultiPolygon 2 x 3 x 50000")
}

fastgeojson_threads(0)
cat(sprintf("\n%d failure(s)\n", fails))
if (fails > 0) quit(status = 1)
