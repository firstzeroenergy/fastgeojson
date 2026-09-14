#!/usr/bin/env Rscript
# as_bytes = TRUE must return exactly the bytes the character path returns,
# for every kind of input and every dispatch route.
#
#   Rscript tools/bench/verify_as_bytes.R

suppressMessages({library(fastgeojson); library(jsonlite); library(sf)})

fails <- 0L
chk <- function(label, x, ...) {
  s <- as.character(as_json(x, ...))
  b <- as_json(x, ..., as_bytes = TRUE)
  if (!is.raw(b)) {
    fails <<- fails + 1L
    cat(sprintf("  FAIL  %-40s not a raw vector (%s)\n", label, class(b)[1]))
    return(invisible(NULL))
  }
  if (!identical(rawToChar(b), s)) {
    fails <<- fails + 1L
    cat(sprintf("  FAIL  %-40s\n        chr : %s\n        raw : %s\n",
                label, substr(s, 1, 120), substr(rawToChar(b), 1, 120)))
    return(invisible(NULL))
  }
  cat(sprintf("  PASS  %-40s %s\n", label, substr(s, 1, 40)))
}

cat("== every input kind ==\n")
chk("data frame rows",      data.frame(a = 1:3, b = c("x", NA, "z"), stringsAsFactors = FALSE))
chk("data frame columns",   data.frame(a = 1:3), dataframe = "columns")
chk("data frame values",    data.frame(a = 1:3), dataframe = "values")
chk("bare double vector",   c(1.5, NA, NaN, Inf, -Inf, 3))
chk("bare integer vector",  c(1L, NA, 3L))
chk("bare logical vector",  c(TRUE, NA, FALSE))
chk("character vector",     c("plain", NA, "café", "中文"))
chk("escapes",              c("a\"b", "c\\d", "e\tf", "<g", paste0("<", "/script>")))
chk("nested list",          list(a = list(b = 1:2), c = "x"))
chk("named list unboxed",   list(a = 1), auto_unbox = TRUE)
chk("NULL",                 NULL)
chk("empty data frame",     data.frame(a = integer(0)))
chk("empty vector",         numeric(0))
chk("Date",                 structure(c(19000, NA), class = "Date"))
chk("POSIXct",              as.POSIXct(c("2020-01-01 12:00:00", NA), tz = "UTC"))
chk("factor",               factor(c("a", NA, "b")))
chk("matrix",               matrix(1:6, nrow = 2))
chk("raw vector",           as.raw(1:4))
chk("complex",              c(1 + 2i, NA))
chk("json verbatim",        structure('{"a":1}', class = "json"), json_verbatim = TRUE)

cat("\n== sf, all three modes ==\n")
p <- st_sf(id = 1:2, v = c(1.5, NA),
           geometry = st_sfc(st_point(c(1, 2)), st_point(c(3, 4)), crs = 4326))
chk("sf FeatureCollection", p)
chk("sf features",          p, sf = "features")
chk("sf dataframe",         p, sf = "dataframe")
poly <- st_sf(id = 1L, geometry = st_sfc(
  st_polygon(list(matrix(c(0,0, 1,0, 1,1, 0,0), ncol = 2, byrow = TRUE))), crs = 4326))
chk("sf polygon",           poly)
gc_ <- st_sf(id = 1L, geometry = st_sfc(st_geometrycollection(list(
  st_point(c(0, 0)), st_linestring(matrix(c(0,0, 1,1), ncol = 2, byrow = TRUE))))))
chk("sf geometrycollection", gc_)

cat("\n== options that change the bytes ==\n")
d <- data.frame(a = c(pi, NA), b = c("x", NA), stringsAsFactors = FALSE)
for (na in c("null", "string")) chk(sprintf("na = %s", na), d, na = na)
for (dg in list(2, NA, Inf)) chk(sprintf("digits = %s", dg), d, digits = dg)
chk("always_decimal",       c(1, 2.5), always_decimal = TRUE)
chk("factor = integer",     factor(c("a", "b")), factor = "integer")

cat("\n== large, so the parallel path and every chunk is used ==\n")
set.seed(1)
big <- data.frame(id = 1:3e5, s = sample(c("a", "bb", NA, "dddd"), 3e5, TRUE),
                  v = rnorm(3e5), stringsAsFactors = FALSE)
chk("300k rows x 3 cols", big)
for (t in c(1, 4, 0)) {
  fastgeojson_threads(t)
  if (!identical(rawToChar(as_json(big, as_bytes = TRUE)), as.character(as_json(big)))) {
    fails <- fails + 1L; cat(sprintf("  FAIL  300k rows at %d threads\n", t))
  }
}
fastgeojson_threads(0)
cat("  PASS  identical at 1, 4 and automatic threads\n")

cat("\n== the incompatible combination is refused ==\n")
e <- tryCatch(as_json(1, as_bytes = TRUE, pretty = TRUE), error = function(e) conditionMessage(e))
if (grepl("incompatible", e)) cat("  PASS  pretty + as_bytes errors\n") else {
  fails <- fails + 1L; cat(sprintf("  FAIL  pretty + as_bytes: %s\n", e))
}

cat("\n== errors still surface through the raw path ==\n")
bytes <- rawToChar(as.raw(c(0x61, 0xff, 0x62))); Encoding(bytes) <- "bytes"
e <- tryCatch(as_json(bytes, as_bytes = TRUE), error = function(e) "ERROR")
if (identical(e, "ERROR")) cat("  PASS  bytes-encoded input still errors\n") else {
  fails <- fails + 1L; cat("  FAIL  bytes-encoded input did not error\n")
}
deep <- 1
for (i in 1:6000) deep <- list(deep)
e <- tryCatch(as_json(deep, as_bytes = TRUE), error = function(e) "ERROR")
if (identical(e, "ERROR")) cat("  PASS  over-deep nesting still errors\n") else {
  fails <- fails + 1L; cat("  FAIL  over-deep nesting did not error\n")
}

cat(sprintf("\n%d failure(s)\n", fails))
if (fails > 0) quit(status = 1)
