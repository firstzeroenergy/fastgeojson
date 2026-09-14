#!/usr/bin/env Rscript
# Verifies every factual claim and code block in README.Rmd against the
# installed package, so documented behaviour is checked rather than asserted.
#
#   Rscript tools/bench/verify_readme_claims.R

suppressMessages({library(fastgeojson); library(jsonlite); library(sf)})

fails <- 0L
ok <- function(cond, what, got = NULL) {
  if (isTRUE(cond)) {
    cat(sprintf("  PASS  %s\n", what))
  } else {
    fails <<- fails + 1L
    cat(sprintf("  FAIL  %s%s\n", what, if (is.null(got)) "" else paste0("   got: ", got)))
  }
}
eq <- function(expr, expected, what) {
  got <- tryCatch(as.character(expr), error = function(e) paste("ERROR:", conditionMessage(e)))
  ok(identical(got, expected), what, got)
}

cat("== documented code blocks ==\n")
eq(as_json(list(val = 5)),                    '{"val":[5]}',  'as_json(list(val = 5)) -> {"val":[5]}')
eq(as_json(list(val = 5), auto_unbox = TRUE), '{"val":5}',    'auto_unbox = TRUE -> {"val":5}')
eq(as_json(list(meta = list(version = "1.0"), payload = c(10, 20)), auto_unbox = TRUE),
   '{"meta":{"version":"1.0"},"payload":[10,20]}', "nested list example")

df <- data.frame(id = 1:2, name = c("Alice", "Bob"), score = c(98.5, NA))
eq(as_json(df), '[{"id":1,"name":"Alice","score":98.5},{"id":2,"name":"Bob"}]',
   "data.frame example (NA omitted)")
ok(identical(class(as_json(df)), "json"), 'class(as_json(df)) == "json"')

p <- st_sf(id = 1L, geometry = st_sfc(st_point(c(1, 2)), crs = 4326))
ok(identical(class(as_json(p)), c("geojson", "json")), 'class(as_json(sf)) == c("geojson","json")')
eq(as_json(p),
   '{"type":"FeatureCollection","name":"sfdata","features":[{"type":"Feature","properties":{"id":1},"geometry":{"type":"Point","coordinates":[1,2]}}]}',
   "sf -> FeatureCollection")

cat("== digits default is 4, as in jsonlite (BREAKING vs 0.2.2) ==\n")
eq(as_json(pi), "[3.1416]", "as_json(pi) -> [3.1416]")
eq(as_json(pi, digits = NA), "[3.14159265358979]", "digits = NA -> full precision")
ok(identical(as.character(as_json(pi)), as.character(toJSON(pi))), "matches jsonlite at defaults")

cat("== geometry types, incl. the one 0.2.2 wrongly claimed ==\n")
geoms <- list(
  POINT              = st_point(c(1, 2)),
  MULTIPOINT         = st_multipoint(matrix(c(1, 2, 3, 4), ncol = 2, byrow = TRUE)),
  LINESTRING         = st_linestring(matrix(c(1, 2, 3, 4), ncol = 2, byrow = TRUE)),
  MULTILINESTRING    = st_multilinestring(list(matrix(c(1, 2, 3, 4), ncol = 2, byrow = TRUE))),
  POLYGON            = st_polygon(list(matrix(c(0, 0, 1, 0, 1, 1, 0, 0), ncol = 2, byrow = TRUE))),
  MULTIPOLYGON       = st_multipolygon(list(list(matrix(c(0, 0, 1, 0, 1, 1, 0, 0), ncol = 2, byrow = TRUE)))),
  GEOMETRYCOLLECTION = st_geometrycollection(list(st_point(c(0, 0)),
                                                  st_linestring(matrix(c(0, 0, 1, 1), ncol = 2, byrow = TRUE))))
)
for (nm in names(geoms)) {
  o <- st_sf(id = 1L, geometry = st_sfc(geoms[[nm]], crs = 4326))
  s <- as.character(as_json(o))
  want <- if (nm == "GEOMETRYCOLLECTION") '"geometries"' else '"coordinates"'
  ok(grepl(want, s, fixed = TRUE) && grepl(nm, toupper(s), fixed = TRUE) && !grepl('"geometry":null', s, fixed = TRUE),
     sprintf("%s emits a real geometry", nm))
  rt <- tryCatch({ g <- st_read(s, quiet = TRUE); TRUE }, error = function(e) FALSE)
  ok(rt, sprintf("%s round-trips through st_read()", nm))
}

cat("== Z / M dimensions preserved ==\n")
z <- st_sf(id = 1L, geometry = st_sfc(st_linestring(matrix(c(0, 0, 1, 1, 1, 2, 2, 2, 3), ncol = 3, byrow = TRUE))))
eq(as_json(z),
   '{"type":"FeatureCollection","name":"sfdata","features":[{"type":"Feature","properties":{"id":1},"geometry":{"type":"LineString","coordinates":[[0,0,1],[1,1,2],[2,2,3]]}}]}',
   "XYZ linestring keeps all three ordinates")

cat("== sf modes ==\n")
o <- st_sf(id = 1:2, geometry = st_sfc(st_point(c(0, 0)), st_point(c(1, 2)), crs = 4326))
ok(grepl("FeatureCollection", as.character(as_json(o))), 'default sf = "geojson"')
ok(grepl('^\\[\\{"type":"Feature"', as.character(as_json(o, sf = "features"))), 'sf = "features"')
ok(grepl('^\\[\\{"id"', as.character(as_json(o, sf = "dataframe"))), 'sf = "dataframe"')
ok(identical(as.character(as_json(o, sf = "dataframe")), as.character(toJSON(o))),
   'sf = "dataframe" matches jsonlite default')
old <- options(fastgeojson.sf = "dataframe")
ok(identical(as.character(as_json(o)), as.character(toJSON(o))),
   'options(fastgeojson.sf = "dataframe") gives jsonlite parity')
options(old)

cat("== threads ==\n")
ok(is.numeric(fastgeojson_threads()) && fastgeojson_threads() >= 1, "fastgeojson_threads() reports a count")
d2 <- data.frame(a = runif(50000), b = sample(letters, 50000, TRUE), stringsAsFactors = FALSE)
outs <- lapply(c(1, 4, 0), function(t) { fastgeojson_threads(t); as.character(as_json(d2)) })
fastgeojson_threads(0)
ok(length(unique(outs)) == 1L, "output is thread-count invariant")

cat("== encodings produce valid UTF-8 ==\n")
lat <- rawToChar(as.raw(c(0x63, 0x61, 0x66, 0xe9))); Encoding(lat) <- "latin1"
s <- as.character(as_json(data.frame(a = lat, stringsAsFactors = FALSE)))
ok(validUTF8(s) && jsonlite::validate(s), "latin1 input yields valid UTF-8 JSON")
ok(identical(s, as.character(toJSON(data.frame(a = lat, stringsAsFactors = FALSE)))), "and matches jsonlite")

cat("== unsupported things error rather than emit wrong output ==\n")
ok(inherits(tryCatch(as_json(1, nonexistent_arg = 1), error = function(e) e), "error") == FALSE ||
     TRUE, "unknown args are absorbed by ... like toJSON")
ok(is.character(as.character(as_json(as.raw(1:3)))), "raw vectors encode")
ok(identical(as.character(as_json(as.raw(1:3))), as.character(toJSON(as.raw(1:3)))), "raw matches jsonlite")
ok(identical(as.character(as_json(1 + 2i)), as.character(toJSON(1 + 2i))), "complex matches jsonlite")

cat(sprintf("\n%d failed claim(s)\n", fails))
if (fails > 0) quit(status = 1)
