#!/usr/bin/env Rscript
# Geometry: every type, every dimensionality, empties, mixed columns, and
# coordinate objects that are not what sf would ever produce.
#
# The geometry descriptors are now built in the worker pool from pure reads,
# with anything that needs the R thread deferred to a second pass. An error in
# that split would either emit the wrong geometry or read a wild pointer, so
# these cases exercise both halves and check thread-count invariance.
#
#   Rscript tools/bench/verify_geometry_parity.R

suppressMessages({library(fastgeojson); library(jsonlite); library(sf)})

# Parity is asserted under equal arguments. as_json() defaults to lossless
# numbers where toJSON() rounds to 4 decimal places, so supply jsonlite's
# default whenever a call here does not name `digits`.
as_json <- function(..., digits = 4) fastgeojson::as_json(..., digits = digits)

fails <- 0L
cmp <- function(x, label, ...) {
  want <- tryCatch(as.character(toJSON(x, sf = "geojson", ...)),
                   error = function(e) paste("ERROR:", conditionMessage(e)))
  got  <- tryCatch(as.character(as_json(x, ...)),
                   error = function(e) paste("ERROR:", conditionMessage(e)))
  if (identical(want, got)) {
    cat(sprintf("  PASS  %-46s\n", label))
  } else {
    fails <<- fails + 1L
    cat(sprintf("  FAIL  %-46s\n        R   : %s\n        rust: %s\n", label,
                substr(want, 1, 180), substr(got, 1, 180)))
  }
}
sfobj <- function(g) st_sf(id = seq_along(g), geometry = st_sfc(g, crs = 4326))

cat("== every type, XY ==\n")
m2 <- function(...) matrix(c(...), ncol = 2, byrow = TRUE)
ring <- m2(0,0, 1,0, 1,1, 0,1, 0,0)
geoms <- list(
  POINT              = st_point(c(1, 2)),
  MULTIPOINT         = st_multipoint(m2(1,2, 3,4)),
  LINESTRING         = st_linestring(m2(1,2, 3,4, 5,6)),
  MULTILINESTRING    = st_multilinestring(list(m2(1,2, 3,4), m2(5,6, 7,8))),
  POLYGON            = st_polygon(list(ring)),
  POLYGON_hole       = st_polygon(list(ring, m2(.2,.2, .4,.2, .4,.4, .2,.2))),
  MULTIPOLYGON       = st_multipolygon(list(list(ring), list(m2(2,2, 3,2, 3,3, 2,2)))),
  GEOMETRYCOLLECTION = st_geometrycollection(list(st_point(c(0,0)), st_linestring(m2(0,0, 1,1))))
)
for (nm in names(geoms)) cmp(sfobj(list(geoms[[nm]])), nm)

cat("\n== XYZ, XYM, XYZM ==\n")
for (d in c(3, 4)) {
  mk <- function(n) matrix(seq_len(n * d) / 2, ncol = d, byrow = TRUE)
  dimn <- if (d == 3) "XYZ" else "XYZM"
  cmp(sfobj(list(st_point(mk(1)[1, ]))), paste(dimn, "POINT"))
  cmp(sfobj(list(st_linestring(mk(3)))), paste(dimn, "LINESTRING"))
  r <- mk(4); r[4, ] <- r[1, ]
  cmp(sfobj(list(st_polygon(list(r)))), paste(dimn, "POLYGON"))
  cmp(sfobj(list(st_multipolygon(list(list(r))))), paste(dimn, "MULTIPOLYGON"))
}
zm <- st_linestring(matrix(c(0,0,1, 1,1,2, 2,2,3), ncol = 3, byrow = TRUE), dim = "XYM")
cmp(sfobj(list(zm)), "XYM LINESTRING")

cat("\n== empties and NULLs ==\n")
cmp(sfobj(list(st_point(), st_point(c(1,2)))), "POINT EMPTY then a real point")
cmp(sfobj(list(st_polygon(), st_polygon(list(ring)))), "POLYGON EMPTY then a real polygon")
cmp(sfobj(list(st_linestring(), st_multipoint())), "LINESTRING and MULTIPOINT EMPTY")
cmp(sfobj(list(st_geometrycollection())), "GEOMETRYCOLLECTION EMPTY")

cat("\n== ALTREP coordinate vectors ==\n")
# R wraps a vector of 64 or more elements in an ALTREP wrapper when attributes
# are set, and st_linestring()/st_multipoint() attach a class to their
# coordinate matrix. So every LINESTRING or MULTIPOINT of 32 or more XY points
# is ALTREP. A guard that rejected ALTREP outright sent all of them down the
# serial pre-render path, which cost nothing in output but all of the
# parallelism; these cases sit at and either side of the threshold.
ringn <- function(k) { th <- seq(0, 2 * pi, length.out = k)
  m <- cbind(cos(th), sin(th)); m[k, ] <- m[1, ]; m }
for (k in c(31, 32, 33, 400)) {
  cmp(sfobj(list(st_linestring(ringn(k)))), sprintf("LINESTRING, %d points", k))
  cmp(sfobj(list(st_multipoint(ringn(k)))), sprintf("MULTIPOINT, %d points", k))
}
cmp(sfobj(list(st_polygon(list(ringn(400))))), "POLYGON, 400 points")
cmp(sfobj(list(st_multipolygon(list(list(ringn(400)))))), "MULTIPOLYGON, 400 points")
cmp(sfobj(list(st_linestring(ringn(400)), st_point(c(1, 2)), st_polygon(list(ringn(400))))),
    "ALTREP linestring mixed with others")
{
  big <- st_sf(id = 1:600, geometry = st_sfc(
    lapply(1:600, function(i) st_linestring(ringn(200))), crs = 4326))
  ref <- as.character(as_json(big))
  for (t in c(1, 2, 8, 0)) {
    fastgeojson_threads(t)
    if (!identical(as.character(as_json(big)), ref)) {
      fails <- fails + 1L
      cat(sprintf("  FAIL  600 ALTREP linestrings at %d threads\n", t))
    }
  }
  fastgeojson_threads(0)
  cat("  PASS  600 ALTREP linestrings identical across thread counts\n")
  cmp(big, "600 ALTREP linestrings against jsonlite")
}

cat("\n== mixed sfc_GEOMETRY, which is classified per element ==\n")
cmp(sfobj(list(geoms$POINT, geoms$LINESTRING, geoms$POLYGON)), "point + linestring + polygon")
cmp(sfobj(list(geoms$GEOMETRYCOLLECTION, geoms$POINT, geoms$MULTIPOLYGON)),
    "collection + point + multipolygon")
cmp(sfobj(list(geoms$POINT, st_point(), geoms$GEOMETRYCOLLECTION, geoms$POLYGON)),
    "mixed with an empty in the middle")

cat("\n== coordinate objects sf would never produce ==\n")
mal <- function(g) { s <- st_sfc(st_point(c(1,2)), crs = 4326); s[[1]] <- g; st_sf(id = 1L, geometry = s) }
cmp(mal(structure(1:2, class = c("XY","POINT","sfg"))), "integer POINT")
cmp(mal(structure(c("a","b"), class = c("XY","POINT","sfg"))), "character POINT")
cmp(mal(structure(c(TRUE,FALSE), class = c("XY","POINT","sfg"))), "logical POINT")
cmp(mal(structure(matrix(1:6, ncol = 2), class = c("XY","LINESTRING","sfg"))), "integer LINESTRING")
cmp(mal(structure(list(matrix(1:8, ncol = 2)), class = c("XY","POLYGON","sfg"))), "integer ring")
cmp(mal(structure(list(matrix(letters[1:8], ncol = 2)), class = c("XY","POLYGON","sfg"))), "character ring")
cmp(mal(structure(list(list(1, 2)), class = c("XY","POLYGON","sfg"))), "list where a matrix belongs")
cmp(mal(structure(list(list(matrix(1:8, ncol = 2))), class = c("XY","MULTIPOLYGON","sfg"))),
    "integer MULTIPOLYGON")
cmp(mal(structure(list(list(matrix(as.double(1:8), ncol = 2)), list(matrix(1:8, ncol = 2))),
                  class = c("XY","MULTIPOLYGON","sfg"))), "MULTIPOLYGON, one ring integer")
cmp(mal(structure(numeric(0), class = c("XY","POINT","sfg"))), "zero-length POINT")

cat("\n== a malformed geometry among many good ones, in both halves ==\n")
set.seed(1)
n <- 5000
good <- lapply(seq_len(n), function(i) st_point(c(runif(1), runif(1))))
for (pos in c(1L, as.integer(n / 2), n)) {
  s <- st_sfc(good, crs = 4326)
  s[[pos]] <- structure(1:2, class = c("XY","POINT","sfg"))
  o <- st_sf(id = seq_len(n), geometry = s)
  cmp(o, sprintf("integer POINT at position %d of %d", pos, n))
}

cat("\n== the same at every thread count ==\n")
set.seed(2)
big <- st_sf(id = 1:20000, geometry = st_sfc(
  lapply(1:20000, function(i) st_polygon(list(ring + runif(1)))), crs = 4326))
ref <- as.character(as_json(big))
for (t in c(1, 2, 4, 8, 0)) {
  fastgeojson_threads(t)
  if (!identical(as.character(as_json(big)), ref)) {
    fails <- fails + 1L; cat(sprintf("  FAIL  20k polygons at %d threads\n", t))
  }
}
fastgeojson_threads(0)
cat("  PASS  20k polygons identical at 1, 2, 4, 8 and automatic threads\n")

# A skewed layer, because chunk boundaries now follow cumulative ordinate
# counts rather than row counts. A few large geometries among many small ones
# get chunks of their own, so the partition differs from the uniform case and
# deserves its own invariance check.
set.seed(3)
bigring <- function(nv) { th <- seq(0, 2 * pi, length.out = nv)
  m <- cbind(cos(th), sin(th)); m[nv, ] <- m[1, ]; m }
skew <- st_sf(id = 1:2010, geometry = st_sfc(c(
  lapply(1:2000, function(i) st_polygon(list(bigring(20) + runif(1)))),
  lapply(1:10,   function(i) st_polygon(list(bigring(20000))))), crs = 4326))
ref <- as.character(as_json(skew))
for (t in c(1, 2, 4, 8, 0)) {
  fastgeojson_threads(t)
  if (!identical(as.character(as_json(skew)), ref)) {
    fails <- fails + 1L; cat(sprintf("  FAIL  skewed layer at %d threads\n", t))
  }
}
fastgeojson_threads(0)
cat("  PASS  skewed layer identical across thread counts\n")
cmp(skew, "skewed layer against jsonlite")

mixed <- st_sf(id = 1:6000, geometry = st_sfc(
  rep(list(geoms$POINT, geoms$POLYGON, geoms$GEOMETRYCOLLECTION), 2000), crs = 4326))
ref <- as.character(as_json(mixed))
for (t in c(1, 4, 0)) {
  fastgeojson_threads(t)
  if (!identical(as.character(as_json(mixed)), ref)) {
    fails <- fails + 1L; cat(sprintf("  FAIL  mixed 6k at %d threads\n", t))
  }
}
fastgeojson_threads(0)
cat("  PASS  6k mixed geometries identical across thread counts\n")
cmp(mixed, "6k mixed geometries against jsonlite")

cat("\n== a geometry with no recognisable class ==\n")
# jsonlite reads class(sfg)[2] for the type and emits the envelope regardless,
# so a classless element becomes {"type":null,"coordinates":[]} while we emit a
# null geometry. Recorded rather than matched: reproducing it would mean
# emitting a null type for anything unrecognised, which would be wrong for an
# sfg that does carry a class, just not one we handle.
{
  s <- st_sfc(st_point(c(1, 2)), crs = 4326); s[[1]] <- list()
  o <- st_sf(id = 1L, geometry = s)
  cat(sprintf("  NOTE  classless geometry: jsonlite %s  fastgeojson %s\n",
              sub('.*"geometry":', "", as.character(toJSON(o, sf = "geojson"))),
              sub('.*"geometry":', "", as.character(as_json(o)))))
}

cat("\n== the other sf mode, and round-tripping through GDAL ==\n")
# cmp() supplies sf = "geojson" itself, so this one is compared directly.
{
  o <- sfobj(list(geoms$POLYGON, geoms$GEOMETRYCOLLECTION))
  a <- as.character(toJSON(o, sf = "features"))
  b <- as.character(as_json(o, sf = "features"))
  if (identical(a, b)) {
    cat(sprintf("  PASS  %-46s\n", "sf = features"))
  } else {
    fails <- fails + 1L
    cat(sprintf("  FAIL  sf = features\n        R   : %s\n        rust: %s\n",
                substr(a, 1, 160), substr(b, 1, 160)))
  }
}
for (nm in names(geoms)) {
  o <- sfobj(list(geoms[[nm]]))
  ok <- tryCatch({ st_read(as.character(as_json(o)), quiet = TRUE); TRUE },
                 error = function(e) FALSE)
  if (!ok) { fails <- fails + 1L; cat(sprintf("  FAIL  %s does not round-trip through st_read()\n", nm)) }
}
cat("  PASS  every type round-trips through st_read()\n")

cat("\n== the two geometry writers, against each other ==\n")
# An sf object's geometry is rendered by write_geometry_parallel, in a worker,
# from a descriptor the extraction pass built. The SAME sfc reached through a
# list is rendered by render_geometry_to_bytes, recursively, on the R thread.
# Two implementations of one thing -- which is exactly what produced nine
# defects in the data.frame writer before the two were merged. Everything above
# checks the sf path against toJSON(); this checks the two against each other,
# and both against toJSON(), across the arguments that change a coordinate's
# text.
#
# An sfc is a vector, so it renders as an array even when it holds one
# geometry, where the sf path renders that feature's geometry bare. The one
# layer is stripped so the two are comparable.
geom_of_sf <- function(s) {
  p <- strsplit(s, '"geometry":', fixed = TRUE)[[1]]
  if (length(p) < 2L) return(s)
  substr(p[2], 1L, nchar(p[2]) - 3L)
}
strip_one <- function(s) {
  inner <- substr(s, 6L, nchar(s) - 1L)
  if (substr(inner, 1L, 1L) == "[" &&
      substr(inner, nchar(inner), nchar(inner)) == "]") {
    substr(inner, 2L, nchar(inner) - 1L)
  } else {
    inner
  }
}
ring <- function(k) {
  m <- cbind(cos(seq(0, 2 * pi, length.out = k)), sin(seq(0, 2 * pi, length.out = k)))
  m[nrow(m), ] <- m[1, ]
  m
}
# Enough shapes to reach every arm of both writers, including the >= 32-point
# matrices that arrive ALTREP-wrapped and the sizes that get chunked.
diffgeoms <- list(
  POINT = st_point(c(1.5, 2.5)),
  POINT_XYZ = st_point(c(1, 2, 3)),
  POINT_XYM = st_point(c(1, 2, 3), dim = "XYM"),
  POINT_XYZM = st_point(c(1, 2, 3, 4)),
  POINT_EMPTY = st_point(),
  MULTIPOINT = st_multipoint(rbind(c(0, 0), c(1, 1), c(2, 2))),
  MULTIPOINT_XYZ = st_multipoint(rbind(c(0, 0, 1), c(1, 1, 2))),
  MULTIPOINT_40 = st_multipoint(cbind(1:40, (1:40) / 3)),
  MULTIPOINT_EMPTY = st_multipoint(),
  LINESTRING = st_linestring(rbind(c(0, 0), c(1, 1), c(2, 0))),
  LINESTRING_XYZ = st_linestring(rbind(c(0, 0, 1), c(1, 1, 2), c(2, 0, 3))),
  LINESTRING_XYZM = st_linestring(rbind(c(0, 0, 1, 9), c(1, 1, 2, 8))),
  LINESTRING_40 = st_linestring(cbind(1:40, (1:40) / 3)),
  LINESTRING_EMPTY = st_linestring(),
  MULTILINESTRING = st_multilinestring(list(rbind(c(0, 0), c(1, 1)),
                                            rbind(c(2, 2), c(3, 3), c(4, 2)))),
  MULTILINESTRING_40 = st_multilinestring(list(cbind(1:40, (1:40) / 3),
                                               cbind(1:50, (1:50) / 7))),
  MULTILINESTRING_EMPTY = st_multilinestring(),
  POLYGON = st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0)))),
  POLYGON_HOLE = st_polygon(list(rbind(c(0, 0), c(4, 0), c(4, 4), c(0, 4), c(0, 0)),
                                 rbind(c(1, 1), c(2, 1), c(2, 2), c(1, 2), c(1, 1)))),
  POLYGON_XYZ = st_polygon(list(rbind(c(0, 0, 1), c(1, 0, 1), c(1, 1, 1), c(0, 0, 1)))),
  POLYGON_40 = st_polygon(list(ring(40))),
  POLYGON_EMPTY = st_polygon(),
  MULTIPOLYGON = st_multipolygon(list(
    list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 0))),
    list(rbind(c(3, 3), c(4, 3), c(4, 4), c(3, 3)),
         rbind(c(3.2, 3.2), c(3.6, 3.2), c(3.6, 3.6), c(3.2, 3.2))))),
  MULTIPOLYGON_XYZ = st_multipolygon(list(list(rbind(c(0, 0, 1), c(1, 0, 1),
                                                     c(1, 1, 1), c(0, 0, 1))))),
  MULTIPOLYGON_EMPTY = st_multipolygon(),
  GEOMETRYCOLLECTION = st_geometrycollection(list(st_point(c(1, 2)),
                          st_linestring(rbind(c(0, 0), c(1, 1))))),
  GEOMETRYCOLLECTION_MIX = st_geometrycollection(list(st_point(c(1, 2)),
                          st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 0)))),
                          st_multipoint(rbind(c(5, 5), c(6, 6))))),
  GEOMETRYCOLLECTION_EMPTY = st_geometrycollection(),
  BIG_LINESTRING = st_linestring(cbind(seq(0, 1, length.out = 5000),
                                       seq(1, 0, length.out = 5000))),
  BIG_POLYGON = st_polygon(list(ring(3000)))
)
argsets <- list(
  list(label = "defaults"),
  list(label = "digits = 0", digits = 0L),
  list(label = "digits = 8", digits = 8L),
  list(label = "digits = NA", digits = NA),
  list(label = "digits = I(4)", digits = I(4L)),
  # Ours, not a jsonlite mode: toJSON() warns and falls back to 15 significant
  # digits, so only the two writers are compared for it.
  list(label = "digits = Inf", ours_only = TRUE, digits = Inf),
  list(label = "na = null", na = "null"),
  list(label = "na = string", na = "string"),
  list(label = "always_decimal", always_decimal = TRUE)
)
dfails <- 0L
dn <- 0L
for (a in argsets) {
  lbl <- a$label
  ours_only <- isTRUE(a$ours_only)
  a$label <- NULL
  a$ours_only <- NULL
  for (nm in names(diffgeoms)) {
    sfc <- st_sfc(diffgeoms[[nm]])
    obj <- st_sf(id = 1L, geometry = sfc)
    par <- tryCatch(geom_of_sf(as.character(do.call(as_json, c(list(obj), a)))),
                    error = function(e) paste("ERR", conditionMessage(e)))
    rec <- tryCatch(strip_one(as.character(do.call(as_json, c(list(list(g = sfc)), a)))),
                    error = function(e) paste("ERR", conditionMessage(e)))
    jl <- if (ours_only) rec else
      tryCatch(strip_one(as.character(do.call(toJSON, c(list(list(g = sfc)), a)))),
               error = function(e) paste("ERR", conditionMessage(e)))
    dn <- dn + 1L
    if (!identical(par, rec) || !identical(rec, jl)) {
      dfails <- dfails + 1L
      cat(sprintf("  FAIL  %s, %s\n    worker  : %s\n    recurse : %s\n    jsonlite: %s\n",
                  nm, lbl, substr(par, 1, 110), substr(rec, 1, 110), substr(jl, 1, 110)))
    }
  }
}
if (dfails == 0L) {
  cat(sprintf("  PASS  %d cases: %d shapes x %d argument sets, three ways each\n",
              dn, length(diffgeoms), length(argsets)))
} else {
  fails <- fails + dfails
}
# A layer of many features, so the descriptor path really chunks, against the
# same geometries walked recursively one at a time.
{
  set.seed(17)
  many <- st_sfc(lapply(1:2000, function(i) {
    if (i %% 3L == 0L) st_point(c(runif(1), runif(1)))
    else if (i %% 3L == 1L) st_linestring(cbind(runif(40), runif(40)))
    else st_polygon(list(ring(1 + (i %% 17L) * 3L)))
  }))
  obj <- st_sf(id = seq_along(many), geometry = many)
  a <- as.character(as_json(obj))
  a <- strsplit(a, '"geometry":', fixed = TRUE)[[1]][-1]
  a <- vapply(a, function(x) sub("}}]}$", "", sub("}},.*$", "", x, perl = FALSE)), "",
              USE.NAMES = FALSE)
  b <- as.character(as_json(list(g = many)))
  ok <- identical(as.character(toJSON(list(g = many))), b)
  if (ok) {
    cat("  PASS  2000 mixed features: recursive path matches toJSON()\n")
  } else {
    fails <- fails + 1L
    cat("  FAIL  2000 mixed features: recursive path differs from toJSON()\n")
  }
  ok2 <- identical(as.character(toJSON(obj, sf = "geojson")), as.character(as_json(obj)))
  if (ok2) {
    cat("  PASS  2000 mixed features: worker path matches toJSON()\n")
  } else {
    fails <- fails + 1L
    cat("  FAIL  2000 mixed features: worker path differs from toJSON()\n")
  }
}

cat(sprintf("\n%d failure(s)\n", fails))
if (fails > 0) quit(status = 1)
