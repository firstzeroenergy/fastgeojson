#!/usr/bin/env Rscript
# Generates the headline numbers quoted in README.Rmd.
#
#   Rscript tools/bench/readme_bench.R
#
# Uses DEFAULT arguments for every package, so the comparison is like-for-like.
# Timings are the minimum of seven blocks; see tools/bench/harness.R.
# Output sizes are reported alongside the times, because a serializer that emits
# twice the bytes is not doing the same job.
#
# Writes tools/bench/readme_numbers.csv, which README.Rmd renders into its
# benchmark tables. After running this, regenerate the README:
#
#   Rscript tools/build_readme.R

suppressMessages({
  library(fastgeojson); library(jsonlite); library(sf)
  ok_yy  <- requireNamespace("yyjsonr",   quietly = TRUE)
  ok_gjs <- requireNamespace("geojsonsf", quietly = TRUE)
  ok_jfy <- requireNamespace("jsonify",   quietly = TRUE)
})

# Same harness the rest of tools/bench uses: the minimum of repeated blocks,
# not the median. Medians of single runs drift enough on a desktop to move a
# published figure by 50%.
source("tools/bench/harness.R")
tm <- function(f, reps = 7L) fgj_time(f, block = 0.5, reps = reps)
row <- function(pkg, s, b) data.frame(pkg = pkg, ms = s * 1000, mb = b / 1048576, stringsAsFactors = FALSE)

# Measures and sizes in one go. Every call site used to serialize a second
# time just to count bytes, which on the slower packages cost as much as the
# measurement itself.
timed <- function(pkg, f) {
  out <- f()
  b <- nchar(if (is.character(out)) out else as.character(out), type = "bytes")
  row(pkg, tm(f), b)
}

# Accumulates every measured row so the README tables come from measurements
# rather than from figures typed in by hand.
collected <- list()
collect <- function(id, d, ref) {
  d$table <- id
  d$ref <- d$pkg == ref
  collected[[length(collected) + 1L]] <<- d
  invisible(d)
}
show <- function(d, ref) {
  d$vs <- sprintf("%.1fx", d$ms[d$pkg == ref] / d$ms)
  d$vs[d$pkg == ref] <- "1.0x (ref)"
  d <- d[order(d$ms, decreasing = TRUE), ]
  for (i in seq_len(nrow(d)))
    cat(sprintf("  %-14s %9.1f ms  %7.1f MB  %10s\n", d$pkg[i], d$ms[i], d$mb[i], d$vs[i]))
}

n <- 1e6
set.seed(42)
df <- data.frame(
  id    = 1:n,
  value = sample(letters, n, TRUE),
  val2  = rnorm(n),
  log   = sample(c(TRUE, FALSE), n, TRUE),
  stringsAsFactors = FALSE
)

cat("### JSON: 1,000,000 rows x 4 mixed columns (all defaults)\n")
res <- timed("fastgeojson", function() as_json(df))
res <- rbind(res, timed("jsonlite", function() toJSON(df)))
if (ok_yy)  res <- rbind(res, timed("yyjsonr", function() yyjsonr::write_json_str(df)))
if (ok_jfy) res <- rbind(res, timed("jsonify", function() jsonify::to_json(df)))
show(res, "jsonlite"); flush(stdout())
json_res <- res

cat("\n  single-threaded:\n")
fastgeojson_threads(1)
st <- tm(function() as_json(df))
cat(sprintf("  %-14s %9.1f ms\n", "fastgeojson", st * 1000))
fastgeojson_threads(0)
collect("json_1m",
        rbind(json_res, row("fastgeojson (1 thread)", st,
                            json_res$mb[json_res$pkg == "fastgeojson"] * 1048576)),
        "jsonlite")

set.seed(123)
pts <- st_as_sf(
  data.frame(lon = runif(n, -125, -66), lat = runif(n, 40, 49),
             value = rnorm(n), category = sample(letters[1:5], n, TRUE)),
  coords = c("lon", "lat"), crs = 4326
)

cat("\n### GeoJSON: 1,000,000 point features (all defaults)\n")
res <- timed("fastgeojson", function() as_json(pts))
if (ok_gjs) res <- rbind(res, timed("geojsonsf", function() geojsonsf::sf_geojson(pts)))
if (ok_yy)  res <- rbind(res, timed("yyjsonr", function() yyjsonr::write_geojson_str(pts)))
# jsonlite is not measured on this shape at all. toJSON(sf = "geojson") over a
# million point features took 117 seconds when it completed, and on a repeat
# run it exhausted memory and killed the whole benchmark. It is excluded from
# the published table anyway, so timing it only risks the run.
show(res, "geojsonsf"); flush(stdout())
pts_res <- res

cat("\n  single-threaded:\n")
fastgeojson_threads(1)
st <- tm(function() as_json(pts))
cat(sprintf("  %-14s %9.1f ms\n", "fastgeojson", st * 1000))
fastgeojson_threads(0)
# jsonlite is left out of this table: toJSON(sf = "geojson") takes about two
# minutes on this shape, which would make every other row unreadable.
collect("points_1m",
        rbind(pts_res[pts_res$pkg != "jsonlite", ],
              row("fastgeojson (1 thread)", st,
                  pts_res$mb[pts_res$pkg == "fastgeojson"] * 1048576)),
        "geojsonsf")

cat("\n### 10,000 polygons x 200 vertices (all defaults)\n")
set.seed(7)
polys <- lapply(1:10000, function(i) {
  cx <- runif(1, -120, -70); cy <- runif(1, 30, 45)
  th <- seq(0, 2 * pi, length.out = 200)
  m <- cbind(cx + cos(th), cy + sin(th)); m[nrow(m), ] <- m[1, ]
  st_polygon(list(m))
})
pl <- st_sf(id = 1:10000, v = rnorm(10000), geometry = st_sfc(polys, crs = 4326))
res <- timed("fastgeojson", function() as_json(pl))
if (ok_gjs) res <- rbind(res, timed("geojsonsf", function() geojsonsf::sf_geojson(pl)))
if (ok_yy)  res <- rbind(res, timed("yyjsonr", function() yyjsonr::write_geojson_str(pl)))
show(res, "geojsonsf"); flush(stdout())
collect("polygons_10k", res, "geojsonsf")

out <- do.call(rbind, collected)
out <- data.frame(table = out$table, package = out$pkg,
                  median_ms = round(out$ms), output_mb = round(out$mb, 1),
                  ref = out$ref, stringsAsFactors = FALSE)
out <- out[order(match(out$table, c("json_1m", "points_1m", "polygons_10k")),
                 -out$median_ms), ]
utils::write.csv(out, "tools/bench/readme_numbers.csv", row.names = FALSE, quote = FALSE)
cat("\nwrote tools/bench/readme_numbers.csv; now run: Rscript tools/build_readme.R\n")

cat(sprintf("threads used: %d\n", fastgeojson_threads()))
