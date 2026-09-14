#!/usr/bin/env Rscript
# fastgeojson benchmark harness.
#
#   Rscript tools/bench/bench.R [--n=200000] [--label=baseline] [--quick]
#                               [--threads=1,0] [--only=chr_short,sf_point]
#                               [--reps=7] [--no-competitors]
#
# Reports median time, output bytes and MB/s for each shape, at each thread
# count, alongside jsonlite / yyjsonr / geojsonsf where applicable. Every
# timed call is checked against a reference digest first, so a "speedup" that
# changed the output is reported as a failure rather than a win.
#
# Writes a tidy CSV to tools/bench/results/<label>.csv so revisions can be
# diffed.

suppressMessages({
  library(fastgeojson)
  library(jsonlite)
  ok_sf   <- requireNamespace("sf", quietly = TRUE)
  ok_yy   <- requireNamespace("yyjsonr", quietly = TRUE)
  ok_gjs  <- requireNamespace("geojsonsf", quietly = TRUE)
  ok_jfy  <- requireNamespace("jsonify", quietly = TRUE)
  if (ok_sf) library(sf)
})

here <- dirname(sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE)[1]))
if (is.na(here) || !nzchar(here)) here <- "tools/bench"
source(file.path(here, "corpus.R"))

# ---- args ----------------------------------------------------------------
args <- commandArgs(TRUE)
getarg <- function(k, default) {
  hit <- grep(paste0("^--", k, "="), args, value = TRUE)
  if (!length(hit)) return(default)
  sub(paste0("^--", k, "="), "", hit[1])
}
flag <- function(k) any(args == paste0("--", k))

QUICK   <- flag("quick")
N       <- as.integer(getarg("n", if (QUICK) 20000L else 200000L))
LABEL   <- getarg("label", "run")
REPS    <- as.integer(getarg("reps", if (QUICK) 3L else 7L))
THREADS <- as.integer(strsplit(getarg("threads", "1,0"), ",")[[1]])
ONLY    <- strsplit(getarg("only", ""), ",")[[1]]
ONLY    <- ONLY[nzchar(ONLY)]
COMPET  <- !flag("no-competitors")

# Cost per row varies by three orders of magnitude across shapes, so scale
# each one to land in a comparable output-size band. Without this, `wide` at
# n = 200k is a 320 MB frame producing ~510 MB of JSON, which measures the
# memory subsystem rather than the serializer.
n_for <- function(name) {
  switch(name,
         wide         = max(200L, N %/% 100L),   # 200 columns
         chr_long     = max(500L, N %/% 20L),    # 200-char strings
         list_column  = max(1000L, N %/% 10L),
         nested_list  = max(1000L, N %/% 10L),
         sf_point     = max(1000L, N %/% 4L),
         sf_point_xyz = max(1000L, N %/% 4L),
         sf_linestring = max(200L, N %/% 100L),  # 20 vertices each
         sf_polygon   = max(100L, N %/% 200L),   # 200 vertices each
         N)
}

# ---- timing --------------------------------------------------------------
# system.time() has ~10ms granularity on Windows, which reports anything fast
# as 0s. Use bench's high-resolution clock and auto-scale the iteration count
# so every measurement spans at least MIN_TIME, then take the median of REPS
# such batches. gc() between batches so a collection triggered by the previous
# batch is not attributed to the next one.
MIN_TIME <- if (QUICK) 0.15 else 0.4

time_it <- function(f, reps = REPS, min_time = MIN_TIME) {
  invisible(f())
  a <- bench::hires_time(); invisible(f()); one <- bench::hires_time() - a
  one <- max(one, 1e-7)
  iters <- max(1L, min(100000L, as.integer(ceiling(min_time / one))))
  ts <- numeric(reps)
  for (i in seq_len(reps)) {
    gc(FALSE)
    a <- bench::hires_time()
    for (j in seq_len(iters)) invisible(f())
    ts[[i]] <- (bench::hires_time() - a) / iters
  }
  stats::median(ts)
}

digest_of <- function(s) {
  s <- as.character(s)
  # Cheap content fingerprint without adding a digest dependency.
  paste0(nchar(s, type = "bytes"), ":", sum(utf8ToInt(substr(s, 1, 4000))), ":",
         substr(s, 1, 40), "|", substr(s, max(1, nchar(s) - 40), nchar(s)))
}

rows <- list()
emit <- function(shape, n, engine, threads, secs, bytes, note = "") {
  rows[[length(rows) + 1L]] <<- data.frame(
    label = LABEL, shape = shape, n = n, engine = engine, threads = threads,
    secs = secs, bytes = bytes,
    mb_s = if (is.na(secs) || secs <= 0) NA_real_ else (bytes / 1048576) / secs,
    note = note, stringsAsFactors = FALSE)
}

shapes <- if (length(ONLY)) ONLY else corpus_names()
if (!ok_sf) shapes <- setdiff(shapes, corpus_names("sf"))

cat(sprintf("fastgeojson bench  label=%s  n=%d  reps=%d  threads=%s\n",
            LABEL, N, REPS, paste(THREADS, collapse = ",")))
cat(sprintf("shapes: %s\n\n", paste(shapes, collapse = ", ")))

for (shape in shapes) {
  n <- n_for(shape)
  x <- try(corpus_build(shape, n), silent = TRUE)
  if (inherits(x, "try-error")) { cat(sprintf("  %-14s BUILD FAILED\n", shape)); next }
  kind <- corpus_kind(shape)

  ref <- try(as.character(fastgeojson::as_json(x)), silent = TRUE)
  if (inherits(ref, "try-error")) {
    cat(sprintf("  %-14s as_json FAILED: %s\n", shape, conditionMessage(attr(ref, "condition"))))
    next
  }
  ref_dig <- digest_of(ref)
  nb <- nchar(ref, type = "bytes")

  cat(sprintf("== %-14s n=%-8d out=%.1f MB\n", shape, n, nb / 1048576))

  for (th in THREADS) {
    fastgeojson_threads(th)
    eff <- fastgeojson_threads()
    got <- as.character(fastgeojson::as_json(x))
    if (!identical(digest_of(got), ref_dig)) {
      emit(shape, n, "fastgeojson", eff, NA_real_, nb, "OUTPUT MISMATCH")
      cat(sprintf("   %-22s OUTPUT MISMATCH\n", paste0("fastgeojson t=", eff)))
      next
    }
    s <- time_it(function() fastgeojson::as_json(x))
    emit(shape, n, "fastgeojson", eff, s, nb)
    cat(sprintf("   %-22s %8.4fs  %8.1f MB/s\n", paste0("fastgeojson t=", eff), s, (nb / 1048576) / s))
  }
  fastgeojson_threads(0)

  if (!COMPET) next

  comp <- list()
  if (kind %in% c("df", "vec", "list")) {
    comp$jsonlite <- function() jsonlite::toJSON(x)
    if (ok_yy && kind == "df") comp$yyjsonr <- function() yyjsonr::write_json_str(x)
    if (ok_jfy) comp$jsonify <- function() jsonify::to_json(x)
  } else if (kind == "sf") {
    comp$jsonlite <- function() jsonlite::toJSON(x, sf = "geojson")
    if (ok_gjs) comp$geojsonsf <- function() geojsonsf::sf_geojson(x)
    if (ok_yy)  comp$yyjsonr   <- function() yyjsonr::write_geojson_str(x)
  }
  for (nm in names(comp)) {
    out <- try(as.character(comp[[nm]]()), silent = TRUE)
    if (inherits(out, "try-error")) { cat(sprintf("   %-22s n/a\n", nm)); next }
    cb <- nchar(out, type = "bytes")
    s <- try(time_it(comp[[nm]]), silent = TRUE)
    if (inherits(s, "try-error")) { cat(sprintf("   %-22s n/a\n", nm)); next }
    emit(shape, n, nm, NA_integer_, s, cb)
    cat(sprintf("   %-22s %8.4fs  %8.1f MB/s  (%.1f MB)\n", nm, s, (cb / 1048576) / s, cb / 1048576))
  }
  cat("\n")
}

res <- do.call(rbind, rows)
outdir <- file.path(here, "results")
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
outfile <- file.path(outdir, paste0(LABEL, ".csv"))
utils::write.csv(res, outfile, row.names = FALSE)
cat(sprintf("\nwrote %s (%d rows)\n", outfile, nrow(res)))

# ---- summary: fastgeojson vs the best competitor per shape ---------------
cat("\n=== speedup vs best competitor (full threads) ===\n")
cat(sprintf("%-16s %10s %10s %10s %8s\n", "shape", "fgj MB/s", "best MB/s", "best", "ratio"))
for (shape in unique(res$shape)) {
  r <- res[res$shape == shape, ]
  f <- r[r$engine == "fastgeojson" & !is.na(r$mb_s), ]
  if (!nrow(f)) next
  f <- f[which.max(f$mb_s), ]
  o <- r[r$engine != "fastgeojson" & !is.na(r$mb_s), ]
  if (!nrow(o)) {
    cat(sprintf("%-16s %10.1f %10s %10s %8s\n", shape, f$mb_s, "-", "-", "-")); next
  }
  b <- o[which.max(o$mb_s), ]
  cat(sprintf("%-16s %10.1f %10.1f %10s %7.2fx\n", shape, f$mb_s, b$mb_s, b$engine, f$mb_s / b$mb_s))
}
