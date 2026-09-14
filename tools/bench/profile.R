#!/usr/bin/env Rscript
# Attribution profiling for fastgeojson.
#
#   Rscript tools/bench/profile.R [--n=200000] [--shape=chr_short]
#
# Answers three questions that a MB/s number cannot:
#
#  1. How much of a call is fixed R-side overhead, independent of size?
#     Measured by timing a 1-row input; whatever that costs is paid by every
#     call regardless of data.
#  2. Where does the R wrapper spend its time? Measured with Rprof over the
#     R layer only, and by timing the wrapper's individual stages.
#  3. How much does the Rust core cost versus everything around it? Measured
#     by comparing as_json() against the raw .Call entry point, which skips
#     argument matching, the class pre-encoding pass and the dispatch chain.

suppressMessages({
  library(fastgeojson)
  library(jsonlite)
  ok_sf <- requireNamespace("sf", quietly = TRUE)
  if (ok_sf) library(sf)
})

here <- dirname(sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE)[1]))
if (is.na(here) || !nzchar(here)) here <- "tools/bench"
source(file.path(here, "corpus.R"))

args <- commandArgs(TRUE)
getarg <- function(k, d) {
  h <- grep(paste0("^--", k, "="), args, value = TRUE)
  if (!length(h)) d else sub(paste0("^--", k, "="), "", h[1])
}
N     <- as.integer(getarg("n", 200000L))
SHAPE <- getarg("shape", "")

timer <- function(f, min_time = 0.3, reps = 5) {
  invisible(f())
  a <- bench::hires_time(); invisible(f()); one <- max(bench::hires_time() - a, 1e-7)
  it <- max(1L, min(200000L, as.integer(ceiling(min_time / one))))
  ts <- numeric(reps)
  for (i in seq_len(reps)) {
    gc(FALSE)
    a <- bench::hires_time()
    for (j in seq_len(it)) invisible(f())
    ts[[i]] <- (bench::hires_time() - a) / it
  }
  stats::median(ts)
}

fmt <- function(s) {
  if (s >= 1) sprintf("%8.3f s ", s)
  else if (s >= 1e-3) sprintf("%8.3f ms", s * 1e3)
  else sprintf("%8.1f us", s * 1e6)
}

impl_df  <- getFromNamespace("df_json_str_impl", "fastgeojson")
impl_obj <- getFromNamespace("obj_json_str_impl", "fastgeojson")
impl_sf  <- getFromNamespace("sf_geojson_str_impl", "fastgeojson")

cat("############ 1. FIXED PER-CALL OVERHEAD ############\n")
cat("Time for a minimal input. Everything here is paid on every call.\n\n")
tiny <- list(
  `1-row data.frame`   = data.frame(a = 1),
  `length-1 vector`    = 1,
  `empty list`         = list(),
  `1-elem named list`  = list(a = 1)
)
for (nm in names(tiny)) {
  x <- tiny[[nm]]
  t_full <- timer(function() fastgeojson::as_json(x))
  t_jl   <- timer(function() jsonlite::toJSON(x))
  cat(sprintf("  %-20s as_json %s   toJSON %s\n", nm, fmt(t_full), fmt(t_jl)))
}

cat("\n  Direct .Call, bypassing the whole R wrapper:\n")
d1 <- data.frame(a = 1)
cat(sprintf("    %-20s %s\n", "as_json(df)", fmt(timer(function() fastgeojson::as_json(d1)))))
cat(sprintf("    %-20s %s\n", "df_json_str_impl",
            fmt(timer(function() impl_df(d1, FALSE, "rows", "smart", "list", "string", 4L, FALSE, FALSE)))))

cat("\n############ 2. R-WRAPPER STAGE COSTS ############\n")
shapes <- if (nzchar(SHAPE)) SHAPE else c("num_random", "chr_short", "wide", "mixed", "nested_list")
for (shape in shapes) {
  if (!shape %in% corpus_names()) next
  n <- N
  x <- corpus_build(shape, n)
  cat(sprintf("\n-- %s (n=%d) --\n", shape, n))

  t_full <- timer(function() fastgeojson::as_json(x), min_time = 0.5, reps = 3)
  kind <- corpus_kind(shape)
  t_core <- if (kind == "df") {
    timer(function() impl_df(x, FALSE, "rows", "smart", "list", "string", 4L, FALSE, FALSE),
          min_time = 0.5, reps = 3)
  } else {
    timer(function() impl_obj(x, FALSE, "smart", "list", "string", 4L, FALSE, FALSE),
          min_time = 0.5, reps = 3)
  }
  cat(sprintf("  as_json total      %s\n", fmt(t_full)))
  cat(sprintf("  Rust core only     %s   (%.1f%% of total)\n", fmt(t_core), 100 * t_core / t_full))
  cat(sprintf("  R wrapper overhead %s   (%.1f%% of total)\n",
              fmt(t_full - t_core), 100 * (t_full - t_core) / t_full))

  # Individual wrapper stages, called directly.
  needs_prep <- getFromNamespace(".needs_prep", "fastgeojson")
  cat(sprintf("    .needs_prep()      %s\n", fmt(timer(function() needs_prep(x), min_time = 0.2, reps = 3))))
  if (inherits(x, "data.frame")) {
    cat(sprintf("    .row_names_info()  %s\n", fmt(timer(function() .row_names_info(x), min_time = 0.2, reps = 3))))
    cat(sprintf("    row.names<-NULL    %s\n", fmt(timer(function() { y <- x; row.names(y) <- NULL; y }, min_time = 0.2, reps = 3))))
  }
}

cat("\n############ 3. THREAD SCALING ############\n")
cat("Where parallelism actually helps. A flat curve means the serial\n")
cat("prepare phase dominates, not the parallel write.\n\n")
for (shape in c("num_random", "chr_short", "wide", "mixed")) {
  x <- corpus_build(shape, N)
  nb <- nchar(as.character(fastgeojson::as_json(x)), type = "bytes")
  cat(sprintf("-- %-12s out=%.1f MB\n", shape, nb / 1048576))
  base <- NA_real_
  for (th in c(1, 2, 4, 8, 16, 0)) {
    fastgeojson_threads(th)
    eff <- fastgeojson_threads()
    s <- timer(function() fastgeojson::as_json(x), min_time = 0.4, reps = 3)
    if (is.na(base)) base <- s
    cat(sprintf("     t=%-3d %s  %7.1f MB/s   speedup %5.2fx\n",
                eff, fmt(s), (nb / 1048576) / s, base / s))
  }
  fastgeojson_threads(0)
  cat("\n")
}

cat("############ 4. Rprof OVER THE R LAYER ############\n")
x <- corpus_build(if (nzchar(SHAPE)) SHAPE else "mixed", N)
pf <- tempfile(fileext = ".out")
Rprof(pf, interval = 0.002, line.profiling = FALSE)
for (i in 1:20) invisible(fastgeojson::as_json(x))
Rprof(NULL)
sp <- summaryRprof(pf)
cat("\nBy self time (top 15):\n")
print(utils::head(sp$by.self, 15))
unlink(pf)
