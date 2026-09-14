#!/usr/bin/env Rscript
# Compare a benchmark run against competitors on BOTH metrics.
#
#   Rscript tools/bench/compare.R [label]
#
# MB/s alone is misleading here: fastgeojson honours jsonlite's digits = 4
# while yyjsonr writes shortest-round-trip, so on numeric data they emit
# almost twice the bytes for the same input. That inflates their MB/s and
# deflates ours for producing more compact output. Wall-clock time on the same
# input is the metric that answers "which serializer is faster"; MB/s answers
# "which moves bytes faster". Report both.

args  <- commandArgs(TRUE)
label <- if (length(args)) args[1] else "fix3"
f     <- file.path("tools", "bench", "results", paste0(label, ".csv"))
if (!file.exists(f)) stop("no results at ", f)
r <- utils::read.csv(f, stringsAsFactors = FALSE)

num <- function(x) suppressWarnings(as.numeric(x))
r$secs <- num(r$secs); r$mb_s <- num(r$mb_s); r$bytes <- num(r$bytes)

shapes <- unique(r$shape)
cat(sprintf("%-15s | %8s %8s | %-9s %8s %8s | %7s %7s\n",
            "shape", "t=1 s", "t=N s", "rival", "rival s", "MB ratio", "t1 spd", "tN spd"))
cat(strrep("-", 92), "\n")

wins1 <- 0; losses1 <- 0; wins_n <- 0
for (s in shapes) {
  d <- r[r$shape == s, ]
  f1 <- d[d$engine == "fastgeojson" & d$threads == 1, ]
  fn <- d[d$engine == "fastgeojson" & d$threads != 1 & !is.na(d$secs), ]
  o  <- d[d$engine != "fastgeojson" & !is.na(d$secs), ]
  if (!nrow(f1) || !nrow(o)) next
  fn <- if (nrow(fn)) fn[which.min(fn$secs), ] else f1
  # Fastest competitor by wall clock, which is the fair comparison.
  b <- o[which.min(o$secs), ]
  sp1 <- b$secs / f1$secs[1]
  spn <- b$secs / fn$secs[1]
  if (sp1 >= 1) wins1 <- wins1 + 1 else losses1 <- losses1 + 1
  if (spn >= 1) wins_n <- wins_n + 1
  cat(sprintf("%-15s | %8.4f %8.4f | %-9s %8.4f %8.2fx | %6.2fx %6.2fx%s\n",
              s, f1$secs[1], fn$secs[1], b$engine, b$secs,
              b$bytes / f1$bytes[1], sp1, spn,
              if (sp1 < 1) "  <- SLOWER 1T" else ""))
}
cat(strrep("-", 92), "\n")
cat(sprintf("single-thread wins: %d   losses: %d   |   full-thread wins: %d of %d\n",
            wins1, losses1, wins_n, wins1 + losses1))
cat("\nspd = competitor_time / our_time  (>1 means we are faster on the same input)\n")
cat("MB ratio = competitor output bytes / our output bytes\n")
