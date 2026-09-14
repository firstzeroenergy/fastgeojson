#!/usr/bin/env Rscript
# Brute-force the Rust date formatter against R's format() over the whole
# admissible range, not just plausible dates.
#
#   Rscript tools/bench/verify_date_parity.R
suppressMessages({library(fastgeojson); library(jsonlite)})

# Parity is asserted under equal arguments. as_json() defaults to lossless
# numbers where toJSON() rounds to 4 decimal places, so supply jsonlite's
# default whenever a call here does not name `digits`.
as_json <- function(..., digits = 4) fastgeojson::as_json(..., digits = digits)
mk <- function(v) structure(as.double(v), class = "Date")

cmp <- function(days, label) {
  d <- mk(days)
  want <- toJSON(d, na = "string")
  got  <- as_json(d, na = "string")
  if (identical(as.character(want), as.character(got))) {
    cat(sprintf("  PASS  %-34s (%d values)\n", label, length(days)))
    return(0L)
  }
  w <- fromJSON(as.character(want)); g <- fromJSON(as.character(got))
  bad <- which(w != g)
  cat(sprintf("  FAIL  %-34s %d/%d differ\n", label, length(bad), length(days)))
  for (i in head(bad, 6)) cat(sprintf("        day %.0f  R=%s  rust=%s\n", days[i], w[i], g[i]))
  1L
}

fails <- 0L
# Every day across four centuries, one at a time: catches leap-year and
# month-length errors that sampling would miss.
fails <- fails + cmp(-36524:36524, "every day 1870-2070")
fails <- fails + cmp(-719162:-718000, "year 1 onwards")
fails <- fails + cmp(2932000:2932896, "up to 9999-12-31")
# Leap-day boundaries around century and 400-year rules.
yrs <- c(1600, 1700, 1800, 1900, 2000, 2100, 2400, 0, 400, 1200)
ld <- unlist(lapply(yrs, function(y) {
  b <- as.numeric(as.Date(sprintf("%04d-01-01", y)))
  b + c(-2:2, 57:62, 364:367)
}))
fails <- fails + cmp(ld, "century/400-year leap boundaries")

set.seed(99)
fails <- fails + cmp(sample(-700000:2932896, 200000, TRUE), "200k random in R's normal range")
fails <- fails + cmp(round(runif(50000, -7.8e11, 7.8e11)), "50k random across the full range")
fails <- fails + cmp(c(784351576776, 784351576777, -784352321506, -784352321507,
                       784351576775, -784352321505), "exact validity boundary")
fails <- fails + cmp(c(0, -0.5, -1.9, 19000.7, 19000.999, -1e-9, 1e-9), "fractional truncation")
fails <- fails + cmp(c(NA, NaN, Inf, -Inf, 0), "non-finite")
fails <- fails + cmp(c(1e12, 1e15, 1e17, -1e12, -1e17, 1e308, -1e308), "beyond R's range -> NA")

# Integer-typed Date must agree too.
di <- structure(c(-36524L, 0L, 19000L, NA_integer_, 2932896L), class = "Date")
if (identical(as.character(toJSON(di, na = "string")), as.character(as_json(di, na = "string")))) {
  cat("  PASS  integer-typed Date\n")
} else { cat("  FAIL  integer-typed Date\n"); fails <- fails + 1L }

cat(sprintf("\n%d failing group(s)\n", fails))
if (fails > 0) quit(status = 1)
