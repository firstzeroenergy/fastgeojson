#!/usr/bin/env Rscript
# Holds the Rust timestamp writer to R's format.POSIXct() across time zones,
# DST transitions, half-hour offsets, pre-1970 instants and the non-finite
# cases. R resolves the UTC offset; everything after that is arithmetic, and
# arithmetic is what this checks.
#
#   Rscript tools/bench/verify_time_parity.R

suppressMessages({library(fastgeojson); library(jsonlite)})

fails <- 0L
cmp <- function(v, label, ...) {
  want <- as.character(toJSON(v, na = "string", ...))
  got  <- as.character(as_json(v, na = "string", ...))
  if (identical(want, got)) {
    cat(sprintf("  PASS  %-46s (%d)\n", label, length(v)))
    return(invisible(NULL))
  }
  w <- tryCatch(fromJSON(want), error = function(e) NULL)
  g <- tryCatch(fromJSON(got),  error = function(e) NULL)
  n <- if (is.null(w) || is.null(g) || length(w) != length(g)) NA else sum(w != g)
  cat(sprintf("  FAIL  %-46s %s differ\n", label, n))
  if (!is.null(w) && !is.null(g) && length(w) == length(g)) {
    for (i in head(which(w != g), 5)) cat(sprintf("        [%d] R=%s rust=%s\n", i, w[i], g[i]))
  } else {
    cat("        R   :", substr(want, 1, 160), "\n        rust:", substr(got, 1, 160), "\n")
  }
  fails <<- fails + 1L
}

tzs <- c("UTC", "America/New_York", "America/Sao_Paulo", "Europe/London",
         "Europe/Dublin", "Asia/Kolkata", "Asia/Kathmandu", "Asia/Tehran",
         "Australia/Lord_Howe", "Pacific/Chatham", "Pacific/Apia",
         "Africa/Casablanca", "Antarctica/Troll")

cat("== a year of timestamps in each zone ==\n")
for (tz in tzs) {
  set.seed(11)
  v <- as.POSIXct("2020-01-01", tz = tz) + sample(3.16e7, 5000, TRUE)
  cmp(v, tz)
}

cat("\n== straddling DST transitions to the second ==\n")
for (tz in c("America/New_York", "Europe/London", "Australia/Lord_Howe", "Pacific/Chatham")) {
  # Both transitions in 2020, plus the seconds either side of each.
  base <- as.POSIXct("2020-01-01", tz = tz)
  secs <- as.numeric(base) + c(
    seq(0, 3.16e7, by = 3600),                 # every hour of the year
    as.numeric(as.POSIXct(c("2020-03-08", "2020-03-29", "2020-04-05",
                            "2020-10-04", "2020-10-25", "2020-11-01"), tz = tz)) -
      as.numeric(base) + rep(c(-2, -1, 0, 1, 2, 3599, 3600, 3601), each = 6)
  )
  cmp(as.POSIXct(secs, tz = tz, origin = "1970-01-01"), paste(tz, "hourly + transitions"))
}

cat("\n== pre-1970, and far from it ==\n")
for (tz in c("UTC", "America/New_York", "Asia/Kolkata")) {
  set.seed(5)
  v <- as.POSIXct(sample(-3e9:3e9, 5000), tz = tz, origin = "1970-01-01")
  cmp(v, paste(tz, "+/- 95 years"))
}

cat("\n== the format = '' whole-vector rule ==\n")
cmp(as.POSIXct(c("2020-01-01", "2021-06-15"), tz = "UTC"), "all midnight -> date only")
cmp(as.POSIXct(c("2020-01-01", "2021-06-15 00:00:01"), tz = "UTC"), "one not midnight -> full")
cmp(as.POSIXct(c("2020-01-01 00:00:00"), tz = "UTC"), "single midnight")
cmp(as.POSIXct(character(0), tz = "UTC"), "zero length")
cmp(as.POSIXct(c(NA, NA), tz = "UTC"), "all NA")
cmp(structure(c(0, NA), class = c("POSIXct", "POSIXt"), tzone = "UTC"), "midnight + NA")

cat("\n== non-finite and fractional ==\n")
cmp(structure(c(NA, NaN, Inf, -Inf, 0), class = c("POSIXct", "POSIXt"), tzone = "UTC"), "non-finite")
cmp(structure(c(-1.5, -0.5, -0.25, 0, 0.25, 0.5, 1.5, 1e9 + 0.7),
              class = c("POSIXct", "POSIXt"), tzone = "UTC"), "fractional seconds truncate")

cat("\n== every POSIXt mode, and UTC ==\n")
v <- as.POSIXct(c("2020-07-04 13:45:12", "2020-01-04 03:05:06"), tz = "America/New_York")
for (m in c("string", "ISO8601", "epoch", "mongo")) {
  cmp(v, sprintf("POSIXt = %-8s", m), POSIXt = m)
  cmp(v, sprintf("POSIXt = %-8s UTC = TRUE", m), POSIXt = m, UTC = TRUE)
}
cmp(v, "time_format supplied", time_format = "%H:%M on %d %b %Y")

cat("\n== data frame columns, all three orientations x all na modes ==\n")
d <- data.frame(i = 1:5,
                t = structure(c(0, 1.6e9, NA, NaN, -1e9),
                              class = c("POSIXct", "POSIXt"), tzone = "America/New_York"))
for (dfm in c("rows", "columns", "values")) for (na in c("null", "string")) {
  want <- as.character(toJSON(d, dataframe = dfm, na = na))
  got  <- as.character(as_json(d, dataframe = dfm, na = na))
  if (identical(want, got)) cat(sprintf("  PASS  df %-8s na=%-7s\n", dfm, na))
  else { cat(sprintf("  FAIL  df %-8s na=%-7s\n        R   :%s\n        rust:%s\n", dfm, na, want, got)); fails <- fails + 1L }
}
for (dfm in c("rows", "columns", "values")) {
  want <- as.character(toJSON(d, dataframe = dfm))
  got  <- as.character(as_json(d, dataframe = dfm))
  if (identical(want, got)) cat(sprintf("  PASS  df %-8s na=<default>\n", dfm))
  else { cat(sprintf("  FAIL  df %-8s na=<default>\n        R   :%s\n        rust:%s\n", dfm, want, got)); fails <- fails + 1L }
}

cat("\n== nested, POSIXlt, digits.secs, auto_unbox ==\n")
cmp(list(t = as.POSIXct("2020-07-04 13:45:12", tz = "UTC")), "inside a list")
cmp(as.POSIXlt("2020-07-04 13:45:12", tz = "America/New_York"), "POSIXlt input")
cmp(as.POSIXct("2020-07-04 13:45:12", tz = "UTC"), "auto_unbox", auto_unbox = TRUE)
old <- options(digits.secs = 3L)
cmp(structure(c(0, 0.5, 0.25), class = c("POSIXct", "POSIXt"), tzone = "UTC"), "digits.secs = 3 -> R fallback")
options(old)
old <- options(digits.secs = 0L)
cmp(structure(c(0, 1.5), class = c("POSIXct", "POSIXt"), tzone = "UTC"), "digits.secs = 0 -> fast path")
options(old)

cat(sprintf("\n%d failing group(s)\n", fails))
if (fails > 0) quit(status = 1)
