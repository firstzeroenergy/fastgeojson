#!/usr/bin/env Rscript
# Number formatting: parity with jsonlite on every digits setting it accepts,
# and exactness for digits = Inf, which jsonlite has no equivalent of.
#
#   Rscript tools/bench/verify_numeric_parity.R

suppressMessages({library(fastgeojson); library(jsonlite)})

fails <- 0L
cmp <- function(x, label, ...) {
  want <- as.character(toJSON(x, ...))
  got  <- as.character(as_json(x, ...))
  if (identical(want, got)) {
    cat(sprintf("  PASS  %-52s\n", label))
  } else {
    fails <<- fails + 1L
    cat(sprintf("  FAIL  %-52s\n", label))
    w <- strsplit(gsub("^\\[|\\]$", "", want), ",")[[1]]
    g <- strsplit(gsub("^\\[|\\]$", "", got), ",")[[1]]
    if (length(w) == length(g)) {
      for (i in head(which(w != g), 6)) cat(sprintf("        [%d] R=%s rust=%s\n", i, w[i], g[i]))
    } else {
      cat("        R   :", substr(want, 1, 150), "\n        rust:", substr(got, 1, 150), "\n")
    }
  }
}

set.seed(7)
pools <- list(
  random_normal   = rnorm(4000),
  coords          = runif(4000, -180, 180),
  tiny            = runif(4000, 1e-12, 1e-5),
  huge            = runif(4000, 1e9, 1e18),
  round2          = round(runif(4000, 0, 1000), 2),
  integral        = as.double(sample(1e9, 4000)),
  near_half       = (sample(2e6, 4000) + 0.5) / 100,
  subnormal       = 5e-324 * sample(1:50, 4000, TRUE),
  edge            = c(0, -0, 1, -1, .Machine$double.xmax, .Machine$double.xmin,
                      5e-324, 1e-5, 1.00001e-5, 2147483647, 2147483648,
                      -2147483648, 1e15, 1e16, 1e17, 0.1, 0.3 - 0.1, 1/3, pi, exp(1))
)

cat("== every digits value jsonlite accepts ==\n")
for (dg in list(0, 1, 2, 3, 4, 5, 7, 10, 15, 16, 17, 22, NA)) {
  bad <- 0L
  for (nm in names(pools)) {
    want <- as.character(toJSON(pools[[nm]], digits = dg))
    got  <- as.character(as_json(pools[[nm]], digits = dg))
    if (!identical(want, got)) {
      bad <- bad + 1L
      if (bad == 1L) {
        w <- strsplit(gsub("^\\[|\\]$", "", want), ",")[[1]]
        g <- strsplit(gsub("^\\[|\\]$", "", got), ",")[[1]]
        d <- if (length(w) == length(g)) head(which(w != g), 3) else integer()
        cat(sprintf("  FAIL  digits=%-4s pool=%-14s %s\n", dg, nm,
                    paste(sprintf("R=%s rust=%s", w[d], g[d]), collapse = "  ")))
      }
    }
  }
  if (bad == 0L) cat(sprintf("  PASS  digits=%-4s all %d pools\n", dg, length(pools)))
  fails <- fails + bad
}

cat("\n== always_decimal, and matrix columns (which used to ignore digits) ==\n")
for (dg in list(2, 4, NA)) {
  cmp(c(1, 2.5, 100), sprintf("always_decimal, digits=%s", dg), digits = dg, always_decimal = TRUE)
  d <- data.frame(i = 1:2)
  d$m <- matrix(c(pi, exp(1), 1/3, sqrt(2)), nrow = 2)
  cmp(d, sprintf("matrix column in a data frame, digits=%s", dg), digits = dg)
  cmp(matrix(c(pi, exp(1), 1/3, sqrt(2)), nrow = 2), sprintf("plain matrix, digits=%s", dg), digits = dg)
}

cat("\n== digits = Inf: lossless, which no jsonlite setting is ==\n")
rt <- function(x, ...) {
  s <- as.character(as_json(x, ...))
  # as.numeric() on both sides because fromJSON() narrows a whole number to
  # integer, which identical() would reject on type alone.
  identical(as.numeric(fromJSON(s)), as.numeric(x))
}
for (nm in names(pools)) {
  x <- pools[[nm]]
  exact_inf <- rt(x, digits = Inf)
  exact_na  <- rt(x, digits = NA)
  exact_22  <- rt(x, digits = 22)
  nb_inf <- nchar(as.character(as_json(x, digits = Inf)), type = "bytes")
  nb_22  <- nchar(as.character(as_json(x, digits = 22)),  type = "bytes")
  cat(sprintf("  %-14s digits=Inf %s  digits=NA %s  digits=22 %s   Inf is %4.1f%% smaller than 22\n",
              nm, if (exact_inf) "EXACT  " else "lossy  ",
              if (exact_na) "EXACT" else "lossy",
              if (exact_22) "EXACT" else "lossy",
              100 * (1 - nb_inf / nb_22)))
  if (!exact_inf) fails <- fails + 1L
}

cat("\n== digits = Inf survives every route into the writer ==\n")
chk <- function(label, got, want) {
  if (identical(as.character(got), want)) cat(sprintf("  PASS  %-40s %s\n", label, want))
  else { cat(sprintf("  FAIL  %-40s got %s want %s\n", label, as.character(got), want)); fails <<- fails + 1L }
}
chk("bare vector",     as_json(c(pi, 0.1, 1), digits = Inf), '[3.141592653589793,0.1,1]')
chk("data frame rows", as_json(data.frame(x = c(pi, 0.1)), digits = Inf), '[{"x":3.141592653589793},{"x":0.1}]')
chk("data frame cols", as_json(data.frame(x = c(pi, 0.1)), digits = Inf, dataframe = "columns"), '{"x":[3.141592653589793,0.1]}')
chk("nested list",     as_json(list(a = list(b = pi)), digits = Inf), '{"a":{"b":[3.141592653589793]}}')
chk("plain matrix",    as_json(matrix(c(pi, 0.1), nrow = 1), digits = Inf), '[[3.141592653589793,0.1]]')
d <- data.frame(i = 1L); d$m <- matrix(c(pi, 0.1), nrow = 1)
chk("matrix column",   as_json(d, digits = Inf), '[{"i":1,"m":[3.141592653589793,0.1]}]')
chk("always_decimal",  as_json(c(1, pi), digits = Inf, always_decimal = TRUE), '[1.0,3.141592653589793]')
chk("non-finite",      as_json(c(NA, NaN, Inf, -Inf), digits = Inf), '["NA","NaN","Inf","-Inf"]')
chk("integral stays int", as_json(c(1, 1e15, -1e15), digits = Inf), '[1,1000000000000000,-1000000000000000]')

suppressMessages(library(sf))
p <- st_sf(id = 1L, geometry = st_sfc(st_point(c(pi, 0.1)), crs = 4326))
chk("sf coordinates", as_json(p, digits = Inf),
    '{"type":"FeatureCollection","name":"sfdata","features":[{"type":"Feature","properties":{"id":1},"geometry":{"type":"Point","coordinates":[3.141592653589793,0.1]}}]}')

cat("\n== digits = I(n): SIGNIFICANT digits, which toJSON() renders as %.*g ==\n")
# This was handled in R by signif()-ing the whole object and then formatting
# the rounded values at 15 digits. Wrong twice over: signif() rounds the binary
# value half-to-even where %g rounds the decimal expansion, and 15 digits never
# selects scientific notation, so 12345 at I(4) came out as 12340 against
# toJSON()'s 1.234e+04. It was also 7x to 11x slower, being an interpreted
# recursive walk that copied the whole object on the way.
sig <- function(x, d, label, ...) {
  want <- as.character(toJSON(x, digits = d, ...))
  got <- as.character(as_json(x, digits = d, ...))
  if (identical(want, got)) {
    cat(sprintf("  PASS  %-48s\n", label))
  } else {
    fails <<- fails + 1L
    cat(sprintf("  FAIL  %-48s\n        R   : %s\n        rust: %s\n", label,
                substr(want, 1, 140), substr(got, 1, 140)))
  }
}
sigvals <- c(12345, 123456789, 0.12345, 2147483646, -2147483646, 1234.5678,
             99.995, 0.0001234567, 0, -0, NA, NaN, Inf, -Inf, 1e-20, 1e20,
             1e-5, 9.99999e-6, 0.5, 2.5, -2.5, 1/3, 2/3, pi, exp(1), 1e308,
             5e-324, .Machine$double.xmax)
# Every precision the mode accepts, including both ends.
for (k in 0:17) sig(sigvals, I(k), sprintf("I(%d) across 24 orders of magnitude", k))
set.seed(11)
for (k in c(1L, 4L, 8L, 15L, 17L)) {
  sig(runif(4000, -1e9, 1e9), I(k), sprintf("I(%d), random over 1e9", k))
  sig(rnorm(4000) * 10^sample(-12:12, 4000, TRUE), I(k),
      sprintf("I(%d), random exponent spread", k))
}
sd <- data.frame(a = sigvals[1:8], b = sigvals[9:16])
sig(sd, I(4L), "data frame rows")
sig(sd, I(4L), "data frame columns", dataframe = "columns")
sig(list(d = sd), I(4L), "nested data frame")
sig(as.list(sigvals), I(4L), "list")
sig(matrix(sigvals[1:8], nrow = 2), I(4L), "matrix")
sig(sigvals, I(4L), "always_decimal", always_decimal = TRUE)
sig(sigvals, I(4L), "na = null", na = "null")
sig(sigvals, I(4L), "na = string", na = "string")
sig(sigvals, I(4L), "auto_unbox", auto_unbox = TRUE)
# use_signif given explicitly, alongside a plain numeric digits.
{
  want <- as.character(toJSON(sigvals, digits = 4, use_signif = TRUE))
  got <- as.character(as_json(sigvals, digits = 4, use_signif = TRUE))
  if (identical(want, got)) {
    cat("  PASS  use_signif = TRUE with a plain digits\n")
  } else {
    fails <- fails + 1L
    cat("  FAIL  use_signif = TRUE with a plain digits\n")
  }
}
# And the decimal-places modes must be untouched by all of this.
for (d in list(0L, 2L, 4L, 8L, 15L, 16L, NA)) {
  sig(sigvals, d, sprintf("digits = %s stays decimal places", d))
}

cat("\n== the whole digits x magnitude grid, where the two writers meet ==\n")
# `digits = d` is served by a fixed-decimal writer for ordinary magnitudes and
# by %.*g outside them, and the two have to agree on the seam. Fixed notation
# carries int_digits + d significant digits while the %g fallback caps its
# precision at 17, so they only agree while |v| < 10^(17-d). The bound used to
# be 2^31 for every d, which sent epoch milliseconds and any count above two
# billion down the %g path at 122 ns per value against 20.
#
# digits = 0 is deliberately left at the old bound: `decimals` is
# ceil(log10|v|) there, which for an exact power of ten equals the exponent
# rather than exceeding it, and %g turns scientific as soon as the exponent
# reaches the precision -- so %.10g of 1e10 is 1e+10 where fixed notation
# writes 10000000000. This grid catches that if the bound is ever widened.
gridfail <- 0L
set.seed(3)
for (d in 0:9) {
  cells <- character(0)
  for (e in -7:17) {
    v <- runif(200, 1, 10) * 10^e
    v <- c(v, -v, round(v, 3), trunc(v))
    ok <- identical(as.character(toJSON(v, digits = d)),
                    as.character(as_json(v, digits = d)))
    cells <- c(cells, if (ok) "." else "X")
    if (!ok) gridfail <- gridfail + 1L
  }
  cat(sprintf("  d=%d  e=-7..17  %s\n", d, paste(cells, collapse = "")))
}
if (gridfail > 0) {
  fails <- fails + 1L
  cat(sprintf("  FAIL  %d grid cells differ from toJSON()\n", gridfail))
} else {
  cat("  PASS  250 cells, digits 0..9 x exponents -7..17\n")
}

# Dense sampling either side of each bound, where a fixed/scientific flip or a
# one-digit precision error would show up and a uniform sweep would not.
lim <- c(2147483647, 1e16, 1e15, 1e14, 1e13, 1e12, 1e11, 1e10, 2147483647, 2147483647)
bfail <- 0L
set.seed(21)
for (d in 0:9) {
  L <- lim[d + 1L]
  sets <- list(
    L * (1 - 10^-(1:15)), L * (1 + 10^-(1:15)),
    c(L, L - 1, L + 1, L / 2, L * 2, L * (1 + 2^-52)),
    runif(1500, L * 0.5, L * 0.999999), runif(1500, L * 1.000001, L * 2),
    round(runif(1500, L * 0.5, L * 0.999999), 3),
    seq(L * 0.9, L * 0.9 + 100, by = 0.5),
    trunc(runif(1500, L * 0.5, L * 0.99)),
    10^(0:17), -(10^(0:17))
  )
  for (v in sets) {
    if (!identical(as.character(toJSON(v, digits = d)),
                   as.character(as_json(v, digits = d)))) bfail <- bfail + 1L
  }
}
if (bfail > 0) {
  fails <- fails + 1L
  cat(sprintf("  FAIL  %d of 100 boundary groups differ from toJSON()\n", bfail))
} else {
  cat("  PASS  100 boundary groups, dense either side of every bound\n")
}

cat("\n== subnormals, where relative precision collapses ==\n")
# %.*g is served from ryu's shortest form when rounding those digits provably
# agrees with rounding the exact value. The argument rests on
# ulp(v)/v <= 2.22e-16, which is a property of NORMAL doubles: for the smallest
# subnormal, 4.9406564584124654e-324, ulp IS the value, so its shortest form
# (ryu writes 5e-324) says nothing about %.2g, which is 4.9e-324. That one
# value is what caught it.
set.seed(99)
subn <- c(2^-1074, 2^-1073, 3 * 2^-1074, 5e-324, 1e-320, 1e-315, 1e-310,
          .Machine$double.xmin, .Machine$double.xmin / 2,
          .Machine$double.xmin * (1 - 2^-52), runif(2000, 1e-320, 1e-300))
tiny <- c(runif(2000, 1e-8, 1e-4), runif(2000, 1e-40, 1e-20))
for (nm in c("subnormal", "tiny normal")) {
  x <- if (nm == "subnormal") subn else tiny
  bad <- 0L
  for (d in 0:9) {
    if (!identical(as.character(toJSON(x, digits = d)),
                   as.character(as_json(x, digits = d)))) bad <- bad + 1L
    if (!identical(as.character(toJSON(x, digits = I(d))),
                   as.character(as_json(x, digits = I(d))))) bad <- bad + 1L
  }
  if (!identical(as.character(toJSON(x, digits = NA)),
                 as.character(as_json(x, digits = NA)))) bad <- bad + 1L
  if (bad == 0L) {
    cat(sprintf("  PASS  %-24s over digits 0..9, I(0..9) and NA\n", nm))
  } else {
    fails <- fails + 1L
    cat(sprintf("  FAIL  %-24s differs in %d of 21 modes\n", nm, bad))
  }
}

cat(sprintf("\n%d failure(s)\n", fails))
if (fails > 0) quit(status = 1)
