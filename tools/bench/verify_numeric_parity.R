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

cat(sprintf("\n%d failure(s)\n", fails))
if (fails > 0) quit(status = 1)
