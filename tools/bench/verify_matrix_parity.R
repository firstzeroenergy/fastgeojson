#!/usr/bin/env Rscript
# Matrix columns: every element type, non-finite values, all na modes, all
# three dataframe orientations, and thread-count invariance.
#
# Numeric matrices are now described for the workers rather than rendered on
# the R thread, reading R's column-major storage directly, so an indexing
# error here would transpose the output or read out of bounds.
#
#   Rscript tools/bench/verify_matrix_parity.R

suppressMessages({library(fastgeojson); library(jsonlite)})

fails <- 0L
cmp <- function(x, label, ...) {
  want <- as.character(toJSON(x, ...))
  got  <- as.character(as_json(x, ...))
  if (identical(want, got)) {
    cat(sprintf("  PASS  %-44s\n", label))
  } else {
    fails <<- fails + 1L
    cat(sprintf("  FAIL  %-44s\n        R   : %s\n        rust: %s\n", label,
                substr(want, 1, 170), substr(got, 1, 170)))
  }
}
withm <- function(m, nr = nrow(m)) { d <- data.frame(i = seq_len(nr)); d$m <- m; d }

cat("== element types ==\n")
cmp(withm(matrix(c(1, 2, 3, 4, 5, 6), nrow = 2)), "double")
cmp(withm(matrix(1:6, nrow = 2)), "integer")
cmp(withm(matrix(c(TRUE, FALSE, NA, TRUE, FALSE, TRUE), nrow = 2)), "logical")
cmp(withm(matrix(letters[1:6], nrow = 2)), "character")
cmp(withm(matrix(c("a", NA, 'q"q', "d", "<e", "f"), nrow = 2)), "character with NA and escapes")
cmp(withm(matrix(complex(real = 1:6, imaginary = 6:1), nrow = 2)), "complex")

cat("\n== non-finite doubles, every na mode ==\n")
nf <- matrix(c(1, NA, NaN, Inf, -Inf, 6), nrow = 2)
cmp(withm(nf), "non-finite, default na")
for (na in c("null", "string")) cmp(withm(nf), sprintf("non-finite, na=%s", na), na = na)
nfi <- matrix(c(1L, NA_integer_, 3L, 4L, 5L, 6L), nrow = 2)
cmp(withm(nfi), "integer with NA, default na")
for (na in c("null", "string")) cmp(withm(nfi), sprintf("integer NA, na=%s", na), na = na)

cat("\n== orientations, digits, always_decimal ==\n")
d <- withm(matrix(c(pi, exp(1), 1/3, sqrt(2), 1e-7, 2^31), nrow = 2))
for (dfm in c("rows", "columns", "values")) cmp(d, sprintf("dataframe = %s", dfm), dataframe = dfm)
# digits = Inf is this package's own, so there is nothing to compare it
# against; verify_numeric_parity.R checks it on its own terms.
for (dg in list(2, 4, NA)) cmp(d, sprintf("digits = %s", dg), digits = dg)
cmp(withm(matrix(c(1, 2, 3, 4), nrow = 2)), "always_decimal", always_decimal = TRUE)
cmp(d, "matrix = columnmajor", matrix = "columnmajor")
cmp(d, "auto_unbox", auto_unbox = TRUE)

cat("\n== shapes ==\n")
cmp(withm(matrix(1:4, nrow = 4)), "single column matrix")
cmp(withm(matrix(1:40, nrow = 4)), "ten column matrix")
cmp(withm(matrix(numeric(0), nrow = 0, ncol = 3), nr = 0), "zero rows")
d2 <- data.frame(i = 1:3)
d2$a <- matrix(1:6, nrow = 3)
d2$b <- matrix(as.double(7:12), nrow = 3)
cmp(d2, "two matrix columns side by side")
d3 <- data.frame(i = 1:3, s = c("x", "y", "z"), stringsAsFactors = FALSE)
d3$m <- matrix(rnorm(6), nrow = 3)
d3$f <- factor(c("a", "b", "a"))
cmp(d3, "matrix mixed with other column types")

cat("\n== large, so the parallel path is exercised ==\n")
set.seed(1)
for (nc in c(3, 10)) {
  big <- data.frame(i = 1:20000)
  big$m <- matrix(rnorm(20000 * nc), nrow = 20000)
  big$m[sample(length(big$m), 200)] <- NA
  cmp(big, sprintf("20k rows x %d-wide matrix", nc))
  ref <- as.character(as_json(big))
  for (t in c(1, 2, 8, 0)) {
    fastgeojson_threads(t)
    if (!identical(as.character(as_json(big)), ref)) {
      fails <- fails + 1L
      cat(sprintf("  FAIL  20k x %d at %d threads\n", nc, t))
    }
  }
  fastgeojson_threads(0)
}
cat("  PASS  identical at 1, 2, 8 and automatic threads\n")

bigi <- data.frame(i = 1:20000)
bigi$m <- matrix(sample(1e6L, 20000 * 4), nrow = 20000)
cmp(bigi, "20k rows x 4-wide integer matrix")

cat("\n== an array column, which is not a matrix ==\n")
# Three dimensions and up are now described for the workers rather than
# rendered serially into an arena, so each type and each argument that changes
# a value's text needs a case, and one shape has to be large enough to be cut
# into chunks.
d4 <- data.frame(i = 1:2)
d4$a <- array(1:12, dim = c(2, 3, 2))
cmp(d4, "3-d array column")
mkarr <- function(v, dim) { d <- data.frame(i = seq_len(dim[1])); d$a <- array(v, dim = dim); d }
cmp(mkarr(as.numeric(1:12), c(2, 3, 2)), "3-d double array column")
cmp(mkarr(c(TRUE, FALSE, NA, rep(TRUE, 9)), c(2, 3, 2)), "3-d logical array column")
cmp(mkarr(letters[1:12], c(2, 3, 2)), "3-d character array column")
cmp(mkarr(complex(real = 1:12, imaginary = 12:1), c(2, 3, 2)), "3-d complex array column")
cmp(mkarr(as.numeric(1:24), c(2, 3, 2, 2)), "4-d double array column")
nfa <- c(1.5, NA, NaN, Inf, -Inf, 2.5, 1e-9, 1e20, 0, -0.0, 7, 8)
cmp(mkarr(nfa, c(2, 3, 2)), "3-d array with non-finite, default na")
for (na in c("null", "string")) {
  cmp(mkarr(nfa, c(2, 3, 2)), sprintf("3-d array non-finite, na=%s", na), na = na)
}
for (dg in list(2, 4, NA)) {
  cmp(mkarr(runif(12), c(2, 3, 2)), sprintf("3-d array digits = %s", dg), digits = dg)
}
cmp(mkarr(as.numeric(1:12), c(2, 3, 2)), "3-d array always_decimal", always_decimal = TRUE)
for (dfm in c("rows", "columns", "values")) {
  cmp(mkarr(as.numeric(1:12), c(2, 3, 2)), sprintf("3-d array, dataframe = %s", dfm),
      dataframe = dfm)
}
cmp(list(d = mkarr(as.numeric(1:12), c(2, 3, 2))), "3-d array column, nested frame")
set.seed(4)
biga <- mkarr(runif(20000 * 20), c(20000, 20))
biga$a <- array(runif(20000 * 4 * 5), dim = c(20000, 4, 5))
cmp(biga, "20k rows x 4 x 5 double array column")
{
  ts <- c(1L, 2L, 8L, 0L)
  outs <- vapply(ts, function(t) { fastgeojson_threads(t); as.character(as_json(biga)) }, "")
  fastgeojson_threads(0)
  if (length(unique(outs)) == 1L) {
    cat("  PASS  array column identical at 1, 2, 8 and automatic threads\n")
  } else {
    fails <- fails + 1L
    cat("  FAIL  array column output depends on the worker count\n")
  }
}

cat(sprintf("\n%d failure(s)\n", fails))
if (fails > 0) quit(status = 1)
