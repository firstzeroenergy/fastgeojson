# Descriptors must not point into a SEXP this package created.
#
# Writing R Extensions is explicit about what is protected for you: an
# argument to .Call is kept alive by the caller for the duration of the call,
# and nothing else is. A POSIXt column cannot be formatted in a worker -- only
# R holds the time zone rules -- so the column builder calls format() on the R
# thread, and format()'s result is a brand-new STRSXP that nothing in the
# caller's frame references.
#
# The column builder used to hand that STRSXP to the CharDirect fast path,
# which keeps a raw pointer to its character data and lets the worker pool
# dereference it much later. With two POSIXt columns in one frame, the second
# format() call could reuse the first's storage, and the first column came out
# holding the second's timestamps -- silently, with no crash and no warning:
# well-formed JSON with the wrong values in it.
#
# gctorture(TRUE) collects on every allocation, which turns that from a rare
# race into a certainty. These tests are cheap because the frames are tiny;
# gctorture makes even a small one slow, so nothing here exceeds a few hundred
# rows.
#
# `as_json()` pre-encodes every POSIXt column in R before the builder sees it
# -- to `fgjtime` on the fast path, to character when the zone or the format
# needs R -- so the branch that calls format() in Rust is now reached only
# through the internal entry point. It is kept as the defensive path, the
# same role the recursive writer's own format() call plays, and it is tested
# here directly: a defensive path nothing exercises is a defensive path that
# rots.
enc <- function(x, ...) fastgeojson:::df_json_str_impl(
  x, FALSE, "rows", "smart", "list", "string", NULL, FALSE, FALSE, TRUE, TRUE, FALSE)

posixt_frame <- function(n = 200L, ncol = 2L, tz = "UTC") {
  base <- as.POSIXct("2024-01-01 00:00:00", tz = tz)
  d <- as.data.frame(
    lapply(seq_len(ncol), function(k) base + seq_len(n) + (k - 1L) * 1000L)
  )
  names(d) <- paste0("t", seq_len(ncol))
  d
}

# The JIT is switched off for the duration. When the package is not
# byte-compiled -- devtools::test(), or an install with --no-byte-compile --
# R's JIT compiles the helpers as_json() calls, and the compiler makes about
# 300,000 allocations per call; under gctorture each one is a full collection,
# which turned a sub-second file into an hour. The collections these tests
# need come from the package's own allocations, which the JIT adds nothing to.
with_gctorture <- function(expr) {
  jit <- compiler::enableJIT(0L)
  on.exit(compiler::enableJIT(jit), add = TRUE)
  gctorture(TRUE)
  on.exit(gctorture(FALSE), add = TRUE)
  force(expr)
}

test_that("a POSIXt column keeps its own values under gctorture", {
  d <- posixt_frame()
  want <- as_json(d)
  expect_identical(with_gctorture(as_json(d)), want)
  # The internal entry point skips as_json's pre-encoding, so it is what
  # reaches the Rust format() branch at all.
  want_direct <- enc(d)
  expect_identical(with_gctorture(enc(d)), want_direct)
})

test_that("several POSIXt columns do not borrow each other's storage", {
  for (ncol in 2:4) {
    d <- posixt_frame(120L, ncol)
    expect_identical(with_gctorture(enc(d)), enc(d))
    expect_identical(with_gctorture(as_json(d)), as_json(d))
  }
})

test_that("a POSIXt column next to other kinds survives a collection", {
  # Any column built after the timestamps can trigger the allocation that
  # reuses their storage, so the neighbours matter.
  base <- as.POSIXct("2024-01-01 00:00:00", tz = "UTC")
  d <- data.frame(
    t = base + 1:150,
    s = sprintf("row %03d", 1:150),
    f = factor(rep(c("a", "b", "c"), 50)),
    i = 1:150,
    x = runif(150),
    l = rep(c(TRUE, FALSE, NA), 50),
    dt = as.Date("2024-01-01") + 1:150,
    stringsAsFactors = FALSE
  )
  expect_identical(with_gctorture(enc(d)), enc(d))
  expect_identical(with_gctorture(as_json(d)), as_json(d))
  for (mode in c("rows", "columns", "values")) {
    expect_identical(with_gctorture(as_json(d, dataframe = mode)),
                     as_json(d, dataframe = mode))
  }
})

test_that("timestamps in a nested frame survive a collection", {
  # The nested builder is a separate call into build_thread_safe_cols.
  inner <- posixt_frame(60L, 2L)
  expect_identical(with_gctorture(as_json(list(a = inner, b = inner))),
                   as_json(list(a = inner, b = inner)))
})

test_that("the POSIXt modes that render in R also survive a collection", {
  d <- posixt_frame(80L, 2L)
  for (mode in c("string", "ISO8601", "epoch", "mongo")) {
    expect_identical(with_gctorture(as_json(d, POSIXt = mode)),
                     as_json(d, POSIXt = mode))
  }
  # A non-UTC zone formats through a different branch of R's own code.
  d2 <- posixt_frame(80L, 2L, tz = "America/New_York")
  expect_identical(with_gctorture(enc(d2)), enc(d2))
})

test_that("non-ASCII strings, which take the arena, survive a collection", {
  # The arena path calls Rf_translateCharUTF8, which allocates on R's vmax
  # stack, so it is the other place where an allocation happens mid-build.
  d <- data.frame(
    t = as.POSIXct("2024-01-01", tz = "UTC") + 1:90,
    s = rep(c("café", "日本", "plain"), 30),
    stringsAsFactors = FALSE
  )
  expect_identical(with_gctorture(enc(d)), enc(d))
  expect_identical(with_gctorture(as_json(d)), as_json(d))
})
