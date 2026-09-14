# Ported verbatim from jsonlite 2.0.0 tests/testthat/test-toJSON-NA-values.R
# Copyright (c) Jeroen Ooms, MIT licence. See helper-jsonlite-parity.R.
# `toJSON()` here is bound to fastgeojson::as_json() by that helper.

test_that("Test NA values", {
  options(stringsAsFactors = FALSE)
  x <- list(foo = c(TRUE, NA, FALSE, TRUE), bar = c(3.14, NA, 42, NA), zoo = c(NA, "bla", "boe", NA))
  x$mydf <- data.frame(col1 = c(FALSE, NA, NA, TRUE), col2 = c(1.23, NA, 23, NA))
  x$mydf$mylist <- list(c(TRUE, NA, FALSE, NA), NA, c("blabla", NA), c(NA, 12, 13, NA, NA, NA, 1001))

  expect_true(validate(toJSON(x)))
  expect_equal(fromJSON(toJSON(x)), x)
  expect_equal(fromJSON(toJSON(x, na = "null")), x)
})
