# Ported verbatim from jsonlite 2.0.0 tests/testthat/test-toJSON-NULL-values.R
# Copyright (c) Jeroen Ooms, MIT licence. See helper-jsonlite-parity.R.
# `toJSON()` here is bound to fastgeojson::as_json() by that helper.

test_that("Test NULL values", {
  # jsonlite writes `.Names =`; R-devel (Sept 2026) notes that spelling as
  # deprecated, so this is the one edit in the ported file.
  namedlist <- structure(list(), names = character(0))
  x <- NULL
  y <- list(a = NULL, b = NA)
  z <- list(a = 1, b = character(0))

  expect_true(validate(toJSON(x)))
  expect_equal(fromJSON(toJSON(x)), namedlist)
  expect_equal(toJSON(x), "{}")
  expect_equal(toJSON(x, null = "list"), "{}")

  expect_true(validate(toJSON(y)))
  expect_equal(toJSON(y, null = "list"), "{\"a\":{},\"b\":[null]}")
  expect_equal(toJSON(y, null = "null"), "{\"a\":null,\"b\":[null]}")
  expect_equal(fromJSON(toJSON(y, null = "null")), y)
  expect_equal(fromJSON(toJSON(y, null = "list")), list(a = namedlist, b = NA))

  expect_true(validate(toJSON(z)))
  expect_equal(toJSON(z), "{\"a\":[1],\"b\":[]}")
  expect_equal(fromJSON(toJSON(z)), list(a = 1, b = list()))
})
