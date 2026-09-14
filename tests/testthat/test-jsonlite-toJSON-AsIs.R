# Ported verbatim from jsonlite 2.0.0 tests/testthat/test-toJSON-AsIs.R
# Copyright (c) Jeroen Ooms, MIT licence. See helper-jsonlite-parity.R.
# `toJSON()` here is bound to fastgeojson::as_json() by that helper.

test_that("Encoding AsIs", {
  expect_equal(toJSON(list(1), auto_unbox = TRUE), "[1]")
  expect_equal(toJSON(list(I(1)), auto_unbox = TRUE), "[[1]]")
  expect_equal(toJSON(I(list(1)), auto_unbox = TRUE), "[1]")

  expect_equal(toJSON(list(x = 1)), "{\"x\":[1]}")
  expect_equal(toJSON(list(x = 1), auto_unbox = TRUE), "{\"x\":1}")
  expect_equal(toJSON(list(x = I(1)), auto_unbox = TRUE), "{\"x\":[1]}")

  expect_equal(toJSON(list(x = I(list(1))), auto_unbox = TRUE), "{\"x\":[1]}")
  expect_equal(toJSON(list(x = list(I(1))), auto_unbox = TRUE), "{\"x\":[[1]]}")
})
