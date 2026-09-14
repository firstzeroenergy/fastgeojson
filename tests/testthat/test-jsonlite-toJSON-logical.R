# Ported verbatim from jsonlite 2.0.0 tests/testthat/test-toJSON-logical.R
# Copyright (c) Jeroen Ooms, MIT licence. See helper-jsonlite-parity.R.
# `toJSON()` here is bound to fastgeojson::as_json() by that helper.

test_that("Encoding Logical", {
  expect_equal(toJSON(TRUE), "[true]")
  expect_equal(toJSON(FALSE), "[false]")
  expect_equal(toJSON(as.logical(NA)), "[null]")
  expect_equal(toJSON(as.logical(NA), na = "string"), "[\"NA\"]")
  expect_equal(toJSON(c(TRUE, NA, FALSE)), "[true,null,false]")
  expect_equal(toJSON(c(TRUE, NA, FALSE), na = "string"), "[true,\"NA\",false]")
  expect_equal(toJSON(logical()), "[]")
})

test_that("Encoding Logical in Data Frame", {
  expect_equal(toJSON(data.frame(foo = TRUE)), "[{\"foo\":true}]")
  expect_equal(toJSON(data.frame(foo = FALSE)), "[{\"foo\":false}]")
  expect_equal(toJSON(data.frame(foo = as.logical(NA))), "[{}]")
  expect_equal(toJSON(data.frame(foo = as.logical(NA)), na = "null"), "[{\"foo\":null}]")
  expect_equal(toJSON(data.frame(foo = as.logical(NA)), na = "string"), "[{\"foo\":\"NA\"}]")
  expect_equal(toJSON(data.frame(foo = c(TRUE, NA, FALSE))), "[{\"foo\":true},{},{\"foo\":false}]")
  expect_equal(toJSON(data.frame(foo = logical())), "[]")
})
