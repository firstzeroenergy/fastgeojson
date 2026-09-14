# Ported verbatim from jsonlite 2.0.0 tests/testthat/test-toJSON-factor.R
# Copyright (c) Jeroen Ooms, MIT licence. See helper-jsonlite-parity.R.
# `toJSON()` here is bound to fastgeojson::as_json() by that helper.

test_that("Encoding Factor Objects", {
  expect_identical(fromJSON(toJSON(iris$Species)), as.character(iris$Species))
  expect_identical(fromJSON(toJSON(iris$Species[1])), as.character(iris$Species[1]))
  expect_equal(fromJSON(toJSON(iris$Species, factor = "integer")), structure(unclass(iris$Species), levels = NULL))
})
