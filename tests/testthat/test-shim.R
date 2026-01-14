library(testthat)
library(fastgeojson)
library(jsonlite)

test_that("global shim enables and disables correctly", {
  pkg_env <- as.environment("package:jsonlite")
  
  # -------------------------------------------------------------------
  # 0. HARD RESET (Safety First)
  # -------------------------------------------------------------------
  real_toJSON <- get("toJSON", envir = getNamespace("jsonlite"))
  
  if (exists("original_toJSON", envir = fastgeojson:::.fastgeojson_cache)) {
    rm("original_toJSON", envir = fastgeojson:::.fastgeojson_cache)
  }
  
  unlockBinding("toJSON", pkg_env)
  assign("toJSON", real_toJSON, pkg_env)
  lockBinding("toJSON", pkg_env)
  
  # -------------------------------------------------------------------
  # 1. Capture Baseline
  # -------------------------------------------------------------------
  baseline_toJSON <- get("toJSON", envir = pkg_env)
  
  # -------------------------------------------------------------------
  # 2. Test Enable
  # -------------------------------------------------------------------
  enable_fast_json()
  
  # Verify the function pointer has changed
  current_toJSON <- get("toJSON", envir = pkg_env)
  expect_false(identical(baseline_toJSON, current_toJSON))
  
  # Verify functionality (shim behavior)
  res <- jsonlite::toJSON(list(a = 1), auto_unbox = TRUE)
  expect_match(as.character(res), '{"a":1}', fixed = TRUE)
  
  # -------------------------------------------------------------------
  # 3. Test Disable
  # -------------------------------------------------------------------
  disable_fast_json()
  
  # Verify strict restoration
  restored_toJSON <- get("toJSON", envir = pkg_env)
  expect_equal(baseline_toJSON, restored_toJSON, ignore_attr = TRUE)
})

test_that("auto_unbox argument works as expected", {
  # Default behavior (FALSE) -> Boxed
  actual_boxed <- as.character(as_json(list(val = 1), auto_unbox = FALSE))
  expect_equal(actual_boxed, '{"val":[1]}')
  
  # Unboxed behavior (TRUE) -> Scalar
  actual_unboxed <- as.character(as_json(list(val = 1), auto_unbox = TRUE))
  expect_equal(actual_unboxed, '{"val":1}')
})