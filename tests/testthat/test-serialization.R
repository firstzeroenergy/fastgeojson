library(testthat)
library(fastgeojson)
library(jsonlite)

test_that("as_json serializes data.frames correctly", {
  # 1. Simple case
  df <- data.frame(id = 1:2, val = c("A", "B"))
  json <- as_json(df)

  # Check class
  expect_s3_class(json, "json")

  # Check content (manually parsing back to verify)
  # We use jsonlite to verify our output is valid JSON
  parsed <- jsonlite::fromJSON(json)
  expect_equal(parsed$id, df$id)
  expect_equal(parsed$val, df$val)
})

test_that("df_json_str handles NAs correctly", {
  df <- data.frame(a = c(1, NA), b = c("x", NA))
  json <- as_json(df)

  # Expect NA to become null or be omitted (depending on your logic)
  # Here we verify it validates as JSON
  expect_true(jsonlite::validate(json))
})

test_that("as_json produces valid GeoJSON", {
  skip_if_not_installed("sf")
  library(sf)

  # Create a simple point
  p1 <- st_point(c(0, 0))
  sf_obj <- st_sf(id = 1, geometry = st_sfc(p1), crs = 4326)

  geojson <- as_json(sf_obj)

  expect_s3_class(geojson, c("geojson", "json"))
  expect_true(grepl("FeatureCollection", geojson))
  expect_true(grepl("coordinates", geojson))
})
