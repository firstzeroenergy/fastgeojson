library(microbenchmark)
library(jsonlite)
library(yyjsonr)
library(geojsonsf)
library(sf)

# Create a large df point dataset: 1 million points
n <- 1e6
df <- data.frame(
  id = 1:n
  , value = sample(letters, size = n, replace = T)
  , val2 = rnorm(n = n)
  , log = sample(c(T,F), size = n, replace = T)
  , stringsAsFactors = FALSE
)

microbenchmark(
  jsonify = {
    jfy <- jsonify::to_json( df )
  },
  jsonlite = {
    jlt <- jsonlite::toJSON( df )
  },
  yyjsonr = {
    yyj <- yyjsonr::write_json_str(df)
  },
  fastgeojson = {
    fst <- fastgeojson::df_json_str(df)
  },
  times = 10L
)

# Create a large sf point dataset: 1 million points
set.seed(123)
n <- 1e6
lon <- runif(n, -125, -66)   # Continental US bounds
lat <- runif(n, 40, 49)
value <- rnorm(n)
category <- sample(letters[1:5], n, replace = TRUE)

large_points_sf <- data.frame(lon, lat, value, category) %>%
  st_as_sf(coords = c("lon", "lat"), crs = 4326)

# GeoJSON BENCHMARK
microbenchmark(
  geojsonsf = {
    gjs <- geojsonsf::sf_geojson(large_points_sf)
  },
  yyjsonr = {
    yyj <- yyjsonr::write_geojson_str(large_points_sf)
  },
  fastgeojson = {
    fst <- fastgeojson::sf_geojson_str(large_points_sf)
  },
  times = 10L
)
