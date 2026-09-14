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

test_that("a data.frame handles NAs correctly", {
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

test_that("numbers are lossless by default", {
  # digits = Inf: the shortest decimal that reads back as the same double.
  # toJSON()'s 4 decimal places are opt-in, never the default.
  #
  # The extreme values are powers of two, which every platform constructs
  # exactly. A literal like 1e300 is not: R's parser scales by powers of ten
  # in long double, and where that is 64 bits (macOS on arm64) it lands a few
  # ulp off, so the double being formatted -- and the correct output for it
  # -- differs from platform to platform.
  x <- c(pi, 0.1 + 0.2, 1e-5, 0.000151481324748, 1234567.125, -2^-1000, 2^1000)
  expect_identical(as.character(as_json(x)),
                   "[3.141592653589793,0.30000000000000004,0.00001,0.000151481324748,1234567.125,-9.332636185032189e-302,1.0715086071862673e301]")
  expect_identical(jsonlite::fromJSON(as_json(x)), x)

  set.seed(42)
  r <- c(runif(2000), rnorm(2000) * 10^sample(-20:20, 2000, TRUE))
  expect_identical(jsonlite::fromJSON(as_json(r)), r)
  df <- data.frame(a = r[1:500], b = r[501:1000])
  expect_identical(jsonlite::fromJSON(as_json(df)), df)
  expect_identical(jsonlite::fromJSON(as_json(df, dataframe = "columns"))$a, df$a)

  # jsonlite's default is one argument away, and every count from 16 up is
  # its 17 significant digits -- including 255, the internal code for Inf
  expect_identical(as.character(as_json(x, digits = 4)),
                   as.character(jsonlite::toJSON(x)))
  for (d in c(16, 22, 255, 1000)) {
    expect_identical(as.character(as_json(x, digits = d)),
                     as.character(suppressWarnings(jsonlite::toJSON(x, digits = d))), info = d)
  }
  expect_identical(as.character(as_json(c(pi, 0.1 + 0.2, 0.000151481324748), digits = 4)),
                   "[3.1416,0.3,0.0002]")

  # complex has no lossless mode in jsonlite (toJSON(z, digits = NA) errors);
  # ours writes each part shortest, in prettyNum()'s layout
  z <- complex(real = c(1.5, 0.1 + 0.2, NA, Inf), imaginary = c(-2, 1/3, 1, 1))
  expect_identical(as.character(as_json(z)),
                   '["1.5-2i","0.30000000000000004+0.3333333333333333i","NA","Inf+1i"]')
  expect_identical(as.character(as_json(z, digits = 4)), as.character(jsonlite::toJSON(z)))
  expect_identical(as.character(as_json(complex(0))), "[]")
})

test_that("the lossless default reaches every writer route", {
  set.seed(7)
  v <- rnorm(60) * 10^sample(-12:12, 60, TRUE)
  rt <- function(json) jsonlite::fromJSON(json)

  # whole doubles: plain digits below 1e16 either side of 2^53; ryu's
  # exponent form from 1e16
  expect_identical(as.character(as_json(c(2^53 - 1, 2^53, -2^53, 2^53 + 2, 1e16, 1e17))),
                   "[9007199254740991,9007199254740992,-9007199254740992,9007199254740994,1e16,1e17]")

  m <- matrix(v[1:12], 3)
  expect_identical(rt(as_json(m)), m)
  expect_identical(rt(as_json(m, matrix = "columnmajor")), t(m))
  expect_identical(rt(as_json(list(a = v[1:5], b = list(c = v[6:9]))))$b$c, v[6:9])
  df <- data.frame(x = v[1:20], y = v[21:40])
  for (mode in c("rows", "columns", "values")) {
    got <- rt(as_json(df, dataframe = mode))
    xs <- if (mode == "values") got[, 1] else got$x
    expect_identical(as.numeric(xs), df$x, info = mode)
  }
  df$m <- matrix(v[41:60], 20, 1)
  expect_identical(unlist(rt(as_json(df))$m), v[41:60])
  expect_identical(rt(as_json(df, pretty = TRUE))$y, df$y)
  expect_identical(rt(as_json(v[1], auto_unbox = TRUE)), v[1])
  expect_identical(rawToChar(as_json(df, as_bytes = TRUE)), as.character(as_json(df)))

  # sf: coordinates and properties, in every sf mode
  pts <- sf::st_as_sf(data.frame(lon = v[1:5] / 1e12, lat = v[6:10] / 1e12, p = v[11:15]),
                      coords = c("lon", "lat"), crs = 4326)
  gj <- rt(as_json(pts))
  expect_identical(do.call(rbind, gj$features$geometry$coordinates),
                   unname(sf::st_coordinates(pts)))
  expect_identical(gj$features$properties$p, pts$p)
  expect_identical(rt(as_json(pts, sf = "dataframe"))$p, pts$p)
  expect_identical(rt(as_json(pts, sf = "features"))$properties$p, pts$p)

  # complex matrices: Re()/Im() keep dim, which used to nest the parts and
  # shred them -- ["[1+[1i","3]+3]i",...]
  cm <- matrix(complex(real = c(1, 2, 3, 4), imaginary = c(0.1 + 0.2, -2, 1/3, 0)), 2)
  expect_identical(as.character(as_json(cm)),
                   '[["1+0.30000000000000004i","3+0.3333333333333333i"],["2-2i","4+0i"]]')
  expect_identical(as.character(as_json(list(m = cm))),
                   '{"m":[["1+0.30000000000000004i","3+0.3333333333333333i"],["2-2i","4+0i"]]}')
  cdf <- data.frame(id = 1:2); cdf$m <- cm
  expect_identical(as.character(as_json(cdf)),
                   '[{"id":1,"m":["1+0.30000000000000004i","3+0.3333333333333333i"]},{"id":2,"m":["2-2i","4+0i"]}]')
  expect_identical(as.character(as_json(cm, digits = 4)), as.character(jsonlite::toJSON(cm)))
  expect_identical(as.character(as_json(cdf, digits = 4)), as.character(jsonlite::toJSON(cdf)))
})
