# Benchmark corpus for fastgeojson.
#
# Each generator isolates a different cost centre, so a change can be
# attributed rather than just observed. `n` scales the row count; the shape is
# what matters.
#
# Deterministic: every generator seeds locally, so runs are comparable across
# code revisions and machines.

corpus <- local({

  gen <- list()

  # ---- numeric ---------------------------------------------------------
  gen$num_random <- function(n) {
    set.seed(1); data.frame(a = runif(n), b = rnorm(n), c = runif(n) * 1e6)
  }
  gen$num_whole <- function(n) {
    set.seed(2); data.frame(a = as.double(sample(1e6, n, TRUE)),
                            b = as.double(sample(1e6, n, TRUE)))
  }
  gen$int_only <- function(n) {
    set.seed(3); data.frame(a = sample(1e6L, n, TRUE), b = seq_len(n))
  }
  gen$logical_only <- function(n) {
    set.seed(4); data.frame(a = sample(c(TRUE, FALSE), n, TRUE),
                            b = sample(c(TRUE, FALSE), n, TRUE))
  }

  # ---- character -------------------------------------------------------
  gen$chr_short <- function(n) {
    set.seed(5); data.frame(a = sample(c("alpha", "beta", "gamma", "delta"), n, TRUE),
                            b = sample(letters, n, TRUE), stringsAsFactors = FALSE)
  }
  gen$chr_long <- function(n) {
    set.seed(6)
    pool <- vapply(1:50, function(i) paste(sample(letters, 200, TRUE), collapse = ""), "")
    data.frame(a = sample(pool, n, TRUE), stringsAsFactors = FALSE)
  }
  gen$chr_escaped <- function(n) {
    set.seed(7)
    pool <- c('quote"here', "back\\slash", "tab\there", "newline\nhere", "plain")
    data.frame(a = sample(pool, n, TRUE), stringsAsFactors = FALSE)
  }
  gen$chr_unicode <- function(n) {
    set.seed(8)
    pool <- c("café", "中文", "\U0001F600", "naïve", "straße")
    data.frame(a = sample(pool, n, TRUE), stringsAsFactors = FALSE)
  }
  gen$factor_only <- function(n) {
    set.seed(9); data.frame(a = factor(sample(letters, n, TRUE)),
                            b = factor(sample(month.name, n, TRUE)))
  }

  # ---- shape -----------------------------------------------------------
  gen$wide <- function(n) {
    set.seed(10)
    k <- 200
    m <- as.data.frame(matrix(runif(n * k), nrow = n))
    names(m) <- paste0("v", seq_len(k))
    m
  }
  gen$narrow_tall <- function(n) {
    set.seed(11); data.frame(a = runif(n))
  }
  gen$mixed <- function(n) {
    set.seed(12)
    data.frame(i = sample(1e6L, n, TRUE), d = rnorm(n),
               l = sample(c(TRUE, FALSE), n, TRUE),
               s = sample(c("aa", "bb", "cc"), n, TRUE),
               f = factor(sample(letters[1:5], n, TRUE)),
               stringsAsFactors = FALSE)
  }

  # ---- NA density ------------------------------------------------------
  gen$na_dense <- function(n) {
    set.seed(13)
    d <- data.frame(a = rnorm(n), b = sample(c("x", "y"), n, TRUE), stringsAsFactors = FALSE)
    d$a[sample(n, n %/% 2)] <- NA
    d$b[sample(n, n %/% 2)] <- NA
    d
  }

  # ---- nested ----------------------------------------------------------
  gen$list_column <- function(n) {
    set.seed(14)
    d <- data.frame(id = seq_len(n))
    d$vals <- replicate(n, runif(3), simplify = FALSE)
    d
  }
  gen$nested_list <- function(n) {
    set.seed(15)
    lapply(seq_len(n), function(i) list(id = i, name = "x", vals = c(1, 2, 3)))
  }

  # ---- atomic vectors --------------------------------------------------
  gen$vec_double <- function(n) { set.seed(16); runif(n) }
  gen$vec_chr    <- function(n) { set.seed(17); sample(c("aa", "bb", "cc"), n, TRUE) }

  # ---- sf --------------------------------------------------------------
  gen$sf_point <- function(n) {
    set.seed(18)
    sf::st_as_sf(
      data.frame(lon = runif(n, -125, -66), lat = runif(n, 25, 49),
                 v = rnorm(n), cat = sample(letters[1:5], n, TRUE)),
      coords = c("lon", "lat"), crs = 4326)
  }
  gen$sf_linestring <- function(n) {
    set.seed(19)
    geoms <- lapply(seq_len(n), function(i) {
      sf::st_linestring(cbind(runif(20, -125, -66), runif(20, 25, 49)))
    })
    sf::st_sf(id = seq_len(n), geometry = sf::st_sfc(geoms, crs = 4326))
  }
  gen$sf_polygon <- function(n) {
    set.seed(20)
    geoms <- lapply(seq_len(n), function(i) {
      cx <- runif(1, -120, -70); cy <- runif(1, 30, 45)
      th <- seq(0, 2 * pi, length.out = 200)
      m <- cbind(cx + cos(th), cy + sin(th))
      m[nrow(m), ] <- m[1, ]
      sf::st_polygon(list(m))
    })
    sf::st_sf(id = seq_len(n), v = rnorm(n), geometry = sf::st_sfc(geoms, crs = 4326))
  }
  gen$sf_point_xyz <- function(n) {
    set.seed(21)
    geoms <- lapply(seq_len(n), function(i) sf::st_point(c(runif(1), runif(1), runif(1))))
    sf::st_sf(id = seq_len(n), geometry = sf::st_sfc(geoms, crs = 4326))
  }

  # Which competitor functions each shape can be compared against.
  kind <- c(
    num_random = "df", num_whole = "df", int_only = "df", logical_only = "df",
    chr_short = "df", chr_long = "df", chr_escaped = "df", chr_unicode = "df",
    factor_only = "df", wide = "df", narrow_tall = "df", mixed = "df",
    na_dense = "df", list_column = "df", nested_list = "list",
    vec_double = "vec", vec_chr = "vec",
    sf_point = "sf", sf_linestring = "sf", sf_polygon = "sf", sf_point_xyz = "sf"
  )

  list(gen = gen, kind = kind)
})

corpus_build <- function(name, n) corpus$gen[[name]](n)
corpus_kind  <- function(name) corpus$kind[[name]]
corpus_names <- function(kinds = NULL) {
  nm <- names(corpus$gen)
  if (is.null(kinds)) return(nm)
  nm[corpus$kind[nm] %in% kinds]
}
