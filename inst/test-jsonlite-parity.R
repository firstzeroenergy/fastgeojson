library(jsonlite)
library(fastgeojson)
library(tibble)
library(dplyr)
library(purrr)

# ---- 1. Define obstacle-course cases ----

cases <- list(
  # 1. Simple numeric
  simple_numeric = data.frame(x = c(1, 2.5, -3), stringsAsFactors = FALSE),
  
  # 2. Integer with NA
  int_na = data.frame(
    x = c(1L, NA_integer_, 3L),
    stringsAsFactors = FALSE
  ),
  
  # 3. Double with NA / NaN / Inf
  dbl_special = data.frame(
    x = c(1, NA_real_, NaN, Inf, -Inf),
    stringsAsFactors = FALSE
  ),
  
  # 4. Logical with NA
  logical_na = data.frame(
    x = c(TRUE, FALSE, NA),
    stringsAsFactors = FALSE
  ),
  
  # 5. Character with NA / "NA" / ""
  char_na = data.frame(
    ch = c("foo", NA, "bar", "NA", ""),
    stringsAsFactors = FALSE
  ),
  
  # 6. Factor with NA and level "NA"
  factor_na = {
    x <- factor(c("a", "b", NA, "NA", "a"), levels = c("a", "b", "NA"))
    data.frame(x = x)
  },
  
  # 7. Mixed types
  mixed = data.frame(
    i  = c(1L, NA_integer_, 3L),
    d  = c(1, NA_real_, 3.5),
    l  = c(TRUE, NA, FALSE),
    ch = c("a", "", NA),
    stringsAsFactors = FALSE
  ),
  
  # 8. Dates
  dates = data.frame(
    d = as.Date(c("2020-01-01", NA, "2020-01-03"))
  ),
  
  # 9. POSIXct
  posixct = data.frame(
    dt = as.POSIXct(
      c("2020-01-01 00:00:00",
        NA,
        "2020-01-03 12:34:56"),
      tz = "UTC"
    )
  ),
  
  # 10. Empty data.frame: 0 rows, 2 columns
  empty_rows = data.frame(
    x = integer(0),
    y = character(0),
    stringsAsFactors = FALSE
  ),
  
  # 11. Zero-column data.frame: 3 rows, df[, 0]-style
  empty_cols = {
    df <- data.frame(x = 1:3, stringsAsFactors = FALSE)
    df[, 0, drop = FALSE]
  },
  
  # 12. Data.frame with weird names
  weird_names = {
    df <- data.frame(
      `sp ace`      = c(1, 2),
      `quote"here`  = c("a", "b"),
      `uniçode`     = c(TRUE, FALSE),
      check.names   = FALSE,
      stringsAsFactors = FALSE
    )
    df
  },
  
  # 13. Nested list column (likely mismatch)
  list_column = data.frame(
    id = c(1L, 2L),
    nested = I(list(
      data.frame(a = 1, b = "x", stringsAsFactors = FALSE),
      data.frame(a = 2, b = "y", stringsAsFactors = FALSE)
    )),
    stringsAsFactors = FALSE
  ),
  
  # 14. List column of atomic vectors
  list_atomic = data.frame(
    id = 1:3,
    vals = I(list(
      c(1, 2, 3),
      NA_real_,
      numeric(0)
    )),
    stringsAsFactors = FALSE
  ),
  
  # 15. Row names present
  rownames_df = {
    df <- data.frame(x = 1:3, y = c("a", "b", "c"), stringsAsFactors = FALSE)
    rownames(df) <- c("r1", "r2", "r3")
    df
  }
)

# ---- 2. Helper to run one case ----
# NOTE: order is (obj, name) to match imap(.x, .y)

run_case <- function(obj, name) {
  case_name <- as.character(name)
  
  # Reference: jsonlite
  ref_res <- tryCatch(
    {
      j <- jsonlite::toJSON(obj, dataframe = "rows", auto_unbox = FALSE)
      list(error = NA_character_, json = as.character(j))
    },
    error = function(e) list(error = conditionMessage(e), json = NA_character_)
  )
  
  # fastgeojson
  mine_res <- tryCatch(
    {
      j <- fastgeojson::as_json(obj)
      list(error = NA_character_, json = as.character(j))
    },
    error = function(e) list(error = conditionMessage(e), json = NA_character_)
  )
  
  # Compare if both succeeded
  identical_flag <- NA
  if (is.na(ref_res$error) && is.na(mine_res$error)) {
    ref_min  <- jsonlite::minify(ref_res$json)
    mine_min <- jsonlite::minify(mine_res$json)
    identical_flag <- identical(ref_min, mine_min)
  }
  
  tibble(
    case        = case_name,
    ref_error   = ref_res$error,
    mine_error  = mine_res$error,
    ref_json    = ref_res$json,
    mine_json   = mine_res$json,
    identical   = identical_flag
  )
}

# ---- 3. Run the obstacle course ----

results <- imap(cases, ~ run_case(.x, .y)) %>% bind_rows()

results
# To see the problematic ones:
# results %>% filter(is.na(ref_error), is.na(mine_error), !identical)
# results %>% filter(!is.na(ref_error) | !is.na(mine_error))
