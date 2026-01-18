#' Fast JSON and GeoJSON serialization for R
#'
#' @description
#' `fastgeojson` provides a high-performance serialization backend for converting
#' R data structures into JSON strings. The core encoders are implemented
#' in Rust using the **extendr** framework and are designed to efficiently handle
#' large spatial datasets, tabular data, and generic R objects.
#'
#' **Recommended Usage:**
#' All users should use the [as_json()] function. It acts as a universal "omnivore"
#' that automatically detects the input type (`sf` object, data frame, list, or vector)
#' and dispatch it to the correct high-performance Rust encoder.
#'
#' The resulting JSON is returned as a character string with an appropriate
#' class (`"geojson"` / `"json"`), allowing it to be passed directly to client-side
#' JavaScript libraries or web frameworks (like Shiny or Plumber) without
#' additional serialization steps.
#'
#' @details
#' For sufficiently large inputs (specifically Data Frames and Spatial objects),
#' encoding is performed in parallel using multiple CPU cores via the Rust
#' **rayon** library.
#'
#' **Performance Note:**
#' There is **no material performance penalty** for using [as_json()] compared to the
#' specialized underlying functions. The internal dispatch mechanism has negligible
#' overhead. Users are strongly encouraged to use [as_json()] exclusively.
#'
#' @section Type handling:
#' \itemize{
#'   \item \strong{Numeric:} Written as JSON numbers. Infinite/NaN values are determined by the `na` argument.
#'   \item \strong{Logical:} Written as JSON booleans.
#'   \item \strong{Character:} Written as JSON strings with UTF-8 escaping.
#'   \item \strong{Matrix:} Serialized row-major as an array of arrays.
#'   \item \strong{Factor:} Encoded using their character levels.
#' }
#'
#' @section Missing Value (NA) Handling:
#' `fastgeojson` employs "Smart" defaults to match standard R JSON conventions
#' (specifically `jsonlite`), while offering strict overrides via the `na` argument:
#'
#' \itemize{
#'   \item **Default (Smart):**
#'     \itemize{
#'       \item In \strong{Column Mode} (`dataframe="columns"`), numeric `NA`s are converted
#'         to `"NA"` strings to maintain array type homogeneity. Non-numeric `NA`s become `null`.
#'       \item In \strong{Row Mode} (`dataframe="rows"`), `NA` values are usually **omitted**
#'         from the object to reduce payload size.
#'     }
#'   \item **Explicit (`na="null"`):** Forces all `NA` values (numeric or otherwise)
#'     to be serialized as JSON `null`.
#'   \item **Explicit (`na="string"`):** Forces all `NA` values to be serialized as `"NA"`.
#' }
#'
#' @param x An input object (e.g., a data.frame, sf object, list, or vector) to serialize.
#' @param auto_unbox Logical. If `TRUE`, atomic vectors of length 1 are
#'   automatically unboxed into scalar JSON values (e.g., `[1]` becomes `1`).
#'   If `FALSE` (default), they remain as single-element arrays (e.g., `[1]`).
#' @param dataframe Character. Defines the output structure for data frames:
#'   `"rows"` (default) output as an array of objects (`[{...}]`),
#'   `"columns"` output as an object of arrays (`{...}`).
#' @param na Character. Controls how `NA` values are serialized:
#'   \itemize{
#'     \item **Default:** Uses "smart" logic (see Type Handling section).
#'     \item **"null":** Forces all `NA` values to be serialized as JSON `null`.
#'     \item **"string":** Forces all `NA` values to be serialized as `"NA"`.
#'   }
#' @param null Character. Controls how `NULL` values (in lists) are serialized:
#'   `"list"` (default) maps to `[]` (empty array) or `{}` (empty object) depending on context.
#'   `"null"` maps to JSON `null`.
#'
#' @return
#' A length-one character vector containing the JSON string with class `"json"`.
#' If the input is an `sf` object, the class is `c("geojson", "json")`.
#'
#' @seealso
#' [as_json()] - The primary function for all serialization tasks.
#'
#' @name fastgeojson
#' @aliases as_json sf_geojson_str df_json_str
#'
#' @examples
#' # 1. Generic Objects
#' as_json(list(a = 1, b = "foo", c = NA))
#'
#' # 2. Auto-unbox
#' as_json(list(val = 5), auto_unbox = TRUE)  # {"val":5}
#' as_json(list(val = 5), auto_unbox = FALSE) # {"val":[5]}
#'
#' # 3. Data Frames (Row vs Column orientation)
#' df <- data.frame(x = 1:2, y = c("a", NA))
#' as_json(df, dataframe = "rows")    # [{"x":1,"y":"a"},{"x":2}]  (NA omitted)
#' as_json(df, dataframe = "columns") # {"x":[1,2],"y":["a",null]}
#'
#' # 4. Controlling NA serialization
#' # Force NA to null in all contexts
#' as_json(c(1, NA, 3), na = "null") # [1, null, 3]
#' # Force NA to string
#' as_json(c(1, NA, 3), na = "string") # [1, "NA", 3]
#'
#' # 5. Spatial Data (sf)
#' if (requireNamespace("sf", quietly = TRUE)) {
#'      nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#'      # automatically detects sf and outputs GeoJSON FeatureCollection
#'      geo_str <- as_json(nc[1:3, ])
#' }
#'
#' @useDynLib fastgeojson, .registration = TRUE
NULL

#' @rdname fastgeojson
#' @export
as_json <- function(x, auto_unbox = FALSE, dataframe = c("rows", "columns"), 
                    na = NULL, null = c("list", "null")) {
  
  # 1. Handle NULL Input
  if (is.null(x)) {
    null <- match.arg(null)
    val <- if (null == "list") "{}" else "null"
    return(structure(val, class = "json"))
  }
  
  # 2. Optimized Dispatch
  if (inherits(x, "sf")) {
    if (is.null(na)) na <- "null"
    return(sf_geojson_str(x, auto_unbox = auto_unbox, na = na, null = null))
  }
  if (inherits(x, "data.frame")) {
    dataframe <- match.arg(dataframe)
    # SMART DEFAULT: jsonlite defaults to "string" for columns, "null" for rows
    if (is.null(na)) {
      na <- if (dataframe == "columns") "smart" else "null"
    } else {
      na <- match.arg(na, c("null", "string"))
    }
    return(df_json_str(x, auto_unbox = auto_unbox, dataframe = dataframe, na = na, null = null))
  }
  
  # 3. Fallback to Generic
  if (is.null(na)) na <- "null"
  null <- match.arg(null)
  obj_json_str_impl(x, auto_unbox, na, null)
}

#' @rdname fastgeojson
#' @export
sf_geojson_str <- function(x, auto_unbox = FALSE, na = c("null", "string"), null = c("list", "null")) {
  # Handle internal calls passing "smart" directly
  if (length(na) == 1 && na == "smart") {
    na_arg <- "smart"
  } else if (missing(na)) {
    na_arg <- "smart"
  } else {
    na_arg <- match.arg(na)
  }
  
  null_arg <- match.arg(null)
  
  if (is.null(x)) return(structure("[]", class = c("geojson", "json")))
  if (!inherits(x, "sf")) stop("Not an sf object", call. = FALSE)
  
  sf_geojson_str_impl(x, auto_unbox, na_arg, null_arg)
}

#' @rdname fastgeojson
#' @export
df_json_str <- function(x, auto_unbox = FALSE, dataframe = c("rows", "columns"), 
                        na = c("null", "string"), null = c("list", "null")) {
  dataframe <- match.arg(dataframe)
  
  # Handle internal calls passing "smart" directly
  if (length(na) == 1 && na == "smart") {
    na_arg <- "smart"
  } else if (missing(na)) {
    na_arg <- "smart"
  } else {
    na_arg <- match.arg(na)
  }
  
  null_arg <- match.arg(null)
  
  if (is.null(x)) return(structure("[]", class = "json"))
  if (!inherits(x, "data.frame")) stop("Not a dataframe object", call. = FALSE)
  
  if (nrow(x) == 0L && dataframe == "rows") {
    return(structure("[]", class = "json"))
  }
  
  df_json_str_impl(x, auto_unbox, dataframe, na_arg, null_arg)
}