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
#' overhead (nanoseconds). Users are strongly encouraged to use [as_json()] exclusively
#' rather than calling `sf_geojson_str()` or `df_json_str()` directly.
#'
#'
#' @section Type handling:
#' \itemize{
#'   \item \strong{Numeric:} Written as JSON numbers. Infinite/NaN values are written as strings (`"Inf"`, `"NaN"`).
#'   \item \strong{Logical:} Written as JSON booleans.
#'   \item \strong{Character:} Written as JSON strings with UTF-8 escaping.
#'   \item \strong{Matrix:} Serialized row-major as an array of arrays.
#'   \item \strong{Factor:} Encoded using their character levels.
#'   \item \strong{Missing values (Data Frames):} `NA` fields are **omitted** from
#'     the JSON object to reduce payload size.
#'   \item \strong{Missing values (Vectors):} `NA` values are converted to
#'     `"NA"` strings. This ensures arrays remain fixed-length and avoids mixing
#'     types (e.g., numbers mixed with nulls) in strict environments.
#' }
#'
#' @param x An input object (e.g., a data.frame, sf object, list, or vector) to serialize.
#' @param auto_unbox Logical. If `TRUE`, atomic vectors of length 1 are 
#'   automatically unboxed into scalar JSON values (e.g., `[1]` becomes `1`). 
#'   If `FALSE` (default), they remain as single-element arrays (e.g., `[1]`).
#'
#' @return
#' \itemize{
#'   \item `as_json()`: Returns a length-one character vector containing the JSON string.
#'     If the input is an `sf` object, the result has class `c("geojson", "json")`.
#'     Otherwise, it has class `"json"`.
#'   \item `sf_geojson_str()`: Returns a length-one character vector with class
#'     `c("geojson", "json")` containing a 'GeoJSON' FeatureCollection string.
#'   \item `df_json_str()`: Returns a length-one character vector with class `"json"`
#'     containing a 'JSON' array string.
#' }
#'
#' @seealso
#' [as_json()] - The primary function for all serialization tasks.
#'
#' \strong{Deprecated:} The functions [sf_geojson_str()] and [df_json_str()] are
#' maintained for backward compatibility but may be removed in future updates.
#' Users should migrate to [as_json()], which offers identical performance
#' with a unified API.
#'
#' @name fastgeojson
#' @aliases as_json sf_geojson_str df_json_str
#'
#' @examples
#' # 1. Generic Objects (Vectors, Lists, Matrices)
#' as_json(list(a = 1, b = "foo", c = NA))
#' 
#' # Auto-unbox example
#' as_json(list(val = 5), auto_unbox = TRUE)  # {"val":5}
#' as_json(list(val = 5), auto_unbox = FALSE) # {"val":[5]}
#'
#' # 2. Spatial Data (sf) - Automatically detects and outputs GeoJSON
#' if (requireNamespace("sf", quietly = TRUE)) {
#'      nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#'      geo_str <- as_json(nc)
#' }
#'
#' # 3. Tabular Data (data.frame)
#' df <- data.frame(x = runif(5), y = letters[1:5])
#' json_str <- as_json(df)
#'
#' @useDynLib fastgeojson, .registration = TRUE
NULL


#' @rdname fastgeojson
#' @export
as_json <- function(x, auto_unbox = FALSE) {
  # 1. Handle NULL (jsonlite returns {})
  if (is.null(x)) return(structure("{}", class = "json"))
  
  # 2. Optimized Dispatch
  # If it's a known special type, route to the parallelized implementations
  # NOTE: passing auto_unbox to the specific functions
  if (inherits(x, "sf")) return(sf_geojson_str(x, auto_unbox = auto_unbox))
  if (inherits(x, "data.frame")) return(df_json_str(x, auto_unbox = auto_unbox))
  
  # 3. Fallback to Generic
  # Handles vectors, lists, matrices, scalars
  obj_json_str_impl(x, auto_unbox)
}

#' @rdname fastgeojson
#' @export
sf_geojson_str <- function(x, auto_unbox = FALSE) {
  # Handle NULL inputs gracefully
  if (is.null(x)) return(structure("[]", class = c("geojson", "json")))
  
  # Basic Type Validation
  if (!inherits(x, "sf")) stop("Not an sf object", call. = FALSE)
  if (is.null(names(x))) stop("Not a valid sf object (no names)", call. = FALSE)
  
  # Validate sf_column attribute existence
  sfcol <- attr(x, "sf_column", exact = TRUE)
  if (is.null(sfcol) || !is.character(sfcol) || length(sfcol) < 1L || !nzchar(sfcol[1])) {
    stop("Not a valid sf object (missing 'sf_column' attribute)", call. = FALSE)
  }
  
  sf_geojson_str_impl(x, auto_unbox)
}

#' @rdname fastgeojson
#' @export
df_json_str <- function(x, auto_unbox = FALSE) {
  # Handle NULL inputs gracefully
  if (is.null(x)) return(structure("[]", class = "json"))
  
  # Basic Type Validation
  if (!inherits(x, "data.frame")) stop("Not a dataframe object", call. = FALSE)
  if (is.null(names(x))) stop("Not a dataframe object (no names)", call. = FALSE)
  
  # Short-circuit ONLY if there are no rows.
  if (nrow(x) == 0L) return(structure("[]", class = "json"))
  
  df_json_str_impl(x, auto_unbox)
}