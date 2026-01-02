#' Fast JSON and GeoJSON serialization for R
#'
#' @description
#' `fastgeojson` provides a high-performance serialization backend for converting
#' common R data structures into JSON strings. The core encoders are implemented
#' in Rust using the **extendr** framework and are designed to efficiently handle
#' large spatial and tabular datasets.
#'
#' The package focuses on two primary use cases:
#' \itemize{
#'   \item Converting `sf` objects into GeoJSON FeatureCollections.
#'   \item Converting rectangular `data.frame` objects into JSON arrays.
#' }
#'
#' The resulting JSON is returned as a character string with an appropriate
#' class (`"geojson"` / `"json"`), allowing it to be passed directly to client-side
#' JavaScript libraries or web frameworks without additional serialization steps.
#'
#' @details
#' For sufficiently large inputs, encoding may be performed in parallel using
#' multiple CPU cores via the Rust **rayon** library. Parallel execution is
#' enabled automatically based on input size and geometry type.
#'
#' By returning pre-serialized JSON strings, these functions allow frameworks
#' such as **Shiny** and **Plumber** to avoid redundant re-encoding during data
#' transfer, which can significantly reduce server-side overhead in interactive
#' or high-throughput applications.
#'
#' @section Type handling:
#' \itemize{
#'   \item \strong{Numeric:} Written as JSON numbers.
#'   \item \strong{Logical:} Written as JSON booleans.
#'   \item \strong{Character:} Written as JSON strings with UTF-8 escaping.
#'   \item \strong{Factor:} Encoded using their character levels.
#'   \item \strong{Missing values:} Missing (`NA`) values may be omitted from
#'     output objects to reduce payload size.
#'   \item \strong{Geometries:} Supports common `sf` geometry types including
#'     POINT, LINESTRING, POLYGON, and their MULTI variants.
#' }
#'
#' @param x An input object (e.g., a data.frame or sf object) to serialize.
#'
#' @seealso
#' [sf_geojson_str()] for spatial data,
#' [df_json_str()] for tabular data.
#'
#' @name fastgeojson
#' @aliases sf_geojson_str df_json_str
#'
#' @examples
#' if (requireNamespace("sf", quietly = TRUE)) {
#'    nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
#'    geo_str <- sf_geojson_str(nc)
#' }
#'
#' df <- data.frame(x = runif(10), y = runif(10))
#' json_str <- df_json_str(df)
#'
#' @useDynLib fastgeojson, .registration = TRUE
NULL


#' @rdname fastgeojson
#' @export
sf_geojson_str <- function(x) {
  if (is.null(x)) return(structure("[]", class = c("geojson", "json")))
  if (!inherits(x, "sf")) stop("Not an sf object", call. = FALSE)
  if (is.null(names(x))) stop("Not a valid sf object (no names)", call. = FALSE)

  sfcol <- attr(x, "sf_column", exact = TRUE)
  if (is.null(sfcol) || !is.character(sfcol) || length(sfcol) < 1L || !nzchar(sfcol[1])) {
    stop("Not a valid sf object (missing 'sf_column' attribute)", call. = FALSE)
  }

  sf_geojson_str_impl(x)
}

#' @rdname fastgeojson
#' @export
df_json_str <- function(x) {
  if (is.null(x)) return(structure("[]", class = "json"))
  if (!inherits(x, "data.frame")) stop("Not a dataframe object", call. = FALSE)
  if (is.null(names(x))) stop("Not a dataframe object (no names)", call. = FALSE)

  if (ncol(x) == 0L || nrow(x) == 0L) return(structure("[]", class = "json"))

  lens <- lengths(x)
  if (any(lens != lens[1L])) {
    stop(sprintf("Not a valid dataframe (ragged columns)"), call. = FALSE)
  }

  df_json_str_impl(x)
}
