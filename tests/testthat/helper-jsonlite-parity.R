# ---------------------------------------------------------------------------
# jsonlite parity harness
#
# The test-jsonlite-*.R files in this directory are jsonlite's own toJSON test
# suite, taken verbatim from jsonlite 2.0.0 (tests/testthat/test-toJSON-*.R and
# test-libjson-{escaping,utf8}.R). jsonlite is Copyright (c) Jeroen Ooms and
# released under the MIT licence; see LICENSE.note.
#
# jsonlite's own helper defines toJSON() as a thin unclass() wrapper around
# jsonlite::toJSON. We keep exactly that shape but point it at as_json(), so
# every expectation jsonlite makes about its own output becomes a drop-in
# parity assertion against ours.
#
# To refresh against a newer jsonlite:
#   download the source tarball, extract tests/testthat/test-toJSON-*.R plus
#   test-libjson-escaping.R and test-libjson-utf8.R into this directory with a
#   test-jsonlite- prefix, and leave this helper alone.
# ---------------------------------------------------------------------------

# The ported files use jsonlite for the non-toJSON side of round-trips, and a
# few need sf. Skip the whole file rather than error if either is absent.
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  testthat::skip("jsonlite is required for the parity suite")
}

fromJSON        <- jsonlite::fromJSON
parse_json      <- jsonlite::parse_json
read_json       <- jsonlite::read_json
write_json      <- jsonlite::write_json
minify          <- jsonlite::minify
prettify        <- jsonlite::prettify
validate        <- jsonlite::validate
serializeJSON   <- jsonlite::serializeJSON
unserializeJSON <- jsonlite::unserializeJSON
unbox           <- jsonlite::unbox
base64_enc      <- jsonlite::base64_enc
base64_dec      <- jsonlite::base64_dec

# The system under test.
#
# Arguments fastgeojson deliberately does not implement raise an error whose
# message ends in "is not yet implemented in fastgeojson". Those become skips,
# so the gap stays visible in the test report without masking a real
# regression; implementing the feature makes the skip disappear on its own.
# Messages must propagate untouched: keep_vec_names emits jsonlite's
# deprecation message and the ported tests assert it with expect_message().
# as_json() writes numbers losslessly by default where toJSON() rounds to 4
# decimal places. These files assert jsonlite's own expectations, so the
# shim supplies jsonlite's default whenever a test does not name `digits`.
toJSON <- function(..., digits = 4) {
  tryCatch(
    unclass(fastgeojson::as_json(..., digits = digits)),
    error = function(e) {
      msg <- conditionMessage(e)
      if (grepl("not yet implemented in fastgeojson", msg, fixed = TRUE)) {
        testthat::skip(msg)
      }
      stop(e)
    }
  )
}

toJSON2 <- function(x) {
  toJSON(x, keep_vec_names = TRUE, auto_unbox = TRUE)
}

toJSON3 <- function(x) {
  toJSON(x, keep_vec_names = TRUE, auto_unbox = TRUE, dataframe = "columns", rownames = FALSE)
}
