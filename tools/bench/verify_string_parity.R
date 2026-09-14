#!/usr/bin/env Rscript
# Character columns: escaping, encodings, and the boundary between the
# direct-read path and the translated arena path.
#
# The direct path trusts a descriptor built by a parallel prepass -- length,
# needs-escape, NA -- so an error there would silently emit wrong bytes or
# read past a string. These cases exercise every branch of it.
#
#   Rscript tools/bench/verify_string_parity.R

suppressMessages({library(fastgeojson); library(jsonlite)})

fails <- 0L
cmp <- function(x, label, ...) {
  want <- as.character(toJSON(x, ...))
  got  <- as.character(as_json(x, ...))
  if (identical(want, got)) {
    cat(sprintf("  PASS  %-50s\n", label))
  } else {
    fails <<- fails + 1L
    cat(sprintf("  FAIL  %-50s\n        R   : %s\n        rust: %s\n",
                label, substr(want, 1, 200), substr(got, 1, 200)))
  }
}

cat("== every escapable byte, alone and in company ==\n")
ctrl <- vapply(1:31, function(i) rawToChar(as.raw(c(65L, i, 66L))), "")
cmp(ctrl, "control bytes 1-31 wrapped in letters")
cmp(c('"', "\\", "\b", "\t", "\n", "\f", "\r", "/", "<", ">"), "the escape set, one char each")
cmp(c('a"b', "a\\b", "a\tb", "a\nb", "a/b", "a<b", "a<>b"), "escapes mid-string")
cmp(c('"lead', 'trail"', '"both"', ""), "escapes at the ends, and empty")

cat("\n== the solidus-after-< rule ==\n")
cmp(c("</script>", "<//>", "a</b", "</", "<", "/", "//", "x</script>y",
      "<<//", "</></>"), "< followed by /")
cmp(paste0(strrep("a", 1:12), "</script>"), "</script> at every offset 1-12")

cat("\n== escape position relative to the 8-byte scan stride ==\n")
for (n in c(0:20, 31, 32, 33, 63, 64, 65)) {
  v <- vapply(seq_len(max(n, 1)), function(i) {
    s <- strrep("a", n)
    if (n > 0) substr(s, i, i) <- '"'
    s
  }, "")
  want <- as.character(toJSON(v)); got <- as.character(as_json(v))
  if (!identical(want, got)) {
    fails <- fails + 1L
    cat(sprintf("  FAIL  quote at each position, length %d\n", n))
  }
}
cat("  PASS  a quote at every position, lengths 0-20, 31-33, 63-65\n")

cat("\n== lengths around the scan boundaries, clean strings ==\n")
cmp(vapply(c(0:20, 31:33, 63:65, 127:129, 255:257, 1023:1025), function(n) strrep("x", n), ""),
    "clean strings of many lengths")
cmp(strrep("x", c(1e4, 1e5)), "10k and 100k byte strings")

cat("\n== NA, mixed with everything ==\n")
cmp(c(NA, "a"), "NA first")
cmp(c("a", NA), "NA last")
cmp(c(NA_character_, NA_character_), "all NA")
cmp(c("a", NA, '"b"', NA, "c"), "NA interleaved with escapes")
for (na in c("null", "string")) cmp(c("a", NA, '"b'), sprintf("NA with na=%s", na), na = na)

cat("\n== encodings, and the fallback they force ==\n")
lat <- rawToChar(as.raw(c(0x63, 0x61, 0x66, 0xe9))); Encoding(lat) <- "latin1"
u8 <- "é中文"
cmp(lat, "a latin1 string")
cmp(u8, "a UTF-8 string")
cmp(c(lat, u8, "ascii"), "latin1 + UTF-8 + ascii in one vector")
cmp(c(rep("ascii", 50), lat), "one latin1 among 50 ascii (forces the arena)")
cmp(c(rep("ascii", 50), u8), "one UTF-8 among 50 ascii (stays direct)")
cmp(c(lat, NA, '"quoted"', u8), "latin1 + NA + escapes + UTF-8")
bytes <- rawToChar(as.raw(c(0x61, 0xff, 0x62))); Encoding(bytes) <- "bytes"
got <- tryCatch(as.character(as_json(bytes)), error = function(e) "ERROR")
cat(sprintf("  %s  a \"bytes\"-encoded string is refused rather than emitted: %s\n",
            if (got == "ERROR") "PASS" else "FAIL", got))
if (got != "ERROR") fails <- fails + 1L

cat("\n== data frames: all orientations, all na modes ==\n")
d <- data.frame(i = 1:6,
                s = c("plain", '"quoted"', NA, "", "</script>", "中文"),
                stringsAsFactors = FALSE)
for (dfm in c("rows", "columns", "values")) {
  cmp(d, sprintf("df %s, default na", dfm), dataframe = dfm)
  for (na in c("null", "string")) cmp(d, sprintf("df %s, na=%s", dfm, na), dataframe = dfm, na = na)
}
d2 <- data.frame(i = 1:2, s = c("a", "b"), stringsAsFactors = FALSE)
d2$f <- factor(c("x", 'y"z'))
cmp(d2, "character and factor columns together")

cat("\n== wide and tall, so the parallel prepass is really used ==\n")
set.seed(1)
big <- data.frame(a = sample(c("plain", '"q"', NA, "</s>", ""), 2e5, TRUE),
                  b = sample(c("x", "yy", "zzz"), 2e5, TRUE), stringsAsFactors = FALSE)
cmp(big, "200k rows x 2 character columns")
for (t in c(1, 2, 8, 0)) {
  fastgeojson_threads(t)
  if (!identical(as.character(as_json(big)), as.character(toJSON(big)))) {
    fails <- fails + 1L; cat(sprintf("  FAIL  200k rows at %d threads\n", t))
  }
}
fastgeojson_threads(0)
cat("  PASS  identical at 1, 2, 8 and automatic threads\n")

bigu <- big
bigu$a[1e5] <- lat
cmp(bigu, "200k rows with one latin1 cell (arena fallback)")

cat(sprintf("\n%d failure(s)\n", fails))
if (fails > 0) quit(status = 1)
