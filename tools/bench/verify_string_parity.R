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
cmp <- function(x, label, ..., .quiet = FALSE) {
  # Compared and reported as BYTES. Some of the encoding cases below produce
  # strings R cannot print or substr() in this locale, and reporting them as
  # text made the verifier itself the thing that failed.
  raw_of <- function(e) tryCatch(charToRaw(as.character(e())),
                                 error = function(c) charToRaw(paste("ERR:", conditionMessage(c))))
  want <- raw_of(function() toJSON(x, ...))
  got  <- raw_of(function() as_json(x, ...))
  if (identical(want, got)) {
    if (!.quiet) cat(sprintf("  PASS  %-50s\n", label))
  } else {
    fails <<- fails + 1L
    n <- min(length(want), length(got))
    i <- which(want[seq_len(n)] != got[seq_len(n)])[1]
    cat(sprintf("  FAIL  %-50s\n        first differing byte: %s\n        R   : %s\n        rust: %s\n",
                label,
                if (is.na(i)) sprintf("(length %d vs %d)", length(want), length(got)) else as.character(i),
                paste(format(utils::head(want, 60)), collapse = " "),
                paste(format(utils::head(got, 60)), collapse = " ")))
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

cat("\n== the json class: spliced only when json_verbatim says so ==\n")
# `json_verbatim = FALSE` is the default, and it exists so that a string
# carrying the `json` class is escaped like the string it is rather than
# spliced into the document. That was honoured in R, and so only for the
# outermost object: a json value nested anywhere was spliced whatever the
# setting, which lets a string decide the shape of the document around it.
#
# The injection case below is the point of the argument. With the class
# applied to "1,\"injected\":true", splicing turns {"v":[...]} into an object
# with a second key.
jv <- structure('{"a":1}', class = "json")
inj <- structure('1,"injected":true', class = "json")
jdf <- data.frame(i = 1L); jdf$j <- jv
for (v in c(FALSE, TRUE)) {
  for (dfm in c("rows", "columns", "values")) {
    cmp(jdf, sprintf("json column, %s, json_verbatim = %s", dfm, v),
        json_verbatim = v, dataframe = dfm)
    cmp(list(z = jdf), sprintf("json column nested, %s, json_verbatim = %s", dfm, v),
        json_verbatim = v, dataframe = dfm)
    cmp(list(x = jv), sprintf("json in a list, %s, json_verbatim = %s", dfm, v),
        json_verbatim = v, dataframe = dfm)
    cmp(list(v = inj), sprintf("injection, %s, json_verbatim = %s", dfm, v),
        json_verbatim = v, dataframe = dfm)
  }
  cmp(jv, sprintf("json at top level, json_verbatim = %s", v), json_verbatim = v)
  cmp(list(a = list(b = jv)), sprintf("json two deep, json_verbatim = %s", v),
      json_verbatim = v)
}
cmp(jv, "json at top level, default")
cmp(list(x = jv), "json in a list, default")
cmp(list(v = inj), "injection, default")
cmp(list(a = list(b = inj)), "injection two deep, default")

cat("\n== our own pre-rendered JSON is always spliced ==\n")
# .prep() renders mongo dates, mongo binaries, complex rows and raw = "js"
# itself and marks them so the writer splices them. They used to carry the
# `json` class too, so turning the user's splicing off turned ours off with it
# and a mongo timestamp came out quoted -- 21 of jsonlite's own tests.
ts <- as.POSIXct(c("2020-01-01 10:00:00", NA), tz = "UTC")
rw <- as.raw(c(98, 108, 97))
cx <- complex(real = c(1, NA), imaginary = c(2, 3))
for (v in c(FALSE, TRUE)) {
  cmp(ts, sprintf("POSIXt = mongo, json_verbatim = %s", v), POSIXt = "mongo", json_verbatim = v)
  cmp(data.frame(t = ts), sprintf("mongo in a frame, json_verbatim = %s", v),
      POSIXt = "mongo", json_verbatim = v)
  cmp(list(t = ts), sprintf("mongo nested, json_verbatim = %s", v),
      POSIXt = "mongo", json_verbatim = v)
  cmp(rw, sprintf("raw = mongo, json_verbatim = %s", v), raw = "mongo", json_verbatim = v)
  cmp(list(r = rw), sprintf("raw = js, json_verbatim = %s", v), raw = "js", json_verbatim = v)
  cmp(data.frame(c = cx), sprintf("complex = list, json_verbatim = %s", v),
      complex = "list", json_verbatim = v)
}


cat("\n== encodings: which cells the workers can read for themselves ==\n")
# A character cell is escaped straight out of R's CHARSXP by the workers
# unless it needs Rf_translateCharUTF8, which allocates on R's vmax stack and
# so has to run on the R thread. Two things decide that, and both were decided
# wrongly.
#
# 1. The test was `Rf_getCharCE(cs) == CE_UTF8`. readLines() and rawToChar()
#    return CE_NATIVE strings whose bytes are already UTF-8 in a UTF-8 locale,
#    so every one of them took the serial path. Rf_charIsUTF8 is the right
#    question and R 4.5.0 added it for exactly this.
#
# 2. latin1 went to R one cell at a time, at 815 ms per 300,000 values. A byte
#    from 0xA0 up is its own code point, so the workers widen those
#    themselves. 0x80..0x9F cannot be done here: R renders it as CP1252 on
#    Windows and a strict ISO-8859-1 iconv renders it as the C1 controls, 27
#    of the 256 bytes disagreeing, so those cells still go to R and get the
#    platform's own answer.
#
# Every single byte is checked, because a conversion table baked into the fast
# path would be wrong on whichever platform it did not match, and checking one
# accented letter would never have found it.
enc_as <- function(v, e) { x <- v; Encoding(x) <- rep(e, length(x)); x }
byte_str <- function(b) rawToChar(as.raw(b))

n_before <- fails
for (b in 1:255) {
  cmp(data.frame(x = enc_as(byte_str(b), "latin1"), stringsAsFactors = FALSE),
      sprintf("latin1 byte %d", b), .quiet = TRUE)
}
cat(sprintf("  %-5s %-50s\n", if (fails == n_before) "PASS" else "FAIL",
            "all 255 latin1 bytes, one at a time"))

allb <- enc_as(vapply(1:255, byte_str, ""), "latin1")
cmp(data.frame(x = allb, stringsAsFactors = FALSE), "latin1, all 255 in one column")
cmp(data.frame(x = enc_as(rawToChar(as.raw(1:255)), "latin1"), stringsAsFactors = FALSE),
    "latin1, all 255 in one string")
cmp(data.frame(x = enc_as(rawToChar(as.raw(160:255)), "latin1"), stringsAsFactors = FALSE),
    "latin1, only the unambiguous range")
cmp(data.frame(x = enc_as(rawToChar(as.raw(128:159)), "latin1"), stringsAsFactors = FALSE),
    "latin1, only the ambiguous range")
cmp(data.frame(x = enc_as(rawToChar(as.raw(c(200, 130, 201))), "latin1"), stringsAsFactors = FALSE),
    "latin1, one ambiguous byte among safe ones")
cmp(data.frame(f = factor(allb)), "latin1 as a factor")
cmp(list(s = allb), "latin1 in a list")
cmp(setNames(as.list(1:3), enc_as(c("\xe9a", "\xfcb", "\xffc"), "latin1")), "latin1 names")
for (dfm in c("rows", "columns", "values")) {
  cmp(data.frame(x = allb, stringsAsFactors = FALSE),
      paste("latin1, all bytes,", dfm), dataframe = dfm)
}
# High bytes next to every escape, including either side of the '<' '/' pair,
# which is the one escaping rule that looks at more than one byte.
tricky <- enc_as(c("<\xe9/", "\xe9</", "</\xe9", "a\\\xe9\"b", "\xe9\n\xe9",
                   "<\xe9/x", "\xe9", "\xff\xfe", "\xa0/", "<\xa0"), "latin1")
for (i in seq_along(tricky)) {
  cmp(data.frame(x = tricky[i], stringsAsFactors = FALSE),
      sprintf("latin1 escape adjacency %d", i))
}
cmp(data.frame(x = tricky, stringsAsFactors = FALSE), "latin1 escape adjacency, one column")

# The native-encoding case, which is what readLines() hands back.
vals <- c("café", "naïve", "Zürich", "日本語", "plain")
for (e in c("unknown", "UTF-8")) {
  cmp(data.frame(x = enc_as(vals, e), stringsAsFactors = FALSE), paste("non-ASCII marked", e))
  cmp(enc_as(vals, e), paste("non-ASCII vector marked", e))
  cmp(list(s = enc_as(vals, e)), paste("non-ASCII list marked", e))
  cmp(data.frame(f = factor(enc_as(vals, e))), paste("non-ASCII factor marked", e))
}
tf <- tempfile(); writeLines(vals, tf); rl <- readLines(tf); unlink(tf)
cmp(rl, "readLines output")
cmp(data.frame(x = rl, stringsAsFactors = FALSE), "readLines in a frame")
mixed <- c("plain", enc_as("café", "latin1"), enc_as("naïve", "unknown"), "more")
cmp(data.frame(x = mixed, stringsAsFactors = FALSE), "three encodings in one column")

cat(sprintf("\n%d failure(s)\n", fails))
if (fails > 0) quit(status = 1)
