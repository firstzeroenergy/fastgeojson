#!/usr/bin/env Rscript
# Regenerates README.md from README.Rmd.
#
#   Rscript tools/build_readme.R           # write README.md
#   Rscript tools/build_readme.R --check   # exit 1 if README.md is stale
#
# Uses knitr directly rather than rmarkdown::render(), so no pandoc is needed
# and the result is reproducible anywhere the package is installed. The
# benchmark tables are built from tools/bench/readme_numbers.csv, which
# tools/bench/readme_bench.R writes, so the figures in README.md are always
# the ones that were last measured.

src <- "README.Rmd"
dst <- "README.md"
check <- "--check" %in% commandArgs(TRUE)

if (!file.exists(src)) stop(src, " not found; run from the package root")

tmp <- tempfile(fileext = ".md")
on.exit(unlink(tmp), add = TRUE)
invisible(knitr::knit(src, tmp, quiet = TRUE))

lines <- readLines(tmp, warn = FALSE, encoding = "UTF-8")

# Drop the YAML front matter: it is knitr input, not README content.
rule <- trimws(lines) == "---"
if (length(rule) && isTRUE(rule[1])) {
  close_at <- which(rule)[2]
  if (!is.na(close_at)) lines <- lines[-seq_len(close_at)]
}

# Collapse the blank-line runs left behind by removed chunks, and trim ends.
blank <- !nzchar(trimws(lines))
lines <- lines[!(blank & c(FALSE, utils::head(blank, -1)))]
while (length(lines) && !nzchar(trimws(lines[1]))) lines <- lines[-1]
while (length(lines) && !nzchar(trimws(lines[length(lines)]))) lines <- lines[-length(lines)]

out <- c(
  paste0("<!-- Generated from ", src, " by tools/build_readme.R. Do not edit by hand. -->"),
  "",
  lines,
  ""
)

if (check) {
  cur <- if (file.exists(dst)) readLines(dst, warn = FALSE, encoding = "UTF-8") else character()
  if (identical(cur, out)) {
    cat("README.md is up to date\n")
  } else {
    n <- which(c(cur, rep(NA, max(0, length(out) - length(cur)))) != out)[1]
    cat(sprintf("README.md is STALE (first difference at line %s) - run: Rscript tools/build_readme.R\n",
                if (is.na(n)) "end" else n))
    quit(status = 1)
  }
} else {
  con <- file(dst, open = "wb")
  writeLines(enc2utf8(out), con, useBytes = TRUE)
  close(con)
  cat(sprintf("wrote %s (%d lines)\n", dst, length(out)))
}
