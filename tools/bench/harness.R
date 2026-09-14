# Shared timing harness.
#
# Takes the MINIMUM across repeated timing blocks rather than the mean or
# median. Background load on a desktop only ever makes a measurement slower,
# so the minimum is the least contaminated estimate of the code's own cost;
# means and medians drift with whatever else the machine is doing. An earlier
# A/B here showed +-18% run to run on medians, which was enough to invent a
# regression that did not exist.

# Put an untouched case first AND last in any A/B list. This machine drifts
# within a single run -- one measured pass had the control 13% faster at the
# end and the next had it 34% slower -- so a control at one end only is not
# enough to tell drift from an effect. Compare first-to-first.

# Times one call, auto-scaling the iteration count so each block runs for
# `block` seconds, and returns the fastest of `reps` blocks.
fgj_time <- function(f, block = 0.25, reps = 7L) {
  invisible(f())
  a <- bench::hires_time(); invisible(f())
  one <- max(bench::hires_time() - a, 1e-7)
  it <- max(1L, as.integer(ceiling(block / one)))
  # One collection before timing, not one per block. gc() costs 0.45 s on a
  # heap holding a million-row frame, which was about 40% of the benchmark's
  # wall clock -- and it buys nothing here: a block that happens to absorb a
  # collection is simply slower, and the minimum discards it.
  gc(FALSE)
  best <- Inf
  for (r in seq_len(reps)) {
    a <- bench::hires_time()
    for (i in seq_len(it)) invisible(f())
    best <- min(best, (bench::hires_time() - a) / it)
  }
  best
}

# Reports the fastest observed time alongside the spread, so a comparison that
# is inside the noise is visible as such.
fgj_time_spread <- function(f, block = 0.25, reps = 7L) {
  invisible(f())
  a <- bench::hires_time(); invisible(f())
  one <- max(bench::hires_time() - a, 1e-7)
  it <- max(1L, as.integer(ceiling(block / one)))
  gc(FALSE)
  ts <- numeric(reps)
  for (r in seq_len(reps)) {
    a <- bench::hires_time()
    for (i in seq_len(it)) invisible(f())
    ts[r] <- (bench::hires_time() - a) / it
  }
  c(min = min(ts), median = stats::median(ts), max = max(ts), iters = it)
}
