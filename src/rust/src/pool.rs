// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// THREAD POOL
// ------------------------------------------------------------------
// rayon's *global* pool is created lazily with one worker per logical CPU and
// can never be resized afterwards. That had three consequences: a three-row
// data frame spawned 32 OS threads on this machine (pure overhead), there was
// no way for a caller to ask for fewer, and `R CMD check` would run the
// examples at full width.
//
// Note on CRAN: the two-core limit in the CRAN Repository Policy applies to
// *checking* (examples, tests and vignettes on the shared check farm), not to
// what a package may do when a user runs it. So the default here is the full
// machine, and we throttle to two only when we can see we are being checked.

/// 0 means "decide from the environment".
pub(crate) static REQUESTED_THREADS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(crate) static POOL: std::sync::Mutex<Option<(usize, std::sync::Arc<rayon::ThreadPool>)>> =
    std::sync::Mutex::new(None);

/// Are we running under `R CMD check`?
///
/// `R CMD check` exports a family of `_R_CHECK_*` variables; their presence is
/// the signal used by other parallel CRAN packages (data.table throttles on
/// `_R_CHECK_LIMIT_CORES_` the same way). Checking the whole prefix rather than
/// one name means we still throttle on flavours that do not set that
/// particular variable.
pub(crate) fn under_r_check() -> bool {
    if let Ok(v) = std::env::var("_R_CHECK_LIMIT_CORES_") {
        let v = v.trim().to_ascii_lowercase();
        if !v.is_empty() && v != "false" && v != "0" && v != "no" {
            return true;
        }
    }
    std::env::vars_os().any(|(k, _)| {
        k.to_str().map(|s| s.starts_with("_R_CHECK_")).unwrap_or(false)
    })
}

/// How many workers to use for the next parallel region.
///
/// Precedence: an explicit `fastgeojson_threads(n)`, then the usual
/// thread-count environment variables, then two if we are being checked, then
/// the whole machine. Normal user code therefore gets full parallelism.
pub(crate) fn desired_threads() -> usize {
    let req = REQUESTED_THREADS.load(std::sync::atomic::Ordering::Relaxed);
    if req > 0 {
        return req;
    }
    for key in [
        "FASTGEOJSON_NUM_THREADS",
        "RAYON_NUM_THREADS",
        "OMP_NUM_THREADS",
        "OMP_THREAD_LIMIT",
    ] {
        if let Ok(v) = std::env::var(key) {
            if let Ok(n) = v.trim().parse::<usize>() {
                if n >= 1 {
                    return n;
                }
            }
        }
    }
    if under_r_check() {
        return 2;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// How many rows each parallel chunk should cover.
///
/// Chunking by raw row count left entire shapes single-threaded: a
/// 2000-row x 200-column frame and a 1000-feature polygon layer each landed in
/// exactly one chunk and measured 1.00x scaling, despite being the two slowest
/// shapes in the benchmark. What matters is total *work*, so callers pass an
/// estimate of the per-row cost (columns for a data.frame, coordinates for a
/// geometry) and this balances three things: stay serial when the whole job is
/// too small to be worth a pool, otherwise give every worker several chunks so
/// one straggler cannot stall the join, and never make a chunk so small that
/// scheduling dominates.
pub(crate) fn rows_per_chunk(n_rows: usize, work_per_row: usize) -> usize {
    if n_rows <= 1 {
        return n_rows.max(1);
    }
    let threads = desired_threads();
    if threads <= 1 {
        return n_rows;
    }
    let w = work_per_row.max(1);
    let total = n_rows.saturating_mul(w);

    // Below this the pool costs more than it saves.
    const MIN_PARALLEL_WORK: usize = 32_768;
    if total < MIN_PARALLEL_WORK {
        return n_rows;
    }
    // Roughly one number's worth of work; smaller chunks are scheduling noise.
    const MIN_CHUNK_WORK: usize = 8_192;

    let by_work = total / MIN_CHUNK_WORK;
    let chunks = by_work.max(threads).min(threads.saturating_mul(4)).max(1);
    ((n_rows + chunks - 1) / chunks).max(1)
}

/// Per-row work estimate for a set of prepared columns.
///
/// Counting one unit per column made a 10000-row frame of 200-byte strings
/// score below the parallel threshold and run serially at 1.01x scaling, even
/// though each row is two orders of magnitude more work than a numeric one.
/// String columns are sampled rather than measured, so this stays O(1) in the
/// column length.
pub(crate) fn estimate_row_work(props: &[(Vec<u8>, ThreadSafeColumn)]) -> usize {
    let mut w = 0usize;
    for (key, col) in props {
        w += 1 + key.len() / 8;
        w += match col.kind {
            ColumnType::CharDirect => unsafe {
                let base = col.data_ptr as *const libR_sys::SEXP;
                let n = col.len;
                if n == 0 {
                    1
                } else {
                    let sample = n.min(16);
                    let step = (n / sample).max(1);
                    let mut total = 0usize;
                    let mut seen = 0usize;
                    let mut i = 0usize;
                    while i < n && seen < sample {
                        let cs = *base.add(i);
                        if !is_na_string(cs) {
                            total += libR_sys::Rf_xlength(cs).max(0) as usize;
                        }
                        seen += 1;
                        i += step;
                    }
                    ((total / seen.max(1)) / 4).max(1)
                }
            },
            ColumnType::Char | ColumnType::JsonRaw => match col.string_arena {
                Some(ref a) if !a.offsets.is_empty() => {
                    ((a.bytes.len() / a.offsets.len()) / 4).max(1)
                }
                _ => 1,
            },
            _ => 1,
        };
    }
    w.max(1)
}

/// Cheap per-feature work estimate for a geometry column.
///
/// Samples a handful of features rather than walking the column, so the cost
/// is independent of length. Only the magnitude matters.
pub(crate) unsafe fn estimate_geom_work(geom_col: libR_sys::SEXP, n: usize) -> usize {
    unsafe fn geom_size(sfg: libR_sys::SEXP, depth: u32) -> usize {
        if depth > 3 {
            return 1;
        }
        let t = typeof_sexp(sfg);
        if t == libR_sys::SEXPTYPE::REALSXP as u32 {
            return sexp_len(sfg).max(1);
        }
        if t == libR_sys::SEXPTYPE::VECSXP as u32 {
            let len = sexp_len(sfg);
            let mut s = 0usize;
            for j in 0..len.min(32) {
                s += geom_size(libR_sys::VECTOR_ELT(sfg, j as isize), depth + 1);
            }
            // Scale the sample back up if the list was longer than we looked at.
            if len > 32 {
                s = s.saturating_mul(len) / 32;
            }
            return s.max(1);
        }
        1
    }

    if n == 0 {
        return 1;
    }
    let sample = n.min(16);
    let step = (n / sample).max(1);
    let mut total = 0usize;
    let mut seen = 0usize;
    let mut i = 0usize;
    while i < n && seen < sample {
        total += geom_size(libR_sys::VECTOR_ELT(geom_col, i as isize), 0);
        seen += 1;
        i += step;
    }
    (total / seen.max(1)).max(1)
}

/// Runs `f` on our own pool, rebuilding it if the requested width changed.
///
/// Single-threaded requests skip rayon entirely, which is also what makes
/// `fastgeojson_threads(1)` a usable baseline when separating algorithmic
/// gains from parallel ones.
pub(crate) fn with_pool<R: Send>(f: impl FnOnce() -> R + Send) -> R {
    let want = desired_threads();
    // Do NOT short-circuit a single-thread request to a bare f(): the closure
    // contains `into_par_iter`, which would then run on rayon's *global* pool
    // with every core, so fastgeojson_threads(1) would silently stay parallel.
    // Install a genuine one-worker pool instead, which is what makes a
    // single-threaded baseline measurable.
    let pool = {
        let mut guard = POOL.lock().unwrap_or_else(|e| e.into_inner());
        let stale = match guard.as_ref() {
            Some((n, _)) => *n != want,
            None => true,
        };
        if stale {
            *guard = rayon::ThreadPoolBuilder::new()
                .num_threads(want)
                .thread_name(|i| format!("fastgeojson-{i}"))
                .build()
                .ok()
                .map(|p| (want, std::sync::Arc::new(p)));
        }
        guard.as_ref().map(|(_, p)| std::sync::Arc::clone(p))
    };
    match pool {
        Some(p) => p.install(f),
        // Pool construction failed (thread limit reached, say). Running
        // inline is slower but always correct.
        None => f(),
    }
}

