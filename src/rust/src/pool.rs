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

/// Background drop tasks that have not finished yet.
///
/// A finished call hands its scratch -- the chunk buffers and the geometry and
/// column descriptors -- to a pool worker to free, so the R thread returns the
/// result without waiting on a few megabytes of sub-megabyte heap frees. This
/// counts the ones in flight so `fastgeojson_release_pool` can wait for them
/// before it joins the workers; freeing after the pool is gone would touch a
/// dropped allocator.
pub(crate) static PENDING_DROPS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

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
/// The resolved default width, cached. 0 means "not yet worked out".
///
/// Resolving it reads four environment variables, calls
/// `available_parallelism`, and — through `under_r_check` — snapshots the
/// entire environment with `env::vars_os()`. That ran on every call to
/// `desired_threads`, which happens about six times per serialization, for a
/// value that cannot change during a session. `fastgeojson_threads()` clears
/// it, so an explicit request still takes effect immediately.
static DEFAULT_THREADS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(crate) fn invalidate_thread_cache() {
    DEFAULT_THREADS.store(0, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn desired_threads() -> usize {
    let req = REQUESTED_THREADS.load(std::sync::atomic::Ordering::Relaxed);
    if req > 0 {
        return req;
    }
    let cached = DEFAULT_THREADS.load(std::sync::atomic::Ordering::Relaxed);
    if cached > 0 {
        return cached;
    }
    let n = resolve_default_threads();
    DEFAULT_THREADS.store(n, std::sync::atomic::Ordering::Relaxed);
    n
}

fn resolve_default_threads() -> usize {
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
/// Below this much total work the pool costs more than it saves.
pub(crate) const MIN_PARALLEL_WORK: usize = 32_768;

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
pub(crate) fn estimate_row_work(props: &[(Key, ThreadSafeColumn)]) -> usize {
    let mut w = 0usize;
    for (key, col) in props {
        w += 1 + key.len() / 8;
        w += match col.kind {
            // The prepass already recorded each cell's byte length in the low
            // 30 bits of its descriptor, so the sample reads those instead of
            // chasing 16 CHARSXPs back through Rf_xlength.
            ColumnType::CharDirect => match col.char_meta {
                Some(ref m) => sampled_mean(m.len(), |i| {
                    let d = m[i];
                    // An arena cell's payload is an index, not a length.
                    if d == CD_NA || d & CD_ARENA != 0 {
                        None
                    } else {
                        // A latin1 cell widens, so it emits up to twice
                        // the bytes R holds.
                        Some(if d & CD_LATIN1 != 0 {
                            ((d & CD_LEN) as usize) * 2
                        } else {
                            (d & CD_LEN) as usize
                        })
                    }
                }),
                None => unsafe {
                    let base = col.data_ptr as *const libR_sys::SEXP;
                    sampled_mean(col.len, |i| {
                        let cs = *base.add(i);
                        if is_na_string(cs) {
                            None
                        } else {
                            Some(libR_sys::Rf_xlength(cs).max(0) as usize)
                        }
                    })
                },
            },
            ColumnType::Char | ColumnType::JsonRaw => match col.string_arena {
                Some(ref a) if !a.offsets.is_empty() => {
                    ((a.bytes.len() / a.offsets.len()) / 4).max(1)
                }
                _ => 1,
            },
            // A matrix column emits one number per matrix column, not one per
            // row. Counting it as 1 put a 2000-row frame carrying a 200-column
            // numeric matrix -- 400000 values -- at 2000 units, below
            // MIN_PARALLEL_WORK, so the shape the worker-side matrix writer
            // exists for was the one kept in a single chunk.
            ColumnType::MatrixReal | ColumnType::MatrixInt | ColumnType::MatrixBool => {
                col.aux as usize
            }
            ColumnType::ArrayDirect => match col.arr_shape {
                Some(ref v) => v[..v.len() / 2].iter().product::<usize>().max(1),
                None => 1,
            },
            _ => 1,
        };
    }
    w.max(1)
}

/// Mean of up to 16 samples spread across `n`, in units of four bytes.
///
/// `f` returns None for a cell that carries no length (an NA, or a cell held
/// in the arena rather than read directly).
#[inline]
fn sampled_mean(n: usize, f: impl Fn(usize) -> Option<usize>) -> usize {
    if n == 0 {
        return 1;
    }
    let sample = n.min(16);
    let step = (n / sample).max(1);
    let mut total = 0usize;
    let mut seen = 0usize;
    let mut i = 0usize;
    while i < n && seen < sample {
        if let Some(len) = f(i) {
            total += len;
        }
        seen += 1;
        i += step;
    }
    ((total / seen.max(1)) / 4).max(1)
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

/// Stops Rust's default panic hook printing to the process's stderr.
///
/// extendr transports an `Err` out of a `#[extendr]` function by panicking
/// with the message and catching it at the C boundary, so the default hook
/// printed three lines of "thread '<unnamed>' panicked at ..." ahead of every
/// ordinary R error -- including ones we raise deliberately, like jsonlite's
/// own "bytes" refusal. It looked like a crash and it was not.
///
/// Nothing is lost by silencing it. Every panic that can reach here is
/// already caught: the entry points wrap their work in `catch_unwind` and
/// turn a real one into an "Internal panic:" R error, the pool closures do
/// the same for workers, and extendr's own is carrying a message it is about
/// to raise. The hook print was pure duplication, on a stream R-exts says
/// compiled code should not be writing to at all.
///
/// `FASTGEOJSON_PANIC_TRACE=1` keeps the default hook, for when the R-level
/// message is not enough and a backtrace is wanted.
#[no_mangle]
pub extern "C" fn fastgeojson_quiet_panics() {
    if std::env::var_os("FASTGEOJSON_PANIC_TRACE").is_some() {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
}

/// Drops the worker pool, which terminates and joins its threads.
///
/// Called from `R_unload_fastgeojson`. R-exts 5.4 documents the hook: "when
/// unloading the object, R looks for a routine named R_unload_lib ... R will
/// invoke it and pass it a single argument describing the DLL". Without it,
/// `dyn.unload()` pulled the code out from under thirty-two live threads,
/// which is a crash waiting for the next `library(fastgeojson)`.
#[no_mangle]
pub extern "C" fn fastgeojson_release_pool() {
    // Let any background drops finish first: they run on the workers this is
    // about to join, and freeing after the pool is gone would touch a dropped
    // allocator. They are microseconds of memory frees, so a short spin is
    // enough; the bound stops a wedged worker hanging package unload forever.
    use std::sync::atomic::Ordering;
    for _ in 0..10_000 {
        if PENDING_DROPS.load(Ordering::SeqCst) == 0 {
            break;
        }
        std::thread::yield_now();
    }
    let taken = {
        let mut guard = POOL.lock().unwrap_or_else(|e| e.into_inner());
        guard.take()
    };
    // Dropped outside the lock: the Arc's destructor joins the workers, and
    // holding the mutex across that would deadlock anything still inside
    // with_pool.
    drop(taken);
}

/// Runs `f` on our own pool, rebuilding it if the requested width changed.
///
/// Single-threaded requests skip rayon entirely, which is also what makes
/// `fastgeojson_threads(1)` a usable baseline when separating algorithmic
/// gains from parallel ones.
/// Frees `t` on a pool worker instead of on the calling (R) thread.
///
/// A serialization holds its per-chunk output buffers and its geometry and
/// column descriptors until the result is built, then drops them -- several
/// megabytes across ~128 sub-megabyte heap blocks, which on Windows serialise
/// on the allocator lock and measured 3-4 ms on the million-point path, all of
/// it after the last work was done and none of it visible to the caller until
/// the call returned.
///
/// Handing that to a worker returns the result immediately. `t` must own
/// everything it frees and borrow nothing (`'static`), and be `Send`; the
/// descriptors already are. The `PENDING_DROPS` counter lets pool teardown
/// wait, so a drop can never outlive the allocator.
///
/// Falls back to an inline drop when there is no pool or it is one worker
/// wide -- the single-threaded path has nowhere to hand off to, and a
/// one-worker pool is the `fastgeojson_threads(1)` baseline where the point is
/// to measure this thread's work.
pub(crate) fn spawn_drop<T: Send + 'static>(t: T) {
    use std::sync::atomic::Ordering;
    let pool = {
        let guard = POOL.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            Some((n, p)) if *n > 1 => Some(std::sync::Arc::clone(p)),
            _ => None,
        }
    };
    match pool {
        Some(p) => {
            PENDING_DROPS.fetch_add(1, Ordering::SeqCst);
            p.spawn(move || {
                drop(t);
                PENDING_DROPS.fetch_sub(1, Ordering::SeqCst);
            });
        }
        None => drop(t),
    }
}

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
        // Pool construction failed -- the thread limit reached, say. The
        // closure still produces the right bytes, but `into_par_iter` inside
        // it now runs on rayon's global pool, so this is the one case where
        // the requested width is not honoured. There is nothing better to fall
        // back to: refusing to serialise would be worse than serialising at
        // the wrong width.
        None => f(),
        Some(p) => p.install(f),
    }
}

// ------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_jobs_stay_in_one_chunk() {
        // Below MIN_PARALLEL_WORK the pool costs more than it saves, so the
        // answer must be the whole range whatever the worker count.
        assert_eq!(rows_per_chunk(0, 1), 1);
        assert_eq!(rows_per_chunk(1, 1), 1);
        assert_eq!(rows_per_chunk(10, 1), 10);
        assert_eq!(rows_per_chunk(1000, 1), 1000);
        // Work per row is what decides it, not the row count: a short but very
        // wide frame has to be split, a long thin one need not be.
        assert!(rows_per_chunk(2000, 200) < 2000);
        assert_eq!(rows_per_chunk(2000, 1), 2000);
    }

    #[test]
    fn chunks_cover_the_range_exactly() {
        for &n in &[1usize, 2, 7, 1000, 32_768, 40_009, 1_000_000] {
            for &w in &[1usize, 4, 200] {
                let cs = rows_per_chunk(n, w);
                assert!(cs >= 1, "n {} w {} gave {}", n, w, cs);
                let chunks = (n + cs - 1) / cs;
                assert!(chunks >= 1);
                // Every row lands in exactly one chunk.
                assert!((chunks - 1) * cs < n.max(1));
            }
        }
    }


    #[test]
    fn row_work_counts_what_a_row_actually_emits() {
        // The estimate is what decides whether the pool is used at all, so a
        // kind that emits many values per row has to say so. A matrix column
        // counted as one unit put a 2000-row frame carrying a 200-column
        // matrix below the threshold.
        let key = Key::from_name(b"x");
        let mk = |kind: ColumnType, aux: u32| {
            vec![(
                Key::from_name(b"x"),
                ThreadSafeColumn {
                    kind,
                    aux,
                    data_ptr: 0,
                    len: 10,
                    cached_levels: None,
                    string_arena: None,
                    char_meta: None,
                    arr_shape: None,
                },
            )]
        };
        let _ = key;
        let plain = estimate_row_work(&mk(ColumnType::Real, 0));
        let matrix = estimate_row_work(&mk(ColumnType::MatrixReal, 200));
        assert!(
            matrix >= plain + 190,
            "a 200-wide matrix column scored {} against a plain column's {}",
            matrix,
            plain
        );
        // Logical and integer matrices count the same way.
        assert_eq!(matrix, estimate_row_work(&mk(ColumnType::MatrixInt, 200)));
        assert_eq!(matrix, estimate_row_work(&mk(ColumnType::MatrixBool, 200)));
        // An array column counts the product of its trailing dimensions.
        let mut arr = mk(ColumnType::ArrayDirect, ARR_REAL);
        arr[0].1.arr_shape = Some(vec![3usize, 4, 1, 3].into_boxed_slice());
        assert!(estimate_row_work(&arr) >= plain + 11);
        // Never zero, whatever the column.
        assert!(estimate_row_work(&mk(ColumnType::Null, 0)) >= 1);
        assert!(estimate_row_work(&[]) >= 1);
    }

    #[test]
    fn a_long_key_costs_more_than_a_short_one() {
        let one = |n: &[u8]| {
            estimate_row_work(&[(
                Key::from_name(n),
                ThreadSafeColumn {
                    kind: ColumnType::Real,
                    aux: 0,
                    data_ptr: 0,
                    len: 1,
                    cached_levels: None,
                    string_arena: None,
                    char_meta: None,
                    arr_shape: None,
                },
            )])
        };
        assert!(one(&[b'k'; 80]) > one(b"k"));
    }

    #[test]
    fn sampled_mean_is_bounded_and_never_zero() {
        assert_eq!(sampled_mean(0, |_| Some(100)), 1);
        // Quarter-bytes, floored at one.
        assert_eq!(sampled_mean(10, |_| Some(0)), 1);
        assert_eq!(sampled_mean(10, |_| Some(40)), 10);
        assert_eq!(sampled_mean(10, |_| None), 1);
        // Sampling is capped, so a huge column costs the same as a small one.
        assert_eq!(sampled_mean(1_000_000, |_| Some(40)), 10);
    }
}
