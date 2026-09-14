// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// EXPORTS
// ------------------------------------------------------------------

#[extendr]
pub(crate) fn sf_geojson_str_impl(x: Robj, auto_unbox: bool, na: Robj, null: Robj, factor: Robj, digits: Robj, envelope: Robj, always_decimal: Robj, matrix_colmajor: Robj, rownames: Robj, json_verbatim: Robj, as_bytes: Robj) -> Result<Robj> {
    str_state_reset();
    let rr = catch_unwind(AssertUnwindSafe(|| sf_geojson_str_impl_inner(x, auto_unbox, na, null, factor, digits, envelope, always_decimal, matrix_colmajor, rownames, json_verbatim, as_bytes)));
    match rr { Ok(r) => r, Err(p) => rerr(format!("Internal panic: {}", panic_message(p))), }
}

pub(crate) fn sf_geojson_str_impl_inner(x: Robj, auto_unbox: bool, na: Robj, null: Robj, factor: Robj, digits: Robj, envelope: Robj, always_decimal: Robj, matrix_colmajor: Robj, rownames: Robj, json_verbatim: Robj, as_bytes: Robj) -> Result<Robj> {
    // "geojson" wraps the features in a FeatureCollection; "features"
    // returns the bare array, which is jsonlite's sf = "features".
    let wrap_fc = parse_r_string_arg(envelope, "geojson") != "features";
    if x.is_null() {
        let mut r = Robj::from(if wrap_fc { EMPTY_FC } else { "[]" });
        r.set_class(&["geojson", "json"])?;
        return Ok(r);
    }
    if !x.inherits("sf") { return rerr("Not an sf object"); }
    let df = x.as_list().ok_or_else(|| Error::Other("Invalid sf list".to_string()))?;
    let n_rows = unsafe { get_df_nrows(x.get()) };
        // A zero-row sf still produces the full envelope; jsonlite and GDAL
    // agree on {"type":"FeatureCollection","name":"sfdata","features":[]}.
    if n_rows == 0 {
        let mut r = Robj::from(if wrap_fc { EMPTY_FC } else { "[]" });
        r.set_class(&["geojson", "json"])?;
        return Ok(r);
    }

    let n_cols_sf = unsafe { sexp_len(x.get()) };
    let colnames = unsafe { utf8_names(x.get(), n_cols_sf) }
        .ok_or_else(|| Error::Other("No names".to_string()))?;
    let sfcol_attr = x.get_attrib("sf_column").ok_or_else(|| Error::Other("No sf_column".to_string()))?;
    let sfcol_vec = sfcol_attr.as_str_vector().ok_or_else(|| Error::Other("sf_column not char".to_string()))?;
    if sfcol_vec.is_empty() { return rerr("sf_column empty"); }

    let geom_name = sfcol_vec[0].as_bytes();
    let geom_idx = colnames.iter().position(|n| n.as_slice() == geom_name).ok_or_else(|| Error::Other("Geometry col not found".to_string()))?;
    let geom_col_robj = df.elt(geom_idx).map_err(|_| Error::Other("Geometry col error".to_string()))?;
    let geom_col = unsafe { geom_col_robj.get() };
    if unsafe { typeof_sexp(geom_col) } != libR_sys::SEXPTYPE::VECSXP as u32 { return rerr("Geometry col not a list"); }

    // A frame whose row.names claim more rows than the geometry column holds
    // is malformed; indexing past the end used to segfault the R process.
    let geom_len = unsafe { sexp_len(geom_col) };
    if geom_len < n_rows {
        return rerr(format!(
            "malformed sf object: geometry column '{}' has {} element(s) but the object declares {} row(s)",
            String::from_utf8_lossy(geom_name), geom_len, n_rows
        ));
    }

    let sfc_type = detect_sfc_type_sexp(geom_col);
    
    // Config setup
    let na_val = parse_r_string_arg(na, "null");
    let na_mode = match na_val.as_str() {
        "string" => NaMode::String,
        "smart" => NaMode::Smart,
        _ => NaMode::Null,
    };
    let null_val = parse_r_string_arg(null, "list");
    let null_mode = if null_val == "null" { NullMode::Null } else { NullMode::List };
    
    // Factor Parsing
    let factor_val = parse_r_string_arg(factor, "string");
    let factor_mode = if factor_val == "integer" { FactorMode::Integer } else { FactorMode::String };

    // 0 = FALSE, 1 = not given, 2 = TRUE; see SerializerConfig::rownames.
    let json_verbatim_flag = json_verbatim.as_bool().unwrap_or(false);
    let rownames_flag = rownames
        .as_integer()
        .or_else(|| rownames.as_real().map(|f| f as i32))
        .unwrap_or(ROWNAMES_REAL as i32)
        .clamp(0, 2) as u8;
    let signif_flag = parse_signif_arg(&digits);
    let digits_opt = parse_digits_arg(digits);
    let always_decimal_flag = always_decimal.as_bool().unwrap_or(false);
    let matrix_colmajor_flag = matrix_colmajor.as_bool().unwrap_or(false);
    let config = SerializerConfig { df: DfMode::Rows, na: na_mode, null: null_mode, factor: factor_mode, auto_unbox, digits: digits_opt, always_decimal: always_decimal_flag, matrix_colmajor: matrix_colmajor_flag, signif: signif_flag, rownames: rownames_flag, json_verbatim: json_verbatim_flag };

    let mut ph = PhaseTimer::new(format_args!(
        "sf {} features, {} property cols, {} workers",
        n_rows, colnames.len().saturating_sub(1), desired_threads()
    ));
    // The sf path needs the plain names too, to find the geometry column, so
    // it is the one caller that builds both.
    let keys: Vec<Key> = colnames.iter().map(|n| Key::from_name(n)).collect();
    let props = build_thread_safe_cols(unsafe { x.get() }, keys, geom_idx, n_rows, config, 0)?;
    ph.lap("build columns");
    // Between phases, on the R thread: the pooled regions below cannot be
    // interrupted from inside, so this is where a Ctrl-C during a long run
    // gets noticed.
    if interrupt_pending() { return rerr(interrupted_msg()); }
    
    // A polygon feature can be hundreds of times the work of a point, so size
    // the chunks by sampled geometry cost plus the property columns.
    let geom_work = unsafe { estimate_geom_work(geom_col, n_rows) };
    let prop_work = estimate_row_work(&props);
    let work_per_row = geom_work + prop_work;
    let chunk_size = rows_per_chunk(n_rows, work_per_row);
    ph.lap("plan chunks");
    let num_chunks = (n_rows + chunk_size - 1) / chunk_size;
    let ranges: Vec<(usize, usize, usize)> = (0..num_chunks).map(|id| (id, id * chunk_size, (id * chunk_size + chunk_size).min(n_rows))).collect();
    // Describing the geometries is pure reads -- SEXP headers, attribute
    // pairlists, data pointers -- so it belongs in the pool. It was the last
    // serial phase of any size on this path: 30.15 ms on one worker and
    // 32.98 ms on 32, while serialization scaled 11.9x over the same range.
    let geom_ptr = geom_col as usize;
    // What the workers may not read for themselves -- each feature's
    // dimension, and its type in a mixed layer -- read here first.
    let infos = unsafe { describe_geometries(geom_col, n_rows, sfc_type) };
    let par = ranges.len() > 1 && desired_threads() > 1;
    let mut chunk_geoms: Vec<(usize, usize, usize, ChunkGeoms)> = with_pool_if(par, || {
        ranges
            .par_iter()
            .map(|(id, start, end)| {
                let cg = extract_geometries_chunk(
                    geom_ptr as libR_sys::SEXP,
                    sfc_type,
                    *start,
                    *end,
                    config,
                    if infos.is_empty() { &[] } else { &infos[*start..*end] },
                );
                (*id, *start, *end, cg)
            })
            .collect()
    });
    // Anything the workers could not describe from pure reads is finished
    // here, on the R thread. For sf's own objects that is nothing.
    for (_, start, _, cg) in chunk_geoms.iter_mut() {
        unsafe { finish_pending_geoms(cg, geom_col, sfc_type, *start, config) };
    }

    ph.lap("extract geometry");

    // Split each chunk's rows by the work they carry, using that chunk's own
    // descriptors. Equal-row boundaries left a layer of many small geometries
    // and a few large ones scaling 1.6x where uniform geometry scaled 15.9x.
    //
    // The split is done *within* each extraction chunk rather than over one
    // merged descriptor array. Merging existed only so this pass could pick
    // its own boundaries, and it cost a serial 32 MB copy of the descriptors
    // for a million features plus a second serial pass to build the work
    // vector: `extract geometry` measured 17.1 ms on 32 workers against
    // 21.6 ms on one, i.e. almost entirely serial. A chunk already owns a
    // contiguous row range, so sub-ranges of it need no rebasing at all, and
    // a chunk holding one enormous geometry still splits it out.
    let bytes_per_work = 16usize;
    // (chunk index, local start, local end, work in that sub-range)
    let mut subs: Vec<(usize, usize, usize, usize)> = Vec::with_capacity(num_chunks * 2);
    for (ci, (_, _, _, cg)) in chunk_geoms.iter().enumerate() {
        let w = &cg.work;
        let total: usize = w.iter().sum::<usize>() + prop_work * w.len();
        subs.push((ci, 0, w.len(), total));
    }
    // Re-split any sub-range carrying far more than its share, so a single
    // heavy chunk cannot stall the pass.
    let total_work: usize = subs.iter().map(|s| s.3).sum();
    let target = (total_work / num_chunks.max(1)).max(1);
    let mut plan: Vec<(usize, usize, usize, usize)> = Vec::with_capacity(subs.len() * 2);
    for (ci, ls, le, w) in subs {
        if w <= target * 2 || le - ls < 2 {
            plan.push((ci, ls, le, w));
            continue;
        }
        let cw = &chunk_geoms[ci].3.work;
        let want = (w / target).max(1).min(le - ls);
        for (_, a, b) in weighted_ranges(&cw[ls..le], want) {
            let (a, b) = (ls + a, ls + b);
            let sub: usize = cw[a..b].iter().sum::<usize>() + prop_work * (b - a);
            plan.push((ci, a, b, sub));
        }
    }
    ph.lap("weigh chunks");

    let parts_res: Vec<PResult<(usize, Vec<u8>)>> = with_pool_if(plan.len() > 1 && desired_threads() > 1, || plan.par_iter().enumerate().map(|(seq, &(ci, ls, le, w))| {
        let rr = catch_unwind(AssertUnwindSafe(|| {
            let (_, chunk_start, _, cg) = &chunk_geoms[ci];
            let cap = (w * bytes_per_work + (le - ls) * 128).clamp(256, 1 << 30);
            let mut writer = JsonWriter::with_capacity(cap);
            for local_i in ls..le {
                if local_i > ls { writer.push_u8(b','); }
                process_feature_parallel(
                    &mut writer,
                    chunk_start + local_i,
                    &props,
                    &cg.geoms[local_i],
                    &cg.batch,
                    config,
                );
            }
            (seq, writer.buf)
        }));
        match rr { Ok(v) => Ok(v), Err(p) => Err(format!("Worker panic: {}", panic_message(p))), }
    }).collect());

    let mut parts: Vec<(usize, Vec<u8>)> = Vec::with_capacity(parts_res.len());
    for r in parts_res { match r { Ok(v) => parts.push(v), Err(msg) => return rerr(msg), } }
    parts.sort_by_key(|(id, _)| *id);

    ph.lap("serialize (parallel)");
    let chunks: Vec<Vec<u8>> = parts.into_iter().map(|(_, v)| v).collect();
    let (prefix, suffix): (&[u8], &[u8]) = if wrap_fc {
        (FC_HEAD, FC_TAIL)
    } else {
        (b"[", b"]")
    };
    let (total, offs) = assembly_layout(prefix, &chunks, suffix);

    // The 2 GB ceiling belongs to the character path alone. R Internals is
    // explicit that long vectors cover raw but not strings -- "Elements of
    // character vectors (CHARSXPs) remain limited to 2^31 - 1 bytes" -- and
    // Rf_mkCharLenCE takes an int length. A RAWSXP has no such limit, so
    // as_bytes = TRUE is checked below rather than here, where it was
    // refusing output it could perfectly well have produced.
    if as_bytes.as_bool().unwrap_or(false) {
        let r = match assemble_into_raw(prefix, &chunks, &offs, suffix, total) {
            Ok(r) => r,
            Err(e) => return rerr(e),
        };
        ph.lap("assemble into R (raw)");
        // The result is done; free the scratch on a worker rather than making
        // the caller wait on a few megabytes of sub-megabyte heap frees.
        spawn_drop((chunks, offs, chunk_geoms, props));
        return Ok(r);
    }
    if total > i32::MAX as usize { return rerr(oversize(total)); }
    let final_out = match assemble_into_vec(prefix, &chunks, &offs, suffix, total) {
        Ok(v) => v,
        Err(e) => return rerr(e),
    };
    ph.lap("assemble chunks");
    // The scratch is dead once final_out exists. Freed on a worker while this
    // thread is inside mkCharLenCE below, which is where the character path
    // spends most of its time -- the frees hide behind it entirely.
    spawn_drop((chunks, offs, chunk_geoms, props));
    let mut robj = match finish_json_string(final_out) {
        Ok(s) => s,
        Err(e) => return rerr(e),
    };
    robj.set_class(&["geojson", "json"])?;
    ph.lap("copy into R");
    Ok(robj)
}

#[extendr]
pub(crate) fn df_json_str_impl(x: Robj, auto_unbox: bool, dataframe: String, na: Robj, null: Robj, factor: Robj, digits: Robj, always_decimal: Robj, matrix_colmajor: Robj, rownames: Robj, json_verbatim: Robj, as_bytes: Robj) -> Result<Robj> {
    str_state_reset();
    let rr = catch_unwind(AssertUnwindSafe(|| df_json_str_impl_inner(x, auto_unbox, dataframe, na, null, factor, digits, always_decimal, matrix_colmajor, rownames, json_verbatim, as_bytes)));
    match rr { Ok(r) => r, Err(p) => rerr(format!("Internal panic: {}", panic_message(p))), }
}

// ------------------------------------------------------------------
// PHASE TIMING
// ------------------------------------------------------------------

/// Set FASTGEOJSON_PROFILE=1 to have each serialization print how long its
/// phases took, on stderr. Off, this costs one relaxed atomic load per phase.
///
/// Added because the scaling curve plateaued at about 2.7x on 32 workers and
/// fitted Amdahl with a serial fraction near 0.37, which no amount of
/// reasoning about the parallel region was going to locate.
pub(crate) fn profiling() -> bool {
    use std::sync::atomic::{AtomicU8, Ordering};
    static ON: AtomicU8 = AtomicU8::new(0);
    match ON.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var_os("FASTGEOJSON_PROFILE")
                .map(|v| v != "0" && !v.is_empty())
                .unwrap_or(false);
            ON.store(if on { 2 } else { 1 }, Ordering::Relaxed);
            on
        }
    }
}

/// Writes to R's error stream rather than the process's.
///
/// R-exts 5.6 is explicit that compiled code must go through R's own
/// channels: "For C++ code do not use `cout` or `cerr`" and the C equivalent
/// is `REprintf`. Writing straight to fd 2 with `eprintln!` bypasses R's
/// sink, its connection redirection and `capture.output()`, which is both a
/// `R CMD check` finding and simply wrong for anyone trying to capture it.
///
/// The format string is `%s` with the text as an argument, never the text as
/// a format string, so a `%` in a label cannot be read as a directive.
fn reprint(msg: &str) {
    if let Ok(c) = std::ffi::CString::new(msg) {
        unsafe { libR_sys::REprintf(b"%s\0".as_ptr() as *const c_char, c.as_ptr()) };
    }
}

pub(crate) struct PhaseTimer {
    pub(crate) t: std::time::Instant,
    pub(crate) on: bool,
}

impl PhaseTimer {
    /// Takes `format_args!`, not a `String`, so the header costs nothing when
    /// profiling is off. The call sites used to build it with `format!`
    /// unconditionally, allocating on every serialization to describe a run
    /// nobody was watching.
    pub(crate) fn new(what: std::fmt::Arguments) -> Self {
        let on = profiling();
        if on {
            reprint(&format!("fastgeojson: {}\n", what));
        }
        PhaseTimer {
            t: std::time::Instant::now(),
            on,
        }
    }
    #[inline]
    pub(crate) fn lap(&mut self, label: &str) {
        if self.on {
            let now = std::time::Instant::now();
            reprint(&format!(
                "fastgeojson:   {:<20} {:>9.2} ms\n",
                label,
                (now - self.t).as_secs_f64() * 1000.0
            ));
            self.t = now;
        }
    }
}

// ------------------------------------------------------------------
/// Runs `f` in the worker pool, or directly when there is nothing to spread.
///
/// `with_pool` locks a mutex, clones an `Arc` and calls `install`, which
/// injects a job from a foreign thread and blocks on a latch. That is a few
/// microseconds, which is nothing against a 30 ms serialization and most of
/// the cost of a ten-row one. Below about 6000 rows, 32 workers measured
/// slower than one.
#[inline]
/// The message for output too large to be an R string.
///
/// Says what the limit is a limit ON, because the obvious next question is
/// whether the whole call is impossible or only this form of the answer.
fn oversize(total: usize) -> String {
    format!(
        "Result is {} bytes; an R character string is limited to {} (2 GB). \
         Use as_bytes = TRUE to get the same output as a raw vector, which has no such limit.",
        total,
        i32::MAX
    )
}

pub(crate) fn with_pool_if<R: Send>(parallel: bool, f: impl FnOnce() -> R + Send) -> R {
    if parallel {
        with_pool(f)
    } else {
        f()
    }
}

/// Byte layout for `open` + `parts` joined by commas + `close`.
///
/// Returns the total size and, per part, the offset it starts at.
/// `usize::MAX` marks an empty part, which contributes nothing and no comma.
pub(crate) fn assembly_layout(prefix: &[u8], parts: &[Vec<u8>], suffix: &[u8]) -> (usize, Vec<usize>) {
    let mut offs = Vec::with_capacity(parts.len());
    let mut at = prefix.len();
    let mut first = true;
    for p in parts {
        if p.is_empty() {
            offs.push(usize::MAX);
            continue;
        }
        if !first {
            at += 1; // the comma before this part
        }
        offs.push(at);
        at += p.len();
        first = false;
    }
    (at + suffix.len(), offs)
}

/// Copies `src` to `dst` with non-temporal (cache-bypassing) stores.
///
/// The assembly copy writes tens of megabytes that this core does not read
/// again: on the `as_bytes` path the raw vector goes back to R and out to a
/// file or socket, so pulling it through the cache only evicts everything else
/// and pays write-allocate read-for-ownership traffic. `_mm_stream_si128`
/// writes straight to memory -- ~6.5 -> ~4.8 ms on a 120 MB result. SSE2 is
/// baseline on x86-64, so there is nothing to detect.
///
/// NOT used on the character path, where `mkCharLenCE` reads every byte back
/// at once to hash it; there the data must stay in cache.
///
/// # Safety
/// `dst` writable for `len`, `src` readable for `len`, non-overlapping.
#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn stream_copy(dst: *mut u8, src: *const u8, len: usize) {
    use std::arch::x86_64::{_mm_loadu_si128, _mm_sfence, _mm_storeu_si128, _mm_stream_si128, __m128i};
    // Below a couple of cache lines the alignment setup is not worth it.
    if len < 128 {
        std::ptr::copy_nonoverlapping(src, dst, len);
        return;
    }
    // Streaming stores want a 16-byte-aligned destination; scalar head to the
    // first boundary.
    let head = (16 - (dst as usize & 15)) & 15;
    std::ptr::copy_nonoverlapping(src, dst, head);
    let mut i = head;
    while i + 64 <= len {
        let s = src.add(i) as *const __m128i;
        let d = dst.add(i) as *mut __m128i;
        _mm_stream_si128(d, _mm_loadu_si128(s));
        _mm_stream_si128(d.add(1), _mm_loadu_si128(s.add(1)));
        _mm_stream_si128(d.add(2), _mm_loadu_si128(s.add(2)));
        _mm_stream_si128(d.add(3), _mm_loadu_si128(s.add(3)));
        i += 64;
    }
    while i + 16 <= len {
        // The trailing <64 bytes: an ordinary unaligned store, a rounding
        // error against the streamed bulk.
        _mm_storeu_si128(dst.add(i) as *mut __m128i, _mm_loadu_si128(src.add(i) as *const __m128i));
        i += 16;
    }
    std::ptr::copy_nonoverlapping(src.add(i), dst.add(i), len - i);
    // Streaming stores are weakly ordered; fence before R sees the vector.
    _mm_sfence();
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
unsafe fn stream_copy(dst: *mut u8, src: *const u8, len: usize) {
    std::ptr::copy_nonoverlapping(src, dst, len);
}

/// A large copy that does not go through the C runtime's `memcpy`.
///
/// On Windows the CRT's `memcpy` is `rep movsb`, and on Zen 4 that is fast
/// only when source and destination share the same offset within a 4 KB page:
/// measured on a 76 MB chunk, 12 ms when they did and 75-86 ms at every other
/// offset. The chunks of the output land at arbitrary offsets, so every
/// assembly copy took the slow path. Unaligned 16-byte loads and stores run
/// at full bandwidth whatever the relative alignment; the compiler does not
/// turn an intrinsics loop back into a `memcpy` call.
///
/// # Safety
///
/// `dst` writable for `len`, `src` readable for `len`, non-overlapping.
#[cfg(target_arch = "x86_64")]
#[inline]
pub(crate) unsafe fn simd_copy(dst: *mut u8, src: *const u8, len: usize) {
    use std::arch::x86_64::{_mm_loadu_si128, _mm_storeu_si128, __m128i};
    if len < 256 {
        std::ptr::copy_nonoverlapping(src, dst, len);
        return;
    }
    let mut i = 0;
    while i + 64 <= len {
        let s = src.add(i) as *const __m128i;
        let d = dst.add(i) as *mut __m128i;
        let a = _mm_loadu_si128(s);
        let b = _mm_loadu_si128(s.add(1));
        let c = _mm_loadu_si128(s.add(2));
        let e = _mm_loadu_si128(s.add(3));
        _mm_storeu_si128(d, a);
        _mm_storeu_si128(d.add(1), b);
        _mm_storeu_si128(d.add(2), c);
        _mm_storeu_si128(d.add(3), e);
        i += 64;
    }
    while i + 16 <= len {
        _mm_storeu_si128(dst.add(i) as *mut __m128i, _mm_loadu_si128(src.add(i) as *const __m128i));
        i += 16;
    }
    std::ptr::copy_nonoverlapping(src.add(i), dst.add(i), len - i);
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
pub(crate) unsafe fn simd_copy(dst: *mut u8, src: *const u8, len: usize) {
    std::ptr::copy_nonoverlapping(src, dst, len);
}

/// Puts one chunk body at its place in the output.
///
/// Streaming stores only when source and destination agree modulo 16, where
/// the loads are aligned too: 12.8 ms for a 76 MB chunk against 188 ms at any
/// other relative offset, measured single-threaded. Everything else goes
/// through `simd_copy`, 14 ms at every offset.
///
/// # Safety
///
/// As for `simd_copy`.
#[inline]
unsafe fn place(dst: *mut u8, src: *const u8, len: usize, nt: bool) {
    if nt && (dst as usize).wrapping_sub(src as usize) & 15 == 0 {
        stream_copy(dst, src, len);
    } else {
        simd_copy(dst, src, len);
    }
}

/// Writes `open`, the parts at their offsets, the separating commas and
/// `close` into `dst`, which must have room for exactly `total` bytes.
///
/// Fuses the assembly with the destination, so the bytes are moved once. The
/// previous shape concatenated the chunks into a Vec and then copied that Vec
/// into R's vector: two full passes over the whole output, and phase timing
/// put them at 11.9 ms and 29 ms for a 49 MB result.
///
/// # Safety
///
/// `dst` must be writable for `total` bytes and not alias any part.
pub(crate) unsafe fn assemble_into(
    dst: *mut u8,
    total: usize,
    prefix: &[u8],
    parts: &[Vec<u8>],
    offs: &[usize],
    suffix: &[u8],
    // Stream the chunk bodies with cache-bypassing stores. True only when the
    // destination will not be read back on this core -- the as_bytes raw
    // vector. See stream_copy.
    nt: bool,
) {
    debug_assert!(total >= prefix.len() + suffix.len());
    std::ptr::copy_nonoverlapping(prefix.as_ptr(), dst, prefix.len());
    std::ptr::copy_nonoverlapping(
        suffix.as_ptr(),
        dst.add(total - suffix.len()),
        suffix.len(),
    );
    // The separators, and the brackets above, are a handful of bytes; only the
    // chunk bodies are worth spreading.
    let mut first = true;
    for (i, _) in parts.iter().enumerate() {
        if offs[i] == usize::MAX {
            continue;
        }
        if !first {
            *dst.add(offs[i] - 1) = b',';
        }
        first = false;
    }

    // Below this the pool costs more than the copy saves.
    const MIN_PARALLEL_BYTES: usize = 1 << 22;
    if total < MIN_PARALLEL_BYTES || parts.len() < 2 || desired_threads() <= 1 {
        for (i, p) in parts.iter().enumerate() {
            if offs[i] == usize::MAX {
                continue;
            }
            place(dst.add(offs[i]), p.as_ptr(), p.len(), nt);
        }
        return;
    }

    // Every destination range is disjoint by construction -- the offsets are a
    // prefix sum over the chunk lengths -- and nothing else touches `dst`
    // while this runs, so the writes need no synchronisation.
    struct Dst(*mut u8);
    unsafe impl Send for Dst {}
    unsafe impl Sync for Dst {}
    impl Dst {
        /// Goes through a method so the closure captures `&Dst`, which is
        /// Sync, rather than the bare `*mut u8` field, which is not.
        #[inline]
        unsafe fn write(&self, at: usize, src: &[u8], nt: bool) {
            place(self.0.add(at), src.as_ptr(), src.len(), nt);
        }
    }
    let base = Dst(dst);
    // Copied at full pool width. Bounding this to six tasks, on the theory
    // that a pure copy saturates memory bandwidth early, measured slower on
    // every shape tried -- 37.2 -> 39.0 ms on a million point features.
    with_pool(|| {
        parts
            .par_iter()
            .zip(offs.par_iter())
            .for_each(|(p, &o)| {
                if o == usize::MAX {
                    return;
                }
                unsafe { base.write(o, p, nt) };
            })
    });
}

/// Hands `parts` back as an R raw vector, assembled straight into R's own
/// storage so the bytes are written exactly once.
pub(crate) fn assemble_into_raw(
    prefix: &[u8],
    parts: &[Vec<u8>],
    offs: &[usize],
    suffix: &[u8],
    total: usize,
) -> PResult<Robj> {
    check_str_state()?;
    unsafe {
        let v = libR_sys::Rf_allocVector(
            libR_sys::SEXPTYPE::RAWSXP,
            total as libR_sys::R_xlen_t,
        );
        libR_sys::Rf_protect(v);
        // Nothing allocates from R between here and the unprotect, so the
        // collector cannot move or reclaim v while it is being filled.
        assemble_into(libR_sys::RAW(v) as *mut u8, total, prefix, parts, offs, suffix, true);
        let r = Robj::from_sexp(v);
        libR_sys::Rf_unprotect(1);
        Ok(r)
    }
}

/// The same assembly into a fresh `Vec`, for the character path.
/// The one allocation big enough to be worth asking about rather than
/// assuming.
///
/// `Vec::with_capacity` calls `handle_alloc_error` when it cannot get the
/// memory, which calls `abort()`. R-exts 5.6 forbids that absolutely: "Under
/// no circumstances should your compiled code ever call abort or exit: these
/// terminate the user's R process, quite possibly losing all unsaved work."
/// And an abort does not unwind, so the `catch_unwind` at every entry point
/// would not see it either. `try_reserve` (stable since 1.57, inside the 1.65
/// MSRV) hands the failure back instead, and the caller turns it into an R
/// condition the user can catch -- which is what the `as_bytes` path already
/// gets for free from `Rf_allocVector`.
pub(crate) fn try_buffer(total: usize) -> PResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    out.try_reserve_exact(total).map_err(|_| {
        format!(
            "Could not allocate {} bytes for the result. \
             as_bytes = TRUE allocates through R instead, which reports the \
             shortfall as a catchable R error rather than failing here.",
            total
        )
    })?;
    Ok(out)
}

pub(crate) fn assemble_into_vec(
    prefix: &[u8],
    parts: &[Vec<u8>],
    offs: &[usize],
    suffix: &[u8],
    total: usize,
) -> PResult<Vec<u8>> {
    let mut out: Vec<u8> = try_buffer(total)?;
    unsafe {
        assemble_into(out.as_mut_ptr(), total, prefix, parts, offs, suffix, false);
        // Every one of the `total` bytes was just written.
        out.set_len(total);
    }
    Ok(out)
}

pub(crate) fn df_json_str_impl_inner(x: Robj, auto_unbox: bool, dataframe: String, na: Robj, null: Robj, factor: Robj, digits: Robj, always_decimal: Robj, matrix_colmajor: Robj, rownames: Robj, json_verbatim: Robj, as_bytes: Robj) -> Result<Robj> {
    if x.is_null() { let mut r = Robj::from("[]"); r.set_class(&["json"])?; return Ok(r); }
    if !x.inherits("data.frame") { return rerr("Not a data.frame"); }

    let n_cols = unsafe { sexp_len(x.get()) };
    let n_rows = unsafe { get_df_nrows(x.get()) };

    if n_cols == 0 {
        if dataframe == "columns" {
             let mut robj = Robj::from("{}"); robj.set_class(&["json"])?; return Ok(robj);
        }
        if n_rows == 0 { let mut r = Robj::from("[]"); r.set_class(&["json"])?; return Ok(r); }
        let mut buf = Vec::with_capacity(n_rows * 3 + 2);
        buf.push(b'[');
        for i in 0..n_rows { if i > 0 { buf.push(b','); } buf.extend_from_slice(b"{}"); }
        buf.push(b']');
        let mut robj = match finish_json_string(buf) {
            Ok(s) => s,
            Err(e) => return rerr(e),
        };
        robj.set_class(&["json"])?;
        return Ok(robj);
    }

    // Kept as a validity check: a data.frame that is not a VECSXP would make
    // the column pointer below meaningless.
    x.as_list().ok_or_else(|| Error::Other("Invalid df structure".to_string()))?;
    let keys = unsafe { escaped_keys(x.get(), n_cols) }
        .ok_or_else(|| Error::Other("No names".to_string()))?;
    
    let df_mode = match dataframe.as_str() {
        "columns" => DfMode::Columns,
        "values" => DfMode::Values,
        _ => DfMode::Rows,
    };
    let na_val = parse_r_string_arg(na, "null");
    let na_mode = match na_val.as_str() {
        "string" => NaMode::String,
        "smart" => NaMode::Smart,
        _ => NaMode::Null,
    };
    let null_val = parse_r_string_arg(null, "list");
    let null_mode = if null_val == "null" { NullMode::Null } else { NullMode::List };
    
    // Factor Parsing
    let factor_val = parse_r_string_arg(factor, "string");
    let factor_mode = if factor_val == "integer" { FactorMode::Integer } else { FactorMode::String };

    // 0 = FALSE, 1 = not given, 2 = TRUE; see SerializerConfig::rownames.
    let json_verbatim_flag = json_verbatim.as_bool().unwrap_or(false);
    let rownames_flag = rownames
        .as_integer()
        .or_else(|| rownames.as_real().map(|f| f as i32))
        .unwrap_or(ROWNAMES_REAL as i32)
        .clamp(0, 2) as u8;
    let signif_flag = parse_signif_arg(&digits);
    let digits_opt = parse_digits_arg(digits);
    let always_decimal_flag = always_decimal.as_bool().unwrap_or(false);
    let matrix_colmajor_flag = matrix_colmajor.as_bool().unwrap_or(false);
    let config = SerializerConfig { df: df_mode, na: na_mode, null: null_mode, factor: factor_mode, auto_unbox, digits: digits_opt, always_decimal: always_decimal_flag, matrix_colmajor: matrix_colmajor_flag, signif: signif_flag, rownames: rownames_flag, json_verbatim: json_verbatim_flag };

    let mut ph = PhaseTimer::new(format_args!(
        "data.frame {} rows x {} cols, {} workers",
        n_rows, n_cols, desired_threads()
    ));
    let props = build_thread_safe_cols(unsafe { x.get() }, keys, usize::MAX, n_rows, config, 0)?;
    ph.lap("build columns");
    // Between phases, on the R thread: the pooled regions below cannot be
    // interrupted from inside, so this is where a Ctrl-C during a long run
    // gets noticed.
    if interrupt_pending() { return rerr(interrupted_msg()); }

	let final_out = if df_mode == DfMode::Columns {
        let chunks = match df_col_chunks(&props, n_rows, config, Some(&mut ph)) {
            Ok(c) => c,
            Err(e) => return rerr(e),
        };
        let (total, offs) = assembly_layout(b"{", &chunks, b"}");
        // Character path only; see the note at the geojson assembly.
        if as_bytes.as_bool().unwrap_or(false) {
            let r = match assemble_into_raw(b"{", &chunks, &offs, b"}", total) {
                Ok(r) => r,
                Err(e) => return rerr(e),
            };
            ph.lap("assemble into R (raw)");
            spawn_drop((chunks, offs, props));
            return Ok(r);
        }
        if total > i32::MAX as usize { return rerr(oversize(total)); }
        let out = match assemble_into_vec(b"{", &chunks, &offs, b"}", total) {
            Ok(v) => v,
            Err(e) => return rerr(e),
        };
        ph.lap("assemble columns");
        spawn_drop((chunks, offs, props));
        out
    } else {
        if n_rows == 0 { let mut r = Robj::from("[]"); r.set_class(&["json"])?; return Ok(r); }
        let chunks = match df_row_chunks(&props, n_rows, estimate_row_work(&props), config, Some(&mut ph)) {
            Ok(c) => c,
            Err(e) => return rerr(e),
        };
        let (total, offs) = assembly_layout(b"[", &chunks, b"]");
        // With as_bytes the destination is R's own vector, so the chunks are
        // written into it directly and nothing is copied twice -- and it is
        // not subject to the character path's 2 GB ceiling.
        if as_bytes.as_bool().unwrap_or(false) {
            let r = match assemble_into_raw(b"[", &chunks, &offs, b"]", total) {
                Ok(r) => r,
                Err(e) => return rerr(e),
            };
            ph.lap("assemble into R (raw)");
            spawn_drop((chunks, offs, props));
            return Ok(r);
        }
        if total > i32::MAX as usize { return rerr(oversize(total)); }
        let out = match assemble_into_vec(b"[", &chunks, &offs, b"]", total) {
            Ok(v) => v,
            Err(e) => return rerr(e),
        };
        ph.lap("assemble chunks");
        spawn_drop((chunks, offs, props));
        out
    };

    if as_bytes.as_bool().unwrap_or(false) {
        let r = match finish_json_raw(final_out) { Ok(r) => r, Err(e) => return rerr(e) };
        ph.lap("copy into R (raw)");
        return Ok(r);
    }
    if final_out.len() > i32::MAX as usize { return rerr(oversize(final_out.len())); }
    let mut robj = match finish_json_string(final_out) {
        Ok(s) => s,
        Err(e) => return rerr(e),
    };
    ph.lap("utf8 finish");
    robj.set_class(&["json"])?;
    ph.lap("copy into R");
    Ok(robj)
}

#[extendr]
pub(crate) fn obj_json_str_impl(x: Robj, auto_unbox: bool, dataframe: String, na: Robj, null: Robj, factor: Robj, digits: Robj, always_decimal: Robj, matrix_colmajor: Robj, rownames: Robj, json_verbatim: Robj, as_bytes: Robj) -> Result<Robj> {
    // The other two entry points already convert a worker panic into an R
    // error; this one called the recursive writer directly, so a panic --
    // capacity overflow, a failed allocation, a slice bound -- would unwind
    // across the C boundary into R.
    str_state_reset();
    let rr = catch_unwind(AssertUnwindSafe(|| {
        obj_json_str_impl_inner(x, auto_unbox, dataframe, na, null, factor, digits, always_decimal, matrix_colmajor, rownames, json_verbatim, as_bytes)
    }));
    match rr { Ok(r) => r, Err(p) => rerr(format!("Internal panic: {}", panic_message(p))) }
}

fn obj_json_str_impl_inner(x: Robj, auto_unbox: bool, dataframe: String, na: Robj, null: Robj, factor: Robj, digits: Robj, always_decimal: Robj, matrix_colmajor: Robj, rownames: Robj, json_verbatim: Robj, as_bytes: Robj) -> Result<Robj> {
    // A modest reservation, left to Vec's growth from there. The previous
    // formula, sexp_len(x) * 16 + 64, was wrong in intent -- sexp_len of a
    // data frame is its column count, so a nested frame reserved 80 bytes for
    // megabytes of output -- but replacing it with a recursive estimate of the
    // real payload measured flat everywhere, including 34.5 MB of nested
    // output (182.8 -> 181.4 ms). Doubling plus in-place realloc absorbs it,
    // so there is nothing here worth computing.
    let est_size = 4096;
    let mut w = JsonWriter::with_capacity(est_size);
    
    let na_val = parse_r_string_arg(na, "null");
    let na_mode = match na_val.as_str() {
        "string" => NaMode::String,
        "smart" => NaMode::Smart,
        _ => NaMode::Null,
    };
    let null_val = parse_r_string_arg(null, "list");
    let null_mode = if null_val == "null" { NullMode::Null } else { NullMode::List };
    
    // Factor Parsing
    let factor_val = parse_r_string_arg(factor, "string");
    let factor_mode = if factor_val == "integer" { FactorMode::Integer } else { FactorMode::String };

    // 0 = FALSE, 1 = not given, 2 = TRUE; see SerializerConfig::rownames.
    let json_verbatim_flag = json_verbatim.as_bool().unwrap_or(false);
    let rownames_flag = rownames
        .as_integer()
        .or_else(|| rownames.as_real().map(|f| f as i32))
        .unwrap_or(ROWNAMES_REAL as i32)
        .clamp(0, 2) as u8;
    let signif_flag = parse_signif_arg(&digits);
    let digits_opt = parse_digits_arg(digits);
    let always_decimal_flag = always_decimal.as_bool().unwrap_or(false);
    let matrix_colmajor_flag = matrix_colmajor.as_bool().unwrap_or(false);
    // `dataframe` never used to reach here, so a data.frame anywhere below the
    // top level was always rendered row-oriented: as_json(list(d = df),
    // dataframe = "columns") silently ignored the argument.
    let df_mode = match dataframe.as_str() {
        "columns" => DfMode::Columns,
        "values" => DfMode::Values,
        _ => DfMode::Rows,
    };
    let config = SerializerConfig { df: df_mode, na: na_mode, null: null_mode, factor: factor_mode, auto_unbox, digits: digits_opt, always_decimal: always_decimal_flag, matrix_colmajor: matrix_colmajor_flag, signif: signif_flag, rownames: rownames_flag, json_verbatim: json_verbatim_flag };

    unsafe { serialize_sexp_to_json_buffer(x.get(), &mut w.buf, config, 0); }
    if as_bytes.as_bool().unwrap_or(false) {
        return match finish_json_raw(w.buf) { Ok(r) => Ok(r), Err(e) => rerr(e) };
    }
    if w.buf.len() > i32::MAX as usize { return rerr(oversize(w.buf.len())); }
    let mut res = match finish_json_string(w.buf) {
        Ok(s) => s,
        Err(e) => return rerr(e),
    };
    res.set_class(&["json"])?;
    Ok(res)
}

// ------------------------------------------------------------------
// PRETTY PRINTING
// ------------------------------------------------------------------
// A post-pass over the compact output. Reproduces jsonlite's layout, which is
// not a generic pretty-printer: an array stays on one line iff it contains no
// nested container (so `[1, 2]` and `[[1, 2], [3, 4]]`'s inner arrays are
// inline, but the outer one expands), objects always expand unless empty, and
// the separators are `, ` and `": "`.

/// For every container opener in `src`, whether it can be rendered inline.
pub(crate) fn scan_inlineable(src: &[u8]) -> Vec<bool> {
    let mut inline = vec![false; src.len()];
    let mut stack: Vec<usize> = Vec::new();
    let mut in_string = false;
    let mut escaped = false;

    for (i, &c) in src.iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                in_string = false;
            }
            continue;
        }
        match c {
            b'"' => in_string = true,
            b'[' | b'{' => {
                // Any enclosing container now holds a container, so it must
                // be expanded.
                if let Some(&parent) = stack.last() {
                    inline[parent] = false;
                }
                // Optimistically inlineable; an object is corrected below.
                inline[i] = c == b'[';
                stack.push(i);
            }
            b']' | b'}' => {
                if let Some(open) = stack.pop() {
                    // An empty container is always inline, object or not.
                    if i == open + 1 {
                        inline[open] = true;
                    }
                }
            }
            _ => {}
        }
    }
    inline
}

pub(crate) fn pretty_json(src: &[u8], indent_width: usize) -> Vec<u8> {
    let inline = scan_inlineable(src);
    // Pretty output is mostly whitespace; this is a generous single allocation.
    let mut out = Vec::with_capacity(src.len() * 2 + 64);
    // Whether each open container is being rendered inline.
    let mut stack: Vec<bool> = Vec::new();
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut escaped = false;

    let newline_indent = |out: &mut Vec<u8>, depth: usize| {
        out.push(b'\n');
        out.resize(out.len() + depth * indent_width, b' ');
    };

    for (i, &c) in src.iter().enumerate() {
        if in_string {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                in_string = false;
            }
            continue;
        }
        match c {
            b'"' => {
                in_string = true;
                out.push(c);
            }
            b'[' | b'{' => {
                let is_inline = inline[i];
                out.push(c);
                stack.push(is_inline);
                if !is_inline {
                    depth += 1;
                    newline_indent(&mut out, depth);
                }
            }
            b']' | b'}' => {
                let was_inline = stack.pop().unwrap_or(true);
                if !was_inline {
                    depth = depth.saturating_sub(1);
                    newline_indent(&mut out, depth);
                }
                out.push(c);
            }
            b',' => {
                out.push(c);
                if stack.last().copied().unwrap_or(true) {
                    out.push(b' ');
                } else {
                    newline_indent(&mut out, depth);
                }
            }
            b':' => out.extend_from_slice(b": "),
            _ => out.push(c),
        }
    }
    out
}

#[extendr]
pub(crate) fn pretty_json_impl(x: Robj, indent: Robj) -> Result<Robj> {
    let s = match x.as_str() {
        Some(s) => s,
        None => return rerr("`x` must be a single JSON string"),
    };
    let width = indent
        .as_integer()
        .or_else(|| indent.as_real().map(|f| f as i32))
        .unwrap_or(2);
    let out = pretty_json(s.as_bytes(), width.unsigned_abs() as usize);
    // Input was a valid UTF-8 &str and we only insert ASCII whitespace.
    let mut res = Robj::from(unsafe { String::from_utf8_unchecked(out) });
    res.set_class(&["json"])?;
    Ok(res)
}

/// Does `x` contain anything that must be re-encoded in R first?
///
/// The R implementation of this walked the whole object with interpreted
/// recursion, and profiling showed it was 92% of the cost of serialising a
/// list of 20000 small lists (68.5ms of 74.3ms, against 4.2ms in the actual
/// serializer). Here it touches only type tags and class attributes.
///
/// Errs towards `true`: a false positive merely runs an unnecessary R pass,
/// while a false negative would emit an unconverted object.
pub(crate) unsafe fn scan_needs_prep(x: libR_sys::SEXP, depth: u32, date_prep: bool) -> bool {
    if depth > MAX_DEPTH {
        // Past what the serializer will accept anyway, so it will raise the
        // depth error itself. Claiming prep is needed here was worse than
        // useless: it sent the whole structure through the interpreted-R
        // .prep() recursion, which exhausts R's node stack at around 1600
        // levels -- so as_json() failed on input obj_json_str_impl handles.
        // The cap was 64. The scan is a couple of microseconds and flat with
        // depth, so there was nothing to save.
        return false;
    }
    let t = typeof_sexp(x);
    if t == libR_sys::SEXPTYPE::CPLXSXP as u32 || t == libR_sys::SEXPTYPE::RAWSXP as u32 {
        return true;
    }
    if ANY_ATTRIB(x) != 0 {
        let cls = classify(x);
        // Date is excluded when the caller intends the writer to format it,
        // which is the default: only Date = "epoch" still needs R.
        let mask = if date_prep {
            CLS_NEEDS_PREP
        } else {
            CLS_NEEDS_PREP & !CLS_DATE
        };
        if cls & mask != 0 {
            return true;
        }
        // An sfc is written natively, so nothing inside it needs R.
        if cls & CLS_SFC != 0 {
            return false;
        }
    }
    if t == libR_sys::SEXPTYPE::VECSXP as u32 {
        let n = sexp_len(x);
        let elems = VECTOR_PTR_RO(x);
        for i in 0..n {
            if scan_needs_prep(*elems.add(i), depth + 1, date_prep) {
                return true;
            }
        }
    }
    false
}

#[extendr]
pub(crate) fn needs_prep_impl(x: Robj, date_prep: bool) -> bool {
    unsafe { scan_needs_prep(x.get(), 0, date_prep) }
}

// Getter/setter for the worker count. `n = NULL` just reports the current
// effective value; `n <= 0` restores environment-driven auto-detection.
// Deliberately a plain comment: rextendr copies `///` docs into
// R/extendr-wrappers.R as roxygen, which would generate an .Rd for an
// unexported internal and trip R CMD check.
#[extendr]
pub(crate) fn threads_impl(n: Robj) -> Result<Robj> {
    if !n.is_null() {
        let v = n.as_integer().or_else(|| n.as_real().map(|f| f as i32));
        match v {
            Some(v) => {
                REQUESTED_THREADS.store(
                    if v <= 0 { 0 } else { v as usize },
                    std::sync::atomic::Ordering::Relaxed,
                );
                // Returning to automatic has to re-resolve the default.
                invalidate_thread_cache();
            }
            None => return rerr("`n` must be a single number or NULL"),
        }
    }
    Ok(Robj::from(desired_threads() as i32))
}

extendr_module! {
    mod fastgeojson;
    fn sf_geojson_str_impl;
    fn df_json_str_impl;
    fn obj_json_str_impl;
    fn threads_impl;
    fn pretty_json_impl;
    fn needs_prep_impl;
}

// ------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// What the assembly is supposed to produce, written the obvious slow way.
    fn naive_join(prefix: &[u8], parts: &[Vec<u8>], suffix: &[u8]) -> Vec<u8> {
        let mut out = prefix.to_vec();
        let mut first = true;
        for p in parts {
            if p.is_empty() {
                continue;
            }
            if !first {
                out.push(b',');
            }
            out.extend_from_slice(p);
            first = false;
        }
        out.extend_from_slice(suffix);
        out
    }

    fn assemble(prefix: &[u8], parts: &[Vec<u8>], suffix: &[u8]) -> Vec<u8> {
        let (total, offs) = assembly_layout(prefix, parts, suffix);
        let mut dst = vec![0u8; total];
        unsafe { assemble_into(dst.as_mut_ptr(), total, prefix, parts, &offs, suffix, true) };
        dst
    }

    #[test]
    fn assembly_matches_a_naive_join() {
        // Each chunk's destination is a prefix sum, and the chunks are then
        // copied in parallel, so an off-by-one in the offsets or a misplaced
        // separator corrupts the whole output rather than one value. Empty
        // parts are the awkward case: they contribute no bytes AND no comma.
        let cases: Vec<Vec<Vec<u8>>> = vec![
            vec![],
            vec![b"a".to_vec()],
            vec![b"a".to_vec(), b"b".to_vec()],
            vec![vec![], b"b".to_vec()],
            vec![b"a".to_vec(), vec![]],
            vec![vec![], vec![]],
            vec![vec![], b"b".to_vec(), vec![], b"d".to_vec(), vec![]],
            vec![b"aaa".to_vec(), b"bb".to_vec(), b"c".to_vec()],
            (0..40).map(|i| vec![b'0' + (i % 10) as u8; i]).collect(),
        ];
        for parts in cases {
            for (prefix, suffix) in [
                (&b""[..], &b""[..]),
                (&b"["[..], &b"]"[..]),
                (&b"{"[..], &b"}"[..]),
                (FC_HEAD, FC_TAIL),
            ] {
                let want = naive_join(prefix, &parts, suffix);
                let (total, _) = assembly_layout(prefix, &parts, suffix);
                assert_eq!(total, want.len(), "size disagrees for {:?}", parts);
                assert_eq!(assemble(prefix, &parts, suffix), want, "for {:?}", parts);
            }
        }
    }

    #[test]
    fn assembly_offsets_point_where_the_bytes_land() {
        let parts: Vec<Vec<u8>> = vec![b"aa".to_vec(), vec![], b"ccc".to_vec(), b"d".to_vec()];
        let (total, offs) = assembly_layout(b"[", &parts, b"]");
        let out = assemble(b"[", &parts, b"]");
        assert_eq!(out.len(), total);
        for (p, &o) in parts.iter().zip(offs.iter()) {
            if p.is_empty() {
                assert_eq!(o, usize::MAX, "an empty part must be marked");
                continue;
            }
            assert_eq!(&out[o..o + p.len()], &p[..], "part is not at its offset");
        }
    }

    // ---- the pretty printer ---------------------------------------

    fn pretty(s: &str, w: usize) -> String {
        String::from_utf8(pretty_json(s.as_bytes(), w)).unwrap()
    }

    #[test]
    fn pretty_printing_follows_jsonlites_layout() {
        // Not a generic pretty-printer: an array stays on one line when it
        // holds no container, an object always expands unless empty, and the
        // separators are ", " and ": ".
        assert_eq!(pretty("[1,2,3]", 2), "[1, 2, 3]");
        assert_eq!(pretty("[]", 2), "[]");
        assert_eq!(pretty("{}", 2), "{}");
        assert_eq!(pretty(r#"{"a":1}"#, 2), "{\n  \"a\": 1\n}");
        assert_eq!(pretty("[[1,2],[3,4]]", 2), "[\n  [1, 2],\n  [3, 4]\n]");
        assert_eq!(
            pretty(r#"[{"a":1},{"a":2}]"#, 2),
            "[\n  {\n    \"a\": 1\n  },\n  {\n    \"a\": 2\n  }\n]"
        );
        assert_eq!(pretty(r#"{"a":1}"#, 4), "{\n    \"a\": 1\n}");
    }

    #[test]
    fn pretty_printing_leaves_strings_alone() {
        // Punctuation inside a string is not structure. An escaped quote must
        // not end the string, and an escaped backslash must not escape the
        // quote that follows it.
        let strip = |t: &str| -> String { t.chars().filter(|c| !c.is_whitespace()).collect() };
        for s in [
            r#"{"a":"[1,2]"}"#,
            r#"{"a":"{\"b\":1}"}"#,
            r#"{"a":"back\\"}"#,
            r#"["a,b","c:d"]"#,
            r#"{"a":"line\nbreak"}"#,
        ] {
            let out = pretty(s, 2);
            assert_eq!(strip(&out), strip(s), "for {}", s);
        }
    }

    #[test]
    fn pretty_printing_survives_truncated_input() {
        // It is only ever handed our own output, but it must not panic or read
        // past the end on anything.
        for s in ["", "[", "{", r#"{"a":"#, r#""unterminated"#, "[1,", r#"\"#] {
            let _ = pretty_json(s.as_bytes(), 2);
        }
    }

    #[test]
    fn inlineable_marks_only_containers_without_children() {
        let src = b"[[1,2],3]";
        let inline = scan_inlineable(src);
        assert_eq!(inline.len(), src.len());
        assert!(!inline[0], "the outer array holds an array, so it expands");
        assert!(inline[1], "the inner array holds only scalars");
    }
}
