// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// EXPORTS
// ------------------------------------------------------------------

#[extendr]
pub(crate) fn sf_geojson_str_impl(x: Robj, auto_unbox: bool, na: Robj, null: Robj, factor: Robj, digits: Robj, envelope: Robj, always_decimal: Robj, matrix_colmajor: Robj, as_bytes: Robj) -> Result<Robj> {
    str_state_reset();
    let rr = catch_unwind(AssertUnwindSafe(|| sf_geojson_str_impl_inner(x, auto_unbox, na, null, factor, digits, envelope, always_decimal, matrix_colmajor, as_bytes)));
    match rr { Ok(r) => r, Err(p) => rerr(format!("Internal panic: {}", panic_message(p))), }
}

pub(crate) fn sf_geojson_str_impl_inner(x: Robj, auto_unbox: bool, na: Robj, null: Robj, factor: Robj, digits: Robj, envelope: Robj, always_decimal: Robj, matrix_colmajor: Robj, as_bytes: Robj) -> Result<Robj> {
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

    let digits_opt = parse_digits_arg(digits);
    let always_decimal_flag = always_decimal.as_bool().unwrap_or(false);
    let matrix_colmajor_flag = matrix_colmajor.as_bool().unwrap_or(false);
    let config = SerializerConfig { df: DfMode::Rows, na: na_mode, null: null_mode, factor: factor_mode, auto_unbox, digits: digits_opt, always_decimal: always_decimal_flag, matrix_colmajor: matrix_colmajor_flag };

    let mut ph = PhaseTimer::new(&format!(
        "sf {} features, {} property cols, {} workers",
        n_rows, colnames.len().saturating_sub(1), desired_threads()
    ));
    let props = build_thread_safe_cols(&df, &colnames, geom_idx, n_rows, config)?;
    ph.lap("build columns");
    
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
    let mut chunk_geoms: Vec<(usize, usize, usize, ChunkGeoms)> = with_pool(|| {
        ranges
            .par_iter()
            .map(|(id, start, end)| {
                let cg = extract_geometries_chunk(
                    geom_ptr as libR_sys::SEXP,
                    sfc_type,
                    *start,
                    *end,
                    config,
                );
                (*id, *start, *end, cg)
            })
            .collect()
    });
    // Anything the workers could not describe from pure reads is finished
    // here, on the R thread. For sf's own objects that is nothing.
    for (_, start, _, cg) in chunk_geoms.iter_mut() {
        unsafe { finish_pending_geoms(cg, geom_col, *start, config) };
    }

    // One descriptor set for the whole column, so serialization can choose
    // its own boundaries rather than inheriting extraction's.
    let (batch, geoms) = merge_chunk_geoms(chunk_geoms);
    ph.lap("extract geometry");

    // Now that the sizes are known, split the rows by the work they actually
    // carry. Equal-row chunks left a layer of many small geometries and a few
    // large ones scaling 1.6x where uniform geometry scaled 15.9x.
    let work: Vec<usize> = geoms.iter().map(|g| geom_ordinates(g, &batch) + prop_work).collect();
    let wranges = weighted_ranges(&work, num_chunks);
    // Bytes per unit of work, from the same figures the flat estimate used.
    let bytes_per_work = 16usize;
    ph.lap("weigh chunks");

    let parts_res: Vec<PResult<(usize, Vec<u8>)>> = with_pool(|| wranges.par_iter().map(|(chunk_id, start, end)| {
        let (chunk_id, start, end) = (*chunk_id, *start, *end);
        let rr = catch_unwind(AssertUnwindSafe(|| {
            let chunk_work: usize = work[start..end].iter().sum();
            let cap = (chunk_work * bytes_per_work + (end - start) * 128).clamp(256, 1 << 30);
            let mut w = JsonWriter::with_capacity(cap);
            for (local_i, row_i) in (start..end).enumerate() {
                if local_i > 0 { w.push_u8(b','); }
                process_feature_parallel(&mut w, row_i, &props, &geoms[row_i], &batch, config);
            }
            (chunk_id, w.buf)
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
    if total > i32::MAX as usize { return rerr(format!("Size {} exceeds 2GB limit", total)); }

    if as_bytes.as_bool().unwrap_or(false) {
        let r = match assemble_into_raw(prefix, &chunks, &offs, suffix, total) {
            Ok(r) => r,
            Err(e) => return rerr(e),
        };
        ph.lap("assemble into R (raw)");
        return Ok(r);
    }
    let final_out = assemble_into_vec(prefix, &chunks, &offs, suffix, total);
    ph.lap("assemble chunks");
    let result_str = match finish_json_string(final_out) {
        Ok(s) => s,
        Err(e) => return rerr(e),
    };
    let mut robj = Robj::from(result_str);
    robj.set_class(&["geojson", "json"])?;
    ph.lap("copy into R");
    Ok(robj)
}

#[extendr]
pub(crate) fn df_json_str_impl(x: Robj, auto_unbox: bool, dataframe: String, na: Robj, null: Robj, factor: Robj, digits: Robj, always_decimal: Robj, matrix_colmajor: Robj, as_bytes: Robj) -> Result<Robj> {
    str_state_reset();
    let rr = catch_unwind(AssertUnwindSafe(|| df_json_str_impl_inner(x, auto_unbox, dataframe, na, null, factor, digits, always_decimal, matrix_colmajor, as_bytes)));
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

pub(crate) struct PhaseTimer {
    pub(crate) t: std::time::Instant,
    pub(crate) on: bool,
}

impl PhaseTimer {
    pub(crate) fn new(what: &str) -> Self {
        let on = profiling();
        if on {
            eprintln!("fastgeojson: {}", what);
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
            eprintln!(
                "fastgeojson:   {:<20} {:>9.2} ms",
                label,
                (now - self.t).as_secs_f64() * 1000.0
            );
            self.t = now;
        }
    }
}

// ------------------------------------------------------------------
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
            std::ptr::copy_nonoverlapping(p.as_ptr(), dst.add(offs[i]), p.len());
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
        unsafe fn write(&self, at: usize, src: &[u8]) {
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.0.add(at), src.len());
        }
    }
    let base = Dst(dst);
    with_pool(|| {
        parts
            .par_iter()
            .zip(offs.par_iter())
            .for_each(|(p, &o)| {
                if o == usize::MAX {
                    return;
                }
                unsafe { base.write(o, p) };
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
        assemble_into(libR_sys::RAW(v) as *mut u8, total, prefix, parts, offs, suffix);
        let r = Robj::from_sexp(v);
        libR_sys::Rf_unprotect(1);
        Ok(r)
    }
}

/// The same assembly into a fresh `Vec`, for the character path.
pub(crate) fn assemble_into_vec(
    prefix: &[u8],
    parts: &[Vec<u8>],
    offs: &[usize],
    suffix: &[u8],
    total: usize,
) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(total);
    unsafe {
        assemble_into(out.as_mut_ptr(), total, prefix, parts, offs, suffix);
        // Every one of the `total` bytes was just written.
        out.set_len(total);
    }
    out
}

pub(crate) fn df_json_str_impl_inner(x: Robj, auto_unbox: bool, dataframe: String, na: Robj, null: Robj, factor: Robj, digits: Robj, always_decimal: Robj, matrix_colmajor: Robj, as_bytes: Robj) -> Result<Robj> {
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
        let mut robj = Robj::from(match finish_json_string(buf) {
            Ok(s) => s,
            Err(e) => return rerr(e),
        });
        robj.set_class(&["json"])?;
        return Ok(robj);
    }

    let df_list = x.as_list().ok_or_else(|| Error::Other("Invalid df structure".to_string()))?;
    let colnames = unsafe { utf8_names(x.get(), n_cols) }
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

    let digits_opt = parse_digits_arg(digits);
    let always_decimal_flag = always_decimal.as_bool().unwrap_or(false);
    let matrix_colmajor_flag = matrix_colmajor.as_bool().unwrap_or(false);
    let config = SerializerConfig { df: df_mode, na: na_mode, null: null_mode, factor: factor_mode, auto_unbox, digits: digits_opt, always_decimal: always_decimal_flag, matrix_colmajor: matrix_colmajor_flag };

    let mut ph = PhaseTimer::new(&format!(
        "data.frame {} rows x {} cols, {} workers",
        n_rows, n_cols, desired_threads()
    ));
    let props = build_thread_safe_cols(&df_list, &colnames, usize::MAX, n_rows, config)?;
    ph.lap("build columns");

	let final_out = if df_mode == DfMode::Columns {
        let column_parts: Vec<PResult<Vec<u8>>> = with_pool(|| props.into_par_iter().map(|(key, col)| {
            let mut w = JsonWriter::with_capacity(n_rows * 16);
            w.push_bytes(&key);
            w.push_u8(b'[');
            for r in 0..n_rows {
                if r > 0 { w.push_u8(b','); }
                write_col_value(&mut w, r, &col, config);
            }
            w.push_u8(b']');
            Ok(w.buf)
        }).collect());

        ph.lap("serialize (parallel)");
        let mut chunks: Vec<Vec<u8>> = Vec::with_capacity(column_parts.len());
        for part in column_parts {
            match part { Ok(p) => chunks.push(p), Err(e) => return rerr(e), }
        }
        let (total, offs) = assembly_layout(b"{", &chunks, b"}");
        if total > i32::MAX as usize { return rerr(format!("Size {} exceeds 2GB limit", total)); }

        if as_bytes.as_bool().unwrap_or(false) {
            let r = match assemble_into_raw(b"{", &chunks, &offs, b"}", total) {
                Ok(r) => r,
                Err(e) => return rerr(e),
            };
            ph.lap("assemble into R (raw)");
            return Ok(r);
        }
        let out = assemble_into_vec(b"{", &chunks, &offs, b"}", total);
        ph.lap("assemble columns");
        out
    } else {
        if n_rows == 0 { let mut r = Robj::from("[]"); r.set_class(&["json"])?; return Ok(r); }
        // Row count alone is the wrong measure: a 2000-row x 200-column frame
        // is more work than a 200000-row x 1-column one, but the old
        // `n_rows < 10000` test put the first in a single chunk.
        let row_work = estimate_row_work(&props);
        let chunk_size = rows_per_chunk(n_rows, row_work);
        // estimate_row_work is roughly a quarter of the bytes a row occupies,
        // so this sizes each chunk buffer from the actual columns instead of a
        // flat 128 bytes per row. Being 20x out in either direction costs
        // either untouched pages or a realloc-and-copy of the whole chunk.
        let est_row_bytes = (row_work * 8).clamp(16, 1 << 16);
        ph.lap("plan chunks");
        let num_chunks = (n_rows + chunk_size - 1) / chunk_size;
        let ranges: Vec<(usize, usize, usize)> = (0..num_chunks).map(|id| (id, id * chunk_size, (id * chunk_size + chunk_size).min(n_rows))).collect();

        let parts_res: Vec<PResult<(usize, Vec<u8>)>> = with_pool(|| ranges.into_par_iter().map(|(chunk_id, start, end)| {
            let rr = catch_unwind(AssertUnwindSafe(|| {
                let mut w = JsonWriter::with_capacity((end - start) * est_row_bytes);
                for i in start..end {
                    if i > start { w.push_u8(b','); }
                    if df_mode == DfMode::Values {
                        process_row_values(&mut w, i, &props, config);
                    } else {
                        process_row_generic(&mut w, i, &props, config);
                    }
                }
                (chunk_id, w.buf)
            }));
            match rr { Ok(v) => Ok(v), Err(p) => Err(format!("Worker panic: {}", panic_message(p))), }
        }).collect());

        ph.lap("serialize (parallel)");
        let mut parts: Vec<(usize, Vec<u8>)> = Vec::with_capacity(parts_res.len());
        for r in parts_res { match r { Ok(v) => parts.push(v), Err(msg) => return rerr(msg), } }
        parts.sort_by_key(|(id, _)| *id);

        let chunks: Vec<Vec<u8>> = parts.into_iter().map(|(_, v)| v).collect();
        let (total, offs) = assembly_layout(b"[", &chunks, b"]");
        if total > i32::MAX as usize { return rerr(format!("Size {} exceeds 2GB limit", total)); }

        // With as_bytes the destination is R's own vector, so the chunks are
        // written into it directly and nothing is copied twice.
        if as_bytes.as_bool().unwrap_or(false) {
            let r = match assemble_into_raw(b"[", &chunks, &offs, b"]", total) {
                Ok(r) => r,
                Err(e) => return rerr(e),
            };
            ph.lap("assemble into R (raw)");
            return Ok(r);
        }
        let out = assemble_into_vec(b"[", &chunks, &offs, b"]", total);
        ph.lap("assemble chunks");
        out
    };

    if final_out.len() > i32::MAX as usize { return rerr(format!("Size {} exceeds 2GB limit", final_out.len())); }
    
    if as_bytes.as_bool().unwrap_or(false) {
        let r = match finish_json_raw(final_out) { Ok(r) => r, Err(e) => return rerr(e) };
        ph.lap("copy into R (raw)");
        return Ok(r);
    }
    let result_str = match finish_json_string(final_out) {
        Ok(s) => s,
        Err(e) => return rerr(e),
    };
    ph.lap("utf8 finish");
    let mut robj = Robj::from(result_str);
    robj.set_class(&["json"])?;
    ph.lap("copy into R");
    Ok(robj)
}

#[extendr]
pub(crate) fn obj_json_str_impl(x: Robj, auto_unbox: bool, na: Robj, null: Robj, factor: Robj, digits: Robj, always_decimal: Robj, matrix_colmajor: Robj, as_bytes: Robj) -> Result<Robj> {
    str_state_reset();
    let est_size = unsafe { sexp_len(x.get()) } * 16 + 64;
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

    let digits_opt = parse_digits_arg(digits);
    let always_decimal_flag = always_decimal.as_bool().unwrap_or(false);
    let matrix_colmajor_flag = matrix_colmajor.as_bool().unwrap_or(false);
    let config = SerializerConfig { df: DfMode::Rows, na: na_mode, null: null_mode, factor: factor_mode, auto_unbox, digits: digits_opt, always_decimal: always_decimal_flag, matrix_colmajor: matrix_colmajor_flag };

    unsafe { serialize_sexp_to_json_buffer(x.get(), &mut w.buf, config, 0); }
    if w.buf.len() > i32::MAX as usize { return rerr(format!("Size {} exceeds 2GB limit", w.buf.len())); }
    
    if as_bytes.as_bool().unwrap_or(false) {
        return match finish_json_raw(w.buf) { Ok(r) => Ok(r), Err(e) => rerr(e) };
    }
    let result_str = match finish_json_string(w.buf) {
        Ok(s) => s,
        Err(e) => return rerr(e),
    };
    let mut res = Robj::from(result_str);
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
    if depth > 64 {
        // Too deep to be worth scanning; let R's slower path decide.
        return true;
    }
    let t = typeof_sexp(x);
    if t == libR_sys::SEXPTYPE::CPLXSXP as u32 || t == libR_sys::SEXPTYPE::RAWSXP as u32 {
        return true;
    }
    if ATTRIB(x) != libR_sys::R_NilValue {
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
            Some(v) => REQUESTED_THREADS.store(
                if v <= 0 { 0 } else { v as usize },
                std::sync::atomic::Ordering::Relaxed,
            ),
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
