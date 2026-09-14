// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// RECURSIVE SERIALIZER
// ------------------------------------------------------------------

// ---- parallel numeric runs ---------------------------------------
//
// The data.frame path has been parallel for a long time; this one never
// entered the pool at all, so a bare vector or a matrix handed straight to
// as_json() stayed serial however large it was. Measured with as_bytes = TRUE,
// to keep R's string interning out of the comparison:
//
//   200000 doubles, bare vector          21.10 ns/value
//   200000 doubles, 20000x10 matrix      22.77 ns/value
//   200000 doubles, matrix COLUMN         4.53 ns/value
//
// The last one is the same formatting reached through the pooled column
// writer, so the whole 5x was parallelism this path was not taking.
//
// Only the three types that are pure pointer reads go this way. A character
// vector needs CHARSXP handling, whose encoding branch allocates on R's vmax
// stack and marks a thread-local, and a list needs the recursion itself; both
// stay serial. REAL/INTEGER/LOGICAL are called on the R thread before any
// worker starts, so an ALTREP vector materialises safely and the workers only
// ever see a plain buffer.

/// Chunk ranges for `0..n`, or None when the job is too small to spread.
///
/// `bytes_each` is the rough output width of one element; `rows_per_chunk`
/// counts work in units of roughly four bytes.
fn par_ranges(n: usize, bytes_each: usize) -> Option<Vec<(usize, usize, usize)>> {
    let work = (bytes_each / 4).max(1);
    // The same threshold rows_per_chunk applies, but tested before anything
    // that costs: this is asked for every scalar in a nested list, and the
    // atomic load behind desired_threads() alone measured 5% on a list of
    // 20000 two-element lists. Testing element *count* instead was wrong -- a
    // 1000 x 20 x 10 array has only 1000 outer elements but 200 values under
    // each, and rejecting it on the count alone made it 5.8x slower.
    if n.saturating_mul(work) < MIN_PARALLEL_WORK || desired_threads() <= 1 {
        return None;
    }
    let chunk = rows_per_chunk(n, work);
    if chunk >= n {
        return None;
    }
    let nc = (n + chunk - 1) / chunk;
    Some(
        (0..nc)
            .map(|k| (k, k * chunk, ((k + 1) * chunk).min(n)))
            .collect(),
    )
}

/// Serialises `ranges` in the pool, `write` filling one chunk at a time.
fn par_parts(
    ranges: Vec<(usize, usize, usize)>,
    bytes_each: usize,
    write: impl Fn(&mut Vec<u8>, usize, usize) + Send + Sync,
) -> Vec<Vec<u8>> {
    let mut parts: Vec<(usize, Vec<u8>)> = with_pool(|| {
        ranges
            .into_par_iter()
            .map(|(id, s, e)| {
                let mut b: Vec<u8> = Vec::with_capacity((e - s) * bytes_each + 16);
                write(&mut b, s, e);
                (id, b)
            })
            .collect()
    });
    parts.sort_by_key(|(id, _)| *id);
    parts.into_iter().map(|(_, v)| v).collect()
}

/// Appends `chunks` to `buf`, comma separated, inside `open` and `close`.
fn join_chunks(buf: &mut Vec<u8>, chunks: &[Vec<u8>], open: u8, close: u8) {
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    buf.reserve(total + chunks.len() + 2);
    buf.push(open);
    let mut first = true;
    for c in chunks {
        if c.is_empty() {
            continue;
        }
        if !first {
            buf.push(b',');
        }
        buf.extend_from_slice(c);
        first = false;
    }
    buf.push(close);
}

/// One double, rendered exactly as the data.frame writer renders it.
#[inline(always)]
pub(crate) fn write_real_elem(b: &mut Vec<u8>, v: f64, config: SerializerConfig) {
    // `is_na_real || is_nan_real` is just `is_nan`, so the whole guard reduces
    // to one is_finite test, and the common path does no NA inspection at all.
    if v.is_finite() {
        write_f64_json(b, v, config);
    } else if config.na == NaMode::String || config.na == NaMode::Smart {
        if v == f64::INFINITY {
            b.extend_from_slice(b"\"Inf\"");
        } else if v == f64::NEG_INFINITY {
            b.extend_from_slice(b"\"-Inf\"");
        } else if unsafe { is_nan_real(v) } {
            b.extend_from_slice(b"\"NaN\"");
        } else {
            b.extend_from_slice(b"\"NA\"");
        }
    } else {
        b.extend_from_slice(b"null");
    }
}

#[inline(always)]
pub(crate) fn write_int_elem(b: &mut Vec<u8>, v: i32, config: SerializerConfig) {
    if !unsafe { is_na_int(v) } {
        let mut tmp = itoa::Buffer::new();
        b.extend_from_slice(tmp.format(v).as_bytes());
    } else if config.na == NaMode::String || config.na == NaMode::Smart {
        b.extend_from_slice(b"\"NA\"");
    } else {
        b.extend_from_slice(b"null");
    }
}

#[inline(always)]
pub(crate) fn write_lgl_elem(b: &mut Vec<u8>, v: i32, config: SerializerConfig) {
    if !unsafe { is_na_int(v) } {
        b.extend_from_slice(if v != 0 { b"true" } else { b"false" });
    } else if config.na == NaMode::String {
        b.extend_from_slice(b"\"NA\"");
    } else {
        b.extend_from_slice(b"null");
    }
}

/// One nested array dimension, brackets included.
///
/// Hoisted out of `serialize_sexp_to_json_buffer`, where it was a nested fn,
/// so `write_dim_range` can call it for a single outer slice.
unsafe fn write_recursive(
    buf: &mut Vec<u8>,
    dims: &[usize],
    strides: &[usize],
    depth: usize,
    offset: usize,
    write_fn: &impl Fn(usize, &mut Vec<u8>),
) {
    let n = dims[depth];
    let stride = strides[depth];
    buf.push(b'[');
    for i in 0..n {
        if i > 0 {
            buf.push(b',');
        }
        let next_offset = offset + i * stride;
        if depth == dims.len() - 1 {
            write_fn(next_offset, buf);
        } else {
            write_recursive(buf, dims, strides, depth + 1, next_offset, write_fn);
        }
    }
    buf.push(b']');
}

/// The elements of the outermost array dimension, comma separated and with no
/// enclosing brackets.
///
/// The separator test is `i > s`, not `i > 0`, so a chunk that starts part-way
/// along does not open with one. Splitting the outermost dimension this way is
/// what lets a matrix be written by the pool.
unsafe fn write_dim_range(
    buf: &mut Vec<u8>,
    dims: &[usize],
    strides: &[usize],
    s: usize,
    e: usize,
    write_fn: &(impl Fn(usize, &mut Vec<u8>) + Sync),
) {
    for i in s..e {
        if i > s {
            buf.push(b',');
        }
        let off = i * strides[0];
        if dims.len() == 1 {
            write_fn(off, buf);
        } else {
            write_recursive(buf, dims, strides, 1, off, write_fn);
        }
    }
}

/// `write_dim_range` over the whole outermost dimension, in the pool when it
/// is long enough to pay for one.
unsafe fn write_dim_all(
    buf: &mut Vec<u8>,
    dims: &[usize],
    strides: &[usize],
    bytes_each: usize,
    write_fn: &(impl Fn(usize, &mut Vec<u8>) + Sync + Send),
) {
    let n = dims[0];
    // Everything under one outer index, so the estimate matches what a chunk
    // actually writes rather than counting each slice as a single value.
    let per: usize = dims[1..].iter().product::<usize>().max(1);
    let each = bytes_each.saturating_mul(per);
    match par_ranges(n, each) {
        Some(ranges) => {
            let chunks = par_parts(ranges, each, |b, s, e| {
                write_dim_range(b, dims, strides, s, e, write_fn)
            });
            join_chunks(buf, &chunks, b'[', b']');
        }
        None => {
            buf.push(b'[');
            write_dim_range(buf, dims, strides, 0, n, write_fn);
            buf.push(b']');
        }
    }
}


pub(crate) unsafe fn serialize_sexp_to_json_buffer(x: libR_sys::SEXP, buf: &mut Vec<u8>, config: SerializerConfig, depth: u32) {
    if depth > MAX_DEPTH {
        // Unbounded recursion here killed the process outright rather than
        // raising a condition R could catch.
        str_state_mark(STR_DEPTH_ERROR);
        buf.extend_from_slice(b"null");
        return;
    }
    if x == libR_sys::R_NilValue {
        if config.null == NullMode::Null { buf.extend_from_slice(b"null"); }
        else { buf.extend_from_slice(b"{}"); }
        return;
    }
    // A plain vector inside a list has no attributes at all, so one check
    // skips both the class walk and the dim walk below.
    let has_attrs = ANY_ATTRIB(x) != 0;
    let cls = if has_attrs { classify(x) } else { 0 };

    // 0. Geometry columns.
    //
    // An `sfc` renders as an array of typed GeoJSON geometry objects wherever
    // it appears -- bare, in a list, at any nesting depth, or inside a
    // data.frame list column -- not only as an sf object's designated
    // geometry column. Without this it degraded to bare coordinate arrays
    // like [0,0] and the geometry type was lost. Its length is unrelated to
    // any enclosing frame's row count.
    if cls & CLS_SFC != 0 {
        let n = sexp_len(x);
        buf.push(b'[');
        for i in 0..n {
            if i > 0 {
                buf.push(b',');
            }
            render_geometry_to_bytes(libR_sys::VECTOR_ELT(x, i as isize), buf, config, 0);
        }
        buf.push(b']');
        return;
    }

    // 1. Array / Matrix Handling
    if has_attrs && cls & CLS_DATA_FRAME == 0 {
        let dim_attr = libR_sys::Rf_getAttrib(x, libR_sys::R_DimSymbol);
        // The type check matters: INTEGER() on a non-INTSXP raises an R error,
        // which longjmps over these frames and skips whatever they hold. R
        // coerces `dim` on assignment, so no ordinary object arrives with a
        // double one, but the column builder has always checked and this had
        // not -- the same inconsistency as the unguarded REAL() on geometry.
        if dim_attr != libR_sys::R_NilValue
            && sexp_len(dim_attr) > 0
            && typeof_sexp(dim_attr) == libR_sys::SEXPTYPE::INTSXP as u32
        {
            let dim_sexp = libR_sys::INTEGER(dim_attr);
            let n_dims = sexp_len(dim_attr);
            let mut dims = Vec::with_capacity(n_dims);
            let mut strides = Vec::with_capacity(n_dims);
            let mut current_stride = 1;
            for i in 0..n_dims {
                let d = *dim_sexp.add(i) as usize;
                dims.push(d);
                strides.push(current_stride);
                current_stride *= d;
            }
            if config.matrix_colmajor {
                // jsonlite's matrix = "columnmajor" nests by the last
                // dimension first: a 2x3 matrix becomes [[1,2],[3,4],[5,6]]
                // instead of [[1,3,5],[2,4,6]].
                dims.reverse();
                strides.reverse();
            }

            if cls & CLS_FACTOR != 0 && typeof_sexp(x) == libR_sys::SEXPTYPE::INTSXP as u32 && config.factor == FactorMode::String {
                let levels_sexp = levels_of(x);
                if levels_sexp != libR_sys::R_NilValue {
                    let n_levels = sexp_len(levels_sexp);
                    let p = libR_sys::INTEGER(x);
                    let writer = |idx: usize, b: &mut Vec<u8>| {
                        let v = *p.add(idx);
                        if is_na_int(v) || v < 1 {
                            if config.na == NaMode::String { b.extend_from_slice(b"\"NA\""); }
                            else { b.extend_from_slice(b"null"); }
                        } else {
                            let lvl_idx = (v - 1) as usize;
                            if lvl_idx < n_levels {
                                let s = libR_sys::STRING_ELT(levels_sexp, lvl_idx as isize);
                                if let Some(bytes) = charsxp_to_utf8_bytes(s) { escape_json_string_into(b, bytes); }
                                else { b.extend_from_slice(b"null"); }
                            } else { b.extend_from_slice(b"null"); }
                        }
                    };
                    write_recursive(buf, &dims, &strides, 0, 0, &writer);
                    return;
                }
            }

            let r_type = typeof_sexp(x);
            match r_type {
                 t if t == libR_sys::SEXPTYPE::INTSXP as u32 => {
                     // The pointer is read here, on the R thread; only its
                     // address travels, because a raw pointer is not Sync.
                     let pu = libR_sys::INTEGER(x) as usize;
                     let writer = move |idx: usize, b: &mut Vec<u8>| {
                         write_int_elem(b, *(pu as *const i32).add(idx), config);
                     };
                     write_dim_all(buf, &dims, &strides, 12, &writer);
                     return;
                 },
                 t if t == libR_sys::SEXPTYPE::REALSXP as u32 => {
                     let pu = libR_sys::REAL(x) as usize;
                     let writer = move |idx: usize, b: &mut Vec<u8>| {
                         write_real_elem(b, *(pu as *const f64).add(idx), config);
                     };
                     write_dim_all(buf, &dims, &strides, 26, &writer);
                     return;
                 },
                 t if t == libR_sys::SEXPTYPE::LGLSXP as u32 => {
                     let pu = libR_sys::LOGICAL(x) as usize;
                     let writer = move |idx: usize, b: &mut Vec<u8>| {
                         write_lgl_elem(b, *(pu as *const i32).add(idx), config);
                     };
                     write_dim_all(buf, &dims, &strides, 6, &writer);
                     return;
                 },
                 t if t == libR_sys::SEXPTYPE::STRSXP as u32 => {
                     let writer = |idx: usize, b: &mut Vec<u8>| {
                         let s_sexp = libR_sys::STRING_ELT(x, idx as isize);
                         if is_na_string(s_sexp) {
                             if config.na == NaMode::String { b.extend_from_slice(b"\"NA\""); }
                             else { b.extend_from_slice(b"null"); }
                         } else if let Some(bytes) = charsxp_to_utf8_bytes(s_sexp) {
                             escape_json_string_into(b, bytes);
                         } else { b.extend_from_slice(b"null"); }
                     };
                     write_recursive(buf, &dims, &strides, 0, 0, &writer);
                     return;
                 },
                 t if t == libR_sys::SEXPTYPE::VECSXP as u32 => {
                     let writer = |idx: usize, b: &mut Vec<u8>| {
                         let val_sexp = libR_sys::VECTOR_ELT(x, idx as isize);
                         serialize_sexp_to_json_buffer(val_sexp, b, config, depth + 1);
                     };
                     write_recursive(buf, &dims, &strides, 0, 0, &writer);
                     return;
                 },
                 _ => {}
            }
        }
    }

    // 2. Linear Vector / Data Frame Handling
    if cls & CLS_FACTOR != 0 && typeof_sexp(x) == libR_sys::SEXPTYPE::INTSXP as u32 {
        if config.factor == FactorMode::String {
            let do_unbox = config.auto_unbox && sexp_len(x) == 1 && cls & CLS_ASIS == 0;
            let levels_sexp = levels_of(x);
            if levels_sexp != libR_sys::R_NilValue {
                let n_levels = sexp_len(levels_sexp);
                let n = sexp_len(x);
                let p = libR_sys::INTEGER(x);
                if !do_unbox { buf.push(b'['); }
                for i in 0..n {
                    if i > 0 { buf.push(b','); }
                    let v = *p.add(i);
                    if is_na_int(v) || v < 1 { 
                        if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                        else { buf.extend_from_slice(b"null"); }
                    } else {
                        let idx = (v - 1) as usize;
                        if idx < n_levels {
                            let level_charsxp = libR_sys::STRING_ELT(levels_sexp, idx as isize);
                            if let Some(bytes) = charsxp_to_utf8_bytes(level_charsxp) { escape_json_string_into(buf, bytes); }
                            else { buf.extend_from_slice(b"null"); }
                        } else { buf.extend_from_slice(b"null"); }
                    }
                }
                if !do_unbox { buf.push(b']'); }
                return;
            }
        }
    }

    if cls & CLS_DATE != 0 {
        // Formatted here rather than by R. Reached for a bare Date vector and
        // for a Date nested anywhere inside a list, which is why it has to
        // honour auto_unbox and AsIs the same way the atomic arms below do.
        let r_type = typeof_sexp(x);
        if r_type == libR_sys::SEXPTYPE::REALSXP as u32
            || r_type == libR_sys::SEXPTYPE::INTSXP as u32
        {
            let n = sexp_len(x);
            let do_unbox = config.auto_unbox && n == 1 && cls & CLS_ASIS == 0;
            if !do_unbox {
                buf.push(b'[');
            }
            if r_type == libR_sys::SEXPTYPE::REALSXP as u32 {
                let p = libR_sys::REAL(x);
                for i in 0..n {
                    if i > 0 {
                        buf.push(b',');
                    }
                    write_date_cell(buf, date_cell(*p.add(i)), config.na);
                }
            } else {
                let p = libR_sys::INTEGER(x);
                for i in 0..n {
                    if i > 0 {
                        buf.push(b',');
                    }
                    write_date_cell(buf, date_cell_i32(*p.add(i)), config.na);
                }
            }
            if !do_unbox {
                buf.push(b']');
            }
            return;
        }
    }

    if cls & CLS_FGJTIME != 0 && typeof_sexp(x) == libR_sys::SEXPTYPE::REALSXP as u32 {
        let fmt = fgj_fmt_code(x).unwrap_or(TFMT_SPACE);
        let n = sexp_len(x);
        let do_unbox = config.auto_unbox && n == 1 && cls & CLS_ASIS == 0;
        let p = libR_sys::REAL(x);
        if !do_unbox {
            buf.push(b'[');
        }
        for i in 0..n {
            if i > 0 {
                buf.push(b',');
            }
            write_time_cell(buf, *p.add(i), fmt, config.na);
        }
        if !do_unbox {
            buf.push(b']');
        }
        return;
    }

    if cls & CLS_POSIXT != 0 {
        // POSIXt is pre-encoded in R, because resolving a time zone needs R's
        // own database; this is the defensive path, so paying for an Robj here
        // costs nothing measurable.
        if let Ok(char_robj) = call!("format", Robj::from_sexp(x)) {
            serialize_sexp_to_json_buffer(char_robj.get(), buf, config, depth + 1);
            return;
        }
    }

    let r_type = typeof_sexp(x);
    // `sexp_len` of a data.frame is its COLUMN count, so a one-column frame
    // used to satisfy this and lose the brackets around its row array,
    // emitting `{"a":1},{"a":2},{"a":3}` with no enclosing `[]` -- structurally
    // invalid JSON. jsonlite never unboxes a data.frame, only the `scalar`
    // class, so exclude them outright.
    let do_unbox = config.auto_unbox
        && sexp_len(x) == 1
        && cls & CLS_ASIS == 0
        && cls & CLS_DATA_FRAME == 0;

    if r_type == libR_sys::SEXPTYPE::INTSXP as u32 {
        let n = sexp_len(x);
        let pu = libR_sys::INTEGER(x) as usize;
        if !do_unbox {
            if let Some(ranges) = par_ranges(n, 12) {
                let chunks = par_parts(ranges, 12, |b, s, e| {
                    for i in s..e {
                        if i > s { b.push(b','); }
                        write_int_elem(b, *(pu as *const i32).add(i), config);
                    }
                });
                join_chunks(buf, &chunks, b'[', b']');
                return;
            }
            buf.push(b'[');
        }
        let p = pu as *const i32;
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            write_int_elem(buf, *p.add(i), config);
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }

    if r_type == libR_sys::SEXPTYPE::REALSXP as u32 {
        let n = sexp_len(x);
        let pu = libR_sys::REAL(x) as usize;
        if !do_unbox {
            if let Some(ranges) = par_ranges(n, 26) {
                let chunks = par_parts(ranges, 26, |b, s, e| {
                    for i in s..e {
                        if i > s { b.push(b','); }
                        write_real_elem(b, *(pu as *const f64).add(i), config);
                    }
                });
                join_chunks(buf, &chunks, b'[', b']');
                return;
            }
            buf.push(b'[');
        }
        let p = pu as *const f64;
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            write_real_elem(buf, *p.add(i), config);
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }

    if r_type == libR_sys::SEXPTYPE::LGLSXP as u32 {
        let n = sexp_len(x);
        let pu = libR_sys::LOGICAL(x) as usize;
        if !do_unbox {
            if let Some(ranges) = par_ranges(n, 6) {
                let chunks = par_parts(ranges, 6, |b, s, e| {
                    for i in s..e {
                        if i > s { b.push(b','); }
                        write_lgl_elem(b, *(pu as *const i32).add(i), config);
                    }
                });
                join_chunks(buf, &chunks, b'[', b']');
                return;
            }
            buf.push(b'[');
        }
        let p = pu as *const i32;
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            write_lgl_elem(buf, *p.add(i), config);
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }


    // [MODIFICATION: Passthrough Support for Generic Vectors]
    if r_type == libR_sys::SEXPTYPE::STRSXP as u32 {
        let is_json = splices_verbatim(cls, config);
        let n = sexp_len(x);
        // jsonlite's asJSON("json") returns the text verbatim and never
        // collapses it into an array, so a length-one `json` value must not
        // gain surrounding brackets.
        let json_scalar = is_json && n == 1;
        let do_unbox = do_unbox || json_scalar;
        if !do_unbox { buf.push(b'['); }
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let s_sexp = libR_sys::STRING_ELT(x, i as isize);
            if is_na_string(s_sexp) { 
                if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            }
            else if let Some(bytes) = charsxp_to_utf8_bytes(s_sexp) { 
                if is_json { buf.extend_from_slice(bytes); } // Passthrough
                else { escape_json_string_into(buf, bytes); } // Escape
            }
            else { buf.extend_from_slice(b"null"); }
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }

    if r_type == libR_sys::SEXPTYPE::VECSXP as u32 {
        if cls & CLS_DATA_FRAME != 0 {
            let n_cols = sexp_len(x);
            // A frame is a frame wherever it sits, so it goes through the same
            // column descriptors and the same pooled row loop the top-level
            // entry point uses. The per-cell loop below is what remains for
            // the two shapes that carry no cells at all -- no columns, or no
            // rows -- and for a frame the builder rejects. See `dfwrite` for
            // the nine defects that came of having two implementations.
            if write_df_into(x, buf, config, depth) {
                return;
            }
            if config.df == DfMode::Columns {
                buf.push(b'{');
                let names_sym = libR_sys::R_NamesSymbol;
                let names_sexp = libR_sys::Rf_getAttrib(x, names_sym);
                let has_names = names_sexp != libR_sys::R_NilValue && sexp_len(names_sexp) == n_cols;
                // Cold path -- the builder declined this frame -- so the
                // names are checked up front rather than as they are written.
                let fixed = if has_names && wide_names_need_fixing(names_sexp, n_cols) {
                    Some(mangled_names(names_sexp, n_cols))
                } else { None };
                let mut first = true;
                for c in 0..n_cols {
                    if !first { buf.push(b','); }
                    if let Some(ref v) = fixed {
                        escape_json_string_into(buf, &v[c]);
                    } else if has_names {
                        let key_charsxp = libR_sys::STRING_ELT(names_sexp, c as isize);
                        if !is_na_string(key_charsxp) {
                            if let Some(key_bytes) = charsxp_to_utf8_bytes(key_charsxp) { escape_json_string_into(buf, key_bytes); }
                            else { buf.extend_from_slice(b"\"\""); }
                        } else { buf.extend_from_slice(b"\"\""); }
                    } else { buf.extend_from_slice(b"\"\""); }
                    buf.push(b':');
                    let col_sexp = libR_sys::VECTOR_ELT(x, c as isize);
                    buf.push(b'[');
                    let n_rows_inner = sexp_len(col_sexp);
                    for r in 0..n_rows_inner {
                        if r > 0 { buf.push(b','); }
                        serialize_element_at_index(col_sexp, r, buf, config, depth + 1);
                    }
                    buf.push(b']');
                    first = false;
                }
                buf.push(b'}');
                return;
            }

            let n_rows = get_df_nrows(x);
            let names_sym = libR_sys::R_NamesSymbol;
            let names_sexp = libR_sys::Rf_getAttrib(x, names_sym);
            let has_names = names_sexp != libR_sys::R_NilValue && sexp_len(names_sexp) == n_cols;
            // Cold path, as above: checked once, outside the row loop.
            let fixed = if has_names && wide_names_need_fixing(names_sexp, n_cols) {
                Some(mangled_names(names_sexp, n_cols))
            } else { None };
            // ATTRIB is not API, and this is the R thread, so getAttrib is fine here.
    // It expands the compact c(NA, -n) form to 1..n, which costs an INTSXP of
    // n and changes nothing: is_default_rownames tests for STRSXP.
    let rn_sexp = libR_sys::Rf_getAttrib(x, libR_sys::R_RowNamesSymbol);
    // Loop-invariant: hoisted out of the row loop below, where it was an O(n)
    // scan per row -- Theta(n^2) overall, measured at 392 ms for a 32,000-row
    // nested frame and growing 4x per doubling.
    let emit_row_names = config.rownames != ROWNAMES_NEVER && !is_default_rownames(rn_sexp);
    let rn_len = sexp_len(rn_sexp);

            if !do_unbox { buf.push(b'['); }
            for r in 0..n_rows {
                if r > 0 { buf.push(b','); }
                buf.push(b'{');
                let mut needs_comma = false;
                for c in 0..n_cols {
                    let col_sexp = libR_sys::VECTOR_ELT(x, c as isize);
                    let col_len = sexp_len(col_sexp);
                    if r < col_len {
                        if needs_comma { buf.push(b','); }
                        if let Some(ref v) = fixed {
                            escape_json_string_into(buf, &v[c]);
                        } else if has_names {
                            let key_charsxp = libR_sys::STRING_ELT(names_sexp, c as isize);
                            if !is_na_string(key_charsxp) {
                                if let Some(key_bytes) = charsxp_to_utf8_bytes(key_charsxp) { escape_json_string_into(buf, key_bytes); }
                                else { buf.extend_from_slice(b"\"\""); }
                            } else { buf.extend_from_slice(b"\"\""); }
                        } else { buf.extend_from_slice(b"\"\""); }
                        buf.push(b':');
                        serialize_element_at_index(col_sexp, r, buf, config, depth + 1);
                        needs_comma = true;
                    }
                }
                // A nested data.frame carries its row names too, exactly as a
                // top-level one does. Omitting this dropped `_row` for any
                // frame that was not the outermost object.
                if emit_row_names && r < rn_len {
                    if needs_comma { buf.push(b','); }
                    buf.extend_from_slice(br#""_row":"#);
                    write_rowname_at(rn_sexp, r, buf);
                }
                buf.push(b'}');
            }
            if !do_unbox { buf.push(b']'); }
            return;
        }

        let n = sexp_len(x);
        let names_sym = libR_sys::R_NamesSymbol;
        let names_sexp = libR_sys::Rf_getAttrib(x, names_sym);
        let has_names = names_sexp != libR_sys::R_NilValue && sexp_len(names_sexp) == n;
        // No interrupt poll in the loops below, deliberately. Even a relaxed
        // atomic counter and a predictable branch measured 4% to 5% on a list
        // of twenty thousand short lists, against a control that moved 1%,
        // and this is the path least likely to run long enough to want
        // interrupting -- the jobs that take seconds are wide frames and
        // large geometries, which poll at their phase boundaries for nothing.
        // One base pointer for the whole list instead of a VECTOR_ELT call
        // per element.
        let elems = VECTOR_PTR_RO(x);

        if has_names {
            // An empty, NA or repeated name has to be rewritten the way R
            // does it -- but almost no list has one, so the keys are written
            // as they are read and the object is only rebuilt if a bad name
            // turns up. That way the ordinary list pays one pointer read per
            // name and nothing else: the repeat check compares CHARSXPs,
            // which R interns, and never calls back into R.
            let np = libR_sys::STRING_PTR_RO(names_sexp);
            let wide = n > NAME_SCAN_MAX;
            let mut fixed: Option<Vec<Vec<u8>>> =
                if wide && wide_names_need_fixing(names_sexp, n) {
                    Some(mangled_names(names_sexp, n))
                } else {
                    None
                };
            let mark = buf.len();
            loop {
                buf.truncate(mark);
                buf.push(b'{');
                let mut bad = false;
                for i in 0..n {
                    if i > 0 { buf.push(b','); }
                    match fixed {
                        Some(ref v) => escape_json_string_into(buf, &v[i]),
                        None => {
                            let cs = *np.add(i);
                            let kb = if is_na_string(cs) { None } else { charsxp_to_utf8_bytes(cs) };
                            match kb {
                                // `wide` means the scan above already cleared
                                // every name, so the quadratic check is skipped.
                                Some(b) if !b.is_empty()
                                    && (wide || !(0..i).any(|j| *np.add(j) == cs)) =>
                                {
                                    escape_json_string_into(buf, b)
                                }
                                _ => { bad = true; break; }
                            }
                        }
                    }
                    buf.push(b':');
                    serialize_sexp_to_json_buffer(*elems.add(i), buf, config, depth + 1);
                }
                if bad {
                    fixed = Some(mangled_names(names_sexp, n));
                    continue;
                }
                buf.push(b'}');
                break;
            }
        } else {
            buf.push(b'[');
            for i in 0..n {
                if i > 0 { buf.push(b','); }
                serialize_sexp_to_json_buffer(*elems.add(i), buf, config, depth + 1);
            }
            buf.push(b']');
        }
        return;
    }
    buf.extend_from_slice(b"{}");
}

pub(crate) unsafe fn serialize_element_at_index(col: libR_sys::SEXP, idx: usize, buf: &mut Vec<u8>, config: SerializerConfig, depth: u32) {
    let r_type = typeof_sexp(col);
    // Was one Robj construction per CELL.
    let cls = classify(col);
    
    if cls & CLS_FACTOR != 0 && r_type == libR_sys::SEXPTYPE::INTSXP as u32 {
        if config.factor == FactorMode::String {
            let levels_sexp = levels_of(col);
            if levels_sexp != libR_sys::R_NilValue {
                let p = libR_sys::INTEGER(col);
                let v = *p.add(idx);
                if is_na_int(v) || v < 1 { 
                    if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                    else { buf.extend_from_slice(b"null"); }
                } else {
                    let lvl_idx = (v - 1) as usize;
                    if lvl_idx < sexp_len(levels_sexp) {
                        let s = libR_sys::STRING_ELT(levels_sexp, lvl_idx as isize);
                        if let Some(bytes) = charsxp_to_utf8_bytes(s) { escape_json_string_into(buf, bytes); }
                        else { buf.extend_from_slice(b"null"); }
                    } else { buf.extend_from_slice(b"null"); }
                }
                return;
            }
        }
    }

    match r_type {
        t if t == libR_sys::SEXPTYPE::INTSXP as u32 => {
            let v = *libR_sys::INTEGER(col).add(idx);
            if is_na_int(v) { 
                if config.na == NaMode::String || config.na == NaMode::Smart { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            } else {
                let mut tmp = itoa::Buffer::new();
                buf.extend_from_slice(tmp.format(v).as_bytes());
            }
        },
        t if t == libR_sys::SEXPTYPE::REALSXP as u32 => {
            let v = *libR_sys::REAL(col).add(idx);
            // `is_na_real || is_nan_real` is just `is_nan`, so the whole guard
                // reduces to one is_finite test -- and the common path then does
                // no NA inspection at all.
                if !v.is_finite() { 
                if config.na == NaMode::String || config.na == NaMode::Smart {
                    if v == f64::INFINITY { buf.extend_from_slice(b"\"Inf\""); }
                    else if v == f64::NEG_INFINITY { buf.extend_from_slice(b"\"-Inf\""); }
                    else if is_nan_real(v) { buf.extend_from_slice(b"\"NaN\""); }
                    else { buf.extend_from_slice(b"\"NA\""); }
                } else { buf.extend_from_slice(b"null"); }
            } else {
                write_f64_json(buf, v, config);
            }
        },
        t if t == libR_sys::SEXPTYPE::LGLSXP as u32 => {
            let v = *libR_sys::LOGICAL(col).add(idx);
            if is_na_int(v) { 
                if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            }
            else if v != 0 { buf.extend_from_slice(b"true"); }
            else { buf.extend_from_slice(b"false"); }
        },
        // [MODIFICATION: Passthrough Support for Rows]
        t if t == libR_sys::SEXPTYPE::STRSXP as u32 => {
            let s = libR_sys::STRING_ELT(col, idx as isize);
            if is_na_string(s) { 
                if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            }
            else if let Some(bytes) = charsxp_to_utf8_bytes(s) { 
                if splices_verbatim(cls, config) { buf.extend_from_slice(bytes); } // Passthrough
                else { escape_json_string_into(buf, bytes); } // Escape
            }
            else { buf.extend_from_slice(b"null"); }
        },
        t if t == libR_sys::SEXPTYPE::VECSXP as u32 => {
             let val = libR_sys::VECTOR_ELT(col, idx as isize);
             serialize_sexp_to_json_buffer(val, buf, config, depth + 1);
        },
        _ => buf.extend_from_slice(b"null"),
    }
}

// ------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(na: NaMode, digits: Option<u8>) -> SerializerConfig {
        SerializerConfig {
            df: DfMode::Rows,
            na,
            null: NullMode::List,
            factor: FactorMode::String,
            auto_unbox: false,
            digits,
            matrix_colmajor: false,
            always_decimal: false,
            signif: false,
            json_verbatim: false,
            rownames: ROWNAMES_REAL,
        }
    }

    fn na_real() -> f64 {
        f64::from_bits(0x7FF0_0000_0000_07A2)
    }

    #[test]
    fn element_writers_match_the_column_writers_spelling() {
        // serialize.rs and columns.rs write the same values by different
        // routes; they have to agree or a vector and a one-column frame would
        // disagree about NA.
        let one = |v: f64, c: SerializerConfig| {
            let mut b = Vec::new();
            write_real_elem(&mut b, v, c);
            String::from_utf8(b).unwrap()
        };
        for na in [NaMode::Null, NaMode::String, NaMode::Smart] {
            let c = cfg(na, Some(4));
            let want_na = if na == NaMode::Null { "null" } else { "\"NA\"" };
            assert_eq!(one(na_real(), c), want_na);
            assert_eq!(one(1.5, c), "1.5");
        }
        let c = cfg(NaMode::String, Some(4));
        assert_eq!(one(f64::NAN, c), "\"NaN\"");
        assert_eq!(one(f64::INFINITY, c), "\"Inf\"");
        assert_eq!(one(f64::NEG_INFINITY, c), "\"-Inf\"");

        let i = |v: i32, c: SerializerConfig| {
            let mut b = Vec::new();
            write_int_elem(&mut b, v, c);
            String::from_utf8(b).unwrap()
        };
        assert_eq!(i(7, cfg(NaMode::Null, Some(4))), "7");
        assert_eq!(i(i32::MIN, cfg(NaMode::Null, Some(4))), "null");
        assert_eq!(i(i32::MIN, cfg(NaMode::String, Some(4))), "\"NA\"");
        // Smart spells an NA integer the same as String does outside a row.
        assert_eq!(i(i32::MIN, cfg(NaMode::Smart, Some(4))), "\"NA\"");

        let l = |v: i32, c: SerializerConfig| {
            let mut b = Vec::new();
            write_lgl_elem(&mut b, v, c);
            String::from_utf8(b).unwrap()
        };
        assert_eq!(l(1, cfg(NaMode::Null, Some(4))), "true");
        assert_eq!(l(0, cfg(NaMode::Null, Some(4))), "false");
        assert_eq!(l(i32::MIN, cfg(NaMode::Null, Some(4))), "null");
        // A logical NA follows String only, not Smart: that is jsonlite's rule.
        assert_eq!(l(i32::MIN, cfg(NaMode::Smart, Some(4))), "null");
        assert_eq!(l(i32::MIN, cfg(NaMode::String, Some(4))), "\"NA\"");
    }

    #[test]
    fn chunk_ranges_tile_the_whole_run() {
        // The split writes each range with `i > s` rather than `i > 0` and the
        // ranges are joined with a comma between them, so a gap or an overlap
        // would show up as a missing or doubled separator.
        for n in [0usize, 1, 2, 1000, 32_768, 40_009, 250_000] {
            for each in [6usize, 12, 26] {
                match par_ranges(n, each) {
                    None => {}
                    Some(r) => {
                        assert!(!r.is_empty());
                        assert_eq!(r[0].1, 0);
                        assert_eq!(r[r.len() - 1].2, n);
                        for (i, x) in r.iter().enumerate() {
                            assert_eq!(x.0, i);
                            assert!(x.1 < x.2);
                        }
                        for w in r.windows(2) {
                            assert_eq!(w[0].2, w[1].1, "ranges do not meet");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn small_runs_are_never_split() {
        // Below the threshold the pool costs more than it saves, and this is
        // asked for every scalar in a nested list.
        for n in [0usize, 1, 2, 100, 1023] {
            assert!(par_ranges(n, 26).is_none(), "split {} elements", n);
        }
    }

    #[test]
    fn joining_chunks_puts_one_separator_between_them() {
        let mut buf = Vec::new();
        join_chunks(&mut buf, &[b"a".to_vec(), b"bb".to_vec()], b'[', b']');
        assert_eq!(String::from_utf8(buf).unwrap(), "[a,bb]");
        // An empty chunk contributes nothing, not an empty slot.
        let mut buf = Vec::new();
        join_chunks(&mut buf, &[b"a".to_vec(), Vec::new(), b"c".to_vec()], b'[', b']');
        assert_eq!(String::from_utf8(buf).unwrap(), "[a,c]");
        let mut buf = Vec::new();
        join_chunks(&mut buf, &[], b'{', b'}');
        assert_eq!(String::from_utf8(buf).unwrap(), "{}");
    }
}
