// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// PARALLEL SAFE COLUMNS
// ------------------------------------------------------------------

pub(crate) unsafe fn is_default_rownames(rn: libR_sys::SEXP) -> bool {
    if rn == libR_sys::R_NilValue { return true; }
    if typeof_sexp(rn) == libR_sys::SEXPTYPE::INTSXP as u32 && sexp_len(rn) == 2 {
        let p = libR_sys::INTEGER(rn);
        if *p == libR_sys::R_NaInt { return true; }
    }
    let n = sexp_len(rn);
    if typeof_sexp(rn) == libR_sys::SEXPTYPE::INTSXP as u32 {
        let p = libR_sys::INTEGER(rn);
        for i in 0..n {
            if *p.add(i) != (i as i32 + 1) { return false; }
        }
        return true;
    }
    if typeof_sexp(rn) == libR_sys::SEXPTYPE::STRSXP as u32 {
        let mut tmp = itoa::Buffer::new();
        for i in 0..n {
            let s_sexp = libR_sys::STRING_ELT(rn, i as isize);
            if is_na_string(s_sexp) { return false; }
            let s_ptr = libR_sys::R_CHAR(s_sexp) as *const c_char;
            let s_slice = CStr::from_ptr(s_ptr).to_bytes();
            let expected = tmp.format(i + 1);
            if s_slice != expected.as_bytes() { return false; }
        }
        return true;
    }
    false
}

// ------------------------------------------------------------------
// CLASS INSPECTION
// ------------------------------------------------------------------
// Every `Robj::from_sexp(x).inherits("...")` cost a great deal more than it
// looked: extendr's Robj constructor takes a GLOBAL mutex to register the
// SEXP in its ownership table (a hash-map refcount update plus an Rf_protect),
// and Drop takes the same global mutex again to unprotect. The recursive
// serializer built one per node and asked it about seven class names, and
// `serialize_element_at_index` built one PER CELL. Reading the class attribute
// once into a bitset removes all of it.
//
// Class names are always ASCII, so the raw CHARSXP bytes can be compared
// directly with no encoding translation.

pub(crate) const CLS_DATA_FRAME: u16 = 1 << 0;
pub(crate) const CLS_SFC: u16 = 1 << 1;
pub(crate) const CLS_FACTOR: u16 = 1 << 2;
pub(crate) const CLS_ASIS: u16 = 1 << 3;
pub(crate) const CLS_DATE: u16 = 1 << 4;
pub(crate) const CLS_POSIXT: u16 = 1 << 5;
pub(crate) const CLS_JSON: u16 = 1 << 6;
pub(crate) const CLS_DIFFTIME: u16 = 1 << 7;
pub(crate) const CLS_INT64: u16 = 1 << 8;
pub(crate) const CLS_BLOB: u16 = 1 << 9;
/// Local civil seconds produced by .encode_posixt(): a POSIXct that R has
/// already shifted by its UTC offset, so the writer can format it without
/// consulting a time zone. Carries the wanted layout in a `fgjfmt` attribute.
pub(crate) const CLS_FGJTIME: u16 = 1 << 10;

/// Classes that must be re-encoded in R before the serializer sees them.
pub(crate) const CLS_NEEDS_PREP: u16 =
    CLS_DATE | CLS_POSIXT | CLS_DIFFTIME | CLS_INT64 | CLS_BLOB;

#[inline]
pub(crate) unsafe fn classify(x: libR_sys::SEXP) -> u16 {
    let cls = libR_sys::Rf_getAttrib(x, libR_sys::R_ClassSymbol);
    if cls == libR_sys::R_NilValue
        || typeof_sexp(cls) != libR_sys::SEXPTYPE::STRSXP as u32
    {
        return 0;
    }
    let n = sexp_len(cls);
    let mut bits = 0u16;
    for i in 0..n {
        let s = libR_sys::STRING_ELT(cls, i as isize);
        if s == libR_sys::R_NilValue {
            continue;
        }
        let len = libR_sys::Rf_xlength(s);
        if len < 0 {
            continue;
        }
        let b = slice::from_raw_parts(libR_sys::R_CHAR(s) as *const u8, len as usize);
        bits |= match b {
            b"data.frame" => CLS_DATA_FRAME,
            b"sfc" => CLS_SFC,
            b"factor" => CLS_FACTOR,
            b"AsIs" => CLS_ASIS,
            b"Date" => CLS_DATE,
            b"POSIXt" => CLS_POSIXT,
            b"json" => CLS_JSON,
            b"difftime" => CLS_DIFFTIME,
            b"integer64" => CLS_INT64,
            b"blob" => CLS_BLOB,
            b"fgjtime" => CLS_FGJTIME,
            _ => 0,
        };
    }
    bits
}

/// Cached `levels` symbol. R never collects symbols, so this is safe to keep.
pub(crate) static LEVELS_SYM: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[inline]
pub(crate) unsafe fn levels_symbol() -> libR_sys::SEXP {
    let cached = LEVELS_SYM.load(std::sync::atomic::Ordering::Relaxed);
    if cached != 0 {
        return cached as libR_sys::SEXP;
    }
    let s = libR_sys::Rf_install(b"levels\0".as_ptr() as *const c_char);
    LEVELS_SYM.store(s as usize, std::sync::atomic::Ordering::Relaxed);
    s
}

/// The `levels` attribute of a factor, or `R_NilValue`.
#[inline]
pub(crate) unsafe fn levels_of(x: libR_sys::SEXP) -> libR_sys::SEXP {
    libR_sys::Rf_getAttrib(x, levels_symbol())
}

/// Writes the row name at `idx` as a JSON string.
///
/// jsonlite always emits `_row` as a string, even when the row names are
/// stored as integers.
pub(crate) unsafe fn write_rowname_at(rn: libR_sys::SEXP, idx: usize, buf: &mut Vec<u8>) {
    let t = typeof_sexp(rn);
    if t == libR_sys::SEXPTYPE::STRSXP as u32 {
        let s = libR_sys::STRING_ELT(rn, idx as isize);
        if is_na_string(s) {
            buf.extend_from_slice(b"null");
            return;
        }
        match charsxp_to_utf8_bytes(s) {
            Some(b) => escape_json_string_into(buf, b),
            None => buf.extend_from_slice(b"null"),
        }
    } else if t == libR_sys::SEXPTYPE::INTSXP as u32 {
        let v = *libR_sys::INTEGER(rn).add(idx);
        if is_na_int(v) {
            buf.extend_from_slice(b"null");
            return;
        }
        let mut tmp = itoa::Buffer::new();
        escape_json_string_into(buf, tmp.format(v).as_bytes());
    } else if t == libR_sys::SEXPTYPE::REALSXP as u32 {
        let v = *libR_sys::REAL(rn).add(idx);
        if !v.is_finite() {
            buf.extend_from_slice(b"null");
            return;
        }
        let mut tmp = itoa::Buffer::new();
        escape_json_string_into(buf, tmp.format(v as i64).as_bytes());
    } else {
        buf.extend_from_slice(b"null");
    }
}

/// Writes element `idx` of an atomic vector as a JSON value.
///
/// Shared by the matrix and array column paths so their NA handling cannot
/// drift apart. `na = "smart"` behaves as `"string"` for a numeric cell,
/// which is what jsonlite does inside an array: there is no key to drop.
unsafe fn write_arr_cell(
    bytes: &mut Vec<u8>,
    sexp: libR_sys::SEXP,
    // Rtype is not Copy, and this recurses, so it travels by reference.
    r_type: &Rtype,
    idx: usize,
    config: SerializerConfig,
) {
    let num_na_as_text = config.na == NaMode::String || config.na == NaMode::Smart;
    match r_type {
        Rtype::Integers => {
            let v = *libR_sys::INTEGER(sexp).add(idx);
            if is_na_int(v) {
                bytes.extend_from_slice(if num_na_as_text { br#""NA""# } else { b"null" });
            } else {
                let mut tmp = itoa::Buffer::new();
                bytes.extend_from_slice(tmp.format(v).as_bytes());
            }
        }
        Rtype::Doubles => {
            let v = *libR_sys::REAL(sexp).add(idx);
            if is_na_real(v) {
                bytes.extend_from_slice(if num_na_as_text { br#""NA""# } else { b"null" });
            } else if v.is_finite() {
                write_f64_json(bytes, v, config.digits, config.always_decimal);
            } else if !num_na_as_text {
                bytes.extend_from_slice(b"null");
            } else if v == f64::INFINITY {
                bytes.extend_from_slice(br#""Inf""#);
            } else if v == f64::NEG_INFINITY {
                bytes.extend_from_slice(br#""-Inf""#);
            } else {
                // A NaN that is not R's NA prints as "NaN"; this used to say
                // "NA".
                bytes.extend_from_slice(br#""NaN""#);
            }
        }
        Rtype::Logicals => {
            let v = *libR_sys::LOGICAL(sexp).add(idx);
            if is_na_int(v) {
                bytes.extend_from_slice(
                    if config.na == NaMode::String { br#""NA""# } else { b"null" },
                );
            } else {
                bytes.extend_from_slice(if v != 0 { b"true" } else { b"false" });
            }
        }
        Rtype::Strings => {
            let cs = libR_sys::STRING_ELT(sexp, idx as isize);
            if is_na_string(cs) {
                bytes.extend_from_slice(
                    if config.na == NaMode::String { br#""NA""# } else { b"null" },
                );
            } else {
                match charsxp_to_utf8_bytes(cs) {
                    Some(u) => escape_json_string_into(bytes, u),
                    None => bytes.extend_from_slice(b"null"),
                }
            }
        }
        // Logical and character arrays both used to land here and emit null
        // for every cell.
        _ => bytes.extend_from_slice(b"null"),
    }
}

/// Writes one row's slice of an array, nesting over dimensions 2..N with the
/// last dimension innermost, which is the shape jsonlite produces.
///
/// `offset` starts at the row index and accumulates `i * strides[j]` as it
/// descends. A two-dimensional array reduces to a flat array of the row's
/// values, so this covers the ordinary matrix case too.
unsafe fn write_arr_slice(
    bytes: &mut Vec<u8>,
    sexp: libR_sys::SEXP,
    r_type: &Rtype,
    dims: &[usize],
    strides: &[usize],
    j: usize,
    offset: usize,
    config: SerializerConfig,
) {
    if j == dims.len() {
        write_arr_cell(bytes, sexp, r_type, offset, config);
        return;
    }
    bytes.push(b'[');
    for i in 0..dims[j] {
        if i > 0 {
            bytes.push(b',');
        }
        write_arr_slice(bytes, sexp, r_type, dims, strides, j + 1, offset + i * strides[j], config);
    }
    bytes.push(b']');
}

pub(crate) fn build_thread_safe_cols(
    df: &List,
    colnames: &[Vec<u8>],
    skip_idx: usize,
    _expected_rows: usize,
    config: SerializerConfig,
) -> Result<Vec<(Vec<u8>, ThreadSafeColumn)>> {
    let mut out = Vec::with_capacity(colnames.len() + 1);

    for (j, nm) in colnames.iter().enumerate() {
        if j == skip_idx { continue; }
        let mut col = df.elt(j).map_err(|_| {
            Error::Other(format!("Column '{}' missing", String::from_utf8_lossy(nm)))
        })?;
        // A POSIXt column has to go through R, because only R holds the time
        // zone rules. A Date column does not: it is plain day arithmetic, and
        // the DateReal/DateInt column types below format it in the worker.
        // Routing Date through format() here cost 4.7 us per value, which was
        // 99% of the time spent serialising a Date column.
        if col.inherits("POSIXt") {
             col = call!("format", &col).map_err(|e| Error::Other(format!("format failed: {:?}", e)))?;
        }
        let sexp = unsafe { col.get() };
        let r_type = col.rtype();
        let key = build_escaped_key_bytes(nm);

        let dim_attr = unsafe { libR_sys::Rf_getAttrib(sexp, libR_sys::R_DimSymbol) };
        // Any array whose first dimension is the row count, not just a
        // matrix. A three-dimensional array column used to fall through to
        // the plain vector arms and emit one number per row instead of the
        // row's slice.
        let arr_dims: Option<Vec<usize>> = unsafe {
            if dim_attr != libR_sys::R_NilValue
                && sexp_len(dim_attr) >= 2
                && typeof_sexp(dim_attr) == libR_sys::SEXPTYPE::INTSXP as u32
                && *libR_sys::INTEGER(dim_attr) == _expected_rows as i32
            {
                let nd = sexp_len(dim_attr);
                let dp = libR_sys::INTEGER(dim_attr);
                Some((0..nd).map(|k| (*dp.add(k)).max(0) as usize).collect())
            } else {
                None
            }
        };
        let is_matrix = arr_dims.is_some();

        if is_matrix {
             let dims = arr_dims.as_ref().unwrap();
             let n_matrix_cols: usize = dims[1..].iter().product();

             // A numeric matrix is described rather than rendered: the writer
             // reads R's column-major storage directly, in the worker. Only
             // the two-dimensional case, because a deeper array needs nesting
             // that the flat cell writer does not express.
             if dims.len() == 2
                 && n_matrix_cols <= u32::MAX as usize
                 && (r_type == Rtype::Doubles || r_type == Rtype::Integers)
             {
                 let (kind, ptr) = if r_type == Rtype::Doubles {
                     (ColumnType::MatrixReal, unsafe { libR_sys::REAL(sexp) as *const u8 as usize })
                 } else {
                     (ColumnType::MatrixInt, unsafe { libR_sys::INTEGER(sexp) as *const u8 as usize })
                 };
                 out.push((key, ThreadSafeColumn {
                     kind,
                     aux: n_matrix_cols as u32,
                     data_ptr: ptr,
                     len: _expected_rows,
                     cached_levels: None,
                     string_arena: None,
                     char_meta: None,
                 }));
                 continue;
             }

             // Everything else is rendered here: a logical or character matrix
             // needs the encoding handling the plain column arms have, and a
             // deeper array needs the nested walk.
             let mut strides = vec![1usize; dims.len()];
             for k in 1..dims.len() {
                 strides[k] = strides[k - 1] * dims[k - 1];
             }
             let mut bytes = Vec::new();
             let mut offsets = Vec::with_capacity(_expected_rows);
             for r in 0.._expected_rows {
                 let start = bytes.len();
                 unsafe {
                     write_arr_slice(&mut bytes, sexp, &r_type, dims, &strides, 1, r, config);
                 }
                 offsets.push((start, bytes.len() - start));
             }
             let col_len = offsets.len();
             let col = ThreadSafeColumn { kind: ColumnType::JsonRaw, aux: 0, data_ptr: 0, len: col_len, cached_levels: None, string_arena: Some(StringArena { bytes, offsets }), char_meta: None };
             out.push((key, col));
             continue;
        }

        // Set by the fgjtime branch below; every other kind leaves it zero.
        let mut aux: u32 = 0;
        let (kind, ptr, cached_levels, arena) =
            if col.inherits("fgjtime") && r_type == Rtype::Doubles {
                aux = unsafe { fgj_fmt_code(sexp) }.unwrap_or(TFMT_SPACE);
                (ColumnType::TimeLocal, unsafe { libR_sys::REAL(sexp) as *const u8 as usize }, None, None)
            } else if col.inherits("factor") && r_type == Rtype::Integers && config.factor == FactorMode::String {
                let levels = col.get_attrib("levels").ok_or_else(|| Error::Other("Factor missing levels".to_string()))?;
                let levels_sexp = unsafe { levels.get() };
                let n = unsafe { sexp_len(levels_sexp) };
                let mut cache = Vec::with_capacity(n);
                for idx in 0..n {
                    let s = unsafe { libR_sys::STRING_ELT(levels_sexp, idx as isize) };
                    let mut buf = Vec::with_capacity(32);
                    unsafe {
                        if let Some(bytes) = charsxp_to_utf8_bytes(s) { escape_json_string_into(&mut buf, bytes); }
                        else { buf.extend_from_slice(b"\"NA\""); }
                    }
                    cache.push(buf);
                }
                (ColumnType::Factor, unsafe { libR_sys::INTEGER(sexp) as *const u8 as usize }, Some(cache), None)
            } else if col.inherits("Date") && (r_type == Rtype::Doubles || r_type == Rtype::Integers) {
                // Formatted in the worker rather than pre-encoded by R. The R
                // side leaves Date alone whenever Date = "ISO8601"; the
                // "epoch" mode is an unclass(), which is already free.
                if r_type == Rtype::Doubles {
                    (ColumnType::DateReal, unsafe { libR_sys::REAL(sexp) as *const u8 as usize }, None, None)
                } else {
                    (ColumnType::DateInt, unsafe { libR_sys::INTEGER(sexp) as *const u8 as usize }, None, None)
                }
            } else if r_type == Rtype::Integers {
                (ColumnType::Int, unsafe { libR_sys::INTEGER(sexp) as *const u8 as usize }, None, None)
            } else if r_type == Rtype::Doubles {
                (ColumnType::Real, unsafe { libR_sys::REAL(sexp) as *const u8 as usize }, None, None)
            } else if r_type == Rtype::Logicals {
                (ColumnType::Bool, unsafe { libR_sys::LOGICAL(sexp) as *const u8 as usize }, None, None)
            } else if r_type == Rtype::Strings {
                let is_json = col.inherits("json");
                let n = unsafe { sexp_len(sexp) };

                // Fast path: if every element is pure ASCII then no encoding
                // translation can be needed (ASCII is valid UTF-8 in any
                // locale), which means the workers can escape R's own CHARSXP
                // bytes directly. That removes an entire copy of the column --
                // the arena below stores *escaped* bytes, which are then
                // copied again into the output -- and moves the escaping into
                // the parallel region. Character columns previously scaled at
                // 1.15x because all of this ran on the R thread.
                //
                // Nothing is copied: the prepass records a 4-byte descriptor
                // per cell and the workers read R's own CHARSXP bytes.
                // Non-ASCII columns fall through to the arena, because
                // Rf_translateCharUTF8 allocates on R's vmax stack and so
                // cannot be called from a worker.
                if !is_json {
                    let base = unsafe { libR_sys::STRING_PTR_RO(sexp) } as usize;
                    // This pass reads only CHARSXP bytes -- no allocation, no
                    // R state touched -- so it parallelises.
                    //
                    // It answers three questions at once: can the cell be read
                    // directly, how long is it, and does it need escaping. The
                    // worker then needs neither Rf_xlength, a cross-DLL call
                    // per cell, nor a second pass over the bytes looking for
                    // escapes. A factor column, whose levels have been
                    // pre-escaped with known lengths all along, measured 29%
                    // faster than this path for identical output, which is
                    // what suggested closing the gap this way.
                    //
                    // A (ptr, len) pair per cell was tried before and cost
                    // ~5ms of a 12.6ms serialise, but that was 16 bytes built
                    // serially on the R thread; this is 4 bytes built in
                    // parallel.
                    let meta: Vec<u32> = with_pool(|| {
                        (0..n)
                            .into_par_iter()
                            .map(|i| unsafe {
                                let cs = *(base as *const libR_sys::SEXP).add(i);
                                if is_na_string(cs) {
                                    return CD_NA;
                                }
                                let len = libR_sys::Rf_xlength(cs);
                                // A string too long for the 30-bit payload
                                // goes through the arena, whose offsets are
                                // usize. Needs a 1 GB single string to reach.
                                if len < 0 || len > CD_LEN as isize {
                                    return CD_XLATE;
                                }
                                let bytes = slice::from_raw_parts(
                                    libR_sys::R_CHAR(cs) as *const u8,
                                    len as usize,
                                );
                                // Raw bytes are safe to emit when they are
                                // already valid UTF-8, which is true for pure
                                // ASCII in any locale and for anything R has
                                // marked CE_UTF8. Rf_translateCharUTF8 would
                                // be a no-op for both, so no R allocation can
                                // be needed. getCharCE is a pure bit read, so
                                // this is safe off the R thread.
                                if !bytes.is_ascii() && Rf_getCharCE(cs) != CE_UTF8 {
                                    return CD_XLATE;
                                }
                                let mut d = len as u32;
                                if find_escape(bytes).is_some() {
                                    d |= CD_ESC;
                                }
                                d
                            })
                            .collect()
                    });
                    // Cells the workers cannot read are translated here, on
                    // the R thread, and only those cells. The column used to
                    // be abandoned wholesale: a single latin1 value among a
                    // million ASCII ones sent every row through the arena,
                    // serially. Now the million stay direct.
                    let mut meta = meta;
                    let mut arena: Option<StringArena> = None;
                    if meta.iter().any(|&d| d == CD_XLATE) {
                        let mut bytes: Vec<u8> = Vec::new();
                        let mut offsets: Vec<(usize, usize)> = Vec::new();
                        let mut over = false;
                        for i in 0..n {
                            if meta[i] != CD_XLATE {
                                continue;
                            }
                            if offsets.len() as u32 > CD_LEN {
                                // Absurd: more than a billion untranslatable
                                // cells. Leave the rest for the arena path.
                                over = true;
                                break;
                            }
                            let s_sexp = unsafe { libR_sys::STRING_ELT(sexp, i as isize) };
                            let start = bytes.len();
                            match unsafe { charsxp_to_utf8_bytes(s_sexp) } {
                                Some(utf8) => escape_json_string_into(&mut bytes, utf8),
                                // Marked as an encoding error by
                                // charsxp_to_utf8_bytes; the entry point turns
                                // that into an R error, so the bytes here are
                                // never seen.
                                None => bytes.extend_from_slice(b"null"),
                            }
                            meta[i] = CD_ARENA | offsets.len() as u32;
                            offsets.push((start, bytes.len() - start));
                        }
                        if !over {
                            arena = Some(StringArena { bytes, offsets });
                        }
                    }
                    if !meta.iter().any(|&d| d == CD_XLATE) {
                        out.push((
                            key,
                            ThreadSafeColumn {
                                kind: ColumnType::CharDirect,
                                aux: 0,
                                data_ptr: base,
                                len: n,
                                cached_levels: None,
                                string_arena: arena,
                                char_meta: Some(meta),
                            },
                        ));
                        continue;
                    }
                }

                let mut bytes = Vec::with_capacity(n * 16);
                let mut offsets = Vec::with_capacity(n);
                for i in 0..n {
                    let s_sexp = unsafe { libR_sys::STRING_ELT(sexp, i as isize) };
                    if unsafe { is_na_string(s_sexp) } {
                        if config.na == NaMode::String {
                            let start = bytes.len();
                            bytes.extend_from_slice(b"\"NA\"");
                            offsets.push((start, 4));
                        } else {
                            offsets.push((usize::MAX, 0));
                        }
                    } else if let Some(utf8) = unsafe { charsxp_to_utf8_bytes(s_sexp) } {
                        let start = bytes.len();
                        if is_json { bytes.extend_from_slice(utf8); }
                        else { escape_json_string_into(&mut bytes, utf8); }
                        offsets.push((start, bytes.len() - start));
                    } else {
                        if config.na == NaMode::String {
                            let start = bytes.len();
                            bytes.extend_from_slice(b"\"NA\"");
                            offsets.push((start, 4));
                        } else {
                            offsets.push((usize::MAX, 0));
                        }
                    }
                }
                let kind = if is_json { ColumnType::JsonRaw } else { ColumnType::Char };
                (kind, 0, None, Some(StringArena { bytes, offsets }))
            } else if col.inherits("sfc") {
                // A geometry column that is NOT the sf object's designated
                // sf_column -- a second sfc column, or any sfc column of a
                // frame that is not classed `sf`. jsonlite gives it a full
                // typed geometry object per row; we previously emitted bare
                // coordinate arrays and lost the type.
                let n = unsafe { sexp_len(sexp) };
                let mut bytes = Vec::with_capacity(_expected_rows * 64);
                let mut offsets = Vec::with_capacity(_expected_rows);
                for r in 0.._expected_rows {
                    let start = bytes.len();
                    if r < n {
                        unsafe {
                            render_geometry_to_bytes(
                                libR_sys::VECTOR_ELT(sexp, r as isize),
                                &mut bytes,
                                config,
                                0,
                            )
                        };
                    } else {
                        bytes.extend_from_slice(b"null");
                    }
                    offsets.push((start, bytes.len() - start));
                }
                (ColumnType::JsonRaw, 0, None, Some(StringArena { bytes, offsets }))
            } else if col.inherits("data.frame") {
                // A data.frame-valued column (as produced by tidyr::nest) is a
                // VECSXP whose elements are the nested frame's COLUMNS. Walking
                // it as an ordinary list column therefore transposed the data:
                // row 1 received the whole first column and later rows nothing.
                // Emit one object per row of the nested frame instead.
                let nested_rows = unsafe { get_df_nrows(sexp) };
                let n_nested_cols = unsafe { sexp_len(sexp) };
                let nested_names = unsafe { utf8_names(sexp, n_nested_cols) };
                let mut bytes = Vec::with_capacity(_expected_rows * 64);
                let mut offsets = Vec::with_capacity(_expected_rows);
                for r in 0.._expected_rows {
                    let start = bytes.len();
                    if r >= nested_rows {
                        bytes.extend_from_slice(b"null");
                    } else {
                        bytes.push(b'{');
                        let mut first = true;
                        for c in 0..n_nested_cols {
                            let ncol = unsafe { libR_sys::VECTOR_ELT(sexp, c as isize) };
                            if r >= unsafe { sexp_len(ncol) } {
                                continue;
                            }
                            // Row-oriented output omits missing fields, so a
                            // skipped value must not leave a separator behind.
                            let mark = bytes.len();
                            if !first {
                                bytes.push(b',');
                            }
                            match nested_names.as_ref().and_then(|v| v.get(c)) {
                                Some(nm) => {
                                    escape_json_string_into(&mut bytes, nm);
                                }
                                None => bytes.extend_from_slice(b"\"\""),
                            }
                            bytes.push(b':');
                            let before = bytes.len();
                            unsafe { serialize_element_at_index(ncol, r, &mut bytes, config, 1); }
                            if config.na == NaMode::Smart && bytes.len() == before + 4
                                && &bytes[before..] == b"null"
                            {
                                bytes.truncate(mark);
                            } else {
                                first = false;
                            }
                        }
                        bytes.push(b'}');
                    }
                    offsets.push((start, bytes.len() - start));
                }
                (ColumnType::JsonRaw, 0, None, Some(StringArena { bytes, offsets }))
            } else if r_type == Rtype::List {
                let n = unsafe { sexp_len(sexp) };
                let mut bytes = Vec::with_capacity(n * 64);
                let mut offsets = Vec::with_capacity(n);
                for i in 0..n {
                    let item = unsafe { libR_sys::VECTOR_ELT(sexp, i as isize) };
                    let start = bytes.len();
                    unsafe { serialize_sexp_to_json_buffer(item, &mut bytes, config, 0); }
                    offsets.push((start, bytes.len() - start));
                }
                (ColumnType::JsonRaw, 0, None, Some(StringArena { bytes, offsets }))
            } else {
                (ColumnType::Null, 0, None, None)
            };
        let col_len = match arena {
            Some(ref a) => a.offsets.len(),
            None => unsafe { sexp_len(sexp) },
        };
        out.push((key, ThreadSafeColumn { kind, aux, data_ptr: ptr, len: col_len, cached_levels, string_arena: arena, char_meta: None }));
    }

    let rn_sexp = unsafe { libR_sys::Rf_getAttrib(df.get(), libR_sys::R_RowNamesSymbol) };
    if !unsafe { is_default_rownames(rn_sexp) } {
        let key = build_escaped_key_bytes(b"_row");
        let n = unsafe { sexp_len(rn_sexp) };
        let mut bytes = Vec::with_capacity(n * 16);
        let mut offsets = Vec::with_capacity(n);
        let rn_type = unsafe { typeof_sexp(rn_sexp) };
        
        if rn_type == libR_sys::SEXPTYPE::STRSXP as u32 {
            for i in 0..n {
                let s_sexp = unsafe { libR_sys::STRING_ELT(rn_sexp, i as isize) };
                if unsafe { is_na_string(s_sexp) } {
                    offsets.push((usize::MAX, 0)); 
                } else if let Some(utf8) = unsafe { charsxp_to_utf8_bytes(s_sexp) } {
                    let start = bytes.len();
                    escape_json_string_into(&mut bytes, utf8);
                    offsets.push((start, bytes.len() - start));
                } else { offsets.push((usize::MAX, 0)); }
            }
        } else if rn_type == libR_sys::SEXPTYPE::INTSXP as u32 {
            let p = unsafe { libR_sys::INTEGER(rn_sexp) };
            for i in 0..n {
                let v = unsafe { *p.add(i) };
                if unsafe { is_na_int(v) } { offsets.push((usize::MAX, 0)); }
                else {
                    let mut tmp = itoa::Buffer::new();
                    let s = tmp.format(v);
                    let start = bytes.len();
                    escape_json_string_into(&mut bytes, s.as_bytes());
                    offsets.push((start, bytes.len() - start));
                }
            }
        }
        let rn_len = offsets.len();
        out.push((key, ThreadSafeColumn { kind: ColumnType::JsonRaw, aux: 0, data_ptr: 0, len: rn_len, cached_levels: None, string_arena: Some(StringArena { bytes, offsets }), char_meta: None }));
    }
    Ok(out)
}

// ------------------------------------------------------------------
// COLUMN WRITER HELPERS
// ------------------------------------------------------------------

/// Is there actually an element at `row` behind this column?
///
/// A data.frame assembled with `structure()` can declare more rows than a
/// column holds, and an `sf` object can carry a geometry column shorter than
/// the frame. Reading past the end used to serialise adjacent heap bytes
/// straight into the JSON output.
#[inline(always)]
pub(crate) fn col_available(col: &ThreadSafeColumn, row: usize) -> bool {
    match col.kind {
        ColumnType::CharDirect => row < col.len,
        ColumnType::Char | ColumnType::JsonRaw => match col.string_arena {
            Some(ref a) => row < a.offsets.len(),
            None => false,
        },
        ColumnType::Null => true,
        // `len` is the row count for a matrix column, not the element count.
        _ => row < col.len,
    }
}

/// Is the cell at `row` missing, in R's sense of `NA`?
///
/// Used only to decide whether row-oriented output should omit the key
/// entirely (`na = "smart"`, which is what jsonlite does by default).
///
/// Verified against jsonlite 2.0.0: `toJSON(data.frame(x = c(1, NA, Inf,
/// -Inf, NaN)))` is `[{"x":1},{},{},{},{}]`, i.e. every non-finite double is
/// omitted in row mode, not just NA.
#[inline(always)]
pub(crate) fn col_is_missing(col: &ThreadSafeColumn, row: usize) -> bool {
    if !col_available(col, row) {
        return true;
    }
    match col.kind {
        ColumnType::Int | ColumnType::Bool => unsafe {
            is_na_int(*(col.data_ptr as *const i32).add(row))
        },
        ColumnType::Factor => unsafe {
            let v = *(col.data_ptr as *const i32).add(row);
            is_na_int(v) || v < 1
        },
        ColumnType::Real => unsafe { !(*(col.data_ptr as *const f64).add(row)).is_finite() },
        // A Date is a string by the time jsonlite emits it, so only a true NA
        // is missing here: NaN and the infinities format to ordinary text and
        // keep their key in row mode.
        ColumnType::DateReal => unsafe {
            matches!(date_cell(*(col.data_ptr as *const f64).add(row)), DateCell::Na)
        },
        ColumnType::DateInt => unsafe {
            is_na_int(*(col.data_ptr as *const i32).add(row))
        },
        // A timestamp is a string by the time jsonlite emits it, so only a
        // true NA is missing: NaN and the infinities format to ordinary text.
        ColumnType::TimeLocal => unsafe {
            let v = *(col.data_ptr as *const f64).add(row);
            v.is_nan() && is_na_real(v)
        },
        // A matrix cell is an array, which jsonlite always emits, so the key
        // is never dropped in row mode.
        ColumnType::MatrixReal | ColumnType::MatrixInt => false,
        ColumnType::CharDirect => match col.char_meta {
            Some(ref m) => m[row] == CD_NA,
            None => unsafe {
                is_na_string(*(col.data_ptr as *const libR_sys::SEXP).add(row))
            },
        },
        ColumnType::Char => match col.string_arena {
            Some(ref a) => a.offsets[row].0 == usize::MAX,
            None => true,
        },
        // Pre-rendered JSON carries the NA sentinel only when it came from a
        // string/`json` column whose value was missing, so honouring it here is
        // what lets a mongo-encoded NA row collapse to `{}`. Genuine list and
        // matrix columns never set it. A column of a type we do not encode is
        // rendered as `null` by jsonlite, so it is not "missing".
        ColumnType::JsonRaw => match col.string_arena {
            Some(ref a) => a.offsets[row].0 == usize::MAX,
            None => false,
        },
        ColumnType::Null => false,
    }
}

/// Writes exactly one well-formed JSON value for `row`.
///
/// This must never write nothing and never write a partial value: the previous
/// implementation pushed the key first and could then bail out of the `Factor`
/// and `Char` arms, leaving a dangling `"key":` in the output.
#[inline(always)]
pub(crate) fn write_col_value(
    out: &mut JsonWriter,
    row: usize,
    col: &ThreadSafeColumn,
    config: SerializerConfig,
) {
    if !col_available(col, row) {
        out.push_bytes(b"null");
        return;
    }
    match col.kind {
        ColumnType::CharDirect => unsafe {
            // Copied straight out of R's CHARSXP into the output, exactly
            // once, in the worker. The prepass already established the length
            // and whether any byte needs escaping, so the common case is a
            // quote, one memcpy and a quote: no Rf_xlength, no second scan.
            let d = match col.char_meta {
                Some(ref m) => *m.get_unchecked(row),
                // Only reachable if a CharDirect column were built without a
                // prepass, which no path does.
                None => CD_NA,
            };
            if d == CD_NA {
                if config.na == NaMode::String { out.push_bytes(b"\"NA\""); }
                else { out.push_bytes(b"null"); }
            } else if d & CD_ARENA != 0 {
                // Needed translating, so the R thread rendered it already.
                let a = col.string_arena.as_ref().unwrap();
                let (start, len) = a.offsets[(d & CD_LEN) as usize];
                out.push_bytes(&a.bytes[start..start + len]);
            } else {
                let cs = *(col.data_ptr as *const libR_sys::SEXP).add(row);
                let s = slice::from_raw_parts(
                    libR_sys::R_CHAR(cs) as *const u8,
                    (d & CD_LEN) as usize,
                );
                if d & CD_ESC == 0 {
                    out.buf.reserve(s.len() + 2);
                    out.buf.push(b'"');
                    out.buf.extend_from_slice(s);
                    out.buf.push(b'"');
                } else {
                    escape_json_string_into(&mut out.buf, s);
                }
            }
        },
        ColumnType::Char => {
            let a = col.string_arena.as_ref().unwrap();
            let (start, len) = a.offsets[row];
            if start != usize::MAX {
                out.push_bytes(&a.bytes[start..start + len]);
            } else if config.na == NaMode::String {
                out.push_bytes(b"\"NA\"");
            } else {
                out.push_bytes(b"null");
            }
        }
        ColumnType::JsonRaw => {
            let a = col.string_arena.as_ref().unwrap();
            let (start, len) = a.offsets[row];
            if start != usize::MAX {
                out.push_bytes(&a.bytes[start..start + len]);
            } else {
                // The sentinel used to fall through to an out-of-range slice
                // index, panicking inside a rayon worker.
                out.push_bytes(b"null");
            }
        }
        ColumnType::Int => unsafe {
            let v = *(col.data_ptr as *const i32).add(row);
            if !is_na_int(v) {
                out.push_i32(v);
            } else if config.na == NaMode::Null {
                out.push_bytes(b"null");
            } else {
                out.push_bytes(b"\"NA\"");
            }
        },
        ColumnType::Real => unsafe {
            let v = *(col.data_ptr as *const f64).add(row);
            if v.is_finite() {
                out.push_f64_cfg(v, config);
            } else if config.na == NaMode::Null {
                out.push_bytes(b"null");
            } else if v == f64::INFINITY {
                out.push_bytes(b"\"Inf\"");
            } else if v == f64::NEG_INFINITY {
                out.push_bytes(b"\"-Inf\"");
            } else if is_nan_real(v) {
                out.push_bytes(b"\"NaN\"");
            } else {
                out.push_bytes(b"\"NA\"");
            }
        },
        ColumnType::DateReal => unsafe {
            let v = *(col.data_ptr as *const f64).add(row);
            write_date_cell(&mut out.buf, date_cell(v), config.na);
        },
        ColumnType::DateInt => unsafe {
            let v = *(col.data_ptr as *const i32).add(row);
            write_date_cell(&mut out.buf, date_cell_i32(v), config.na);
        },
        ColumnType::TimeLocal => unsafe {
            let v = *(col.data_ptr as *const f64).add(row);
            write_time_cell(&mut out.buf, v, col.aux, config.na);
        },
        ColumnType::MatrixReal => unsafe {
            let nrow = col.len;
            let ncol = col.aux as usize;
            let p = col.data_ptr as *const f64;
            out.buf.reserve(ncol * 26 + 2);
            out.push_u8(b'[');
            for c in 0..ncol {
                if c > 0 {
                    out.push_u8(b',');
                }
                // R stores a matrix column-major.
                let v = *p.add(c * nrow + row);
                if is_na_real(v) {
                    if config.na == NaMode::String || config.na == NaMode::Smart {
                        out.push_bytes(b"\"NA\"");
                    } else {
                        out.push_bytes(b"null");
                    }
                } else if v.is_finite() {
                    out.push_f64_cfg(v, config);
                } else if config.na == NaMode::String || config.na == NaMode::Smart {
                    if v == f64::INFINITY {
                        out.push_bytes(b"\"Inf\"");
                    } else if v == f64::NEG_INFINITY {
                        out.push_bytes(b"\"-Inf\"");
                    } else {
                        out.push_bytes(b"\"NaN\"");
                    }
                } else {
                    out.push_bytes(b"null");
                }
            }
            out.push_u8(b']');
        },
        ColumnType::MatrixInt => unsafe {
            let nrow = col.len;
            let ncol = col.aux as usize;
            let p = col.data_ptr as *const i32;
            out.buf.reserve(ncol * 12 + 2);
            out.push_u8(b'[');
            for c in 0..ncol {
                if c > 0 {
                    out.push_u8(b',');
                }
                let v = *p.add(c * nrow + row);
                if is_na_int(v) {
                    if config.na == NaMode::String || config.na == NaMode::Smart {
                        out.push_bytes(b"\"NA\"");
                    } else {
                        out.push_bytes(b"null");
                    }
                } else {
                    out.push_i32(v);
                }
            }
            out.push_u8(b']');
        },
        ColumnType::Bool => unsafe {
            let v = *(col.data_ptr as *const i32).add(row);
            if !is_na_int(v) {
                out.push_bool(v != 0);
            } else if config.na == NaMode::String {
                out.push_bytes(b"\"NA\"");
            } else {
                out.push_bytes(b"null");
            }
        },
        ColumnType::Factor => unsafe {
            let v = *(col.data_ptr as *const i32).add(row);
            let mut wrote = false;
            if !is_na_int(v) && v > 0 {
                if let Some(ref levels) = col.cached_levels {
                    let idx = (v as usize) - 1;
                    if idx < levels.len() {
                        out.push_bytes(&levels[idx]);
                        wrote = true;
                    }
                }
            }
            if !wrote {
                // Includes the case of an integer code outside the declared
                // levels, which previously emitted `"key":` and nothing else.
                if config.na == NaMode::String {
                    out.push_bytes(b"\"NA\"");
                } else {
                    out.push_bytes(b"null");
                }
            }
        },
        ColumnType::Null => out.push_bytes(b"null"),
    }
}

/// Writes `"key":value` for `row`, or nothing at all.
///
/// Returns whether anything was written, so the caller can manage separators.
/// The skip decision is taken before any byte is emitted.
#[inline(always)]
pub(crate) fn try_write_kv(
    out: &mut JsonWriter,
    row: usize,
    key: &[u8],
    col: &ThreadSafeColumn,
    config: SerializerConfig,
) -> bool {
    if config.na == NaMode::Smart && col_is_missing(col, row) {
        return false;
    }
    out.push_bytes(key);
    write_col_value(out, row, col, config);
    true
}

