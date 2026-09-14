// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// PARALLEL SAFE COLUMNS
// ------------------------------------------------------------------

/// Whether a frame's `row.names` are the automatic 1..n.
///
/// Safe to call with either the stored attribute or `Rf_getAttrib`'s result.
/// `getAttrib` special-cases `R_RowNamesSymbol` and expands the compact
/// `c(NA, -n)` form into a 1..n INTSXP, which costs an allocation of n
/// integers -- but not the answer: the test below is "is it a STRSXP", and
/// compact and expanded are both INTSXP, so they agree. Character row names
/// come back unchanged either way.
pub(crate) unsafe fn is_default_rownames(rn: libR_sys::SEXP) -> bool {
    // jsonlite's own condition, from asJSON.data.frame:
    //
    //   isTRUE(rownames) || (is.null(rownames) &&
    //     is.character(attr(x, "row.names")) &&
    //     !all(grepl("^\\d+$", row.names(x))))
    //
    // So `_row` appears only when the attribute is a *character* vector and at
    // least one element is not all digits. Anything else -- automatic names,
    // an integer vector, or character names that all look like numbers -- is
    // uninformative and omitted.
    //
    // This used to compare against 1..n instead, which emitted `_row` for
    // integer row names c(5,6,7) and for character c("5","6","7") where
    // jsonlite emits nothing.
    if rn == libR_sys::R_NilValue {
        return true;
    }
    if typeof_sexp(rn) != libR_sys::SEXPTYPE::STRSXP as u32 {
        return true;
    }
    let n = sexp_len(rn);
    for i in 0..n {
        let s_sexp = libR_sys::STRING_ELT(rn, i as isize);
        if is_na_string(s_sexp) {
            return false;
        }
        let len = libR_sys::Rf_xlength(s_sexp);
        // `\d+` needs at least one digit, so "" is informative.
        if len <= 0 {
            return false;
        }
        let bytes = slice::from_raw_parts(libR_sys::R_CHAR(s_sexp) as *const u8, len as usize);
        if !bytes.iter().all(|b| b.is_ascii_digit()) {
            return false;
        }
    }
    true
}

/// Is the `row.names` attribute the compact automatic form, `c(NA, -n)`?
///
/// Distinct from `is_default_rownames`, which answers a different question --
/// whether jsonlite OMITS `_row` when `rownames` was not given. Integer row
/// names and character ones that are all digits are omitted by default but are
/// still real names, and `rownames = TRUE` prints those rather than 1..n.
#[inline]
pub(crate) unsafe fn is_compact_rownames(rn: libR_sys::SEXP) -> bool {
    if rn == libR_sys::R_NilValue {
        return true;
    }
    typeof_sexp(rn) == libR_sys::SEXPTYPE::INTSXP as u32
        && sexp_len(rn) == 2
        && is_na_int(*libR_sys::INTEGER(rn))
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
/// JSON text that `.prep()` rendered itself -- a mongo date or binary, a
/// complex row -- and that is always spliced.
///
/// Distinct from `json`, which is a class a USER can put on any string and
/// which `json_verbatim = FALSE`, the default, means "escape it like the
/// string it is". Both used to be `json`, so turning the user's off turned
/// ours off too and mongo timestamps came out quoted: 21 tests.
pub(crate) const CLS_FGJSON: u16 = 1 << 11;

/// Is this value's text spliced into the output rather than escaped?
#[inline]
pub(crate) fn splices_verbatim(cls: u16, config: SerializerConfig) -> bool {
    cls & CLS_FGJSON != 0 || (cls & CLS_JSON != 0 && config.json_verbatim)
}

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
            b"fgjson" => CLS_FGJSON,
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
                write_f64_json(bytes, v, config);
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

/// One cell of a numeric or logical array, read through a raw pointer.
///
/// The same three element writers the recursive serializer uses, so an array
/// column and a bare array cannot drift apart.
#[inline]
unsafe fn write_arr_num_cell(
    bytes: &mut Vec<u8>,
    ptr: usize,
    tag: u32,
    idx: usize,
    config: SerializerConfig,
) {
    match tag {
        ARR_REAL => write_real_elem(bytes, *(ptr as *const f64).add(idx), config),
        ARR_INT => write_int_elem(bytes, *(ptr as *const i32).add(idx), config),
        _ => write_lgl_elem(bytes, *(ptr as *const i32).add(idx), config),
    }
}

/// `write_arr_slice` for a column described rather than pre-rendered.
///
/// `shape` is the dimensions after the row one followed by their strides, so
/// `nd` is half its length.
pub(crate) unsafe fn write_arr_num_slice(
    bytes: &mut Vec<u8>,
    ptr: usize,
    tag: u32,
    shape: &[usize],
    j: usize,
    offset: usize,
    config: SerializerConfig,
) {
    let nd = shape.len() / 2;
    if j == nd {
        write_arr_num_cell(bytes, ptr, tag, offset, config);
        return;
    }
    bytes.push(b'[');
    for i in 0..shape[j] {
        if i > 0 {
            bytes.push(b',');
        }
        write_arr_num_slice(bytes, ptr, tag, shape, j + 1, offset + i * shape[nd + j], config);
    }
    bytes.push(b']');
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

/// `depth` is the recursion depth of the caller. The list-column and
/// nested-frame branches recurse back into the serializer, which now
/// serialises a nested data.frame through this same builder, so the counter
/// has to travel with it: hardcoding 0 here would let a chain of frames
/// joined by list columns recurse without a bound.
/// `Rtype` for a SEXP without building an `Robj` to ask.
///
/// Everything the branch chain below distinguishes is here; the rest maps to
/// `Null`, which is where those types already ended up.
#[inline]
unsafe fn rtype_of(sexp: libR_sys::SEXP) -> Rtype {
    match typeof_sexp(sexp) {
        t if t == libR_sys::SEXPTYPE::REALSXP as u32 => Rtype::Doubles,
        t if t == libR_sys::SEXPTYPE::INTSXP as u32 => Rtype::Integers,
        t if t == libR_sys::SEXPTYPE::LGLSXP as u32 => Rtype::Logicals,
        t if t == libR_sys::SEXPTYPE::STRSXP as u32 => Rtype::Strings,
        t if t == libR_sys::SEXPTYPE::VECSXP as u32 => Rtype::List,
        _ => Rtype::Null,
    }
}

pub(crate) fn build_thread_safe_cols(
    df: libR_sys::SEXP,
    // Already escaped, padded and colon-terminated, from `escaped_keys`, and
    // consumed rather than borrowed: the descriptor owns its key, so
    // borrowing here only meant cloning it back out.
    keys: Vec<Key>,
    skip_idx: usize,
    _expected_rows: usize,
    config: SerializerConfig,
    depth: u32,
) -> Result<Vec<(Key, ThreadSafeColumn)>> {
    let mut out = Vec::with_capacity(keys.len() + 1);
    // One base pointer for the frame, and no Robj per column.
    //
    // `List::elt` returns an Robj, whose constructor takes extendr's global
    // ownership mutex, inserts into a hash map and calls Rf_protect, with Drop
    // taking the same mutex again; `inherits` then fetches and walks the class
    // attribute once per name asked, and this asked up to six. Together they
    // measured 0.62 us per column of pure setup -- against about 30 ns to
    // write a three-row integer column -- so 200 small nested frames of two
    // columns spent nearly all of their time here. The recursive serializer
    // had already replaced exactly this pattern with a single `classify`
    // bitset; the column builder had not inherited it.
    let n_df_cols = unsafe { sexp_len(df) };
    let elems = unsafe { list_elems(df) };

    for (j, key) in keys.into_iter().enumerate() {
        if j == skip_idx { continue; }
        let raw = match elems {
            Some(p) if j < n_df_cols => unsafe { *p.add(j) },
            _ => return Err(Error::Other(format!("Column {} missing", j + 1))),
        };
        let mut cls = unsafe { classify(raw) };
        // A POSIXt column has to go through R, because only R holds the time
        // zone rules. A Date column does not: it is plain day arithmetic, and
        // the DateReal/DateInt column types below format it in the worker.
        // Routing Date through format() here cost 4.7 us per value, which was
        // 99% of the time spent serialising a Date column.
        //
        // `owned` holds format()'s result, which is a SEXP WE created rather
        // than one the caller handed us. R-exts puts the distinction plainly
        // (6.2 Allocating storage): an argument is protected by the caller for
        // the duration of the call, and nothing else is. This one is protected
        // only by `owned`, which dies at the end of this iteration -- so no
        // descriptor may point into it. See the `derived` test below, which is
        // what keeps that true.
        let mut sexp = raw;
        let mut owned: Option<Robj> = None;
        if cls & CLS_POSIXT != 0 {
            let f = call!("format", Robj::from_sexp(raw))
                .map_err(|e| Error::Other(format!("format failed: {:?}", e)))?;
            sexp = unsafe { f.get() };
            cls = unsafe { classify(sexp) };
            owned = Some(f);
        }
        // True when the column's bytes live in a SEXP this function created.
        let derived = owned.is_some();
        let r_type = unsafe { rtype_of(sexp) };

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
             // A logical matrix belongs here with the numeric ones: it needs
             // no encoding at all, so there was never a reason to render it
             // serially into an arena on the R thread.
             if dims.len() == 2
                 && n_matrix_cols <= u32::MAX as usize
                 && (r_type == Rtype::Doubles
                     || r_type == Rtype::Integers
                     || r_type == Rtype::Logicals)
             {
                 let (kind, ptr) = match r_type {
                     Rtype::Doubles => (ColumnType::MatrixReal, unsafe { libR_sys::REAL(sexp) as *const u8 as usize }),
                     Rtype::Logicals => (ColumnType::MatrixBool, unsafe { libR_sys::LOGICAL(sexp) as *const u8 as usize }),
                     _ => (ColumnType::MatrixInt, unsafe { libR_sys::INTEGER(sexp) as *const u8 as usize }),
                 };
                 out.push((key, ThreadSafeColumn {
                     kind,
                     aux: n_matrix_cols as u32,
                     data_ptr: ptr,
                     len: _expected_rows,
                     cached_levels: None,
                     string_arena: None,
                     char_meta: None,
                     arr_shape: None,
                 }));
                 continue;
             }

             let mut strides = vec![1usize; dims.len()];
             for k in 1..dims.len() {
                 strides[k] = strides[k - 1] * dims[k - 1];
             }

             // Three dimensions or more, and pointer-readable: described for
             // the workers like the two-dimensional case above, rather than
             // rendered serially into an arena. A 1000 x 20 x 10 double array
             // column cost 30.1 ns per value that way; the descriptor brings
             // it to what the matrix column costs.
             let tag = match r_type {
                 Rtype::Doubles => Some(ARR_REAL),
                 Rtype::Integers => Some(ARR_INT),
                 Rtype::Logicals => Some(ARR_LGL),
                 _ => None,
             };
             if let Some(tag) = tag {
                 let ptr = match tag {
                     ARR_REAL => unsafe { libR_sys::REAL(sexp) as *const u8 as usize },
                     ARR_INT => unsafe { libR_sys::INTEGER(sexp) as *const u8 as usize },
                     _ => unsafe { libR_sys::LOGICAL(sexp) as *const u8 as usize },
                 };
                 let mut shape: Vec<usize> = Vec::with_capacity((dims.len() - 1) * 2);
                 shape.extend_from_slice(&dims[1..]);
                 shape.extend_from_slice(&strides[1..]);
                 out.push((key, ThreadSafeColumn {
                     kind: ColumnType::ArrayDirect,
                     aux: tag,
                     data_ptr: ptr,
                     len: _expected_rows,
                     cached_levels: None,
                     string_arena: None,
                     char_meta: None,
                     arr_shape: Some(shape.into_boxed_slice()),
                 }));
                 continue;
             }

             // A character array is left here: its cells need the encoding
             // handling the plain column arms have, which allocates on R's
             // vmax stack and so cannot happen in a worker.
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
             let col = ThreadSafeColumn { kind: ColumnType::JsonRaw, aux: 0, data_ptr: 0, len: col_len, cached_levels: None, string_arena: Some(StringArena { bytes, offsets }), char_meta: None, arr_shape: None };
             out.push((key, col));
             continue;
        }

        // Set by the fgjtime branch below; every other kind leaves it zero.
        let mut aux: u32 = 0;
        let (kind, ptr, cached_levels, arena) =
            if cls & CLS_FGJTIME != 0 && r_type == Rtype::Doubles {
                aux = unsafe { fgj_fmt_code(sexp) }.unwrap_or(TFMT_SPACE);
                (ColumnType::TimeLocal, unsafe { libR_sys::REAL(sexp) as *const u8 as usize }, None, None)
            } else if cls & CLS_FACTOR != 0 && r_type == Rtype::Integers && config.factor == FactorMode::String {
                let levels_sexp = unsafe { levels_of(sexp) };
                if levels_sexp == unsafe { libR_sys::R_NilValue } {
                    return Err(Error::Other("Factor missing levels".to_string()));
                }
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
            } else if cls & CLS_DATE != 0 && (r_type == Rtype::Doubles || r_type == Rtype::Integers) {
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
                let is_json = splices_verbatim(cls, config);
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
                // `derived` takes the arena path below instead, which copies
                // the bytes out while they are still alive. CharDirect stores
                // a raw pointer into the STRSXP and the workers dereference it
                // long after this iteration has dropped `owned`: with two
                // POSIXt columns in one frame, the second format() call reused
                // the first's storage and every value in the first column came
                // out as the second's. Silently -- no crash, correct-looking
                // JSON with the wrong timestamps. Reproducible under
                // gctorture(TRUE); see tests/testthat/test-gctorture.R.
                if !is_json && !derived {
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
                    // Below this the pool costs more than the scan saves.
                    // There was no threshold at all, so a three-element
                    // character column fanned out over every worker.
                    const MIN_PAR_CELLS: usize = 4096;
                    let scan = |i: usize| -> u32 { unsafe {
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
                                // The encoding mark alone is not enough.
                                // `Encoding<-` sets it without validating, so
                                // a CE_UTF8 string can hold bytes that are not
                                // UTF-8 -- which is why base R ships
                                // validUTF8(). Admitting one here sent it
                                // straight to the output, and because this
                                // prepass runs on workers while STR_STATE is
                                // thread-local, nothing marked STR_NON_ASCII,
                                // so finish_json_string took the *unchecked*
                                // from_utf8_unchecked branch on invalid bytes.
                                // That is undefined behaviour, reachable from
                                //   x <- rawToChar(as.raw(0xe9))
                                //   Encoding(x) <- "UTF-8"
                                //
                                // The mark still has to be checked: latin1
                                // bytes that happen to form valid UTF-8 must
                                // be translated, not copied. Failing either
                                // test routes the cell to the arena, where
                                // charsxp_to_utf8_bytes sets STR_NON_ASCII and
                                // the checked branch turns it into a clean R
                                // error instead.
                                // Rf_charIsUTF8 rather than the encoding
                                // mark: readLines() and rawToChar() hand back
                                // CE_NATIVE strings whose bytes are already
                                // UTF-8 in a UTF-8 locale, and testing the
                                // mark alone sent all of them to the arena --
                                // 22.70 ms against 7.56 for the same 300,000
                                // values and byte-identical output. It answers
                                // from the header bits and the locale and does
                                // not validate, so the from_utf8 check below
                                // still has to run: rawToChar(as.raw(0xe9)) is
                                // CE_NATIVE and is not valid UTF-8. latin1
                                // still answers false, which is what keeps a
                                // latin1 string that happens to be valid UTF-8
                                // going through translation.
                                if !bytes.is_ascii()
                                    && (Rf_charIsUTF8(cs) == 0
                                        || std::str::from_utf8(bytes).is_err())
                                {
                                    // A latin1 byte from 0xA0 up is its own
                                    // code point, so widening it needs no
                                    // table, no locale and no call into R --
                                    // and that covers accented text, which is
                                    // what latin1 columns are made of.
                                    //
                                    // 0x80..=0x9F is the exception and has to
                                    // go to R. What R means by "latin1" there
                                    // is not fixed: this box maps 0x80 to the
                                    // euro sign and 0x9F to Y-diaeresis, which
                                    // is CP1252, while a strict ISO-8859-1
                                    // iconv maps them to the C1 controls. 27
                                    // of the 256 bytes differ between the two,
                                    // and a table baked in here would be wrong
                                    // on whichever platform it did not match.
                                    // Sending only those cells through
                                    // Rf_translateCharUTF8 keeps the answer
                                    // the platform's own.
                                    if Rf_charIsLatin1(cs) != 0
                                        && !bytes.iter().any(|&b| (0x80..0xA0).contains(&b))
                                    {
                                        return CD_LATIN1 | len as u32;
                                    }
                                    return CD_XLATE;
                                }
                                let mut d = len as u32;
                                if find_escape(bytes).is_some() {
                                    d |= CD_ESC;
                                }
                                d
                            } };
                    let meta: Vec<u32> = if n >= MIN_PAR_CELLS && desired_threads() > 1 {
                        with_pool(|| (0..n).into_par_iter().map(&scan).collect())
                    } else {
                        (0..n).map(&scan).collect()
                    };
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
                        // Each translated string is R_alloc'd and would be held
                        // to the end of the .Call; the mark is restored once
                        // its bytes are safely in `bytes`, so the column costs
                        // one string of vmax rather than all of them.
                        let vmax = unsafe { vmaxget() };
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
                            // The slice above is dead by here -- its bytes were
                            // copied or escaped into `bytes` -- so the stack
                            // can go back.
                            unsafe { vmaxset(vmax) };
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
                                arr_shape: None,
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
                // A `json` column is asJSON("json"), which returns its text
                // verbatim and never collapses it into an array, so a
                // length-one one must not gain brackets. In column-oriented
                // output the column IS that value, so it is written whole.
                // toJSON() errors on a longer json column in this mode, so
                // there is nothing to match past one.
                if is_json && config.df == DfMode::Columns && n == 1 {
                    let len = bytes.len();
                    out.push((key, ThreadSafeColumn {
                        kind: ColumnType::JsonWhole,
                        aux: 0,
                        data_ptr: 0,
                        len: 1,
                        cached_levels: None,
                        string_arena: Some(StringArena { bytes, offsets: vec![(0, len)] }),
                        char_meta: None,
                        arr_shape: None,
                    }));
                    continue;
                }
                let kind = if is_json { ColumnType::JsonRaw } else { ColumnType::Char };
                (kind, 0, None, Some(StringArena { bytes, offsets }))
            } else if cls & CLS_SFC != 0 {
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
            } else if cls & CLS_DATA_FRAME != 0 {
                // A data.frame-valued column (as produced by tidyr::nest) is a
                // VECSXP whose elements are the nested frame's COLUMNS. Walking
                // it as an ordinary list column therefore transposed the data:
                // row 1 received the whole first column and later rows nothing.
                // Emit one object per row of the nested frame instead.
                let nested_rows = unsafe { get_df_nrows(sexp) };
                let n_nested_cols = unsafe { sexp_len(sexp) };
                // Described once, then written per row -- the same treatment
                // the frame would get at top level. The previous loop called
                // the per-cell writer, which reclassified the column and
                // re-escaped its name for every row and had no idea what a
                // Date or a matrix column was: `{"d":18262}` for a Date and
                // `{"m":1}` for the row [1,3].
                let nested_props = unsafe { escaped_keys(sexp, n_nested_cols) }
                    .and_then(|nested_keys| {
                        build_thread_safe_cols(
                            sexp,
                            nested_keys,
                            usize::MAX,
                            nested_rows,
                            config,
                            depth + 1,
                        )
                        .ok()
                    });
                // The nested frame inherits the enclosing orientation, which
                // it used to ignore: toJSON() renders it column-oriented under
                // `dataframe = "columns"` -- one object for the whole column,
                // not one per row -- and as a bare array of values under
                // `"values"`.
                if config.df == DfMode::Columns {
                    let mut w = JsonWriter::with_capacity(nested_rows * 16 + 32);
                    match nested_props {
                        Some(ref props) => {
                            w.push_u8(b'{');
                            let mut first = true;
                            for (k, c) in props.iter() {
                                if !first {
                                    w.push_u8(b',');
                                }
                                w.push_key(k);
                                w.push_u8(b'[');
                                for r in 0..nested_rows {
                                    if r > 0 {
                                        w.push_u8(b',');
                                    }
                                    write_col_value(&mut w, r, c, config);
                                }
                                w.push_u8(b']');
                                first = false;
                            }
                            w.push_u8(b'}');
                        }
                        None => w.push_bytes(b"null"),
                    }
                    let len = w.buf.len();
                    let arena = StringArena { bytes: w.buf, offsets: vec![(0, len)] };
                    out.push((key, ThreadSafeColumn {
                        kind: ColumnType::JsonWhole,
                        aux: 0,
                        data_ptr: 0,
                        len: 1,
                        cached_levels: None,
                        string_arena: Some(arena),
                        char_meta: None,
                        arr_shape: None,
                    }));
                    continue;
                }
                let values = config.df == DfMode::Values;
                let mut w = JsonWriter::with_capacity(_expected_rows * 64);
                let mut offsets = Vec::with_capacity(_expected_rows);
                for r in 0.._expected_rows {
                    let start = w.buf.len();
                    match nested_props {
                        // A frame shorter than the enclosing one leaves the
                        // remaining rows null, not an empty object.
                        Some(ref props) if r < nested_rows => {
                            if values {
                                process_row_values(&mut w, r, props, config)
                            } else {
                                process_row_generic(&mut w, r, props, config)
                            }
                        }
                        _ => w.push_bytes(b"null"),
                    }
                    offsets.push((start, w.buf.len() - start));
                }
                (ColumnType::JsonRaw, 0, None, Some(StringArena { bytes: w.buf, offsets }))
            } else if r_type == Rtype::List {
                let n = unsafe { sexp_len(sexp) };
                let mut bytes = Vec::with_capacity(n * 64);
                let mut offsets = Vec::with_capacity(n);
                for i in 0..n {
                    let item = unsafe { libR_sys::VECTOR_ELT(sexp, i as isize) };
                    let start = bytes.len();
                    unsafe { serialize_sexp_to_json_buffer(item, &mut bytes, config, depth + 1); }
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
        out.push((key, ThreadSafeColumn { kind, aux, data_ptr: ptr, len: col_len, cached_levels, string_arena: arena, char_meta: None, arr_shape: None }));
    }

    // The stored attribute, so automatic row names are recognised from the
    // compact c(NA, -n) in constant time. Rf_getAttrib expands them to a 1..n
    // sequence, which cost an O(n) scan and a 4n-byte materialisation here for
    // every frame that has no row names to emit -- i.e. the common case.
    // Rf_getAttrib rather than an ATTRIB walk: ATTRIB is not part of R's API
    // (R-exts 6.21.6) and R CMD check reports it. This runs on the R thread,
    // where getAttrib's header write is harmless, and its expansion of the
    // compact form does not change what is_default_rownames answers.
    let rn_sexp = unsafe { libR_sys::Rf_getAttrib(df, libR_sys::R_RowNamesSymbol) };
    // toJSON()'s rule, established against it directly: `rownames = TRUE`
    // always emits `_row` and renders row.names(x) by type -- an integer
    // unquoted, a character quoted -- while the absent case emits it only when
    // `is_default_rownames` says the names are informative. The two tests are
    // different questions: c("7", "8") is omitted by default but printed, as
    // strings, when asked for.
    let force = config.rownames == ROWNAMES_ALWAYS;
    let rn_informative = !unsafe { is_default_rownames(rn_sexp) };
    if force && unsafe { is_compact_rownames(rn_sexp) } {
        // Asked for row names that are not stored at all, so row.names()
        // materialises 1..n, which renders as unquoted integers.
        let key = Key::from_name(b"_row");
        let mut bytes = Vec::with_capacity(_expected_rows * 8);
        let mut offsets = Vec::with_capacity(_expected_rows);
        for i in 0.._expected_rows {
            let start = bytes.len();
            let mut tmp = itoa::Buffer::new();
            bytes.extend_from_slice(tmp.format(i + 1).as_bytes());
            offsets.push((start, bytes.len() - start));
        }
        let rn_len = offsets.len();
        out.push((key, ThreadSafeColumn { kind: ColumnType::JsonRaw, aux: 0, data_ptr: 0, len: rn_len, cached_levels: None, string_arena: Some(StringArena { bytes, offsets }), char_meta: None, arr_shape: None }));
        return Ok(out);
    }
    if config.rownames != ROWNAMES_NEVER && (force || rn_informative) {
        let key = Key::from_name(b"_row");
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
            // Unquoted, which is what toJSON() writes for integer row names.
            // Only reachable under `rownames = TRUE`: without it, integer row
            // names are among the ones jsonlite omits.
            let p = unsafe { libR_sys::INTEGER(rn_sexp) };
            for i in 0..n {
                let v = unsafe { *p.add(i) };
                if unsafe { is_na_int(v) } { offsets.push((usize::MAX, 0)); }
                else {
                    let start = bytes.len();
                    let mut tmp = itoa::Buffer::new();
                    bytes.extend_from_slice(tmp.format(v).as_bytes());
                    offsets.push((start, bytes.len() - start));
                }
            }
        }
        let rn_len = offsets.len();
        out.push((key, ThreadSafeColumn { kind: ColumnType::JsonRaw, aux: 0, data_ptr: 0, len: rn_len, cached_levels: None, string_arena: Some(StringArena { bytes, offsets }), char_meta: None, arr_shape: None }));
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
        // Rendered once for the whole column, so every row sees it.
        ColumnType::JsonWhole => true,
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
        // A timestamp is a string by the time jsonlite emits it, so NaN and
        // the infinities format to ordinary text and are not missing. A true
        // NA is, and so is an instant format() cannot render, which R hands
        // back as NA; see time_cell_is_missing.
        ColumnType::TimeLocal => unsafe {
            time_cell_is_missing(*(col.data_ptr as *const f64).add(row))
        },
        // A matrix cell is an array, which jsonlite always emits, so the key
        // is never dropped in row mode.
        ColumnType::MatrixReal | ColumnType::MatrixInt | ColumnType::MatrixBool
        | ColumnType::ArrayDirect => false,
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
        ColumnType::JsonWhole | ColumnType::Null => false,
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
            } else if d & CD_LATIN1 != 0 {
                // Widened here rather than by Rf_translateCharUTF8, which
                // would have to run on the R thread, one iconv call per cell.
                let cs = *(col.data_ptr as *const libR_sys::SEXP).add(row);
                let src = slice::from_raw_parts(
                    libR_sys::R_CHAR(cs) as *const u8,
                    (d & CD_LEN) as usize,
                );
                // Taken out and put back so the escaper can borrow `buf`
                // while this borrows `scratch`; a Vec swap, not a copy.
                let mut wide = std::mem::take(&mut out.scratch);
                wide.clear();
                widen_latin1_into(&mut wide, src);
                escape_json_string_into(&mut out.buf, &wide);
                out.scratch = wide;
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
        // R stores a matrix column-major, so this row's values sit `nrow`
        // apart. Walking a pointer by that stride removes the multiply and the
        // address computation from what is now a very hot numeric kernel and
        // gives LLVM one strided stream instead of an indexed load.
        ColumnType::MatrixReal => unsafe {
            let nrow = col.len;
            let ncol = col.aux as usize;
            let mut p = (col.data_ptr as *const f64).add(row);
            out.buf.reserve(ncol * 26 + 2);
            out.push_u8(b'[');
            for c in 0..ncol {
                if c > 0 {
                    out.push_u8(b',');
                }
                let v = *p;
                p = p.add(nrow);
                // Finite first, as every other numeric writer here does. This
                // one asked is_na_real before is_finite, so an ordinary number
                // paid for an NA test it could never satisfy.
                if v.is_finite() {
                    out.push_f64_cfg(v, config);
                } else if config.na == NaMode::String || config.na == NaMode::Smart {
                    if v == f64::INFINITY {
                        out.push_bytes(b"\"Inf\"");
                    } else if v == f64::NEG_INFINITY {
                        out.push_bytes(b"\"-Inf\"");
                    } else if is_na_real(v) {
                        out.push_bytes(b"\"NA\"");
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
            let mut p = (col.data_ptr as *const i32).add(row);
            out.buf.reserve(ncol * 12 + 2);
            out.push_u8(b'[');
            for c in 0..ncol {
                if c > 0 {
                    out.push_u8(b',');
                }
                let v = *p;
                p = p.add(nrow);
                if !is_na_int(v) {
                    out.push_i32(v);
                } else if config.na == NaMode::String || config.na == NaMode::Smart {
                    out.push_bytes(b"\"NA\"");
                } else {
                    out.push_bytes(b"null");
                }
            }
            out.push_u8(b']');
        },
        ColumnType::ArrayDirect => unsafe {
            let shape = match col.arr_shape {
                Some(ref v) => &v[..],
                None => {
                    out.push_bytes(b"null");
                    return;
                }
            };
            write_arr_num_slice(&mut out.buf, col.data_ptr, col.aux, shape, 0, row, config);
        },
        ColumnType::MatrixBool => unsafe {
            let nrow = col.len;
            let ncol = col.aux as usize;
            let mut p = (col.data_ptr as *const i32).add(row);
            out.buf.reserve(ncol * 6 + 2);
            out.push_u8(b'[');
            for c in 0..ncol {
                if c > 0 {
                    out.push_u8(b',');
                }
                let v = *p;
                p = p.add(nrow);
                if !is_na_int(v) {
                    out.push_bool(v != 0);
                } else if config.na == NaMode::String {
                    out.push_bytes(b"\"NA\"");
                } else {
                    out.push_bytes(b"null");
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
        // Only reachable if a whole-column blob were asked for cell by cell,
        // which only the column-oriented writer produces and only it consumes.
        ColumnType::JsonWhole => {
            let a = col.string_arena.as_ref().unwrap();
            let (start, len) = a.offsets[0];
            out.push_bytes(&a.bytes[start..start + len]);
        }
        ColumnType::Null => out.push_bytes(b"null"),
    }
}

/// Writes `"key":value` for `row`, or nothing at all.
///
/// Returns whether anything was written, so the caller can manage separators.
/// The skip decision is taken before any byte is emitted.
#[inline(always)]
/// Writes `"key":value` for one cell, or nothing at all when row-oriented
/// output would drop the key.
///
/// The default `na = "smart"` used to cost four dispatches on the column kind
/// and two loads of the cell to write one number: `col_is_missing` calls
/// `col_available` and then matches and loads, and `write_col_value` calls
/// `col_available` again and matches and loads again. For the three kinds that
/// dominate, the test and the write come off one load below, and the arms are
/// the exact negation of `col_is_missing`'s -- `is_na_int` for Int and Bool,
/// `!is_finite` for Real -- so the two cannot drift.
///
/// The key is still written only after the decision is made. Writing it first
/// and rewinding would work, since the caller marks the buffer before calling,
/// but the split exists because an earlier version pushed the key and then
/// bailed out of the Factor and Char arms, leaving a dangling `"key":` in the
/// output. Not reintroducing that shape.
pub(crate) fn try_write_kv(
    out: &mut JsonWriter,
    row: usize,
    key: &Key,
    col: &ThreadSafeColumn,
    config: SerializerConfig,
) -> bool {
    if config.na != NaMode::Smart {
        // Nothing is ever omitted, so there is no decision to make.
        out.push_key(key);
        write_col_value(out, row, col, config);
        return true;
    }
    match col.kind {
        ColumnType::Int => unsafe {
            if row >= col.len {
                return false;
            }
            let v = *(col.data_ptr as *const i32).add(row);
            if is_na_int(v) {
                return false;
            }
            out.push_key(key);
            out.push_i32(v);
        },
        ColumnType::Real => unsafe {
            if row >= col.len {
                return false;
            }
            let v = *(col.data_ptr as *const f64).add(row);
            if !v.is_finite() {
                return false;
            }
            out.push_key(key);
            out.push_f64_cfg(v, config);
        },
        ColumnType::Bool => unsafe {
            if row >= col.len {
                return false;
            }
            let v = *(col.data_ptr as *const i32).add(row);
            if is_na_int(v) {
                return false;
            }
            out.push_key(key);
            out.push_bool(v != 0);
        },
        // Everything else keeps the split. A matrix or array cell is never
        // missing, so its test is a constant; the string and date writers are
        // long enough that fusing them would duplicate real logic.
        _ => {
            if col_is_missing(col, row) {
                return false;
            }
            out.push_key(key);
            write_col_value(out, row, col, config);
        }
    }
    true
}

// ------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------
// `ThreadSafeColumn` is plain data over a raw pointer, so a column can be
// built here over a `Vec` and the cell writers exercised with no R object
// anywhere. The kinds that read a CHARSXP directly are the exception and are
// left to the R-level suites; the arena-backed string kinds are covered, since
// those read only the arena.

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(na: NaMode) -> SerializerConfig {
        SerializerConfig {
            df: DfMode::Rows,
            na,
            null: NullMode::List,
            factor: FactorMode::String,
            auto_unbox: false,
            digits: Some(4),
            matrix_colmajor: false,
            always_decimal: false,
            signif: false,
            json_verbatim: false,
            rownames: ROWNAMES_REAL,
        }
    }

    fn bare(kind: ColumnType, ptr: usize, len: usize) -> ThreadSafeColumn {
        ThreadSafeColumn {
            kind,
            aux: 0,
            data_ptr: ptr,
            len,
            cached_levels: None,
            string_arena: None,
            char_meta: None,
            arr_shape: None,
        }
    }

    fn cell(col: &ThreadSafeColumn, row: usize, c: SerializerConfig) -> String {
        let mut w = JsonWriter::with_capacity(0);
        write_col_value(&mut w, row, col, c);
        String::from_utf8(w.buf).unwrap()
    }

    fn kv(col: &ThreadSafeColumn, row: usize, c: SerializerConfig) -> Option<String> {
        let k = Key::from_name(b"x");
        let mut w = JsonWriter::with_capacity(0);
        if try_write_kv(&mut w, row, &k, col, c) {
            Some(String::from_utf8(w.buf).unwrap())
        } else {
            assert!(w.buf.is_empty(), "an omitted field left bytes behind");
            None
        }
    }

    fn na_real() -> f64 {
        f64::from_bits(0x7FF0_0000_0000_07A2)
    }

    #[test]
    fn numeric_cells_spell_the_non_finite_values() {
        let v = vec![1.5f64, na_real(), f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
        let col = bare(ColumnType::Real, v.as_ptr() as usize, v.len());
        let want_null = ["1.5", "null", "null", "null", "null"];
        let want_str = ["1.5", "\"NA\"", "\"NaN\"", "\"Inf\"", "\"-Inf\""];
        for i in 0..v.len() {
            assert_eq!(cell(&col, i, cfg(NaMode::Null)), want_null[i], "row {}", i);
            assert_eq!(cell(&col, i, cfg(NaMode::String)), want_str[i], "row {}", i);
        }
    }

    #[test]
    fn integer_and_logical_cells() {
        let iv = vec![7i32, i32::MIN, -3];
        let ic = bare(ColumnType::Int, iv.as_ptr() as usize, iv.len());
        assert_eq!(cell(&ic, 0, cfg(NaMode::Null)), "7");
        assert_eq!(cell(&ic, 1, cfg(NaMode::Null)), "null");
        assert_eq!(cell(&ic, 1, cfg(NaMode::String)), "\"NA\"");
        assert_eq!(cell(&ic, 2, cfg(NaMode::Null)), "-3");

        let lv = vec![1i32, 0, i32::MIN];
        let lc = bare(ColumnType::Bool, lv.as_ptr() as usize, lv.len());
        assert_eq!(cell(&lc, 0, cfg(NaMode::Null)), "true");
        assert_eq!(cell(&lc, 1, cfg(NaMode::Null)), "false");
        assert_eq!(cell(&lc, 2, cfg(NaMode::Null)), "null");
        assert_eq!(cell(&lc, 2, cfg(NaMode::String)), "\"NA\"");
    }

    #[test]
    fn the_fused_arms_agree_with_the_pair_they_replaced() {
        // try_write_kv decides and writes from one load for Int, Real and
        // Bool. The tests it fuses are meant to be the exact negation of
        // col_is_missing's, so the fused answer must equal what the split pair
        // would have produced for every row of every kind.
        let dv = vec![1.5f64, na_real(), f64::NAN, f64::INFINITY, -0.0, 1e-9];
        let iv = vec![7i32, i32::MIN, 0, -1];
        let lv = vec![1i32, 0, i32::MIN];
        let cols = [
            bare(ColumnType::Real, dv.as_ptr() as usize, dv.len()),
            bare(ColumnType::Int, iv.as_ptr() as usize, iv.len()),
            bare(ColumnType::Bool, lv.as_ptr() as usize, lv.len()),
        ];
        for col in &cols {
            for na in [NaMode::Null, NaMode::String, NaMode::Smart] {
                let c = cfg(na);
                for row in 0..col.len {
                    let fused = kv(col, row, c);
                    // What the split pair would have done.
                    let split = if na == NaMode::Smart && col_is_missing(col, row) {
                        None
                    } else {
                        let k = Key::from_name(b"x");
                        let mut w = JsonWriter::with_capacity(0);
                        w.push_key(&k);
                        write_col_value(&mut w, row, col, c);
                        Some(String::from_utf8(w.buf).unwrap())
                    };
                    assert_eq!(fused, split, "kind {:?} row {} na {:?}", col.kind, row, na);
                }
            }
        }
    }

    #[test]
    fn a_row_past_the_end_is_never_read() {
        // A frame built with structure() can declare more rows than a column
        // holds. Reading past the end used to serialise adjacent heap bytes.
        let v = vec![1.5f64];
        let col = bare(ColumnType::Real, v.as_ptr() as usize, 1);
        assert_eq!(cell(&col, 5, cfg(NaMode::Null)), "null");
        assert_eq!(kv(&col, 5, cfg(NaMode::Smart)), None);
        assert!(col_is_missing(&col, 5));
        assert!(!col_available(&col, 5));
    }

    #[test]
    fn matrix_cells_walk_the_column_major_stride() {
        // Three rows, two matrix columns: R stores them column-major, so row 1
        // is elements 1 and 4.
        let m = vec![1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0];
        let mut col = bare(ColumnType::MatrixReal, m.as_ptr() as usize, 3);
        col.aux = 2;
        assert_eq!(cell(&col, 0, cfg(NaMode::Null)), "[1,4]");
        assert_eq!(cell(&col, 1, cfg(NaMode::Null)), "[2,5]");
        assert_eq!(cell(&col, 2, cfg(NaMode::Null)), "[3,6]");
        // Never missing: an array is always emitted, so the key stays.
        assert!(!col_is_missing(&col, 0));

        let mi = vec![1i32, 2, 3, i32::MIN, 5, 6];
        let mut ic = bare(ColumnType::MatrixInt, mi.as_ptr() as usize, 3);
        ic.aux = 2;
        assert_eq!(cell(&ic, 0, cfg(NaMode::Null)), "[1,null]");
        assert_eq!(cell(&ic, 0, cfg(NaMode::String)), "[1,\"NA\"]");

        let ml = vec![1i32, 0, i32::MIN, 1, 0, 1];
        let mut lc = bare(ColumnType::MatrixBool, ml.as_ptr() as usize, 3);
        lc.aux = 2;
        assert_eq!(cell(&lc, 0, cfg(NaMode::Null)), "[true,true]");
        assert_eq!(cell(&lc, 2, cfg(NaMode::Null)), "[null,true]");
    }

    #[test]
    fn array_cells_nest_over_the_trailing_dimensions() {
        // A 2 x 3 x 2 double array: shape carries the dimensions after the row
        // one, then their strides.
        let a: Vec<f64> = (1..=12).map(|i| i as f64).collect();
        let mut col = bare(ColumnType::ArrayDirect, a.as_ptr() as usize, 2);
        col.aux = ARR_REAL;
        col.arr_shape = Some(vec![3usize, 2, 2, 6].into_boxed_slice());
        // Row 0 takes elements 0, 2, 4 (j) crossed with 0, 6 (k).
        assert_eq!(cell(&col, 0, cfg(NaMode::Null)), "[[1,7],[3,9],[5,11]]");
        assert_eq!(cell(&col, 1, cfg(NaMode::Null)), "[[2,8],[4,10],[6,12]]");
        assert!(!col_is_missing(&col, 0));
    }

    #[test]
    fn arena_backed_strings_come_out_verbatim() {
        // Char and JsonRaw read only the arena, so no CHARSXP is involved.
        let arena = StringArena {
            bytes: b"\"a\"\"bb\"null".to_vec(),
            offsets: vec![(0, 3), (3, 4), (usize::MAX, 0), (7, 4)],
        };
        let mut col = bare(ColumnType::Char, 0, 4);
        col.string_arena = Some(arena);
        assert_eq!(cell(&col, 0, cfg(NaMode::Null)), "\"a\"");
        assert_eq!(cell(&col, 1, cfg(NaMode::Null)), "\"bb\"");
        assert_eq!(cell(&col, 2, cfg(NaMode::Null)), "null");
        assert_eq!(cell(&col, 2, cfg(NaMode::String)), "\"NA\"");
        assert!(col_is_missing(&col, 2));
        assert!(!col_is_missing(&col, 0));
    }

    #[test]
    fn a_factor_reads_its_pre_escaped_levels() {
        let codes = vec![1i32, 2, i32::MIN, 0, 99];
        let mut col = bare(ColumnType::Factor, codes.as_ptr() as usize, codes.len());
        col.cached_levels = Some(vec![b"\"a\"".to_vec(), b"\"b\"".to_vec()]);
        assert_eq!(cell(&col, 0, cfg(NaMode::Null)), "\"a\"");
        assert_eq!(cell(&col, 1, cfg(NaMode::Null)), "\"b\"");
        assert_eq!(cell(&col, 2, cfg(NaMode::Null)), "null");
        // A code of zero or past the levels is missing, not a panic.
        assert_eq!(cell(&col, 3, cfg(NaMode::Null)), "null");
        assert_eq!(cell(&col, 4, cfg(NaMode::Null)), "null");
        assert!(col_is_missing(&col, 2));
        assert!(col_is_missing(&col, 3));
    }

    #[test]
    fn a_row_writer_never_leaves_a_dangling_key() {
        // The split between deciding and writing exists because an earlier
        // version pushed the key and then bailed out of an arm. Whatever is
        // written must be a complete object.
        let dv = vec![1.5f64, na_real()];
        let col = bare(ColumnType::Real, dv.as_ptr() as usize, dv.len());
        let props = vec![(Key::from_name(b"a"), col)];
        for na in [NaMode::Null, NaMode::String, NaMode::Smart] {
            for row in 0..2 {
                let mut w = JsonWriter::with_capacity(0);
                process_row_generic(&mut w, row, &props, cfg(na));
                let s = String::from_utf8(w.buf).unwrap();
                assert!(s.starts_with('{') && s.ends_with('}'), "{:?}", s);
                assert!(!s.contains(":}"), "dangling key in {:?}", s);
                assert!(!s.contains(",}"), "dangling separator in {:?}", s);
            }
        }
        // Smart drops the whole field, leaving an empty object.
        let mut w = JsonWriter::with_capacity(0);
        process_row_generic(&mut w, 1, &props, cfg(NaMode::Smart));
        assert_eq!(String::from_utf8(w.buf).unwrap(), "{}");
    }

    #[test]
    fn values_rows_drop_the_keys_but_keep_the_positions() {
        let dv = vec![1.5f64, na_real()];
        let iv = vec![7i32, 8];
        let props = vec![
            (Key::from_name(b"a"), bare(ColumnType::Real, dv.as_ptr() as usize, 2)),
            (Key::from_name(b"b"), bare(ColumnType::Int, iv.as_ptr() as usize, 2)),
        ];
        let mut w = JsonWriter::with_capacity(0);
        process_row_values(&mut w, 0, &props, cfg(NaMode::Null));
        assert_eq!(String::from_utf8(w.buf).unwrap(), "[1.5,7]");
        // A missing value still occupies its position, or the array would
        // stop lining up with the columns.
        let mut w = JsonWriter::with_capacity(0);
        process_row_values(&mut w, 1, &props, cfg(NaMode::Null));
        assert_eq!(String::from_utf8(w.buf).unwrap(), "[null,8]");
    }
}
