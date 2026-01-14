use extendr_api::prelude::*;
use extendr_ffi as libR_sys;
use rayon::prelude::*;
use std::ffi::{CStr, c_char};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;

// ------------------------------------------------------------------
// CONFIGURATION & CONSTANTS
// ------------------------------------------------------------------
const PAR_CHUNK_ROWS: usize = 2048;

// Lookup Table for String Escaping (0=Safe, 1=", 2=\, 3=Control)
static ESCAPE_LUT: [u8; 256] = {
    let mut table = [0u8; 256];
    let mut i = 0;
    while i < 32 { table[i] = 3; i += 1; }
    table[b'"' as usize] = 1;
    table[b'\\' as usize] = 2;
    table
};

const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";

const FC_HEAD: &[u8] = br#"{"type":"FeatureCollection","features":["#;
const FC_TAIL: &[u8] = br#"]}"#;
const FEAT_HEAD: &[u8] = br#"{"type":"Feature","properties":{"#;
const FEAT_MID: &[u8] = br#"},"geometry":"#;

#[derive(Clone, Copy, Debug)]
enum ColumnType {
    Int,
    Real,
    Bool,
    Char,
    Factor,
    JsonRaw,
    Null,
}

#[derive(PartialEq, Clone, Copy, Debug)]
enum SfcType {
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    GeometryCollection,
    Unknown,
}

type PResult<T> = std::result::Result<T, String>;

#[inline]
fn rerr<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Other(msg.into()))
}

fn panic_message(p: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic (unknown payload)".to_string()
    }
}

// ------------------------------------------------------------------
// JSON WRITER (DIRECT FORMATTING OPTIMIZED)
// ------------------------------------------------------------------

struct JsonWriter {
    buf: Vec<u8>,
}

impl JsonWriter {
    #[inline]
    fn with_capacity(cap: usize) -> Self {
        Self { buf: Vec::with_capacity(cap) }
    }

    #[inline(always)]
    fn reserve(&mut self, additional: usize) {
        self.buf.reserve(additional);
    }

    #[inline(always)]
    fn push_u8(&mut self, b: u8) {
        self.buf.push(b);
    }

    #[inline(always)]
    fn push_bytes(&mut self, s: &[u8]) {
        self.buf.extend_from_slice(s);
    }

    #[inline(always)]
    fn push_i32(&mut self, v: i32) {
        let mut tmp = itoa::Buffer::new();
        self.push_bytes(tmp.format(v).as_bytes());
    }

    #[inline(always)]
    fn push_f64(&mut self, v: f64) {
        if !v.is_finite() {
            self.push_bytes(b"null");
            return;
        }
        // Format integer-like floats as integers (e.g. 10.0 -> 10)
        if v.fract() == 0.0 && v >= (i32::MIN as f64) && v <= (i32::MAX as f64) {
            self.push_i32(v as i32);
        } else {
            self.push_f64_direct(v);
        }
    }

    // OPTIMIZATION: Write float directly to the buffer, avoiding stack copy.
    #[inline(always)]
    fn push_f64_direct(&mut self, v: f64) {
        // ryu guarantees max 24 bytes for f64
        self.reserve(24);
        let len = self.buf.len();
        unsafe {
            let ptr = self.buf.as_mut_ptr().add(len);
            let written = ryu::raw::format64(v, ptr);
            self.buf.set_len(len + written);
        }
    }

    #[inline(always)]
    fn push_bool(&mut self, v: bool) {
        if v { self.push_bytes(b"true"); } else { self.push_bytes(b"false"); }
    }
}

#[inline]
fn escape_json_string_into(out: &mut Vec<u8>, bytes: &[u8]) {
    out.push(b'"');
    let mut start = 0;
    let len = bytes.len();

    while start < len {
        // FAST SCAN: Find next char needing escape using LUT
        let offset = bytes[start..].iter().position(|&b| ESCAPE_LUT[b as usize] != 0);

        match offset {
            Some(i) => {
                let esc_idx = start + i;
                out.extend_from_slice(&bytes[start..esc_idx]);
                let b = bytes[esc_idx];
                match ESCAPE_LUT[b as usize] {
                    1 => out.extend_from_slice(br#"\""#),
                    2 => out.extend_from_slice(br#"\\"#),
                    3 => {
                        out.extend_from_slice(br#"\u00"#);
                        out.push(HEX_DIGITS[(b >> 4) as usize]);
                        out.push(HEX_DIGITS[(b & 0x0F) as usize]);
                    }
                    _ => {}
                }
                start = esc_idx + 1;
            }
            None => {
                out.extend_from_slice(&bytes[start..]);
                break;
            }
        }
    }
    out.push(b'"');
}

#[inline]
fn build_escaped_key_bytes(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(name.len() + 4);
    escape_json_string_into(&mut key, name.as_bytes());
    key.push(b':');
    key
}

// ------------------------------------------------------------------
// C-API HELPERS
// ------------------------------------------------------------------

#[inline(always)]
unsafe fn sexp_len(x: libR_sys::SEXP) -> usize {
    let n = libR_sys::Rf_xlength(x); 
    if n < 0 { return 0; }
    n as usize
}

#[inline(always)]
unsafe fn typeof_sexp(x: libR_sys::SEXP) -> u32 {
    libR_sys::TYPEOF(x) as u32
}

#[inline(always)]
unsafe fn is_na_int(v: i32) -> bool {
    v == i32::MIN
}

#[inline(always)]
unsafe fn is_na_real(v: f64) -> bool {
    libR_sys::R_IsNA(v) != 0
}

#[inline(always)]
unsafe fn is_nan_real(v: f64) -> bool {
    libR_sys::R_IsNaN(v) != 0
}

#[inline(always)]
unsafe fn is_na_string(sexp: libR_sys::SEXP) -> bool {
    sexp == libR_sys::R_NaString
}

#[inline]
unsafe fn charsxp_to_utf8_bytes(charsxp: libR_sys::SEXP) -> Option<&'static [u8]> {
    if charsxp == libR_sys::R_NilValue { return None; }
    let len = libR_sys::Rf_xlength(charsxp);
    let ptr = libR_sys::R_CHAR(charsxp) as *const u8;
    Some(slice::from_raw_parts(ptr, len as usize))
}

#[inline]
unsafe fn get_df_nrows(sexp: libR_sys::SEXP) -> usize {
    let rn_sym = libR_sys::R_RowNamesSymbol;
    let rn = libR_sys::Rf_getAttrib(sexp, rn_sym);
    if rn == libR_sys::R_NilValue { return 0; }
    if typeof_sexp(rn) == libR_sys::SEXPTYPE::INTSXP as u32 && sexp_len(rn) == 2 {
        let p = libR_sys::INTEGER(rn);
        let first = *p;
        if first == libR_sys::R_NaInt {
            let second = *p.add(1);
            return second.abs() as usize;
        }
    }
    sexp_len(rn)
}

// ------------------------------------------------------------------
// SERIALIZER HELPERS
// ------------------------------------------------------------------

unsafe fn try_serialize_matrix(x: libR_sys::SEXP, r_type: u32, buf: &mut Vec<u8>) -> bool {
    let dim_sym = libR_sys::R_DimSymbol;
    let dim = libR_sys::Rf_getAttrib(x, dim_sym);
    
    if dim == libR_sys::R_NilValue || sexp_len(dim) != 2 || typeof_sexp(dim) != libR_sys::SEXPTYPE::INTSXP as u32 {
        return false;
    }

    let dim_ptr = libR_sys::INTEGER(dim);
    let nrows = *dim_ptr as usize;
    let ncols = *dim_ptr.add(1) as usize;

    buf.push(b'[');
    for r in 0..nrows {
        if r > 0 { buf.push(b','); }
        buf.push(b'[');
        for c in 0..ncols {
            if c > 0 { buf.push(b','); }
            let idx = r + c * nrows;
            match r_type {
                t if t == libR_sys::SEXPTYPE::INTSXP as u32 => {
                    let v = *libR_sys::INTEGER(x).add(idx);
                    if is_na_int(v) { buf.extend_from_slice(b"\"NA\""); }
                    else {
                        let mut tmp = itoa::Buffer::new();
                        buf.extend_from_slice(tmp.format(v).as_bytes());
                    }
                },
                t if t == libR_sys::SEXPTYPE::REALSXP as u32 => {
                    let v = *libR_sys::REAL(x).add(idx);
                    if is_na_real(v) { buf.extend_from_slice(b"\"NA\""); }
                    else if is_nan_real(v) { buf.extend_from_slice(b"\"NaN\""); }
                    else if v == f64::INFINITY { buf.extend_from_slice(b"\"Inf\""); }
                    else if v == f64::NEG_INFINITY { buf.extend_from_slice(b"\"-Inf\""); }
                    else {
                        if v.fract() == 0.0 && v >= (i32::MIN as f64) && v <= (i32::MAX as f64) {
                            let mut tmp = itoa::Buffer::new();
                            buf.extend_from_slice(tmp.format(v as i32).as_bytes());
                        } else {
                            // Inline direct write
                            buf.reserve(24);
                            let len = buf.len();
                            let ptr = buf.as_mut_ptr().add(len);
                            let written = ryu::raw::format64(v, ptr);
                            buf.set_len(len + written);
                        }
                    }
                },
                t if t == libR_sys::SEXPTYPE::LGLSXP as u32 => {
                    let v = *libR_sys::LOGICAL(x).add(idx);
                    if is_na_int(v) { buf.extend_from_slice(b"\"NA\""); }
                    else if v != 0 { buf.extend_from_slice(b"true"); }
                    else { buf.extend_from_slice(b"false"); }
                },
                t if t == libR_sys::SEXPTYPE::STRSXP as u32 => {
                    let s_sexp = libR_sys::STRING_ELT(x, idx as isize);
                    if is_na_string(s_sexp) { buf.extend_from_slice(b"\"NA\""); }
                    else if let Some(bytes) = charsxp_to_utf8_bytes(s_sexp) { escape_json_string_into(buf, bytes); }
                    else { buf.extend_from_slice(b"\"NA\""); }
                },
                _ => buf.extend_from_slice(b"null")
            }
        }
        buf.push(b']');
    }
    buf.push(b']');
    true
}

unsafe fn serialize_sexp_to_json_buffer(x: libR_sys::SEXP, buf: &mut Vec<u8>) {
    if x == libR_sys::R_NilValue {
        buf.extend_from_slice(b"{}");
        return;
    }
    let robj = Robj::from_sexp(x);

    // Factors
    if robj.inherits("factor") && typeof_sexp(x) == libR_sys::SEXPTYPE::INTSXP as u32 {
        if let Some(levels) = robj.get_attrib("levels") {
            let levels_sexp = levels.get();
            let n_levels = sexp_len(levels_sexp);
            let n = sexp_len(x);
            let p = libR_sys::INTEGER(x);

            buf.push(b'[');
            for i in 0..n {
                if i > 0 { buf.push(b','); }
                let v = *p.add(i);
                if is_na_int(v) || v < 1 { buf.extend_from_slice(b"\"NA\""); }
                else {
                    let idx = (v - 1) as usize;
                    if idx < n_levels {
                        let level_charsxp = libR_sys::STRING_ELT(levels_sexp, idx as isize);
                        if let Some(bytes) = charsxp_to_utf8_bytes(level_charsxp) { escape_json_string_into(buf, bytes); }
                        else { buf.extend_from_slice(b"\"NA\""); }
                    } else { buf.extend_from_slice(b"\"NA\""); }
                }
            }
            buf.push(b']');
            return;
        }
    }

    // Date / POSIXt
    if robj.inherits("Date") || robj.inherits("POSIXt") {
        if let Ok(char_robj) = call!("format", robj.clone()) {
            serialize_sexp_to_json_buffer(char_robj.get(), buf);
            return;
        }
    }

    let r_type = typeof_sexp(x);
    if try_serialize_matrix(x, r_type, buf) { return; }

    if r_type == libR_sys::SEXPTYPE::INTSXP as u32 {
        let n = sexp_len(x);
        let p = libR_sys::INTEGER(x);

        buf.push(b'[');
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let v = *p.add(i);
            if is_na_int(v) { buf.extend_from_slice(b"\"NA\""); }
            else {
                let mut tmp = itoa::Buffer::new();
                buf.extend_from_slice(tmp.format(v).as_bytes());
            }
        }
        buf.push(b']');
        return;
    }

    if r_type == libR_sys::SEXPTYPE::REALSXP as u32 {
        let n = sexp_len(x);
        let p = libR_sys::REAL(x);

        buf.push(b'[');
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let v = *p.add(i);
            if is_na_real(v) { buf.extend_from_slice(b"\"NA\""); }
            else if is_nan_real(v) { buf.extend_from_slice(b"\"NaN\""); }
            else if v == f64::INFINITY { buf.extend_from_slice(b"\"Inf\""); }
            else if v == f64::NEG_INFINITY { buf.extend_from_slice(b"\"-Inf\""); }
            else {
                if v.fract() == 0.0 && v >= (i32::MIN as f64) && v <= (i32::MAX as f64) {
                    let mut tmp = itoa::Buffer::new();
                    buf.extend_from_slice(tmp.format(v as i32).as_bytes());
                } else {
                    buf.reserve(24);
                    let len = buf.len();
                    let ptr = buf.as_mut_ptr().add(len);
                    let written = ryu::raw::format64(v, ptr);
                    buf.set_len(len + written);
                }
            }
        }
        buf.push(b']');
        return;
    }

    if r_type == libR_sys::SEXPTYPE::LGLSXP as u32 {
        let n = sexp_len(x);
        let p = libR_sys::LOGICAL(x);

        buf.push(b'[');
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let v = *p.add(i);
            if is_na_int(v) { buf.extend_from_slice(b"\"NA\""); }
            else if v != 0 { buf.extend_from_slice(b"true"); }
            else { buf.extend_from_slice(b"false"); }
        }
        buf.push(b']');
        return;
    }

    if r_type == libR_sys::SEXPTYPE::STRSXP as u32 {
        let n = sexp_len(x);
        
        buf.push(b'[');
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let s_sexp = libR_sys::STRING_ELT(x, i as isize);
            if is_na_string(s_sexp) { buf.extend_from_slice(b"\"NA\""); }
            else if let Some(bytes) = charsxp_to_utf8_bytes(s_sexp) { escape_json_string_into(buf, bytes); }
            else { buf.extend_from_slice(b"\"NA\""); }
        }
        buf.push(b']');
        return;
    }

    if r_type == libR_sys::SEXPTYPE::VECSXP as u32 {
        if robj.inherits("data.frame") {
            let n_rows = get_df_nrows(x);
            let n_cols = sexp_len(x);
            let names_sym = libR_sys::R_NamesSymbol;
            let names_sexp = libR_sys::Rf_getAttrib(x, names_sym);
            let has_names = names_sexp != libR_sys::R_NilValue && sexp_len(names_sexp) == n_cols;

            buf.push(b'[');
            for r in 0..n_rows {
                if r > 0 { buf.push(b','); }
                buf.push(b'{');
                let mut needs_comma = false;
                for c in 0..n_cols {
                    let col_sexp = libR_sys::VECTOR_ELT(x, c as isize);
                    let col_len = sexp_len(col_sexp);
                    if r < col_len {
                        if needs_comma { buf.push(b','); }
                        if has_names {
                            let key_charsxp = libR_sys::STRING_ELT(names_sexp, c as isize);
                            if !is_na_string(key_charsxp) {
                                if let Some(key_bytes) = charsxp_to_utf8_bytes(key_charsxp) { escape_json_string_into(buf, key_bytes); }
                                else { buf.extend_from_slice(b"\"\""); }
                            } else { buf.extend_from_slice(b"\"\""); }
                        } else { buf.extend_from_slice(b"\"\""); }
                        buf.push(b':');
                        serialize_element_at_index(col_sexp, r, buf);
                        needs_comma = true;
                    }
                }
                buf.push(b'}');
            }
            buf.push(b']');
            return;
        }

        let n = sexp_len(x);
        let names_sym = libR_sys::R_NamesSymbol;
        let names_sexp = libR_sys::Rf_getAttrib(x, names_sym);
        let has_names = names_sexp != libR_sys::R_NilValue && sexp_len(names_sexp) == n;
        
        if has_names {
            buf.push(b'{');
            for i in 0..n {
                if i > 0 { buf.push(b','); }
                let key_charsxp = libR_sys::STRING_ELT(names_sexp, i as isize);
                if !is_na_string(key_charsxp) {
                    if let Some(key_bytes) = charsxp_to_utf8_bytes(key_charsxp) { escape_json_string_into(buf, key_bytes); }
                    else { buf.extend_from_slice(b"\"\""); }
                } else { buf.extend_from_slice(b"\"\""); }
                buf.push(b':');
                let val_sexp = libR_sys::VECTOR_ELT(x, i as isize);
                serialize_sexp_to_json_buffer(val_sexp, buf);
            }
            buf.push(b'}');
        } else {
            buf.push(b'[');
            for i in 0..n {
                if i > 0 { buf.push(b','); }
                let val_sexp = libR_sys::VECTOR_ELT(x, i as isize);
                serialize_sexp_to_json_buffer(val_sexp, buf);
            }
            buf.push(b']');
        }
        return;
    }
    buf.extend_from_slice(b"{}");
}

unsafe fn serialize_element_at_index(col: libR_sys::SEXP, idx: usize, buf: &mut Vec<u8>) {
    let r_type = typeof_sexp(col);
    let robj = Robj::from_sexp(col);
    if robj.inherits("factor") && r_type == libR_sys::SEXPTYPE::INTSXP as u32 {
        if let Some(levels) = robj.get_attrib("levels") {
            let levels_sexp = levels.get();
            let p = libR_sys::INTEGER(col);
            let v = *p.add(idx);
            if is_na_int(v) || v < 1 { buf.extend_from_slice(b"null"); }
            else {
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
    match r_type {
        _ if r_type == libR_sys::SEXPTYPE::INTSXP as u32 => {
            let v = *libR_sys::INTEGER(col).add(idx);
            if is_na_int(v) { buf.extend_from_slice(b"null"); }
            else {
                let mut tmp = itoa::Buffer::new();
                buf.extend_from_slice(tmp.format(v).as_bytes());
            }
        },
        _ if r_type == libR_sys::SEXPTYPE::REALSXP as u32 => {
            let v = *libR_sys::REAL(col).add(idx);
            if is_na_real(v) || is_nan_real(v) || !v.is_finite() { buf.extend_from_slice(b"null"); }
            else {
                if v.fract() == 0.0 && v >= (i32::MIN as f64) && v <= (i32::MAX as f64) {
                    let mut tmp = itoa::Buffer::new();
                    buf.extend_from_slice(tmp.format(v as i32).as_bytes());
                } else {
                    // Inline direct write
                    buf.reserve(24);
                    let len = buf.len();
                    let ptr = buf.as_mut_ptr().add(len);
                    let written = ryu::raw::format64(v, ptr);
                    buf.set_len(len + written);
                }
            }
        },
        _ if r_type == libR_sys::SEXPTYPE::LGLSXP as u32 => {
            let v = *libR_sys::LOGICAL(col).add(idx);
            if is_na_int(v) { buf.extend_from_slice(b"null"); }
            else if v != 0 { buf.extend_from_slice(b"true"); }
            else { buf.extend_from_slice(b"false"); }
        },
        _ if r_type == libR_sys::SEXPTYPE::STRSXP as u32 => {
            let s = libR_sys::STRING_ELT(col, idx as isize);
            if is_na_string(s) { buf.extend_from_slice(b"null"); }
            else if let Some(bytes) = charsxp_to_utf8_bytes(s) { escape_json_string_into(buf, bytes); }
            else { buf.extend_from_slice(b"null"); }
        },
        _ if r_type == libR_sys::SEXPTYPE::VECSXP as u32 => {
             let val = libR_sys::VECTOR_ELT(col, idx as isize);
             serialize_sexp_to_json_buffer(val, buf);
        },
        _ => buf.extend_from_slice(b"null"),
    }
}

// ------------------------------------------------------------------
// PARALLEL SAFE COLUMNS
// ------------------------------------------------------------------

struct StringArena {
    bytes: Vec<u8>,
    offsets: Vec<(usize, usize)>,
}
unsafe impl Send for StringArena {}
unsafe impl Sync for StringArena {}

struct ThreadSafeColumn {
    kind: ColumnType,
    data_ptr: usize,
    cached_levels: Option<Vec<Vec<u8>>>,
    string_arena: Option<StringArena>,
}
unsafe impl Send for ThreadSafeColumn {}
unsafe impl Sync for ThreadSafeColumn {}

unsafe fn is_default_rownames(rn: libR_sys::SEXP) -> bool {
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

fn build_thread_safe_cols(
    df: &List,
    colnames: &[String],
    skip_idx: usize,
    _expected_rows: usize,
) -> Result<Vec<(Vec<u8>, ThreadSafeColumn)>> {
    let mut out = Vec::with_capacity(colnames.len() + 1);

    for (j, nm) in colnames.iter().enumerate() {
        if j == skip_idx { continue; }
        let mut col = df.elt(j).map_err(|_| Error::Other(format!("Column '{}' missing", nm)))?;
        let is_date = col.inherits("Date");
        let is_posixt = col.inherits("POSIXt");
        if is_date || is_posixt {
             col = call!("format", &col).map_err(|e| Error::Other(format!("format failed: {:?}", e)))?;
        }
        let sexp = unsafe { col.get() };
        let r_type = col.rtype();
        let key = build_escaped_key_bytes(nm);

        let dim_attr = unsafe { libR_sys::Rf_getAttrib(sexp, libR_sys::R_DimSymbol) };
        let is_matrix = unsafe { 
            dim_attr != libR_sys::R_NilValue && 
            sexp_len(dim_attr) == 2 && 
            *libR_sys::INTEGER(dim_attr) == _expected_rows as i32 
        };

        if is_matrix {
             let dim_ptr = unsafe { libR_sys::INTEGER(dim_attr) };
             let n_matrix_cols = unsafe { *dim_ptr.add(1) } as usize;
             let mut bytes = Vec::new();
             let mut offsets = Vec::with_capacity(_expected_rows);
             
             for r in 0.._expected_rows {
                 let start = bytes.len();
                 bytes.push(b'[');
                 for c in 0..n_matrix_cols {
                     if c > 0 { bytes.push(b','); }
                     let idx = c * _expected_rows + r;
                     if r_type == Rtype::Integers {
                         let p = unsafe { libR_sys::INTEGER(sexp) };
                         let v = unsafe { *p.add(idx) };
                         if unsafe { is_na_int(v) } { bytes.extend_from_slice(b"\"NA\""); }
                         else {
                             let mut tmp = itoa::Buffer::new();
                             bytes.extend_from_slice(tmp.format(v).as_bytes());
                         }
                     } else if r_type == Rtype::Doubles {
                         let p = unsafe { libR_sys::REAL(sexp) };
                         let v = unsafe { *p.add(idx) };
                         if unsafe { is_na_real(v) || is_nan_real(v) || !v.is_finite() } { bytes.extend_from_slice(b"\"NA\""); }
                         else {
                             if v.fract() == 0.0 && v >= (i32::MIN as f64) && v <= (i32::MAX as f64) {
                                 let mut tmp = itoa::Buffer::new();
                                 bytes.extend_from_slice(tmp.format(v as i32).as_bytes());
                             } else {
                                 // Inline direct write
                                 bytes.reserve(24);
                                 let len = bytes.len();
                                 unsafe {
                                     let ptr = bytes.as_mut_ptr().add(len);
                                     let written = ryu::raw::format64(v, ptr);
                                     bytes.set_len(len + written);
                                 }
                             }
                         }
                     } else if r_type == Rtype::Logicals {
                         let p = unsafe { libR_sys::LOGICAL(sexp) };
                         let v = unsafe { *p.add(idx) };
                         if unsafe { is_na_int(v) } { bytes.extend_from_slice(b"\"NA\""); }
                         else if v != 0 { bytes.extend_from_slice(b"true"); }
                         else { bytes.extend_from_slice(b"false"); }
                     } else if r_type == Rtype::Strings {
                         let s = unsafe { libR_sys::STRING_ELT(sexp, idx as isize) };
                         if unsafe { is_na_string(s) } { bytes.extend_from_slice(b"\"NA\""); }
                         else if let Some(utf8) = unsafe { charsxp_to_utf8_bytes(s) } { escape_json_string_into(&mut bytes, utf8); }
                         else { bytes.extend_from_slice(b"\"NA\""); }
                     } else if r_type == Rtype::List {
                         let item = unsafe { libR_sys::VECTOR_ELT(sexp, idx as isize) };
                         unsafe { serialize_sexp_to_json_buffer(item, &mut bytes); }
                     } else { bytes.extend_from_slice(b"null"); }
                 }
                 bytes.push(b']');
                 offsets.push((start, bytes.len() - start));
             }
             let col = ThreadSafeColumn { kind: ColumnType::JsonRaw, data_ptr: 0, cached_levels: None, string_arena: Some(StringArena { bytes, offsets }) };
             out.push((key, col));
             continue;
        }

        let (kind, ptr, cached_levels, arena) =
            if col.inherits("factor") && r_type == Rtype::Integers {
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
            } else if r_type == Rtype::Integers {
                (ColumnType::Int, unsafe { libR_sys::INTEGER(sexp) as *const u8 as usize }, None, None)
            } else if r_type == Rtype::Doubles {
                (ColumnType::Real, unsafe { libR_sys::REAL(sexp) as *const u8 as usize }, None, None)
            } else if r_type == Rtype::Logicals {
                (ColumnType::Bool, unsafe { libR_sys::LOGICAL(sexp) as *const u8 as usize }, None, None)
            } else if r_type == Rtype::Strings {
                let n = unsafe { sexp_len(sexp) };
                let mut bytes = Vec::with_capacity(n * 16);
                let mut offsets = Vec::with_capacity(n);
                for i in 0..n {
                    let s_sexp = unsafe { libR_sys::STRING_ELT(sexp, i as isize) };
                    if unsafe { is_na_string(s_sexp) } {
                        offsets.push((usize::MAX, 0));
                    } else if let Some(utf8) = unsafe { charsxp_to_utf8_bytes(s_sexp) } {
                        let start = bytes.len();
                        bytes.extend_from_slice(utf8);
                        offsets.push((start, bytes.len() - start));
                    } else {
                        offsets.push((usize::MAX, 0));
                    }
                }
                (ColumnType::Char, 0, None, Some(StringArena { bytes, offsets }))
            } else if r_type == Rtype::List {
                let n = unsafe { sexp_len(sexp) };
                let mut bytes = Vec::with_capacity(n * 64);
                let mut offsets = Vec::with_capacity(n);
                for i in 0..n {
                    let item = unsafe { libR_sys::VECTOR_ELT(sexp, i as isize) };
                    let start = bytes.len();
                    unsafe { serialize_sexp_to_json_buffer(item, &mut bytes); }
                    offsets.push((start, bytes.len() - start));
                }
                (ColumnType::JsonRaw, 0, None, Some(StringArena { bytes, offsets }))
            } else {
                (ColumnType::Null, 0, None, None)
            };
        out.push((key, ThreadSafeColumn { kind, data_ptr: ptr, cached_levels, string_arena: arena }));
    }

    let rn_sexp = unsafe { libR_sys::Rf_getAttrib(df.get(), libR_sys::R_RowNamesSymbol) };
    if !unsafe { is_default_rownames(rn_sexp) } {
        let key = build_escaped_key_bytes("_row");
        let n = unsafe { sexp_len(rn_sexp) };
        let mut bytes = Vec::with_capacity(n * 16);
        let mut offsets = Vec::with_capacity(n);
        let rn_type = unsafe { typeof_sexp(rn_sexp) };
        if rn_type == libR_sys::SEXPTYPE::STRSXP as u32 {
            for i in 0..n {
                let s_sexp = unsafe { libR_sys::STRING_ELT(rn_sexp, i as isize) };
                if unsafe { is_na_string(s_sexp) } { offsets.push((usize::MAX, 0)); }
                else if let Some(utf8) = unsafe { charsxp_to_utf8_bytes(s_sexp) } {
                    let start = bytes.len();
                    bytes.extend_from_slice(utf8);
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
                    bytes.extend_from_slice(s.as_bytes());
                    offsets.push((start, bytes.len() - start));
                }
            }
        }
        out.push((key, ThreadSafeColumn { kind: ColumnType::Char, data_ptr: 0, cached_levels: None, string_arena: Some(StringArena { bytes, offsets }) }));
    }
    Ok(out)
}

#[inline(always)]
fn try_write_kv(out: &mut JsonWriter, row: usize, key: &[u8], col: &ThreadSafeColumn) -> bool {
    match col.kind {
        ColumnType::Char => {
            if let Some(ref a) = col.string_arena {
                let (start, len) = a.offsets[row];
                if start != usize::MAX {
                    out.push_bytes(key);
                    escape_json_string_into(&mut out.buf, &a.bytes[start..start + len]);
                    return true;
                }
            }
        }
        ColumnType::JsonRaw => {
            if let Some(ref a) = col.string_arena {
                let (start, len) = a.offsets[row];
                out.push_bytes(key);
                out.push_bytes(&a.bytes[start..start + len]);
                return true;
            }
        }
        ColumnType::Int => unsafe {
            let v = *(col.data_ptr as *const i32).add(row);
            if !is_na_int(v) {
                out.push_bytes(key);
                out.push_i32(v);
                return true;
            }
        },
        ColumnType::Real => unsafe {
            let v = *(col.data_ptr as *const f64).add(row);
            if !is_na_real(v) && !is_nan_real(v) && v.is_finite() {
                out.push_bytes(key);
                out.push_f64(v); 
                return true;
            }
        },
        ColumnType::Bool => unsafe {
            let v = *(col.data_ptr as *const i32).add(row);
            if !is_na_int(v) {
                out.push_bytes(key);
                out.push_bool(v != 0);
                return true;
            }
        },
        ColumnType::Factor => unsafe {
            let v = *(col.data_ptr as *const i32).add(row);
            if !is_na_int(v) && v > 0 {
                if let Some(ref levels) = col.cached_levels {
                    let idx = (v as usize).saturating_sub(1);
                    if idx < levels.len() {
                        out.push_bytes(key);
                        out.push_bytes(&levels[idx]);
                        return true;
                    }
                }
            }
        },
        ColumnType::Null => {}
    }
    false
}

// ------------------------------------------------------------------
// GEOMETRY LOGIC (ARENA OPTIMIZED)
// ------------------------------------------------------------------

fn detect_sfc_type_sexp(geom_col_sexp: libR_sys::SEXP) -> SfcType {
    unsafe {
        let classes = libR_sys::Rf_getAttrib(geom_col_sexp, libR_sys::R_ClassSymbol);
        if classes != libR_sys::R_NilValue {
            let n = sexp_len(classes);
            for i in 0..n {
                let s = libR_sys::STRING_ELT(classes, i as isize);
                let p = libR_sys::R_CHAR(s);
                let c_str = CStr::from_ptr(p as *const c_char);
                match c_str.to_str().unwrap_or("") {
                    "sfc_POINT" => return SfcType::Point,
                    "sfc_MULTIPOINT" => return SfcType::MultiPoint,
                    "sfc_LINESTRING" => return SfcType::LineString,
                    "sfc_MULTILINESTRING" => return SfcType::MultiLineString,
                    "sfc_POLYGON" => return SfcType::Polygon,
                    "sfc_MULTIPOLYGON" => return SfcType::MultiPolygon,
                    "sfc_GEOMETRY" | "sfc_GEOMETRYCOLLECTION" => return SfcType::GeometryCollection,
                    _ => continue,
                }
            }
        }
    }
    SfcType::Unknown
}

fn get_row_sfg_type(sfg: libR_sys::SEXP) -> SfcType {
    unsafe {
        let classes = libR_sys::Rf_getAttrib(sfg, libR_sys::R_ClassSymbol);
        if classes != libR_sys::R_NilValue {
            let n = sexp_len(classes);
            for i in 0..n {
                let s = libR_sys::STRING_ELT(classes, i as isize);
                let p = libR_sys::R_CHAR(s);
                let c_str = CStr::from_ptr(p as *const c_char);
                match c_str.to_str().unwrap_or("") {
                    "POINT" => return SfcType::Point,
                    "MULTIPOINT" => return SfcType::MultiPoint,
                    "LINESTRING" => return SfcType::LineString,
                    "MULTILINESTRING" => return SfcType::MultiLineString,
                    "POLYGON" => return SfcType::Polygon,
                    "MULTIPOLYGON" => return SfcType::MultiPolygon,
                    _ => continue,
                }
            }
        }
    }
    SfcType::Unknown
}

#[derive(Clone, Copy, Debug)]
struct CoordPtr {
    ptr: usize,
    len: usize,
}
unsafe impl Send for CoordPtr {}
unsafe impl Sync for CoordPtr {}

struct GeometryBatch {
    coords: Vec<CoordPtr>,
    counts: Vec<usize>, 
}

#[derive(Clone, Copy)]
enum FastGeom {
    Null,
    Point(f64, f64),
    Single(CoordPtr, SfcType),
    FlatList { start: u32, len: u32, typ: SfcType },
    MultiPolygon { coords_start: u32, counts_start: u32, n_polys: u32 },
}
unsafe impl Send for FastGeom {}
unsafe impl Sync for FastGeom {}

fn extract_geometries_chunk(
    geom_col: libR_sys::SEXP, 
    sfc_type: SfcType, 
    start: usize, 
    end: usize
) -> (GeometryBatch, Vec<FastGeom>) {
    let capacity_est = end - start;
    let mut batch = GeometryBatch {
        coords: Vec::with_capacity(capacity_est * 2),
        counts: Vec::with_capacity(capacity_est), 
    };
    let mut out = Vec::with_capacity(capacity_est);

    let get_coord_ptr = |x: libR_sys::SEXP| -> CoordPtr {
        unsafe { CoordPtr { ptr: libR_sys::REAL(x) as usize, len: sexp_len(x) } }
    };

    for i in start..end {
        let sfg = unsafe { libR_sys::VECTOR_ELT(geom_col, i as isize) };
        if sfg == unsafe { libR_sys::R_NilValue } {
            out.push(FastGeom::Null); continue;
        }
        let row_type = if sfc_type == SfcType::GeometryCollection || sfc_type == SfcType::Unknown {
            get_row_sfg_type(sfg)
        } else { sfc_type };

        match row_type {
            SfcType::Point => unsafe {
                if sexp_len(sfg) >= 2 {
                    let p = libR_sys::REAL(sfg);
                    out.push(FastGeom::Point(*p, *p.add(1)));
                } else { out.push(FastGeom::Null); }
            },
            SfcType::MultiPoint | SfcType::LineString => {
                out.push(FastGeom::Single(get_coord_ptr(sfg), row_type));
            },
            SfcType::MultiLineString | SfcType::Polygon => {
                let n = unsafe { sexp_len(sfg) };
                let start_idx = batch.coords.len() as u32;
                for j in 0..n {
                    batch.coords.push(get_coord_ptr(unsafe { libR_sys::VECTOR_ELT(sfg, j as isize) }));
                }
                out.push(FastGeom::FlatList { start: start_idx, len: n as u32, typ: row_type });
            }
            SfcType::MultiPolygon => {
                let n_polys = unsafe { sexp_len(sfg) };
                let counts_start = batch.counts.len() as u32;
                let coords_start = batch.coords.len() as u32;
                for j in 0..n_polys {
                    let poly_sfg = unsafe { libR_sys::VECTOR_ELT(sfg, j as isize) };
                    let n_rings = unsafe { sexp_len(poly_sfg) };
                    batch.counts.push(n_rings);
                    for k in 0..n_rings {
                        batch.coords.push(get_coord_ptr(unsafe { libR_sys::VECTOR_ELT(poly_sfg, k as isize) }));
                    }
                }
                out.push(FastGeom::MultiPolygon { coords_start, counts_start, n_polys: n_polys as u32 });
            }
            _ => out.push(FastGeom::Null),
        }
    }
    (batch, out)
}

fn write_coords_flat(out: &mut JsonWriter, cp: &CoordPtr) {
    let nrow = cp.len / 2;
    let p = cp.ptr as *const f64;
    out.push_u8(b'[');
    if nrow > 0 {
        unsafe {
            // First point
            out.push_u8(b'[');
            out.push_f64_direct(*p);
            out.push_u8(b',');
            out.push_f64_direct(*p.add(nrow));
            out.push_u8(b']');
            // Subsequent points
            for i in 1..nrow {
                // Combine ",[" to reduce push calls
                out.push_bytes(b",[");
                out.push_f64_direct(*p.add(i));
                out.push_u8(b',');
                out.push_f64_direct(*p.add(i + nrow));
                out.push_u8(b']');
            }
        }
    }
    out.push_u8(b']');
}

fn write_geometry_parallel(out: &mut JsonWriter, geom: &FastGeom, batch: &GeometryBatch) {
    match geom {
        FastGeom::Point(x, y) => {
            out.push_bytes(br#"{"type":"Point","coordinates":["#);
            out.push_f64_direct(*x); 
            out.push_u8(b','); 
            out.push_f64_direct(*y);
            out.push_bytes(br#"]}"#);
        }
        FastGeom::Single(cp, typ) => {
            match typ {
                SfcType::MultiPoint => out.push_bytes(br#"{"type":"MultiPoint","coordinates":"#),
                SfcType::LineString => out.push_bytes(br#"{"type":"LineString","coordinates":"#),
                _ => { out.push_bytes(b"null"); return; }
            }
            write_coords_flat(out, cp); out.push_u8(b'}');
        }
        FastGeom::FlatList { start, len, typ } => {
            match typ {
                SfcType::MultiLineString => out.push_bytes(br#"{"type":"MultiLineString","coordinates":["#),
                SfcType::Polygon => out.push_bytes(br#"{"type":"Polygon","coordinates":["#),
                _ => { out.push_bytes(b"null"); return; }
            }
            let s = *start as usize;
            let l = *len as usize;
            for (i, cp) in batch.coords[s..s+l].iter().enumerate() {
                if i > 0 { out.push_u8(b','); }
                write_coords_flat(out, cp);
            }
            out.push_bytes(br#"]}"#);
        }
        FastGeom::MultiPolygon { coords_start, counts_start, n_polys } => {
            out.push_bytes(br#"{"type":"MultiPolygon","coordinates":["#);
            let mut c_idx = *coords_start as usize;
            let cnt_start = *counts_start as usize;
            let cnt_len = *n_polys as usize;
            for (i, &n_rings) in batch.counts[cnt_start..cnt_start+cnt_len].iter().enumerate() {
                if i > 0 { out.push_u8(b','); }
                out.push_u8(b'[');
                for k in 0..n_rings {
                    if k > 0 { out.push_u8(b','); }
                    write_coords_flat(out, &batch.coords[c_idx]);
                    c_idx += 1;
                }
                out.push_u8(b']');
            }
            out.push_bytes(br#"]}"#);
        }
        FastGeom::Null => out.push_bytes(b"null"),
    }
}

// ------------------------------------------------------------------
// WORKER FUNCTIONS
// ------------------------------------------------------------------

fn process_feature_parallel(
    out: &mut JsonWriter, 
    row: usize, 
    props: &[(Vec<u8>, ThreadSafeColumn)], 
    geom: &FastGeom,
    batch: &GeometryBatch
) {
    out.push_bytes(FEAT_HEAD);
    let mut needs_comma = false;
    for (key, col) in props {
        if needs_comma {
             out.push_u8(b',');
             if !try_write_kv(out, row, key, col) {
                 out.buf.pop(); 
             } else {
                 needs_comma = true;
             }
        } else {
            if try_write_kv(out, row, key, col) {
                needs_comma = true;
            }
        }
    }
    out.push_bytes(FEAT_MID);
    write_geometry_parallel(out, geom, batch);
    out.push_u8(b'}');
}

fn process_row_generic(out: &mut JsonWriter, row: usize, props: &[(Vec<u8>, ThreadSafeColumn)]) {
    out.push_u8(b'{');
    let mut needs_comma = false;
    for (key, col) in props {
        if needs_comma {
             out.push_u8(b',');
             if !try_write_kv(out, row, key, col) {
                 out.buf.pop(); 
             } else {
                 needs_comma = true;
             }
        } else {
            if try_write_kv(out, row, key, col) {
                needs_comma = true;
            }
        }
    }
    out.push_u8(b'}');
}

// ------------------------------------------------------------------
// UNBOX POST-PROCESSOR
// ------------------------------------------------------------------

// Identifies if a JSON fragment [ ... ] contains exactly one primitive (number, bool, null, or string).
fn check_singleton(slice: &[u8]) -> Option<usize> {
    if slice.is_empty() { return None; }
    let mut len = 0;
    
    // Case 1: String "..."
    if slice[0] == b'"' {
        len += 1;
        let mut escaped = false;
        loop {
            if len >= slice.len() { return None; }
            let c = slice[len];
            len += 1;
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                break;
            }
        }
        // Must end with ']' immediately
        if len < slice.len() && slice[len] == b']' {
            return Some(len);
        }
        return None;
    }

    // Case 2: Primitive (true, false, null, numbers)
    // Scan until ']', ensuring no structural chars like ',' or '{' or '['
    while len < slice.len() {
        let c = slice[len];
        if c == b']' {
            if len == 0 { return None; } // empty []
            return Some(len);
        }
        if c == b',' || c == b'[' || c == b'{' { 
            return None; 
        }
        len += 1;
    }
    None
}

// Scans the valid JSON string and removes brackets from singleton arrays
fn post_process_unbox(json: String) -> String {
    let bytes = json.as_bytes();
    let mut out = Vec::with_capacity(json.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        
        // Skip strings to avoid processing brackets inside them
        if b == b'"' {
            out.push(b);
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                out.push(c);
                i += 1;
                if c == b'\\' {
                    if i < bytes.len() {
                        out.push(bytes[i]);
                        i += 1;
                    }
                } else if c == b'"' {
                    break;
                }
            }
        } 
        // Found Array Start '['
        else if b == b'[' {
            // Check content ahead
            if let Some(len) = check_singleton(&bytes[i+1..]) {
                // It is a singleton: [ "val" ] or [ 123 ]
                // Append just the content "val" or 123
                out.extend_from_slice(&bytes[i+1 .. i+1+len]);
                // Skip the parsed content + closing bracket ']'
                i += 1 + len + 1; 
            } else {
                // Not a singleton or complex structure: keep '['
                out.push(b);
                i += 1;
            }
        } else {
            out.push(b);
            i += 1;
        }
    }
    unsafe { String::from_utf8_unchecked(out) }
}

// ------------------------------------------------------------------
// EXPORTS
// ------------------------------------------------------------------

#[extendr]
fn sf_geojson_str_impl(x: Robj, auto_unbox: bool) -> Result<Robj> {
    let rr = catch_unwind(AssertUnwindSafe(|| sf_geojson_str_impl_inner(x, auto_unbox)));
    match rr {
        Ok(r) => r,
        Err(p) => rerr(format!("Internal panic: {}", panic_message(p))),
    }
}

fn sf_geojson_str_impl_inner(x: Robj, auto_unbox: bool) -> Result<Robj> {
    if x.is_null() {
        let mut robj = Robj::from("[]");
        robj.set_class(&["geojson", "json"])?;
        return Ok(robj);
    }
    if !x.inherits("sf") { return rerr("Not an sf object"); }

    let df = x.as_list().ok_or_else(|| Error::Other("Invalid sf list".to_string()))?;
    let n_rows = unsafe { get_df_nrows(x.get()) };
    if n_rows == 0 {
        let mut robj = Robj::from("[]");
        robj.set_class(&["geojson", "json"])?;
        return Ok(robj);
    }

    let names = x.names().ok_or_else(|| Error::Other("No names".to_string()))?;
    let colnames: Vec<String> = names.map(|s| s.to_string()).collect();
    let sfcol_attr = x.get_attrib("sf_column").ok_or_else(|| Error::Other("No sf_column".to_string()))?;
    let sfcol_vec = sfcol_attr.as_str_vector().ok_or_else(|| Error::Other("sf_column not char".to_string()))?;
    if sfcol_vec.is_empty() { return rerr("sf_column empty"); }
    
    let geom_name = &sfcol_vec[0];
    let geom_idx = colnames.iter().position(|n| n == geom_name).ok_or_else(|| Error::Other("Geometry col not found".to_string()))?;
    let geom_col_robj = df.elt(geom_idx).map_err(|_| Error::Other("Geometry col error".to_string()))?;
    let geom_col = unsafe { geom_col_robj.get() };
    if unsafe { typeof_sexp(geom_col) } != libR_sys::SEXPTYPE::VECSXP as u32 { return rerr("Geometry col not a list"); }

    let sfc_type = detect_sfc_type_sexp(geom_col);
    let props = build_thread_safe_cols(&df, &colnames, geom_idx, n_rows)?;
    
    let num_chunks = (n_rows + PAR_CHUNK_ROWS - 1) / PAR_CHUNK_ROWS;
    let ranges: Vec<(usize, usize, usize)> = (0..num_chunks)
        .map(|id| (id, id * PAR_CHUNK_ROWS, (id * PAR_CHUNK_ROWS + PAR_CHUNK_ROWS).min(n_rows)))
        .collect();

    let mut chunk_geoms = Vec::with_capacity(num_chunks);
    for (id, start, end) in &ranges {
        let (batch, geoms) = extract_geometries_chunk(geom_col, sfc_type, *start, *end);
        chunk_geoms.push((*id, *start, *end, batch, geoms));
    }

    let parts_res: Vec<PResult<(usize, Vec<u8>)>> = chunk_geoms
        .into_par_iter()
        .map(|(chunk_id, start, end, batch, geoms)| {
            let rr = catch_unwind(AssertUnwindSafe(|| {
                let mut w = JsonWriter::with_capacity((end - start) * 2048);
                for (local_i, row_i) in (start..end).enumerate() {
                    if local_i > 0 { w.push_u8(b','); }
                    process_feature_parallel(&mut w, row_i, &props, &geoms[local_i], &batch);
                }
                (chunk_id, w.buf)
            }));
            match rr {
                Ok(v) => Ok(v),
                Err(p) => Err(format!("Worker panic: {}", panic_message(p))),
            }
        })
        .collect();

    let mut parts: Vec<(usize, Vec<u8>)> = Vec::with_capacity(parts_res.len());
    for r in parts_res {
        match r {
            Ok(v) => parts.push(v),
            Err(msg) => return rerr(msg),
        }
    }
    parts.sort_by_key(|(id, _)| *id);

    let total_bytes: usize = parts.iter().map(|(_, v)| v.len()).sum();
    let mut final_out = Vec::with_capacity(total_bytes + n_rows + 64);
    final_out.extend_from_slice(FC_HEAD);
    let mut first = true;
    for (_, chunk) in parts {
        if chunk.is_empty() { continue; }
        if !first { final_out.push(b','); }
        first = false;
        final_out.extend_from_slice(&chunk);
    }
    final_out.extend_from_slice(FC_TAIL);

    if final_out.len() > i32::MAX as usize { return rerr(format!("Size {} exceeds 2GB limit", final_out.len())); }

    let result_str = unsafe { String::from_utf8_unchecked(final_out) };
    let final_json = if auto_unbox { post_process_unbox(result_str) } else { result_str };

    let mut robj = Robj::from(final_json);
    robj.set_class(&["geojson", "json"])?;
    Ok(robj)
}


#[extendr]
fn df_json_str_impl(x: Robj, auto_unbox: bool) -> Result<Robj> {
    let rr = catch_unwind(AssertUnwindSafe(|| df_json_str_impl_inner(x, auto_unbox)));
    match rr {
        Ok(r) => r,
        Err(p) => rerr(format!("Internal panic: {}", panic_message(p))),
    }
}

fn df_json_str_impl_inner(x: Robj, auto_unbox: bool) -> Result<Robj> {
    if x.is_null() {
        let mut robj = Robj::from("[]");
        robj.set_class(&["json"])?;
        return Ok(robj);
    }
    if !x.inherits("data.frame") { return rerr("Not a data.frame"); }

    let n_cols = unsafe { sexp_len(x.get()) };
    let n_rows = unsafe { get_df_nrows(x.get()) };

    if n_cols == 0 {
        if n_rows == 0 {
             let mut robj = Robj::from("[]");
             robj.set_class(&["json"])?;
             return Ok(robj);
        }
        let mut buf = Vec::with_capacity(n_rows * 3 + 2);
        buf.push(b'[');
        for i in 0..n_rows {
            if i > 0 { buf.push(b','); }
            buf.extend_from_slice(b"{}");
        }
        buf.push(b']');
        let mut robj = Robj::from(unsafe { String::from_utf8_unchecked(buf) });
        robj.set_class(&["json"])?;
        return Ok(robj);
    }

    let df_list = x.as_list().ok_or_else(|| Error::Other("Invalid df structure".to_string()))?;
    let names = x.names().ok_or_else(|| Error::Other("No names".to_string()))?;
    let colnames: Vec<String> = names.map(|s| s.to_string()).collect();
    let props = build_thread_safe_cols(&df_list, &colnames, usize::MAX, n_rows)?;

    let chunk_size = if n_rows < 10000 { n_rows } else { PAR_CHUNK_ROWS };
    let num_chunks = (n_rows + chunk_size - 1) / chunk_size;
    let ranges: Vec<(usize, usize, usize)> = (0..num_chunks)
        .map(|id| (id, id * chunk_size, (id * chunk_size + chunk_size).min(n_rows)))
        .collect();

    let parts_res: Vec<PResult<(usize, Vec<u8>)>> = ranges
        .into_par_iter()
        .map(|(chunk_id, start, end)| {
            let rr = catch_unwind(AssertUnwindSafe(|| {
                let mut w = JsonWriter::with_capacity((end - start) * 128);
                for i in start..end {
                    if i > start { w.push_u8(b','); }
                    process_row_generic(&mut w, i, &props);
                }
                (chunk_id, w.buf)
            }));
            match rr {
                Ok(v) => Ok(v),
                Err(p) => Err(format!("Worker panic: {}", panic_message(p))),
            }
        })
        .collect();

    let mut parts: Vec<(usize, Vec<u8>)> = Vec::with_capacity(parts_res.len());
    for r in parts_res {
        match r {
            Ok(v) => parts.push(v),
            Err(msg) => return rerr(msg),
        }
    }
    parts.sort_by_key(|(id, _)| *id);

    let total_bytes: usize = parts.iter().map(|(_, v)| v.len()).sum();
    let mut final_out = Vec::with_capacity(total_bytes + n_rows + 2);

    final_out.push(b'[');
    let mut first = true;
    for (_, chunk) in parts {
        if chunk.is_empty() { continue; }
        if !first { final_out.push(b','); }
        first = false;
        final_out.extend_from_slice(&chunk);
    }
    final_out.push(b']');

    if final_out.len() > i32::MAX as usize { return rerr(format!("Size {} exceeds 2GB limit", final_out.len())); }
    
    let result_str = unsafe { String::from_utf8_unchecked(final_out) };
    let final_json = if auto_unbox { post_process_unbox(result_str) } else { result_str };

    let mut robj = Robj::from(final_json);
    robj.set_class(&["json"])?;
    Ok(robj)
}

#[extendr]
fn obj_json_str_impl(x: Robj, auto_unbox: bool) -> Result<Robj> {
    let est_size = unsafe { sexp_len(x.get()) } * 16 + 64; 
    let mut w = JsonWriter::with_capacity(est_size);
    unsafe { serialize_sexp_to_json_buffer(x.get(), &mut w.buf); }
    if w.buf.len() > i32::MAX as usize { return rerr(format!("Size {} exceeds 2GB limit", w.buf.len())); }
    
    let result_str = unsafe { String::from_utf8_unchecked(w.buf) };
    let final_json = if auto_unbox { post_process_unbox(result_str) } else { result_str };

    let mut res = Robj::from(final_json);
    res.set_class(&["json"])?;
    Ok(res)
}

extendr_module! {
    mod fastgeojson;
    fn sf_geojson_str_impl;
    fn df_json_str_impl;
    fn obj_json_str_impl;
}