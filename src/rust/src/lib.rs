use extendr_api::prelude::*;
use libR_sys;
use libR_sys::SEXPTYPE::{INTSXP, LGLSXP, REALSXP, STRSXP, VECSXP};
use rayon::prelude::*;
use std::ffi::CStr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::thread;

const PAR_THRESHOLD_ROWS: usize = 2000;
const PAR_CHUNK_ROWS: usize = 2048;

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
        if v.is_finite() {
            let mut tmp = ryu::Buffer::new();
            self.push_bytes(tmp.format_finite(v).as_bytes());
        } else {
            self.push_bytes(b"null");
        }
    }

    #[inline(always)]
    fn push_bool(&mut self, v: bool) {
        if v { self.push_bytes(b"true"); } else { self.push_bytes(b"false"); }
    }

    #[inline(always)]
    unsafe fn push_u8_unchecked(&mut self, b: u8) {
        let len = self.buf.len();
        let p = self.buf.as_mut_ptr().add(len);
        ptr::write(p, b);
        self.buf.set_len(len + 1);
    }

    #[inline(always)]
    unsafe fn push_f64_unchecked(&mut self, v: f64) {
        if v.is_finite() {
            let mut tmp = ryu::Buffer::new();
            let s = tmp.format_finite(v).as_bytes();
            let len = self.buf.len();
            let p = self.buf.as_mut_ptr().add(len);
            ptr::copy_nonoverlapping(s.as_ptr(), p, s.len());
            self.buf.set_len(len + s.len());
        } else {
            let len = self.buf.len();
            let p = self.buf.as_mut_ptr().add(len);
            ptr::copy_nonoverlapping(b"null".as_ptr(), p, 4);
            self.buf.set_len(len + 4);
        }
    }

    #[inline]
    unsafe fn push_charsxp_as_json_string(&mut self, charsxp: libR_sys::SEXP) {
        if let Some(bytes) = charsxp_to_utf8_bytes(charsxp) {
            escape_json_string_into(&mut self.buf, bytes);
        } else {
            self.push_bytes(b"null");
        }
    }
}

#[inline]
fn escape_json_string_into(out: &mut Vec<u8>, bytes: &[u8]) {
    out.push(b'"');
    let mut start = 0;

    for (i, &b) in bytes.iter().enumerate() {
        let esc: &[u8] = match b {
            b'"' => br#"\""#,
            b'\\' => br#"\\"#,
            b'\n' => br#"\n"#,
            b'\r' => br#"\r"#,
            b'\t' => br#"\t"#,
            0x00..=0x1F => {
                out.extend_from_slice(&bytes[start..i]);
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.extend_from_slice(br#"\u00"#);
                out.push(HEX[(b >> 4) as usize]);
                out.push(HEX[(b & 0x0F) as usize]);
                start = i + 1;
                continue;
            }
            _ => continue,
        };

        out.extend_from_slice(&bytes[start..i]);
        out.extend_from_slice(esc);
        start = i + 1;
    }

    if start < bytes.len() {
        out.extend_from_slice(&bytes[start..]);
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

#[inline(always)]
unsafe fn sexp_len(x: libR_sys::SEXP) -> usize {
    libR_sys::Rf_length(x) as usize
}

#[inline(always)]
unsafe fn typeof_sexp(x: libR_sys::SEXP) -> libR_sys::SEXPTYPE {
    libR_sys::TYPEOF(x)
}

#[inline(always)]
unsafe fn safe_vector_elt(x: libR_sys::SEXP, i: usize) -> Option<libR_sys::SEXP> {
    if x == libR_sys::R_NilValue || typeof_sexp(x) != VECSXP || i >= sexp_len(x) {
        None
    } else {
        Some(libR_sys::VECTOR_ELT(x, i as isize))
    }
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
    if charsxp == libR_sys::R_NilValue || is_na_string(charsxp) {
        return None;
    }
    let cptr = libR_sys::Rf_translateCharUTF8(charsxp);
    if cptr.is_null() {
        return None;
    }
    Some(CStr::from_ptr(cptr).to_bytes())
}

// -------------------- single-thread properties --------------------

struct PropertyColumn {
    name_key: Vec<u8>,
    kind: ColumnType,
    data_ptr: *const u8,
    cached_levels: Option<Vec<Vec<u8>>>,
    len: usize,
}

fn build_property_columns_single(df: &List, colnames: &[String], geom_idx: usize) -> Vec<PropertyColumn> {
    let mut out = Vec::with_capacity(colnames.len());

    for (j, nm) in colnames.iter().enumerate() {
        if j == geom_idx {
            continue;
        }

        let col = match df.elt(j) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let col_sexp = unsafe { col.get() };
        let len = unsafe { sexp_len(col_sexp) };
        let sexp_type = unsafe { typeof_sexp(col_sexp) };
        let name_key = build_escaped_key_bytes(nm);

        let (kind, ptr_addr, cached_levels) = if col.inherits("factor") && sexp_type == INTSXP {
            let levels = match col.get_attrib("levels") { Some(l) => l, None => continue };
            let levels_sexp = unsafe { levels.get() };
            let n_lev = unsafe { sexp_len(levels_sexp) };

            let mut cache = Vec::with_capacity(n_lev);
            for k in 0..n_lev {
                let lev_charsxp = unsafe { libR_sys::STRING_ELT(levels_sexp, k as isize) };
                let mut buf = Vec::with_capacity(32);
                unsafe {
                    if let Some(bytes) = charsxp_to_utf8_bytes(lev_charsxp) {
                        escape_json_string_into(&mut buf, bytes);
                    } else {
                        buf.extend_from_slice(b"null");
                    }
                }
                cache.push(buf);
            }

            (ColumnType::Factor, unsafe { libR_sys::INTEGER(col_sexp) as *const u8 }, Some(cache))
        } else if sexp_type == INTSXP {
            (ColumnType::Int, unsafe { libR_sys::INTEGER(col_sexp) as *const u8 }, None)
        } else if sexp_type == REALSXP {
            (ColumnType::Real, unsafe { libR_sys::REAL(col_sexp) as *const u8 }, None)
        } else if sexp_type == LGLSXP {
            (ColumnType::Bool, unsafe { libR_sys::LOGICAL(col_sexp) as *const u8 }, None)
        } else if sexp_type == STRSXP {
            (ColumnType::Char, unsafe { libR_sys::DATAPTR(col_sexp) as *const u8 }, None)
        } else {
            (ColumnType::Null, ptr::null(), None)
        };

        out.push(PropertyColumn { name_key, kind, data_ptr: ptr_addr, cached_levels, len });
    }

    out
}

#[inline]
unsafe fn write_prop_value_single(out: &mut JsonWriter, pc: &PropertyColumn, row: usize) -> bool {
    if row >= pc.len {
        return false;
    }

    match pc.kind {
        ColumnType::Int => {
            let v = *(pc.data_ptr as *const i32).add(row);
            if !is_na_int(v) { out.push_i32(v); true } else { false }
        }
        ColumnType::Real => {
            let v = *(pc.data_ptr as *const f64).add(row);
            if !is_na_real(v) && !is_nan_real(v) { out.push_f64(v); true } else { false }
        }
        ColumnType::Bool => {
            let v = *(pc.data_ptr as *const i32).add(row);
            if !is_na_int(v) { out.push_bool(v != 0); true } else { false }
        }
        ColumnType::Char => {
            let elt = *(pc.data_ptr as *const libR_sys::SEXP).add(row);
            if elt != libR_sys::R_NilValue && !is_na_string(elt) {
                out.push_charsxp_as_json_string(elt);
                true
            } else {
                false
            }
        }
        ColumnType::Factor => {
            let code = *(pc.data_ptr as *const i32).add(row);
            if !is_na_int(code) && code > 0 {
                if let Some(ref levels) = pc.cached_levels {
                    let idx = (code as usize).saturating_sub(1);
                    if idx < levels.len() {
                        out.push_bytes(&levels[idx]);
                        return true;
                    }
                }
            }
            false
        }
        ColumnType::Null => false,
    }
}

// -------------------- single-thread geometries --------------------

fn write_coords_matrix_single(out: &mut JsonWriter, geom: libR_sys::SEXP) {
    let len = unsafe { sexp_len(geom) };
    if len < 2 {
        out.push_bytes(b"[]");
        return;
    }
    let nrow = len / 2;
    out.reserve(nrow * 55);

    out.push_u8(b'[');
    if unsafe { typeof_sexp(geom) } == REALSXP {
        let p = unsafe { libR_sys::REAL(geom) };
        for i in 0..nrow {
            if i > 0 {
                unsafe { out.push_u8_unchecked(b','); }
            }
            unsafe {
                let x = *p.add(i);
                let y = *p.add(i + nrow);
                out.push_u8_unchecked(b'[');
                out.push_f64_unchecked(x);
                out.push_u8_unchecked(b',');
                out.push_f64_unchecked(y);
                out.push_u8_unchecked(b']');
            }
        }
    }
    out.push_u8(b']');
}

fn write_point_single(out: &mut JsonWriter, geom: libR_sys::SEXP) {
    let p = unsafe { libR_sys::REAL(geom) };
    out.push_u8(b'[');
    unsafe {
        out.push_f64(*p);
        out.push_u8(b',');
        out.push_f64(*p.add(1));
    }
    out.push_u8(b']');
}

fn write_geom_list_single(out: &mut JsonWriter, list: libR_sys::SEXP, writer: fn(&mut JsonWriter, libR_sys::SEXP)) {
    let n = unsafe { sexp_len(list) };
    out.push_u8(b'[');
    for i in 0..n {
        if i > 0 { out.push_u8(b','); }
        if let Some(elt) = unsafe { safe_vector_elt(list, i) } { writer(out, elt); }
        else { out.push_bytes(b"null"); }
    }
    out.push_u8(b']');
}

// -------------------- parallel-safe columns --------------------

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

fn build_thread_safe_cols(
    df: &List,
    colnames: &[String],
    skip_idx: usize,
    expected_rows: usize,
) -> Result<Vec<(Vec<u8>, ThreadSafeColumn)>> {
    let mut out = Vec::with_capacity(colnames.len());

    for (j, nm) in colnames.iter().enumerate() {
        if j == skip_idx {
            continue;
        }

        let col = df.elt(j).map_err(|_| Error::Other(format!("Column '{}' missing", nm)))?;
        let sexp = unsafe { col.get() };

        let len = unsafe { sexp_len(sexp) };
        if len != expected_rows {
            return rerr(format!(
                "Data inconsistency: Column '{}' has length {}, expected {}",
                nm, len, expected_rows
            ));
        }

        let sexp_type = unsafe { typeof_sexp(sexp) };
        let key = build_escaped_key_bytes(nm);

        let (kind, ptr, cached_levels, arena) = if col.inherits("factor") && sexp_type == INTSXP {
            let levels = col
                .get_attrib("levels")
                .ok_or_else(|| Error::Other("Factor missing levels".to_string()))?;
            let levels_sexp = unsafe { levels.get() };
            let n = unsafe { sexp_len(levels_sexp) };

            let mut cache = Vec::with_capacity(n);
            for idx in 0..n {
                let s = unsafe { libR_sys::STRING_ELT(levels_sexp, idx as isize) };
                let mut buf = Vec::with_capacity(32);
                unsafe {
                    if let Some(bytes) = charsxp_to_utf8_bytes(s) { escape_json_string_into(&mut buf, bytes); }
                    else { buf.extend_from_slice(b"null"); }
                }
                cache.push(buf);
            }

            (ColumnType::Factor, unsafe { libR_sys::INTEGER(sexp) as *const u8 as usize }, Some(cache), None)
        } else if sexp_type == INTSXP {
            (ColumnType::Int, unsafe { libR_sys::INTEGER(sexp) as *const u8 as usize }, None, None)
        } else if sexp_type == REALSXP {
            (ColumnType::Real, unsafe { libR_sys::REAL(sexp) as *const u8 as usize }, None, None)
        } else if sexp_type == LGLSXP {
            (ColumnType::Bool, unsafe { libR_sys::LOGICAL(sexp) as *const u8 as usize }, None, None)
        } else if sexp_type == STRSXP {
            let n = unsafe { sexp_len(sexp) };
            let mut bytes = Vec::with_capacity(n * 16);
            let mut offsets = Vec::with_capacity(n);

            for i in 0..n {
                let s_sexp = unsafe { libR_sys::STRING_ELT(sexp, i as isize) };
                if unsafe { is_na_string(s_sexp) } {
                    offsets.push((0, 0));
                } else if let Some(utf8) = unsafe { charsxp_to_utf8_bytes(s_sexp) } {
                    let start = bytes.len();
                    bytes.extend_from_slice(utf8);
                    offsets.push((start, bytes.len() - start));
                } else {
                    offsets.push((0, 0));
                }
            }

            (ColumnType::Char, 0, None, Some(StringArena { bytes, offsets }))
        } else {
            (ColumnType::Null, 0, None, None)
        };

        out.push((key, ThreadSafeColumn { kind, data_ptr: ptr, cached_levels, string_arena: arena }));
    }

    Ok(out)
}

#[inline(always)]
fn kv_present(row: usize, col: &ThreadSafeColumn) -> bool {
    match col.kind {
        ColumnType::Null => false,
        ColumnType::Char => col
            .string_arena
            .as_ref()
            .map(|a| a.offsets[row].1 > 0)
            .unwrap_or(false),

        _ => unsafe {
            let p = col.data_ptr as *const u8;
            match col.kind {
                ColumnType::Int => {
                    let v = *(p as *const i32).add(row);
                    !is_na_int(v)
                }
                ColumnType::Real => {
                    let v = *(p as *const f64).add(row);
                    !is_na_real(v) && !is_nan_real(v)
                }
                ColumnType::Bool => {
                    let v = *(p as *const i32).add(row);
                    !is_na_int(v)
                }
                ColumnType::Factor => {
                    let v = *(p as *const i32).add(row);
                    if is_na_int(v) || v <= 0 {
                        false
                    } else if let Some(ref levels) = col.cached_levels {
                        let idx = (v as usize).saturating_sub(1);
                        idx < levels.len()
                    } else {
                        false
                    }
                }
                _ => false,
            }
        },
    }
}

#[inline(always)]
fn write_kv(out: &mut JsonWriter, row: usize, key: &[u8], col: &ThreadSafeColumn) {
    match col.kind {
        ColumnType::Char => {
            let a = col.string_arena.as_ref().unwrap();
            let (start, len) = a.offsets[row];
            out.push_bytes(key);
            escape_json_string_into(&mut out.buf, &a.bytes[start..start + len]);
        }
        ColumnType::Int => unsafe {
            let p = col.data_ptr as *const i32;
            out.push_bytes(key);
            out.push_i32(*p.add(row));
        }
        ColumnType::Real => unsafe {
            let p = col.data_ptr as *const f64;
            out.push_bytes(key);
            out.push_f64(*p.add(row));
        }
        ColumnType::Bool => unsafe {
            let p = col.data_ptr as *const i32;
            out.push_bytes(key);
            out.push_bool(*p.add(row) != 0);
        }
        ColumnType::Factor => unsafe {
            let p = col.data_ptr as *const i32;
            let v = *p.add(row);
            let levels = col.cached_levels.as_ref().unwrap();
            let idx = (v as usize).saturating_sub(1);
            out.push_bytes(key);
            out.push_bytes(&levels[idx]);
        }
        _ => {}
    }
}

// -------------------- geometry detection/extraction --------------------

fn detect_sfc_type_sexp(geom_col_sexp: libR_sys::SEXP) -> SfcType {
    unsafe {
        let classes = libR_sys::Rf_getAttrib(geom_col_sexp, libR_sys::R_ClassSymbol);
        if classes != libR_sys::R_NilValue {
            let n = sexp_len(classes);
            for i in 0..n {
                let s = libR_sys::STRING_ELT(classes, i as isize);
                let p = libR_sys::R_CHAR(s);
                let c_str = CStr::from_ptr(p as *const i8);
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
                let c_str = CStr::from_ptr(p as *const i8);
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

#[derive(Clone, Copy)]
struct CoordPtr {
    ptr: usize,
    len: usize,
}
unsafe impl Send for CoordPtr {}
unsafe impl Sync for CoordPtr {}

enum RustGeom {
    Point(f64, f64),
    MultiPoint(CoordPtr),
    LineString(CoordPtr),
    MultiLineString(Vec<CoordPtr>),
    Polygon(Vec<CoordPtr>),
    MultiPolygon(Vec<Vec<CoordPtr>>),
    Null,
}
unsafe impl Send for RustGeom {}
unsafe impl Sync for RustGeom {}

fn extract_geometries_chunk(geom_col: libR_sys::SEXP, sfc_type: SfcType, start: usize, end: usize) -> Vec<RustGeom> {
    let mut out = Vec::with_capacity(end - start);

    let get_coord_ptr = |x: libR_sys::SEXP| -> CoordPtr {
        unsafe { CoordPtr { ptr: libR_sys::REAL(x) as usize, len: sexp_len(x) } }
    };

    for i in start..end {
        let sfg = unsafe { libR_sys::VECTOR_ELT(geom_col, i as isize) };
        if sfg == unsafe { libR_sys::R_NilValue } {
            out.push(RustGeom::Null);
            continue;
        }

        let row_type = if sfc_type == SfcType::GeometryCollection || sfc_type == SfcType::Unknown {
            get_row_sfg_type(sfg)
        } else {
            sfc_type
        };

        let geom = match row_type {
            SfcType::Point => unsafe {
                if sexp_len(sfg) >= 2 {
                    let p = libR_sys::REAL(sfg);
                    RustGeom::Point(*p, *p.add(1))
                } else {
                    RustGeom::Null
                }
            },
            SfcType::MultiPoint => RustGeom::MultiPoint(get_coord_ptr(sfg)),
            SfcType::LineString => RustGeom::LineString(get_coord_ptr(sfg)),
            SfcType::MultiLineString => {
                let n = unsafe { sexp_len(sfg) };
                let mut parts = Vec::with_capacity(n);
                for j in 0..n {
                    parts.push(get_coord_ptr(unsafe { libR_sys::VECTOR_ELT(sfg, j as isize) }));
                }
                RustGeom::MultiLineString(parts)
            }
            SfcType::Polygon => {
                let n = unsafe { sexp_len(sfg) };
                let mut rings = Vec::with_capacity(n);
                for j in 0..n {
                    rings.push(get_coord_ptr(unsafe { libR_sys::VECTOR_ELT(sfg, j as isize) }));
                }
                RustGeom::Polygon(rings)
            }
            SfcType::MultiPolygon => {
                let n = unsafe { sexp_len(sfg) };
                let mut polys = Vec::with_capacity(n);
                for j in 0..n {
                    let poly_sfg = unsafe { libR_sys::VECTOR_ELT(sfg, j as isize) };
                    let n_rings = unsafe { sexp_len(poly_sfg) };
                    let mut rings = Vec::with_capacity(n_rings);
                    for k in 0..n_rings {
                        rings.push(get_coord_ptr(unsafe { libR_sys::VECTOR_ELT(poly_sfg, k as isize) }));
                    }
                    polys.push(rings);
                }
                RustGeom::MultiPolygon(polys)
            }
            _ => RustGeom::Null,
        };

        out.push(geom);
    }

    out
}

fn write_coords_flat(out: &mut JsonWriter, cp: &CoordPtr) {
    let nrow = cp.len / 2;
    let p = cp.ptr as *const f64;

    out.push_u8(b'[');
    for i in 0..nrow {
        if i > 0 { out.push_u8(b','); }
        out.push_u8(b'[');
        unsafe {
            out.push_f64(*p.add(i));
            out.push_u8(b',');
            out.push_f64(*p.add(i + nrow));
        }
        out.push_u8(b']');
    }
    out.push_u8(b']');
}

fn write_geometry_parallel(out: &mut JsonWriter, geom: &RustGeom) {
    match geom {
        RustGeom::Point(x, y) => {
            out.push_bytes(br#"{"type":"Point","coordinates":["#);
            out.push_f64(*x);
            out.push_u8(b',');
            out.push_f64(*y);
            out.push_bytes(br#"]}"#);
        }
        RustGeom::MultiPoint(cp) => {
            out.push_bytes(br#"{"type":"MultiPoint","coordinates":"#);
            write_coords_flat(out, cp);
            out.push_u8(b'}');
        }
        RustGeom::LineString(cp) => {
            out.push_bytes(br#"{"type":"LineString","coordinates":"#);
            write_coords_flat(out, cp);
            out.push_u8(b'}');
        }
        RustGeom::MultiLineString(parts) => {
            out.push_bytes(br#"{"type":"MultiLineString","coordinates":["#);
            for (j, p) in parts.iter().enumerate() {
                if j > 0 { out.push_u8(b','); }
                write_coords_flat(out, p);
            }
            out.push_bytes(br#"]}"#);
        }
        RustGeom::Polygon(rings) => {
            out.push_bytes(br#"{"type":"Polygon","coordinates":["#);
            for (j, r) in rings.iter().enumerate() {
                if j > 0 { out.push_u8(b','); }
                write_coords_flat(out, r);
            }
            out.push_bytes(br#"]}"#);
        }
        RustGeom::MultiPolygon(polys) => {
            out.push_bytes(br#"{"type":"MultiPolygon","coordinates":["#);
            for (j, poly) in polys.iter().enumerate() {
                if j > 0 { out.push_u8(b','); }
                out.push_u8(b'[');
                for (k, ring) in poly.iter().enumerate() {
                    if k > 0 { out.push_u8(b','); }
                    write_coords_flat(out, ring);
                }
                out.push_u8(b']');
            }
            out.push_bytes(br#"]}"#);
        }
        RustGeom::Null => out.push_bytes(b"null"),
    }
}

fn process_feature_parallel(out: &mut JsonWriter, row: usize, props: &[(Vec<u8>, ThreadSafeColumn)], geom: &RustGeom) {
    out.push_bytes(FEAT_HEAD);

    let mut needs_comma = false;
    for (key, col) in props {
        if kv_present(row, col) {
            if needs_comma { out.push_u8(b','); }
            write_kv(out, row, key, col);
            needs_comma = true;
        }
    }

    out.push_bytes(FEAT_MID);
    write_geometry_parallel(out, geom);
    out.push_u8(b'}');
}

fn process_row_generic(out: &mut JsonWriter, row: usize, props: &[(Vec<u8>, ThreadSafeColumn)]) {
    out.push_u8(b'{');

    let mut needs_comma = false;
    for (key, col) in props {
        if kv_present(row, col) {
            if needs_comma { out.push_u8(b','); }
            write_kv(out, row, key, col);
            needs_comma = true;
        }
    }

    out.push_u8(b'}');
}

// -------------------- sf_geojson_str_impl --------------------

#[extendr]
fn sf_geojson_str_impl(x: Robj) -> Result<Robj> {
    let rr = catch_unwind(AssertUnwindSafe(|| sf_geojson_str_impl_inner(x)));
    match rr {
        Ok(r) => r,
        Err(p) => rerr(format!("Internal panic: {}", panic_message(p))),
    }
}

fn sf_geojson_str_impl_inner(x: Robj) -> Result<Robj> {
    if x.is_null() {
        let mut robj = Robj::from("[]");
        robj.set_class(&["geojson", "json"])?;
        return Ok(robj);
    }

    if !x.inherits("sf") {
        return rerr("Not an sf object");
    }

    let df = x
        .as_list()
        .ok_or_else(|| Error::Other("Internal error: invalid object structure (not a list)".to_string()))?;

    if df.len() == 0 {
        let mut robj = Robj::from("[]");
        robj.set_class(&["geojson", "json"])?;
        return Ok(robj);
    }

    let first_col = df.elt(0).map_err(|_| Error::Other("Not a valid sf object (no columns)".to_string()))?;
    let n_rows = unsafe { sexp_len(first_col.get()) };

    if n_rows == 0 {
        let mut robj = Robj::from("[]");
        robj.set_class(&["geojson", "json"])?;
        return Ok(robj);
    }

    let names = x.names().ok_or_else(|| Error::Other("Not a valid sf object (no names)".to_string()))?;
    let colnames: Vec<String> = names.map(|s| s.to_string()).collect();

    let sfcol_attr = x.get_attrib("sf_column").ok_or_else(|| {
        Error::Other("Not a valid sf object (missing 'sf_column' attribute)".to_string())
    })?;
    let sfcol_vec = sfcol_attr
        .as_str_vector()
        .ok_or_else(|| Error::Other("Not a valid sf object ('sf_column' is not character)".to_string()))?;

    if sfcol_vec.is_empty() {
        return rerr("Not a valid sf object ('sf_column' empty)");
    }

    let geom_name = &sfcol_vec[0];
    let geom_idx = colnames
        .iter()
        .position(|n| n == geom_name)
        .ok_or_else(|| Error::Other(format!("Not a valid sf object (Geometry column '{}' not found)", geom_name)))?;

    let geom_col_robj = df.elt(geom_idx).map_err(|_| Error::Other("Internal error: geometry column index invalid".to_string()))?;
    let geom_col = unsafe { geom_col_robj.get() };
    if unsafe { typeof_sexp(geom_col) } != VECSXP {
        return rerr("Not a valid sf object (Geometry column is not a list)");
    }

    let sfc_type = detect_sfc_type_sexp(geom_col);
    let threads = thread::available_parallelism().map(|x| x.get()).unwrap_or(1);
    let force_single = n_rows < PAR_THRESHOLD_ROWS
        || threads == 1
        || sfc_type == SfcType::GeometryCollection
        || sfc_type == SfcType::Unknown;

    if force_single {
        let props = build_property_columns_single(&df, &colnames, geom_idx);
        let mut out = JsonWriter::with_capacity(256 + n_rows * 1024);

        out.push_bytes(FC_HEAD);

        for i in 0..n_rows {
            if i > 0 { out.push_u8(b','); }

            out.push_bytes(FEAT_HEAD);

            let mut needs_comma = false;
            for pc in &props {
                let start_len = out.buf.len();
                if needs_comma { out.push_u8(b','); }
                out.push_bytes(&pc.name_key);

                let wrote = unsafe { write_prop_value_single(&mut out, pc, i) };
                if !wrote {
                    out.buf.truncate(start_len);
                } else {
                    needs_comma = true;
                }
            }

            out.push_bytes(FEAT_MID);

            if let Some(sfg) = unsafe { safe_vector_elt(geom_col, i) } {
                let row_type = if sfc_type == SfcType::GeometryCollection || sfc_type == SfcType::Unknown {
                    get_row_sfg_type(sfg)
                } else {
                    sfc_type
                };

                match row_type {
                    SfcType::Point => {
                        out.push_u8(b'{');
                        out.push_bytes(br#""type":"Point","coordinates":"#);
                        write_point_single(&mut out, sfg);
                        out.push_u8(b'}');
                    }
                    SfcType::MultiPoint => {
                        out.push_u8(b'{');
                        out.push_bytes(br#""type":"MultiPoint","coordinates":"#);
                        write_coords_matrix_single(&mut out, sfg);
                        out.push_u8(b'}');
                    }
                    SfcType::LineString => {
                        out.push_u8(b'{');
                        out.push_bytes(br#""type":"LineString","coordinates":"#);
                        write_coords_matrix_single(&mut out, sfg);
                        out.push_u8(b'}');
                    }
                    SfcType::MultiLineString => {
                        out.push_u8(b'{');
                        out.push_bytes(br#""type":"MultiLineString","coordinates":"#);
                        write_geom_list_single(&mut out, sfg, write_coords_matrix_single);
                        out.push_u8(b'}');
                    }
                    SfcType::Polygon => {
                        out.push_u8(b'{');
                        out.push_bytes(br#""type":"Polygon","coordinates":"#);
                        write_geom_list_single(&mut out, sfg, write_coords_matrix_single);
                        out.push_u8(b'}');
                    }
                    SfcType::MultiPolygon => {
                        out.push_u8(b'{');
                        out.push_bytes(br#""type":"MultiPolygon","coordinates":"#);
                        write_geom_list_single(&mut out, sfg, |o, x| write_geom_list_single(o, x, write_coords_matrix_single));
                        out.push_u8(b'}');
                    }
                    _ => out.push_bytes(b"null"),
                }
            } else {
                out.push_bytes(b"null");
            }

            out.push_u8(b'}');
        }

        out.push_bytes(FC_TAIL);

        let mut robj = Robj::from(unsafe { String::from_utf8_unchecked(out.buf) });
        robj.set_class(&["geojson", "json"])?;
        return Ok(robj);
    }

    let props = build_thread_safe_cols(&df, &colnames, geom_idx, n_rows)?;

    let num_chunks = (n_rows + PAR_CHUNK_ROWS - 1) / PAR_CHUNK_ROWS;
    let ranges: Vec<(usize, usize, usize)> = (0..num_chunks)
        .map(|id| (id, id * PAR_CHUNK_ROWS, (id * PAR_CHUNK_ROWS + PAR_CHUNK_ROWS).min(n_rows)))
        .collect();

    let mut chunk_geoms: Vec<(usize, usize, usize, Vec<RustGeom>)> = Vec::with_capacity(num_chunks);
    for (id, start, end) in &ranges {
        chunk_geoms.push((*id, *start, *end, extract_geometries_chunk(geom_col, sfc_type, *start, *end)));
    }

    let parts_res: Vec<PResult<(usize, Vec<u8>)>> = chunk_geoms
        .into_par_iter()
        .map(|(chunk_id, start, end, geoms)| {
            let rr = catch_unwind(AssertUnwindSafe(|| {
                let mut w = JsonWriter::with_capacity((end - start) * 2048);
                for (local_i, row_i) in (start..end).enumerate() {
                    if local_i > 0 { w.push_u8(b','); }
                    process_feature_parallel(&mut w, row_i, &props, &geoms[local_i]);
                }
                (chunk_id, w.buf)
            }));
            match rr {
                Ok(v) => Ok(v),
                Err(p) => Err(format!("Internal panic in parallel worker: {}", panic_message(p))),
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

    let mut robj = Robj::from(unsafe { String::from_utf8_unchecked(final_out) });
    robj.set_class(&["geojson", "json"])?;
    Ok(robj)
}

// -------------------- df_json_str_impl --------------------

#[extendr]
fn df_json_str_impl(x: Robj) -> Result<Robj> {
    let rr = catch_unwind(AssertUnwindSafe(|| df_json_str_impl_inner(x)));
    match rr {
        Ok(r) => r,
        Err(p) => rerr(format!("Internal panic: {}", panic_message(p))),
    }
}

fn df_json_str_impl_inner(x: Robj) -> Result<Robj> {
    if x.is_null() {
        let mut robj = Robj::from("[]");
        robj.set_class(&["json"])?;
        return Ok(robj);
    }

    if !x.inherits("data.frame") {
        return rerr("Not a dataframe object");
    }

    let df = x
        .as_list()
        .ok_or_else(|| Error::Other("Internal error: invalid object structure (not a list)".to_string()))?;

    if df.len() == 0 {
        let mut robj = Robj::from("[]");
        robj.set_class(&["json"])?;
        return Ok(robj);
    }

    let names = x.names().ok_or_else(|| Error::Other("Not a dataframe object (no names)".to_string()))?;
    let colnames: Vec<String> = names.map(|s| s.to_string()).collect();

    let first_col = df.elt(0).map_err(|_| Error::Other("Not a dataframe object (no columns)".to_string()))?;
    let n_rows = unsafe { sexp_len(first_col.get()) };

    if n_rows == 0 {
        let mut robj = Robj::from("[]");
        robj.set_class(&["json"])?;
        return Ok(robj);
    }

    let props = build_thread_safe_cols(&df, &colnames, usize::MAX, n_rows)?;

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
                Err(p) => Err(format!("Internal panic in parallel worker: {}", panic_message(p))),
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

    let mut robj = Robj::from(unsafe { String::from_utf8_unchecked(final_out) });
    robj.set_class(&["json"])?;
    Ok(robj)
}

extendr_module! {
    mod fastgeojson;
    fn sf_geojson_str_impl;
    fn df_json_str_impl;
}
