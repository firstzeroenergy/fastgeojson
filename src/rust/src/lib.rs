use extendr_api::prelude::*;
use extendr_ffi as libR_sys;
use rayon::prelude::*;
use std::ffi::{CStr, c_char};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;

// ------------------------------------------------------------------
// ADDITIONAL R C-API DECLARATIONS
// ------------------------------------------------------------------
// Exported by R (verified against R.dll's PE export table and libR.so) but
// not re-exported by extendr-ffi 0.8. Declared here rather than reaching for
// a newer extendr, so the MSRV and the vendored dependency set stay put.
// `SEXP` is `*mut SEXPREC`, and extendr-ffi declares `SEXPREC` as an opaque
// non-exhaustive struct, which trips `improper_ctypes`. It is exactly the type
// R itself uses across this boundary; extendr-ffi's own extern blocks suppress
// the same lint.
#[allow(improper_ctypes)]
extern "C" {
    /// Returns a UTF-8 C string for `x`. A no-op (returns the same pointer)
    /// when the CHARSXP is already UTF-8 or pure ASCII. Allocates on R's
    /// vmax stack otherwise, so the result must be copied immediately and
    /// this must only ever be called from the R thread.
    fn Rf_translateCharUTF8(x: libR_sys::SEXP) -> *const c_char;
    /// `cetype_t`, a C enum, hence `c_int`.
    fn Rf_getCharCE(x: libR_sys::SEXP) -> i32;
}

const CE_BYTES: i32 = 3;

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
static REQUESTED_THREADS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

static POOL: std::sync::Mutex<Option<(usize, std::sync::Arc<rayon::ThreadPool>)>> =
    std::sync::Mutex::new(None);

/// Are we running under `R CMD check`?
///
/// `R CMD check` exports a family of `_R_CHECK_*` variables; their presence is
/// the signal used by other parallel CRAN packages (data.table throttles on
/// `_R_CHECK_LIMIT_CORES_` the same way). Checking the whole prefix rather than
/// one name means we still throttle on flavours that do not set that
/// particular variable.
fn under_r_check() -> bool {
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
fn desired_threads() -> usize {
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

/// Runs `f` on our own pool, rebuilding it if the requested width changed.
///
/// Single-threaded requests skip rayon entirely, which is also what makes
/// `fastgeojson_threads(1)` a usable baseline when separating algorithmic
/// gains from parallel ones.
fn with_pool<R: Send>(f: impl FnOnce() -> R + Send) -> R {
    let want = desired_threads();
    if want <= 1 {
        return f();
    }
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

// ------------------------------------------------------------------
// CONFIGURATION & CONSTANTS
// ------------------------------------------------------------------
const PAR_CHUNK_ROWS: usize = 2048;

/// Guards `serialize_sexp_to_json_buffer`'s recursion.
///
/// Measured on this platform: depth 50,000 serialises fine, depth 200,000
/// terminates the R process outright - no R error, no catchable condition,
/// just a dead session. Refuse an order of magnitude below the known-good
/// depth and report an ordinary R error instead. (R's own node-stack limit
/// trips around 2-3k for the R-level helpers, so nothing that works today
/// gets rejected by this.)
const MAX_DEPTH: u32 = 5_000;

// Escape actions, indexed by byte. Chosen to match jsonlite byte-for-byte:
// short escapes for \b \t \n \f \r, lowercase \u00xx for the remaining
// control bytes, and NO escaping of '/' or DEL (0x7F).
const ESC_NONE: u8 = 0;
const ESC_QUOTE: u8 = 1;
const ESC_BACKSLASH: u8 = 2;
const ESC_UNICODE: u8 = 3;
const ESC_B: u8 = 4;
const ESC_T: u8 = 5;
const ESC_N: u8 = 6;
const ESC_F: u8 = 7;
const ESC_R: u8 = 8;
const ESC_SLASH: u8 = 9;

static ESCAPE_LUT: [u8; 256] = {
    let mut table = [ESC_NONE; 256];
    let mut i = 0;
    while i < 32 {
        table[i] = ESC_UNICODE;
        i += 1;
    }
    table[0x08] = ESC_B;
    table[0x09] = ESC_T;
    table[0x0A] = ESC_N;
    table[0x0C] = ESC_F;
    table[0x0D] = ESC_R;
    table[b'"' as usize] = ESC_QUOTE;
    table[b'\\' as usize] = ESC_BACKSLASH;
    // Needs a look-behind, so it only gets flagged for inspection here; see
    // ESC_SLASH in escape_json_string_into.
    table[b'/' as usize] = ESC_SLASH;
    table
};

// jsonlite emits lowercase hex in \u escapes.
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

const FC_HEAD: &[u8] = br#"{"type":"FeatureCollection","name":"sfdata","features":["#;
const FC_TAIL: &[u8] = br#"]}"#;
const EMPTY_FC: &str = r#"{"type":"FeatureCollection","name":"sfdata","features":[]}"#;
const FEAT_HEAD: &[u8] = br#"{"type":"Feature","properties":{"#;
const FEAT_MID: &[u8] = br#"},"geometry":"#;

#[derive(Clone, Copy, PartialEq, Debug)]
enum DfMode { Rows, Columns, Values }

// 3-State Logic
#[derive(Clone, Copy, PartialEq, Debug)]
enum NaMode { 
    Null,   
    String, 
    Smart   
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum NullMode { List, Null }

#[derive(Clone, Copy, PartialEq, Debug)]
enum FactorMode { String, Integer }

#[derive(Clone, Copy, Debug)]
struct SerializerConfig {
    df: DfMode,
    na: NaMode,
    null: NullMode,
    factor: FactorMode,
    auto_unbox: bool,
    digits: Option<u8>,
    /// jsonlite's `matrix = "columnmajor"`: nest a matrix/array by its last
    /// dimension first rather than its first.
    matrix_colmajor: bool,
    /// jsonlite's `always_decimal`: render whole doubles as `100.0` rather
    /// than `100`, so a numeric column never looks like an integer column.
    always_decimal: bool,
}

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

// ------------------------------------------------------------------
// STRUCT DEFINITIONS
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
    /// Number of elements actually behind `data_ptr`. A data.frame built with
    /// `structure()` can declare more rows than a column holds; without this
    /// the row loop read past the end and serialised adjacent heap bytes.
    len: usize,
    cached_levels: Option<Vec<Vec<u8>>>,
    string_arena: Option<StringArena>,
}
unsafe impl Send for ThreadSafeColumn {}
unsafe impl Sync for ThreadSafeColumn {}

#[derive(Clone, Copy, Debug)]
struct CoordPtr {
    ptr: usize,
    len: usize,
    /// Number of coordinate columns, taken from the sfg matrix's `dim`.
    ///
    /// sf stores a ring/line as an nrow x ncol column-major matrix where ncol
    /// is 2 (XY), 3 (XYZ or XYM) or 4 (XYZM). Assuming 2 here is what corrupted
    /// every XYZ/XYM/XYZM geometry: a 3x3 matrix was read as 4 points of
    /// garbage. The XY/XYZ/XYM/XYZM class token itself is irrelevant -- every
    /// ordinate present is emitted verbatim, exactly as jsonlite does.
    ncol: usize,
}
unsafe impl Send for CoordPtr {}
unsafe impl Sync for CoordPtr {}

struct GeometryBatch {
    coords: Vec<CoordPtr>,
    counts: Vec<usize>,
    /// Geometries rendered to bytes up front on the R thread, for shapes that
    /// need recursive traversal of R objects (GEOMETRYCOLLECTION) and so
    /// cannot be walked from a worker.
    raw: Vec<u8>,
}

#[derive(Clone, Copy)]
enum FastGeom {
    Null,
    /// A bare coordinate vector: `[x, y]`, `[x, y, z]` or `[x, y, z, m]`.
    Point(CoordPtr),
    Single(CoordPtr, SfcType),
    FlatList { start: u32, len: u32, typ: SfcType },
    MultiPolygon { coords_start: u32, counts_start: u32, n_polys: u32 },
    /// Byte range into `GeometryBatch::raw`.
    Prerendered { start: u32, len: u32 },
}
unsafe impl Send for FastGeom {}
unsafe impl Sync for FastGeom {}

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

fn parse_r_string_arg(x: Robj, default: &str) -> String {
    if x.is_null() { return default.to_string(); }
    if let Some(s) = x.as_str() { return s.to_string(); }
    if let Some(v) = x.as_str_vector() {
        if !v.is_empty() { return v[0].to_string(); }
    }
    default.to_string()
}

fn parse_digits_arg(x: Robj) -> Option<u8> {
    if x.is_null() { return None; }
    if let Some(i) = x.as_integer() {
        let d = i.max(0).min(16) as u8;
        return Some(d);
    }
    if let Some(f) = x.as_real() {
        if f.is_finite() {
            let i = f.round() as i32;
            let d = i.max(0).min(16) as u8;
            return Some(d);
        }
    }
    None
}

// ------------------------------------------------------------------
// JSON WRITER
// ------------------------------------------------------------------

struct JsonWriter {
    buf: Vec<u8>,
}

// ------------------------------------------------------------------
// FLOAT FORMATTING (digits support)
// ------------------------------------------------------------------

#[inline(always)]
fn write_f64_json(buf: &mut Vec<u8>, v: f64, digits: Option<u8>, always_decimal: bool) {
    let mark = buf.len();
    write_f64_json_inner(buf, v, digits);
    if always_decimal {
        // jsonlite's always_decimal keeps a double looking like a double.
        // Only touch plain integer output: anything already carrying a '.' or
        // an exponent, and any non-finite placeholder such as "NA", is left
        // exactly as written.
        let wrote = &buf[mark..];
        let plain_int = !wrote.is_empty()
            && wrote
                .iter()
                .all(|&c| c.is_ascii_digit() || c == b'-' || c == b'+');
        if plain_int {
            buf.extend_from_slice(b".0");
        }
    }
}

#[inline(always)]
/// C's `%.*g`, which is what jsonlite falls back to outside its fast path.
///
/// `%g` picks fixed or scientific notation by exponent, treats the precision as
/// *significant* digits, strips trailing zeros, and writes the exponent with a
/// sign and at least two digits (`1e-05`, `1e+20`).
fn write_g_format(buf: &mut Vec<u8>, v: f64, precision: i32) {
    let p = precision.max(1) as usize;

    if v == 0.0 {
        // Preserve the sign of -0.0 the way printf does.
        if v.is_sign_negative() {
            buf.push(b'-');
        }
        buf.push(b'0');
        return;
    }

    // Take the exponent from the *rounded* value, so a carry such as
    // 9.99 at precision 2 -> 1.0e1 selects the right branch.
    let sci = format!("{:.*e}", p - 1, v);
    let (mantissa, exp) = match sci.split_once('e') {
        Some((m, e)) => (m, e.parse::<i32>().unwrap_or(0)),
        None => (sci.as_str(), 0),
    };

    if exp >= -4 && (exp as i64) < p as i64 {
        // Fixed notation with p - 1 - exp digits after the point.
        let decimals = (p as i32 - 1 - exp).max(0) as usize;
        let s = format!("{:.*}", decimals, v);
        buf.extend_from_slice(trim_fixed(&s).as_bytes());
    } else {
        // Scientific notation: trimmed mantissa, then e(+|-)NN.
        buf.extend_from_slice(trim_fixed(mantissa).as_bytes());
        buf.push(b'e');
        if exp < 0 {
            buf.push(b'-');
        } else {
            buf.push(b'+');
        }
        let a = exp.unsigned_abs();
        if a < 10 {
            buf.push(b'0');
        }
        let mut tmp = itoa::Buffer::new();
        buf.extend_from_slice(tmp.format(a).as_bytes());
    }
}

/// Drops trailing fractional zeros, and a trailing point if one is left.
fn trim_fixed(s: &str) -> &str {
    if !s.contains('.') {
        return s;
    }
    let t = s.trim_end_matches('0');
    t.strip_suffix('.').unwrap_or(t)
}

/// Formats a double exactly as jsonlite's `num_to_char` does.
///
/// Mirrors jsonlite's src/num_to_char.c branch for branch, because `digits` is
/// NOT plain decimal rounding there: the fixed-decimal path is taken only for
/// moderate magnitudes, and everything else falls back to `%g` so that very
/// small and very large values keep their magnitude. Rounding unconditionally
/// turned 1e-15 into 0 and -1e14 into -99999999999999.984.
#[inline(always)]
fn write_f64_json_inner(buf: &mut Vec<u8>, v: f64, digits: Option<u8>) {
    match digits {
        // digits = NA: 15 significant digits.
        None => write_g_format(buf, v, 15),
        Some(d) => {
            let d = d as i32;
            if d > -1 && d < 10 && v.abs() < 2_147_483_647.0 && v.abs() > 1e-5 {
                // Fixed decimal digits: the common, fast case.
                write_fixed_decimals(buf, v, d as usize);
            } else {
                // Convert decimal digits into significant digits, as jsonlite
                // does: ceil(min(17, max(1, log10(|v|)) + digits)).
                let l = if v == 0.0 {
                    1.0f64
                } else {
                    v.abs().log10().max(1.0)
                };
                let decimals = (l + d as f64).min(17.0).ceil() as i32;
                write_g_format(buf, v, decimals);
            }
        }
    }
}

/// `d` digits after the decimal point, trailing zeros removed.
#[inline]
fn write_fixed_decimals(buf: &mut Vec<u8>, v: f64, d: usize) {
    use std::fmt::Write;
    let mut sb = StackFmtBuf::new();
    if write!(&mut sb, "{:.*}", d, v).is_ok() {
        let s = std::str::from_utf8(sb.as_bytes()).unwrap_or("0");
        buf.extend_from_slice(trim_fixed(s).as_bytes());
    } else {
        write_g_format(buf, v, 15);
    }
}

struct StackFmtBuf {
    buf: [u8; 128],
    len: usize,
}

impl StackFmtBuf {
    #[inline]
    fn new() -> Self {
        Self { buf: [0u8; 128], len: 0 }
    }
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl std::fmt::Write for StackFmtBuf {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let b = s.as_bytes();
        if self.len + b.len() > self.buf.len() {
            return Err(std::fmt::Error);
        }
        self.buf[self.len..self.len + b.len()].copy_from_slice(b);
        self.len += b.len();
        Ok(())
    }
}

impl JsonWriter {
    #[inline]
    fn with_capacity(cap: usize) -> Self {
        Self { buf: Vec::with_capacity(cap) }
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
    fn push_f64_cfg(&mut self, v: f64, config: SerializerConfig) {
        write_f64_json(&mut self.buf, v, config.digits, config.always_decimal);
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
        let offset = bytes[start..].iter().position(|&b| ESCAPE_LUT[b as usize] != 0);
        match offset {
            Some(i) => {
                let esc_idx = start + i;
                out.extend_from_slice(&bytes[start..esc_idx]);
                let b = bytes[esc_idx];
                match ESCAPE_LUT[b as usize] {
                    ESC_QUOTE => out.extend_from_slice(br#"\""#),
                    ESC_BACKSLASH => out.extend_from_slice(br#"\\"#),
                    ESC_B => out.extend_from_slice(br#"\b"#),
                    ESC_T => out.extend_from_slice(br#"\t"#),
                    ESC_N => out.extend_from_slice(br#"\n"#),
                    ESC_F => out.extend_from_slice(br#"\f"#),
                    ESC_R => out.extend_from_slice(br#"\r"#),
                    ESC_SLASH => {
                        // jsonlite escapes the solidus only when it follows a
                        // '<', so that a payload containing "</script>" cannot
                        // terminate an enclosing <script> block. See
                        // jsonlite's src/escape_chars.c.
                        if esc_idx > 0 && bytes[esc_idx - 1] == b'<' {
                            out.extend_from_slice(br#"\/"#);
                        } else {
                            out.push(b'/');
                        }
                    }
                    ESC_UNICODE => {
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
fn build_escaped_key_bytes(name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(name.len() + 4);
    escape_json_string_into(&mut key, name);
    key.push(b':');
    key
}

/// UTF-8 name bytes for each element of `x`'s `names` attribute.
///
/// Deliberately does not go through extendr's `Robj::names()`, which hands back
/// `&str` built from the untranslated `R_CHAR` bytes: for a latin1-marked name
/// that is not valid UTF-8, so the resulting keys were emitted as raw
/// non-UTF-8 bytes.
unsafe fn utf8_names(x: libR_sys::SEXP, n: usize) -> Option<Vec<Vec<u8>>> {
    let names_sexp = libR_sys::Rf_getAttrib(x, libR_sys::R_NamesSymbol);
    if names_sexp == libR_sys::R_NilValue
        || typeof_sexp(names_sexp) != libR_sys::SEXPTYPE::STRSXP as u32
        || sexp_len(names_sexp) != n
    {
        return None;
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let cs = libR_sys::STRING_ELT(names_sexp, i as isize);
        if is_na_string(cs) {
            out.push(Vec::new());
            continue;
        }
        match charsxp_to_utf8_bytes(cs) {
            Some(b) => out.push(b.to_vec()),
            None => out.push(Vec::new()),
        }
    }
    Some(out)
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
unsafe fn is_na_int(v: i32) -> bool { v == i32::MIN }
#[inline(always)]
unsafe fn is_na_real(v: f64) -> bool { libR_sys::R_IsNA(v) != 0 }
#[inline(always)]
unsafe fn is_nan_real(v: f64) -> bool { libR_sys::R_IsNaN(v) != 0 }
#[inline(always)]
unsafe fn is_na_string(sexp: libR_sys::SEXP) -> bool { sexp == libR_sys::R_NaString }

// Per-call state for string handling. Only ever touched from the R thread
// (every CHARSXP read is hoisted out of the rayon regions), so a plain Cell
// is sufficient.
const STR_NON_ASCII: u8 = 1;
const STR_ENC_ERROR: u8 = 2;
const STR_DEPTH_ERROR: u8 = 4;

thread_local! {
    static STR_STATE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[inline]
fn str_state_reset() {
    STR_STATE.with(|c| c.set(0));
}
#[inline]
fn str_state_mark(bit: u8) {
    STR_STATE.with(|c| c.set(c.get() | bit));
}
#[inline]
fn str_state_has(bit: u8) -> bool {
    STR_STATE.with(|c| c.get() & bit != 0)
}

/// Returns the UTF-8 bytes of a CHARSXP.
///
/// `R_CHAR` alone returns the CHARSXP's *native* bytes, which are not UTF-8
/// for latin1-marked strings; emitting those produced invalid UTF-8 output and
/// made `String::from_utf8_unchecked` undefined behaviour. jsonlite always goes
/// through `Rf_translateCharUTF8`, so we do too.
///
/// The returned slice is NOT really `'static`: when a translation happens the
/// bytes live on R's vmax stack and are valid only until the end of the
/// enclosing `.Call`. Every caller copies immediately (into an arena or the
/// output buffer), which is what makes this sound. Do not hold the slice.
#[inline]
unsafe fn charsxp_to_utf8_bytes(charsxp: libR_sys::SEXP) -> Option<&'static [u8]> {
    if charsxp == libR_sys::R_NilValue {
        return None;
    }
    let len = libR_sys::Rf_xlength(charsxp);
    if len < 0 {
        return None;
    }
    let ptr = libR_sys::R_CHAR(charsxp) as *const u8;
    let raw = slice::from_raw_parts(ptr, len as usize);

    // The overwhelmingly common case. Vectorised, and needs no call into R.
    if raw.is_ascii() {
        return Some(raw);
    }

    if Rf_getCharCE(charsxp) == CE_BYTES {
        // jsonlite raises `translating strings with "bytes" encoding is not
        // allowed`. Match that, but record it and unwind through our own error
        // path rather than letting Rf_error longjmp over these Rust frames
        // (which would skip every destructor).
        str_state_mark(STR_ENC_ERROR);
        return None;
    }

    str_state_mark(STR_NON_ASCII);

    let p = Rf_translateCharUTF8(charsxp);
    if p.is_null() {
        return None;
    }
    if p == ptr as *const c_char {
        // Already UTF-8: reuse the length we have instead of paying a strlen.
        return Some(raw);
    }
    Some(CStr::from_ptr(p).to_bytes())
}

/// Converts the finished buffer into the `String` handed back to R.
///
/// When nothing non-ASCII was ever read, every byte in the buffer is ASCII by
/// construction and the unchecked conversion is provably sound. Otherwise pay
/// for the validation rather than risk undefined behaviour.
#[inline]
fn finish_json_string(buf: Vec<u8>) -> PResult<String> {
    if str_state_has(STR_ENC_ERROR) {
        return Err(
            "translating strings with \"bytes\" encoding is not allowed".to_string()
        );
    }
    if str_state_has(STR_DEPTH_ERROR) {
        return Err(format!(
            "object is nested more than {} levels deep; refusing to recurse further",
            MAX_DEPTH
        ));
    }
    if !str_state_has(STR_NON_ASCII) {
        debug_assert!(std::str::from_utf8(&buf).is_ok());
        return Ok(unsafe { String::from_utf8_unchecked(buf) });
    }
    String::from_utf8(buf).map_err(|e| {
        format!(
            "internal error: serializer produced invalid UTF-8 at byte {}",
            e.utf8_error().valid_up_to()
        )
    })
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
// RECURSIVE SERIALIZER
// ------------------------------------------------------------------

unsafe fn serialize_sexp_to_json_buffer(x: libR_sys::SEXP, buf: &mut Vec<u8>, config: SerializerConfig, depth: u32) {
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
    let robj = Robj::from_sexp(x);

    // 0. Geometry columns.
    //
    // An `sfc` renders as an array of typed GeoJSON geometry objects wherever
    // it appears -- bare, in a list, at any nesting depth, or inside a
    // data.frame list column -- not only as an sf object's designated
    // geometry column. Without this it degraded to bare coordinate arrays
    // like [0,0] and the geometry type was lost. Its length is unrelated to
    // any enclosing frame's row count.
    if robj.inherits("sfc") {
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
    if !robj.inherits("data.frame") {
        let dim_attr = libR_sys::Rf_getAttrib(x, libR_sys::R_DimSymbol);
        if dim_attr != libR_sys::R_NilValue && sexp_len(dim_attr) > 0 {
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

            unsafe fn write_recursive(
                buf: &mut Vec<u8>, 
                dims: &[usize], 
                strides: &[usize], 
                depth: usize, 
                offset: usize,
                write_fn: &impl Fn(usize, &mut Vec<u8>)
            ) {
                let n = dims[depth];
                let stride = strides[depth];
                buf.push(b'[');
                for i in 0..n {
                    if i > 0 { buf.push(b','); }
                    let next_offset = offset + i * stride;
                    if depth == dims.len() - 1 {
                        write_fn(next_offset, buf);
                    } else {
                        write_recursive(buf, dims, strides, depth + 1, next_offset, write_fn);
                    }
                }
                buf.push(b']');
            }

            if robj.inherits("factor") && typeof_sexp(x) == libR_sys::SEXPTYPE::INTSXP as u32 && config.factor == FactorMode::String {
                if let Some(levels) = robj.get_attrib("levels") {
                    let levels_sexp = levels.get();
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
                     let p = libR_sys::INTEGER(x);
                     let writer = |idx: usize, b: &mut Vec<u8>| {
                         let v = *p.add(idx);
                         if is_na_int(v) {
                             if config.na == NaMode::String || config.na == NaMode::Smart { b.extend_from_slice(b"\"NA\""); }
                             else { b.extend_from_slice(b"null"); }
                         } else {
                             let mut tmp = itoa::Buffer::new();
                             b.extend_from_slice(tmp.format(v).as_bytes());
                         }
                     };
                     write_recursive(buf, &dims, &strides, 0, 0, &writer);
                     return;
                 },
                 t if t == libR_sys::SEXPTYPE::REALSXP as u32 => {
                     let p = libR_sys::REAL(x);
                     let writer = |idx: usize, b: &mut Vec<u8>| {
                         let v = *p.add(idx);
                         if is_na_real(v) || is_nan_real(v) || !v.is_finite() {
                             if config.na == NaMode::String || config.na == NaMode::Smart {
                                 if v == f64::INFINITY { b.extend_from_slice(b"\"Inf\""); }
                                 else if v == f64::NEG_INFINITY { b.extend_from_slice(b"\"-Inf\""); }
                                 else if is_nan_real(v) { b.extend_from_slice(b"\"NaN\""); }
                                 else { b.extend_from_slice(b"\"NA\""); }
                             } else { b.extend_from_slice(b"null"); }
                         } else {
                             write_f64_json(b, v, config.digits, config.always_decimal);
                         }
                     };
                     write_recursive(buf, &dims, &strides, 0, 0, &writer);
                     return;
                 },
                 t if t == libR_sys::SEXPTYPE::LGLSXP as u32 => {
                     let p = libR_sys::LOGICAL(x);
                     let writer = |idx: usize, b: &mut Vec<u8>| {
                         let v = *p.add(idx);
                         if is_na_int(v) {
                             if config.na == NaMode::String { b.extend_from_slice(b"\"NA\""); }
                             else { b.extend_from_slice(b"null"); }
                         } else if v != 0 { b.extend_from_slice(b"true"); }
                         else { b.extend_from_slice(b"false"); }
                     };
                     write_recursive(buf, &dims, &strides, 0, 0, &writer);
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
    if robj.inherits("factor") && typeof_sexp(x) == libR_sys::SEXPTYPE::INTSXP as u32 {
        if config.factor == FactorMode::String {
            let do_unbox = config.auto_unbox && sexp_len(x) == 1 && !robj.inherits("AsIs");
            if let Some(levels) = robj.get_attrib("levels") {
                let levels_sexp = levels.get();
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

    if robj.inherits("Date") || robj.inherits("POSIXt") {
        if let Ok(char_robj) = call!("format", robj.clone()) {
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
        && !robj.inherits("AsIs")
        && !robj.inherits("data.frame");

    if r_type == libR_sys::SEXPTYPE::INTSXP as u32 {
        let n = sexp_len(x);
        let p = libR_sys::INTEGER(x);
        if !do_unbox { buf.push(b'['); }
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let v = *p.add(i);
            if is_na_int(v) { 
                if config.na == NaMode::String || config.na == NaMode::Smart { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            } else {
                let mut tmp = itoa::Buffer::new();
                buf.extend_from_slice(tmp.format(v).as_bytes());
            }
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }

    if r_type == libR_sys::SEXPTYPE::REALSXP as u32 {
        let n = sexp_len(x);
        let p = libR_sys::REAL(x);
        if !do_unbox { buf.push(b'['); }
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let v = *p.add(i);
            if is_na_real(v) || is_nan_real(v) || !v.is_finite() { 
                if config.na == NaMode::String || config.na == NaMode::Smart {
                    if v == f64::INFINITY { buf.extend_from_slice(b"\"Inf\""); }
                    else if v == f64::NEG_INFINITY { buf.extend_from_slice(b"\"-Inf\""); }
                    else if is_nan_real(v) { buf.extend_from_slice(b"\"NaN\""); }
                    else { buf.extend_from_slice(b"\"NA\""); }
                } else { buf.extend_from_slice(b"null"); }
            } else {
                write_f64_json(buf, v, config.digits, config.always_decimal);
            }
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }

    if r_type == libR_sys::SEXPTYPE::LGLSXP as u32 {
        let n = sexp_len(x);
        let p = libR_sys::LOGICAL(x);
        if !do_unbox { buf.push(b'['); }
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let v = *p.add(i);
            if is_na_int(v) { 
                if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            }
            else if v != 0 { buf.extend_from_slice(b"true"); }
            else { buf.extend_from_slice(b"false"); }
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }

    // [MODIFICATION: Passthrough Support for Generic Vectors]
    if r_type == libR_sys::SEXPTYPE::STRSXP as u32 {
        let is_json = robj.inherits("json"); // Check class
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
        if robj.inherits("data.frame") {
            let n_cols = sexp_len(x);
            if config.df == DfMode::Columns {
                buf.push(b'{');
                let names_sym = libR_sys::R_NamesSymbol;
                let names_sexp = libR_sys::Rf_getAttrib(x, names_sym);
                let has_names = names_sexp != libR_sys::R_NilValue && sexp_len(names_sexp) == n_cols;
                let mut first = true;
                for c in 0..n_cols {
                    if !first { buf.push(b','); }
                    if has_names {
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
            let rn_sexp = libR_sys::Rf_getAttrib(x, libR_sys::R_RowNamesSymbol);

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
                        if has_names {
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
                if !is_default_rownames(rn_sexp) && r < sexp_len(rn_sexp) {
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
                serialize_sexp_to_json_buffer(val_sexp, buf, config, depth + 1);
            }
            buf.push(b'}');
        } else {
            buf.push(b'[');
            for i in 0..n {
                if i > 0 { buf.push(b','); }
                let val_sexp = libR_sys::VECTOR_ELT(x, i as isize);
                serialize_sexp_to_json_buffer(val_sexp, buf, config, depth + 1);
            }
            buf.push(b']');
        }
        return;
    }
    buf.extend_from_slice(b"{}");
}

unsafe fn serialize_element_at_index(col: libR_sys::SEXP, idx: usize, buf: &mut Vec<u8>, config: SerializerConfig, depth: u32) {
    let r_type = typeof_sexp(col);
    let robj = Robj::from_sexp(col);
    
    if robj.inherits("factor") && r_type == libR_sys::SEXPTYPE::INTSXP as u32 {
        if config.factor == FactorMode::String {
            if let Some(levels) = robj.get_attrib("levels") {
                let levels_sexp = levels.get();
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
            if is_na_real(v) || is_nan_real(v) || !v.is_finite() { 
                if config.na == NaMode::String || config.na == NaMode::Smart {
                    if v == f64::INFINITY { buf.extend_from_slice(b"\"Inf\""); }
                    else if v == f64::NEG_INFINITY { buf.extend_from_slice(b"\"-Inf\""); }
                    else if is_nan_real(v) { buf.extend_from_slice(b"\"NaN\""); }
                    else { buf.extend_from_slice(b"\"NA\""); }
                } else { buf.extend_from_slice(b"null"); }
            } else {
                write_f64_json(buf, v, config.digits, config.always_decimal);
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
                if robj.inherits("json") { buf.extend_from_slice(bytes); } // Passthrough
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
// PARALLEL SAFE COLUMNS
// ------------------------------------------------------------------

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

/// Writes the row name at `idx` as a JSON string.
///
/// jsonlite always emits `_row` as a string, even when the row names are
/// stored as integers.
unsafe fn write_rowname_at(rn: libR_sys::SEXP, idx: usize, buf: &mut Vec<u8>) {
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

fn build_thread_safe_cols(
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
                         if unsafe { is_na_int(v) } { 
                             if config.na == NaMode::String || config.na == NaMode::Smart { bytes.extend_from_slice(b"\"NA\""); }
                             else { bytes.extend_from_slice(b"null"); }
                         } else {
                             let mut tmp = itoa::Buffer::new();
                             bytes.extend_from_slice(tmp.format(v).as_bytes());
                         }
                     } else if r_type == Rtype::Doubles {
                         let p = unsafe { libR_sys::REAL(sexp) };
                         let v = unsafe { *p.add(idx) };
                         if unsafe { is_na_real(v) } { 
                             if config.na == NaMode::String || config.na == NaMode::Smart { bytes.extend_from_slice(b"\"NA\""); }
                             else { bytes.extend_from_slice(b"null"); }
                         } else {
                             if v.is_finite() {
                                 let mut tmp = itoa::Buffer::new();
                                 if v.fract() == 0.0 && v >= (i32::MIN as f64) && v <= (i32::MAX as f64) {
                                     bytes.extend_from_slice(tmp.format(v as i32).as_bytes());
                                 } else {
                                     bytes.reserve(24);
                                     let len = bytes.len();
                                     unsafe {
                                         let ptr = bytes.as_mut_ptr().add(len);
                                         let written = ryu::raw::format64(v, ptr);
                                         bytes.set_len(len + written);
                                     }
                                 }
                             } else { 
                                 if config.na == NaMode::String || config.na == NaMode::Smart {
                                     if v == f64::INFINITY { bytes.extend_from_slice(b"\"Inf\""); }
                                     else if v == f64::NEG_INFINITY { bytes.extend_from_slice(b"\"-Inf\""); }
                                     else { bytes.extend_from_slice(b"\"NA\""); }
                                 } else { bytes.extend_from_slice(b"null"); }
                             }
                         }
                     } else { bytes.extend_from_slice(b"null"); }
                 }
                 bytes.push(b']');
                 offsets.push((start, bytes.len() - start));
             }
             let col_len = offsets.len();
             let col = ThreadSafeColumn { kind: ColumnType::JsonRaw, data_ptr: 0, len: col_len, cached_levels: None, string_arena: Some(StringArena { bytes, offsets }) };
             out.push((key, col));
             continue;
        }

        let (kind, ptr, cached_levels, arena) =
            if col.inherits("factor") && r_type == Rtype::Integers && config.factor == FactorMode::String {
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
                // [MODIFICATION: Passthrough Support for Data Frames]
                let is_json = col.inherits("json");
                let n = unsafe { sexp_len(sexp) };
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
        out.push((key, ThreadSafeColumn { kind, data_ptr: ptr, len: col_len, cached_levels, string_arena: arena }));
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
        out.push((key, ThreadSafeColumn { kind: ColumnType::JsonRaw, data_ptr: 0, len: rn_len, cached_levels: None, string_arena: Some(StringArena { bytes, offsets }) }));
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
fn col_available(col: &ThreadSafeColumn, row: usize) -> bool {
    match col.kind {
        ColumnType::Char | ColumnType::JsonRaw => match col.string_arena {
            Some(ref a) => row < a.offsets.len(),
            None => false,
        },
        ColumnType::Null => true,
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
fn col_is_missing(col: &ThreadSafeColumn, row: usize) -> bool {
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
fn write_col_value(
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
fn try_write_kv(
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

// ------------------------------------------------------------------
// GEOMETRY & WORKER FUNCTIONS
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
                    // Without this arm a GEOMETRYCOLLECTION fell through to
                    // Unknown and its geometry was silently emitted as null.
                    "GEOMETRYCOLLECTION" => return SfcType::GeometryCollection,
                    _ => continue,
                }
            }
        }
    }
    SfcType::Unknown
}

/// Reads an sfg coordinate matrix (or bare vector) into a thread-safe pointer.
///
/// `ncol` comes from the `dim` attribute, so XYZ/XYM/XYZM geometries carry all
/// their ordinates. A bare vector (as POINT uses) reports ncol equal to its
/// length, i.e. a single row.
unsafe fn read_coord_ptr(x: libR_sys::SEXP) -> CoordPtr {
    let len = sexp_len(x);
    let dim = libR_sys::Rf_getAttrib(x, libR_sys::R_DimSymbol);
    let ncol = if dim != libR_sys::R_NilValue && sexp_len(dim) == 2 {
        let d = libR_sys::INTEGER(dim);
        let c = *d.add(1);
        if c > 0 { c as usize } else { 1 }
    } else {
        // No dim: treat the whole vector as one row.
        len.max(1)
    };
    CoordPtr { ptr: libR_sys::REAL(x) as usize, len, ncol }
}

/// Renders a GEOMETRYCOLLECTION (or any geometry) to bytes on the R thread.
///
/// GEOMETRYCOLLECTION uses a recursive `geometries` array in place of
/// `coordinates`, and walking it needs `VECTOR_ELT`, so it cannot be done from
/// a rayon worker. These are rare enough that pre-rendering costs nothing.
unsafe fn render_geometry_to_bytes(
    sfg: libR_sys::SEXP,
    buf: &mut Vec<u8>,
    config: SerializerConfig,
    depth: u32,
) {
    if sfg == libR_sys::R_NilValue || depth > 64 {
        buf.extend_from_slice(b"null");
        return;
    }
    let typ = get_row_sfg_type(sfg);
    match typ {
        SfcType::GeometryCollection => {
            buf.extend_from_slice(br#"{"type":"GeometryCollection","geometries":["#);
            let n = sexp_len(sfg);
            for i in 0..n {
                if i > 0 {
                    buf.push(b',');
                }
                render_geometry_to_bytes(
                    libR_sys::VECTOR_ELT(sfg, i as isize),
                    buf,
                    config,
                    depth + 1,
                );
            }
            buf.extend_from_slice(b"]}");
        }
        SfcType::Point => {
            buf.extend_from_slice(br#"{"type":"Point","coordinates":"#);
            let cp = read_coord_ptr(sfg);
            write_point_coords(buf, &cp, config);
            buf.push(b'}');
        }
        SfcType::MultiPoint | SfcType::LineString => {
            buf.extend_from_slice(if typ == SfcType::MultiPoint {
                br#"{"type":"MultiPoint","coordinates":"#
            } else {
                br#"{"type":"LineString","coordinates":"#
            });
            let cp = read_coord_ptr(sfg);
            write_coord_matrix(buf, &cp, config);
            buf.push(b'}');
        }
        SfcType::MultiLineString | SfcType::Polygon => {
            buf.extend_from_slice(if typ == SfcType::Polygon {
                br#"{"type":"Polygon","coordinates":["#
            } else {
                br#"{"type":"MultiLineString","coordinates":["#
            });
            let n = sexp_len(sfg);
            for i in 0..n {
                if i > 0 {
                    buf.push(b',');
                }
                let cp = read_coord_ptr(libR_sys::VECTOR_ELT(sfg, i as isize));
                write_coord_matrix(buf, &cp, config);
            }
            buf.extend_from_slice(b"]}");
        }
        SfcType::MultiPolygon => {
            buf.extend_from_slice(br#"{"type":"MultiPolygon","coordinates":["#);
            let n_polys = sexp_len(sfg);
            for i in 0..n_polys {
                if i > 0 {
                    buf.push(b',');
                }
                let poly = libR_sys::VECTOR_ELT(sfg, i as isize);
                buf.push(b'[');
                let n_rings = sexp_len(poly);
                for k in 0..n_rings {
                    if k > 0 {
                        buf.push(b',');
                    }
                    let cp = read_coord_ptr(libR_sys::VECTOR_ELT(poly, k as isize));
                    write_coord_matrix(buf, &cp, config);
                }
                buf.push(b']');
            }
            buf.extend_from_slice(b"]}");
        }
        SfcType::Unknown => buf.extend_from_slice(b"null"),
    }
}

fn extract_geometries_chunk(
    geom_col: libR_sys::SEXP,
    sfc_type: SfcType,
    start: usize,
    end: usize,
    config: SerializerConfig,
) -> (GeometryBatch, Vec<FastGeom>) {
    let capacity_est = end - start;
    let mut batch = GeometryBatch {
        coords: Vec::with_capacity(capacity_est * 2),
        counts: Vec::with_capacity(capacity_est),
        raw: Vec::new(),
    };
    let mut out = Vec::with_capacity(capacity_est);
    // Defence in depth: `VECTOR_ELT` past the end of the list dereferences a
    // wild SEXP and segfaults the R process. The caller also rejects such
    // objects up front, but never index out of range from here.
    let geom_len = unsafe { sexp_len(geom_col) };

    for i in start..end {
        if i >= geom_len {
            out.push(FastGeom::Null);
            continue;
        }
        let sfg = unsafe { libR_sys::VECTOR_ELT(geom_col, i as isize) };
        if sfg == unsafe { libR_sys::R_NilValue } {
            out.push(FastGeom::Null);
            continue;
        }
        // The sfc's own class is only a hint: a homogeneous sfc_POINT column
        // can still be indexed per element, and sfc_GEOMETRY holds mixed
        // types. jsonlite reads class(sfg)[2] for every element, so match that
        // whenever the column class is not a reliable single type.
        let row_type = if sfc_type == SfcType::GeometryCollection || sfc_type == SfcType::Unknown {
            get_row_sfg_type(sfg)
        } else {
            sfc_type
        };

        match row_type {
            SfcType::Point => {
                let cp = unsafe { read_coord_ptr(sfg) };
                if cp.len == 0 {
                    // POINT EMPTY.
                    out.push(FastGeom::Null);
                } else {
                    out.push(FastGeom::Point(cp));
                }
            }
            SfcType::MultiPoint | SfcType::LineString => {
                out.push(FastGeom::Single(unsafe { read_coord_ptr(sfg) }, row_type));
            }
            SfcType::MultiLineString | SfcType::Polygon => {
                let n = unsafe { sexp_len(sfg) };
                let start_idx = batch.coords.len() as u32;
                for j in 0..n {
                    batch
                        .coords
                        .push(unsafe { read_coord_ptr(libR_sys::VECTOR_ELT(sfg, j as isize)) });
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
                        batch.coords.push(unsafe {
                            read_coord_ptr(libR_sys::VECTOR_ELT(poly_sfg, k as isize))
                        });
                    }
                }
                out.push(FastGeom::MultiPolygon {
                    coords_start,
                    counts_start,
                    n_polys: n_polys as u32,
                });
            }
            // GEOMETRYCOLLECTION and anything unrecognised: render here, on the
            // R thread, and hand the workers a byte range.
            _ => {
                let s = batch.raw.len() as u32;
                unsafe { render_geometry_to_bytes(sfg, &mut batch.raw, config, 0) };
                let l = batch.raw.len() as u32 - s;
                out.push(FastGeom::Prerendered { start: s, len: l });
            }
        }
    }
    (batch, out)
}

/// Writes one coordinate, honouring `digits` and the `na` mode.
///
/// sf represents an empty or missing ordinate as NA_real_/NaN, and jsonlite
/// keeps those distinct: NA -> "NA", NaN -> "NaN", Inf -> "Inf". Previously
/// these went through ryu unchecked and emitted values like
/// 1.797693134863096e308.
#[inline(always)]
fn write_coord_value(buf: &mut Vec<u8>, v: f64, config: SerializerConfig) {
    if v.is_finite() {
        write_f64_json(buf, v, config.digits, config.always_decimal);
    } else if config.na == NaMode::Null {
        buf.extend_from_slice(b"null");
    } else if v == f64::INFINITY {
        buf.extend_from_slice(b"\"Inf\"");
    } else if v == f64::NEG_INFINITY {
        buf.extend_from_slice(b"\"-Inf\"");
    } else if unsafe { is_nan_real(v) } {
        buf.extend_from_slice(b"\"NaN\"");
    } else {
        buf.extend_from_slice(b"\"NA\"");
    }
}

/// A bare coordinate vector, as POINT stores it: `[x, y]` / `[x, y, z]`.
fn write_point_coords(buf: &mut Vec<u8>, cp: &CoordPtr, config: SerializerConfig) {
    let p = cp.ptr as *const f64;
    buf.push(b'[');
    for i in 0..cp.len {
        if i > 0 {
            buf.push(b',');
        }
        write_coord_value(buf, unsafe { *p.add(i) }, config);
    }
    buf.push(b']');
}

/// An nrow x ncol column-major coordinate matrix as an array of rows.
fn write_coord_matrix(buf: &mut Vec<u8>, cp: &CoordPtr, config: SerializerConfig) {
    let ncol = cp.ncol.max(1);
    let nrow = cp.len / ncol;
    let p = cp.ptr as *const f64;
    buf.push(b'[');
    for i in 0..nrow {
        if i > 0 {
            buf.push(b',');
        }
        buf.push(b'[');
        for j in 0..ncol {
            if j > 0 {
                buf.push(b',');
            }
            // Column-major: element (i, j) lives at i + j * nrow.
            write_coord_value(buf, unsafe { *p.add(i + j * nrow) }, config);
        }
        buf.push(b']');
    }
    buf.push(b']');
}

fn write_geometry_parallel(
    out: &mut JsonWriter,
    geom: &FastGeom,
    batch: &GeometryBatch,
    config: SerializerConfig,
) {
    match geom {
        FastGeom::Point(cp) => {
            out.push_bytes(br#"{"type":"Point","coordinates":"#);
            write_point_coords(&mut out.buf, cp, config);
            out.push_u8(b'}');
        }
        FastGeom::Single(cp, typ) => {
            match typ {
                SfcType::MultiPoint => out.push_bytes(br#"{"type":"MultiPoint","coordinates":"#),
                SfcType::LineString => out.push_bytes(br#"{"type":"LineString","coordinates":"#),
                _ => {
                    out.push_bytes(b"null");
                    return;
                }
            }
            write_coord_matrix(&mut out.buf, cp, config);
            out.push_u8(b'}');
        }
        FastGeom::FlatList { start, len, typ } => {
            match typ {
                SfcType::MultiLineString => {
                    out.push_bytes(br#"{"type":"MultiLineString","coordinates":["#)
                }
                SfcType::Polygon => out.push_bytes(br#"{"type":"Polygon","coordinates":["#),
                _ => {
                    out.push_bytes(b"null");
                    return;
                }
            }
            let s = *start as usize;
            let l = *len as usize;
            for (i, cp) in batch.coords[s..s + l].iter().enumerate() {
                if i > 0 {
                    out.push_u8(b',');
                }
                write_coord_matrix(&mut out.buf, cp, config);
            }
            out.push_bytes(br#"]}"#);
        }
        FastGeom::MultiPolygon { coords_start, counts_start, n_polys } => {
            out.push_bytes(br#"{"type":"MultiPolygon","coordinates":["#);
            let mut c_idx = *coords_start as usize;
            let cnt_start = *counts_start as usize;
            let cnt_len = *n_polys as usize;
            for (i, &n_rings) in batch.counts[cnt_start..cnt_start + cnt_len].iter().enumerate() {
                if i > 0 {
                    out.push_u8(b',');
                }
                out.push_u8(b'[');
                for k in 0..n_rings {
                    if k > 0 {
                        out.push_u8(b',');
                    }
                    if c_idx < batch.coords.len() {
                        write_coord_matrix(&mut out.buf, &batch.coords[c_idx], config);
                    } else {
                        out.push_bytes(b"[]");
                    }
                    c_idx += 1;
                }
                out.push_u8(b']');
            }
            out.push_bytes(br#"]}"#);
        }
        FastGeom::Prerendered { start, len } => {
            let s = *start as usize;
            let l = *len as usize;
            if s + l <= batch.raw.len() {
                out.push_bytes(&batch.raw[s..s + l]);
            } else {
                out.push_bytes(b"null");
            }
        }
        FastGeom::Null => out.push_bytes(b"null"),
    }
}

fn process_feature_parallel(out: &mut JsonWriter, row: usize, props: &[(Vec<u8>, ThreadSafeColumn)], geom: &FastGeom, batch: &GeometryBatch, config: SerializerConfig) {
    out.push_bytes(FEAT_HEAD);
    let mut needs_comma = false;
    for (key, col) in props {
        // Rewind to here if the field turns out to be omitted, so the
        // separator never survives a skipped key.
        let mark = out.buf.len();
        if needs_comma { out.push_u8(b','); }
        if try_write_kv(out, row, key, col, config) {
            needs_comma = true;
        } else {
            out.buf.truncate(mark);
        }
    }
    out.push_bytes(FEAT_MID);
    write_geometry_parallel(out, geom, batch, config);
    out.push_u8(b'}');
}

/// jsonlite's dataframe = "values": each row is a bare array of its values,
/// with the row name appended as a final element when one is emitted.
fn process_row_values(out: &mut JsonWriter, row: usize, props: &[(Vec<u8>, ThreadSafeColumn)], config: SerializerConfig) {
    out.push_u8(b'[');
    for (i, (_key, col)) in props.iter().enumerate() {
        if i > 0 { out.push_u8(b','); }
        write_col_value(out, row, col, config);
    }
    out.push_u8(b']');
}

fn process_row_generic(out: &mut JsonWriter, row: usize, props: &[(Vec<u8>, ThreadSafeColumn)], config: SerializerConfig) {
    out.push_u8(b'{');
    let mut needs_comma = false;
    for (key, col) in props {
        // Rewind to here if the field turns out to be omitted, so the
        // separator never survives a skipped key.
        let mark = out.buf.len();
        if needs_comma { out.push_u8(b','); }
        if try_write_kv(out, row, key, col, config) {
            needs_comma = true;
        } else {
            out.buf.truncate(mark);
        }
    }
    out.push_u8(b'}');
}

// ------------------------------------------------------------------
// EXPORTS
// ------------------------------------------------------------------

#[extendr]
fn sf_geojson_str_impl(x: Robj, auto_unbox: bool, na: Robj, null: Robj, factor: Robj, digits: Robj, envelope: Robj, always_decimal: Robj, matrix_colmajor: Robj) -> Result<Robj> {
    str_state_reset();
    let rr = catch_unwind(AssertUnwindSafe(|| sf_geojson_str_impl_inner(x, auto_unbox, na, null, factor, digits, envelope, always_decimal, matrix_colmajor)));
    match rr { Ok(r) => r, Err(p) => rerr(format!("Internal panic: {}", panic_message(p))), }
}

fn sf_geojson_str_impl_inner(x: Robj, auto_unbox: bool, na: Robj, null: Robj, factor: Robj, digits: Robj, envelope: Robj, always_decimal: Robj, matrix_colmajor: Robj) -> Result<Robj> {
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

    let props = build_thread_safe_cols(&df, &colnames, geom_idx, n_rows, config)?;
    
    let num_chunks = (n_rows + PAR_CHUNK_ROWS - 1) / PAR_CHUNK_ROWS;
    let ranges: Vec<(usize, usize, usize)> = (0..num_chunks).map(|id| (id, id * PAR_CHUNK_ROWS, (id * PAR_CHUNK_ROWS + PAR_CHUNK_ROWS).min(n_rows))).collect();
    let mut chunk_geoms = Vec::with_capacity(num_chunks);
    for (id, start, end) in &ranges {
        let (batch, geoms) = extract_geometries_chunk(geom_col, sfc_type, *start, *end, config);
        chunk_geoms.push((*id, *start, *end, batch, geoms));
    }

    let parts_res: Vec<PResult<(usize, Vec<u8>)>> = with_pool(|| chunk_geoms.into_par_iter().map(|(chunk_id, start, end, batch, geoms)| {
        let rr = catch_unwind(AssertUnwindSafe(|| {
            let mut w = JsonWriter::with_capacity((end - start) * 2048);
            for (local_i, row_i) in (start..end).enumerate() {
                if local_i > 0 { w.push_u8(b','); }
                process_feature_parallel(&mut w, row_i, &props, &geoms[local_i], &batch, config);
            }
            (chunk_id, w.buf)
        }));
        match rr { Ok(v) => Ok(v), Err(p) => Err(format!("Worker panic: {}", panic_message(p))), }
    }).collect());

    let mut parts: Vec<(usize, Vec<u8>)> = Vec::with_capacity(parts_res.len());
    for r in parts_res { match r { Ok(v) => parts.push(v), Err(msg) => return rerr(msg), } }
    parts.sort_by_key(|(id, _)| *id);

    let total_bytes: usize = parts.iter().map(|(_, v)| v.len()).sum();
    let mut final_out = Vec::with_capacity(total_bytes + n_rows + 64);
    if wrap_fc { final_out.extend_from_slice(FC_HEAD); } else { final_out.push(b'['); }
    let mut first = true;
    for (_, chunk) in parts {
        if chunk.is_empty() { continue; }
        if !first { final_out.push(b','); } first = false;
        final_out.extend_from_slice(&chunk);
    }
    if wrap_fc { final_out.extend_from_slice(FC_TAIL); } else { final_out.push(b']'); }
    if final_out.len() > i32::MAX as usize { return rerr(format!("Size {} exceeds 2GB limit", final_out.len())); }
    
    let result_str = match finish_json_string(final_out) {
        Ok(s) => s,
        Err(e) => return rerr(e),
    };
    let mut robj = Robj::from(result_str);
    robj.set_class(&["geojson", "json"])?;
    Ok(robj)
}

#[extendr]
fn df_json_str_impl(x: Robj, auto_unbox: bool, dataframe: String, na: Robj, null: Robj, factor: Robj, digits: Robj, always_decimal: Robj, matrix_colmajor: Robj) -> Result<Robj> {
    str_state_reset();
    let rr = catch_unwind(AssertUnwindSafe(|| df_json_str_impl_inner(x, auto_unbox, dataframe, na, null, factor, digits, always_decimal, matrix_colmajor)));
    match rr { Ok(r) => r, Err(p) => rerr(format!("Internal panic: {}", panic_message(p))), }
}

fn df_json_str_impl_inner(x: Robj, auto_unbox: bool, dataframe: String, na: Robj, null: Robj, factor: Robj, digits: Robj, always_decimal: Robj, matrix_colmajor: Robj) -> Result<Robj> {
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

    let props = build_thread_safe_cols(&df_list, &colnames, usize::MAX, n_rows, config)?;

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

        let est_size = n_rows * colnames.len() * 8;
        let mut out = Vec::with_capacity(est_size);
        out.push(b'{');
        for (i, part) in column_parts.into_iter().enumerate() {
            if i > 0 { out.push(b','); }
            match part { Ok(p) => out.extend_from_slice(&p), Err(e) => return rerr(e), }
        }
        out.push(b'}');
        out
    } else {
        if n_rows == 0 { let mut r = Robj::from("[]"); r.set_class(&["json"])?; return Ok(r); }
        let chunk_size = if n_rows < 10000 { n_rows } else { PAR_CHUNK_ROWS };
        let num_chunks = (n_rows + chunk_size - 1) / chunk_size;
        let ranges: Vec<(usize, usize, usize)> = (0..num_chunks).map(|id| (id, id * chunk_size, (id * chunk_size + chunk_size).min(n_rows))).collect();

        let parts_res: Vec<PResult<(usize, Vec<u8>)>> = with_pool(|| ranges.into_par_iter().map(|(chunk_id, start, end)| {
            let rr = catch_unwind(AssertUnwindSafe(|| {
                let mut w = JsonWriter::with_capacity((end - start) * 128);
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

        let mut parts: Vec<(usize, Vec<u8>)> = Vec::with_capacity(parts_res.len());
        for r in parts_res { match r { Ok(v) => parts.push(v), Err(msg) => return rerr(msg), } }
        parts.sort_by_key(|(id, _)| *id);

        let total_bytes: usize = parts.iter().map(|(_, v)| v.len()).sum();
        let mut out = Vec::with_capacity(total_bytes + n_rows + 2);
        out.push(b'[');
        let mut first = true;
        for (_, chunk) in parts {
            if chunk.is_empty() { continue; }
            if !first { out.push(b','); } first = false;
            out.extend_from_slice(&chunk);
        }
        out.push(b']');
        out
    };

    if final_out.len() > i32::MAX as usize { return rerr(format!("Size {} exceeds 2GB limit", final_out.len())); }
    
    let result_str = match finish_json_string(final_out) {
        Ok(s) => s,
        Err(e) => return rerr(e),
    };
    let mut robj = Robj::from(result_str);
    robj.set_class(&["json"])?;
    Ok(robj)
}

#[extendr]
fn obj_json_str_impl(x: Robj, auto_unbox: bool, na: Robj, null: Robj, factor: Robj, digits: Robj, always_decimal: Robj, matrix_colmajor: Robj) -> Result<Robj> {
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
fn scan_inlineable(src: &[u8]) -> Vec<bool> {
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

fn pretty_json(src: &[u8], indent_width: usize) -> Vec<u8> {
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
fn pretty_json_impl(x: Robj, indent: Robj) -> Result<Robj> {
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

// Getter/setter for the worker count. `n = NULL` just reports the current
// effective value; `n <= 0` restores environment-driven auto-detection.
// Deliberately a plain comment: rextendr copies `///` docs into
// R/extendr-wrappers.R as roxygen, which would generate an .Rd for an
// unexported internal and trip R CMD check.
#[extendr]
fn threads_impl(n: Robj) -> Result<Robj> {
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
}