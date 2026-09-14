// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// CONFIGURATION & CONSTANTS
// ------------------------------------------------------------------
/// Guards `serialize_sexp_to_json_buffer`'s recursion.
///
/// Measured on this platform: depth 50,000 serialises fine, depth 200,000
/// terminates the R process outright - no R error, no catchable condition,
/// just a dead session. Refuse an order of magnitude below the known-good
/// depth and report an ordinary R error instead. (R's own node-stack limit
/// trips around 2-3k for the R-level helpers, so nothing that works today
/// gets rejected by this.)
pub(crate) const MAX_DEPTH: u32 = 5_000;

// Escape actions, indexed by byte. Chosen to match jsonlite byte-for-byte:
// short escapes for \b \t \n \f \r, lowercase \u00xx for the remaining
// control bytes, and NO escaping of '/' or DEL (0x7F).
pub(crate) const ESC_NONE: u8 = 0;
pub(crate) const ESC_QUOTE: u8 = 1;
pub(crate) const ESC_BACKSLASH: u8 = 2;
pub(crate) const ESC_UNICODE: u8 = 3;
pub(crate) const ESC_B: u8 = 4;
pub(crate) const ESC_T: u8 = 5;
pub(crate) const ESC_N: u8 = 6;
pub(crate) const ESC_F: u8 = 7;
pub(crate) const ESC_R: u8 = 8;
pub(crate) const ESC_LT: u8 = 9;

pub(crate) static ESCAPE_LUT: [u8; 256] = {
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
    // The solidus is escaped only when it FOLLOWS '<', so flagging every '/'
    // would push all URL-bearing data onto the slow path. Flag the much rarer
    // '<' and look forward one byte instead; see ESC_LT below.
    table[b'<' as usize] = ESC_LT;
    table
};

// jsonlite emits lowercase hex in \u escapes.
pub(crate) const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

pub(crate) const FC_HEAD: &[u8] = br#"{"type":"FeatureCollection","name":"sfdata","features":["#;
pub(crate) const FC_TAIL: &[u8] = br#"]}"#;
pub(crate) const EMPTY_FC: &str = r#"{"type":"FeatureCollection","name":"sfdata","features":[]}"#;
pub(crate) const FEAT_HEAD: &[u8] = br#"{"type":"Feature","properties":{"#;
pub(crate) const FEAT_MID: &[u8] = br#"},"geometry":"#;

#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum DfMode { Rows, Columns, Values }

// 3-State Logic
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum NaMode { 
    Null,   
    String, 
    Smart   
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum NullMode { List, Null }

#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum FactorMode { String, Integer }

#[derive(Clone, Copy, Debug)]
pub(crate) struct SerializerConfig {
    pub(crate) df: DfMode,
    pub(crate) na: NaMode,
    pub(crate) null: NullMode,
    pub(crate) factor: FactorMode,
    pub(crate) auto_unbox: bool,
    pub(crate) digits: Option<u8>,
    /// jsonlite's `matrix = "columnmajor"`: nest a matrix/array by its last
    /// dimension first rather than its first.
    pub(crate) matrix_colmajor: bool,
    /// jsonlite's `always_decimal`: render whole doubles as `100.0` rather
    /// than `100`, so a numeric column never looks like an integer column.
    pub(crate) always_decimal: bool,
    /// jsonlite's `json_verbatim`. When false -- the default -- a value
    /// carrying the `json` class is an ordinary character string and is
    /// escaped like one; only when true is its text spliced into the output.
    ///
    /// This used to be honoured in R, and so only for the outermost object: a
    /// `json` value nested anywhere was spliced whatever the setting, which
    /// lets a string decide the shape of the document around it.
    pub(crate) json_verbatim: bool,
    /// `digits` counts SIGNIFICANT digits rather than decimal places, which is
    /// what `digits = I(n)` asks for. Renders through `%.*g`.
    pub(crate) signif: bool,
    /// jsonlite's `rownames`, which has three states rather than two:
    /// `ROWNAMES_NEVER` when it was given as FALSE, `ROWNAMES_REAL` when it was
    /// not given at all (emit `_row` only for row names that are really there),
    /// and `ROWNAMES_ALWAYS` when it was given as TRUE, which emits `_row` even
    /// for the automatic 1..n -- as unquoted integers, since that is what
    /// toJSON() writes for them.
    ///
    /// It used to be applied in R, which meant it reached only the outermost
    /// frame: `as_json(list(d = df), rownames = FALSE)` still emitted `_row`.
    pub(crate) rownames: u8,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ColumnType {
    Int,
    /// Character column escaped straight from R's own CHARSXP bytes by the
    /// workers, with no intermediate arena. Only used when every element is
    /// pure ASCII, which is valid UTF-8 whatever the locale, so no
    /// translation (and therefore no R allocation) can be needed.
    /// `string_arena.offsets` holds (ptr, len); `bytes` is empty.
    CharDirect,
    Real,
    Bool,
    Char,
    Factor,
    /// A `Date` column, formatted to `"YYYY-MM-DD"` in the worker straight from
    /// the day numbers. R's `format()` costs ~4.7 us per value, which made
    /// Date the slowest type in the package by two orders of magnitude.
    DateReal,
    /// The same, for a `Date` built over an integer vector.
    DateInt,
    /// Local civil seconds since the epoch, formatted in the worker. `aux`
    /// holds the layout code; see `write_time_cell`.
    TimeLocal,
    /// A numeric matrix column, written in the worker straight from R's
    /// column-major storage. `len` is the row count and `aux` the column
    /// count, so cell (row, c) sits at `data_ptr[c * len + row]`.
    ///
    /// These used to be rendered into an arena on the R thread, one cell at a
    /// time, which cost about 30 ns per cell against 4 for a plain column and
    /// scaled 1.2x against 6x.
    MatrixReal,
    /// The same for an integer matrix.
    MatrixInt,
    MatrixBool,
    /// An array of three or more dimensions, read in the worker. `aux` says
    /// which of the three pointer-readable types it is; `arr_shape` carries
    /// the nesting.
    ArrayDirect,
    /// A whole column rendered once rather than cell by cell: its arena holds
    /// exactly one entry and the column writer emits it in place of the usual
    /// `[cell, cell, ...]`. Used for a data.frame-valued column under
    /// `dataframe = "columns"`, which toJSON() renders as one column-oriented
    /// object rather than one object per row.
    JsonWhole,
    JsonRaw,
    Null,
}

#[derive(PartialEq, Clone, Copy, Debug)]
pub(crate) enum SfcType {
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    GeometryCollection,
    Unknown,
}

pub(crate) type PResult<T> = std::result::Result<T, String>;

// ------------------------------------------------------------------
// STRUCT DEFINITIONS
// ------------------------------------------------------------------

pub(crate) struct StringArena {
    pub(crate) bytes: Vec<u8>,
    pub(crate) offsets: Vec<(usize, usize)>,
}
unsafe impl Send for StringArena {}
unsafe impl Sync for StringArena {}

/// Descriptor for one cell of a `CharDirect` column, as produced by the
/// parallel prepass. Checked in this order:
///
///   `CD_NA`               the cell is NA
///   `CD_ARENA` set        low bits index `string_arena.offsets`, whose bytes
///                         are already escaped and quoted
///   `CD_LATIN1` set       read R's bytes directly and widen them to UTF-8
///   `CD_ESC` set          read R's bytes directly, but they need escaping
///   otherwise             read R's bytes directly and copy them verbatim
///
/// `CD_NA` sets every bit, so it has to be tested before the flags.
pub(crate) const CD_NA: u32 = u32::MAX;
/// Cell needs Rf_translateCharUTF8, which only the R thread may call. A
/// prepass-internal value: it never survives into the finished descriptors.
pub(crate) const CD_XLATE: u32 = u32::MAX - 1;
/// Cell contains at least one byte needing a JSON escape.
pub(crate) const CD_ESC: u32 = 0x8000_0000;
/// Cell was pre-rendered into the column's arena by the R thread.
pub(crate) const CD_ARENA: u32 = 0x4000_0000;
/// Cell is latin1 and the worker widens it to UTF-8 itself.
///
/// latin1 is ISO-8859-1, whose 256 code points ARE the first 256 of Unicode,
/// so the conversion is two lines and needs no table and no locale: a byte
/// below 0x80 stands for itself, and one above becomes `0xC0 | b >> 6`,
/// `0x80 | b & 0x3F`. Doing it here rather than through
/// `Rf_translateCharUTF8` is what lets a latin1 column stay in the workers;
/// R's route is an iconv call per cell on the R thread, and measured 814 ms
/// against 7.5 for the same 300,000 values held as UTF-8.
pub(crate) const CD_LATIN1: u32 = 0x2000_0000;
/// Payload mask: a byte length, or an arena index.
///
/// Twenty-nine bits since `CD_LATIN1` took one, so a single cell is capped at
/// 512 MB and a column at half a billion arena entries.
pub(crate) const CD_LEN: u32 = 0x1FFF_FFFF;

/// `aux` codes for an `ArrayDirect` column.
pub(crate) const ARR_REAL: u32 = 0;
pub(crate) const ARR_INT: u32 = 1;
pub(crate) const ARR_LGL: u32 = 2;

pub(crate) struct ThreadSafeColumn {
    pub(crate) kind: ColumnType,
    /// Per-kind scalar payload. Only `TimeLocal` uses it, to carry the layout
    /// code; every other kind leaves it zero.
    pub(crate) aux: u32,
    pub(crate) data_ptr: usize,
    /// Number of elements actually behind `data_ptr`. A data.frame built with
    /// `structure()` can declare more rows than a column holds; without this
    /// the row loop read past the end and serialised adjacent heap bytes.
    pub(crate) len: usize,
    pub(crate) cached_levels: Option<Vec<Vec<u8>>>,
    pub(crate) string_arena: Option<StringArena>,
    /// One descriptor per cell for `CharDirect`, produced by the same
    /// parallel prepass that decides the column can be read directly. See
    /// `CD_NA` for the encoding.
    pub(crate) char_meta: Option<Vec<u32>>,
    /// Shape of an `ArrayDirect` column: the dimensions after the row one,
    /// then their strides, in one allocation. Every other kind leaves it None,
    /// which costs eight bytes in the descriptor and nothing per cell.
    pub(crate) arr_shape: Option<Box<[usize]>>,
}
unsafe impl Send for ThreadSafeColumn {}
unsafe impl Sync for ThreadSafeColumn {}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CoordPtr {
    pub(crate) ptr: usize,
    pub(crate) len: usize,
    /// Number of coordinate columns, taken from the sfg matrix's `dim`.
    ///
    /// sf stores a ring/line as an nrow x ncol column-major matrix where ncol
    /// is 2 (XY), 3 (XYZ or XYM) or 4 (XYZM). Assuming 2 here is what corrupted
    /// every XYZ/XYM/XYZM geometry: a 3x3 matrix was read as 4 points of
    /// garbage. The XY/XYZ/XYM/XYZM class token itself is irrelevant -- every
    /// ordinate present is emitted verbatim, exactly as jsonlite does.
    pub(crate) ncol: usize,
}
unsafe impl Send for CoordPtr {}
unsafe impl Sync for CoordPtr {}

pub(crate) struct GeometryBatch {
    pub(crate) coords: Vec<CoordPtr>,
    pub(crate) counts: Vec<usize>,
    /// Geometries rendered to bytes up front on the R thread, for shapes that
    /// need recursive traversal of R objects (GEOMETRYCOLLECTION) and so
    /// cannot be walked from a worker.
    pub(crate) raw: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(crate) enum FastGeom {
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
pub(crate) fn rerr<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Other(msg.into()))
}

pub(crate) fn panic_message(p: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic (unknown payload)".to_string()
    }
}

pub(crate) fn parse_r_string_arg(x: Robj, default: &str) -> String {
    if x.is_null() { return default.to_string(); }
    if let Some(s) = x.as_str() { return s.to_string(); }
    if let Some(v) = x.as_str_vector() {
        if !v.is_empty() { return v[0].to_string(); }
    }
    default.to_string()
}

/// True when `digits` carries R's `AsIs` class, which is how `I(n)` asks for
/// significant rather than decimal digits. jsonlite decides the same way:
/// `use_signif = is(digits, "AsIs")`.
/// `rownames` states; see `SerializerConfig::rownames`.
pub(crate) const ROWNAMES_NEVER: u8 = 0;
pub(crate) const ROWNAMES_REAL: u8 = 1;
pub(crate) const ROWNAMES_ALWAYS: u8 = 2;

pub(crate) fn parse_signif_arg(x: &Robj) -> bool {
    x.inherits("AsIs")
}

pub(crate) fn parse_digits_arg(x: Robj) -> Option<u8> {
    if x.is_null() { return None; }
    if let Some(i) = x.as_integer() {
        if i == DIGITS_SHORTEST as i32 {
            return Some(DIGITS_SHORTEST);
        }
        let d = i.max(0).min(17) as u8;
        return Some(d);
    }
    if let Some(f) = x.as_real() {
        // Accepted here as well as in R, so calling the entry points directly
        // with digits = Inf behaves the same as as_json(digits = Inf).
        if f.is_infinite() && f > 0.0 {
            return Some(DIGITS_SHORTEST);
        }
        if f.is_finite() {
            let i = f.round() as i32;
            if i == DIGITS_SHORTEST as i32 {
                return Some(DIGITS_SHORTEST);
            }
            let d = i.max(0).min(17) as u8;
            return Some(d);
        }
    }
    None
}

