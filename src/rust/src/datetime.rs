// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// DATE FORMATTING
// ------------------------------------------------------------------

/// One `Date` element, resolved before any byte is written.
///
/// `Date` reaches JSON as a *string* in jsonlite, because its asJSON method
/// runs `format()` first. That is what fixes the NA/NaN split below: NA
/// becomes NA_character_ and so follows the `na` argument, while NaN and the
/// infinities survive `format()` as the literal text "NaN", "Inf", "-Inf" and
/// are therefore ordinary strings whatever `na` says.
pub(crate) enum DateCell {
    Na,
    Nan,
    Inf,
    NegInf,
    Ymd(i64, u32, u32),
}

/// Days since 1970-01-01 to a proleptic Gregorian year/month/day.
///
/// Hinnant's `civil_from_days`, which is branch-free apart from the era split
/// and exact over the whole i64 range we admit. R gets here through
/// `as.POSIXlt` and `strftime`, at ~4.7 us per value; this is ~5 ns.
#[inline]
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (y + if m <= 2 { 1 } else { 0 }, m, d)
}

/// R's own limit, reproduced exactly: `strftime` fills a `struct tm` whose
/// `tm_year` is the year less 1900, so the year must fit an `int`. Outside
/// that, `format()` yields NA rather than a date, and so must this.
pub(crate) const DATE_YEAR_MAX: i64 = 2_147_483_647;
pub(crate) const DATE_YEAR_MIN: i64 = -2_147_481_747;

#[inline]
pub(crate) fn date_cell(v: f64) -> DateCell {
    if v.is_nan() {
        // NA_real_ is a NaN; the payload is what separates the two, and they
        // are formatted differently.
        return if unsafe { is_na_real(v) } {
            DateCell::Na
        } else {
            DateCell::Nan
        };
    }
    if v == f64::INFINITY {
        return DateCell::Inf;
    }
    if v == f64::NEG_INFINITY {
        return DateCell::NegInf;
    }
    // `format()` truncates towards -Inf, so -0.5 is 1969-12-31, not 1970-01-01.
    let d = v.floor();
    // Keeps `d as i64` and the `+ 719468` below far away from overflow. R's
    // real cutoff is |d| < 8e11, checked again on the year.
    if !(-1.0e15..=1.0e15).contains(&d) {
        return DateCell::Na;
    }
    let (y, m, dd) = civil_from_days(d as i64);
    if y > DATE_YEAR_MAX || y < DATE_YEAR_MIN {
        DateCell::Na
    } else {
        DateCell::Ymd(y, m, dd)
    }
}

#[inline]
pub(crate) fn date_cell_i32(v: i32) -> DateCell {
    if unsafe { is_na_int(v) } {
        DateCell::Na
    } else {
        let (y, m, d) = civil_from_days(v as i64);
        DateCell::Ymd(y, m, d)
    }
}

/// Writes the quoted `"YYYY-MM-DD"`, or the NA/NaN/Inf text jsonlite produces.
///
/// `%Y` pads to four characters *including the sign*, so year 1 is `0001` and
/// year -1 is `-001`. Rust's `{:04}` has exactly that rule.
#[inline]
pub(crate) fn write_date_cell(buf: &mut Vec<u8>, cell: DateCell, na: NaMode) {
    match cell {
        DateCell::Na => {
            if na == NaMode::String {
                buf.extend_from_slice(b"\"NA\"");
            } else {
                buf.extend_from_slice(b"null");
            }
        }
        DateCell::Nan => buf.extend_from_slice(b"\"NaN\""),
        DateCell::Inf => buf.extend_from_slice(b"\"Inf\""),
        DateCell::NegInf => buf.extend_from_slice(b"\"-Inf\""),
        DateCell::Ymd(y, m, d) => {
            buf.push(b'"');
            write_year_month_day(buf, y, m, d);
            buf.push(b'"');
        }
    }
}

// ------------------------------------------------------------------
// TIME FORMATTING
// ------------------------------------------------------------------

/// Layout codes for `write_time_cell`, chosen in R because the `format = ""`
/// rule inspects the whole vector before picking one.
pub(crate) const TFMT_DATE: u32 = 0; // %Y-%m-%d          (every element is midnight)
pub(crate) const TFMT_SPACE: u32 = 1; // %Y-%m-%d %H:%M:%S
#[allow(dead_code)] // chosen in R; Rust only needs to know it is not SPACE or TZ
pub(crate) const TFMT_T: u32 = 2; // %Y-%m-%dT%H:%M:%S
pub(crate) const TFMT_TZ: u32 = 3; // %Y-%m-%dT%H:%M:%SZ

/// Writes one timestamp from *local civil* seconds since the epoch.
///
/// R has already added the UTC offset, which is the only part that needs a
/// time zone database, so this is pure arithmetic. `format()` costs ~5.3 us
/// per value; `as.POSIXlt` plus this costs ~0.1 us.
#[inline]
pub(crate) fn write_time_cell(buf: &mut Vec<u8>, v: f64, fmt: u32, na: NaMode) {
    if v.is_nan() {
        if unsafe { is_na_real(v) } {
            if na == NaMode::String {
                buf.extend_from_slice(b"\"NA\"");
            } else {
                buf.extend_from_slice(b"null");
            }
        } else {
            buf.extend_from_slice(b"\"NaN\"");
        }
        return;
    }
    if v == f64::INFINITY {
        buf.extend_from_slice(b"\"Inf\"");
        return;
    }
    if v == f64::NEG_INFINITY {
        buf.extend_from_slice(b"\"-Inf\"");
        return;
    }
    // Truncation is towards -Inf here too, so -0.5 is 23:59:59 the day before.
    let secs = v.floor();
    if !(-3.0e17..=3.0e17).contains(&secs) {
        if na == NaMode::String {
            buf.extend_from_slice(b"\"NA\"");
        } else {
            buf.extend_from_slice(b"null");
        }
        return;
    }
    let secs = secs as i64;
    // Euclidean division, so a negative epoch still lands on the right day
    // with a positive time of day.
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, mo, d) = civil_from_days(days);
    if y > DATE_YEAR_MAX || y < DATE_YEAR_MIN {
        if na == NaMode::String {
            buf.extend_from_slice(b"\"NA\"");
        } else {
            buf.extend_from_slice(b"null");
        }
        return;
    }

    buf.push(b'"');
    write_year_month_day(buf, y, mo, d);
    if fmt != TFMT_DATE {
        buf.push(if fmt == TFMT_SPACE { b' ' } else { b'T' });
        let h = (sod / 3600) as u32;
        let mi = (sod / 60 % 60) as u32;
        let se = (sod % 60) as u32;
        push_2(buf, h);
        buf.push(b':');
        push_2(buf, mi);
        buf.push(b':');
        push_2(buf, se);
        if fmt == TFMT_TZ {
            buf.push(b'Z');
        }
    }
    buf.push(b'"');
}

#[inline(always)]
pub(crate) fn push_2(buf: &mut Vec<u8>, v: u32) {
    buf.push(b'0' + (v / 10) as u8);
    buf.push(b'0' + (v % 10) as u8);
}

/// `YYYY-MM-DD`, unquoted. `%Y` pads to four characters *including the sign*,
/// so year 1 is `0001` and year -1 is `-001`; Rust's `{:04}` has that rule.
#[inline]
pub(crate) fn write_year_month_day(buf: &mut Vec<u8>, y: i64, m: u32, d: u32) {
    if (0..=9999).contains(&y) {
        // The overwhelmingly common case: four fixed digits, no formatting
        // machinery at all.
        let y = y as u32;
        buf.push(b'0' + (y / 1000) as u8);
        buf.push(b'0' + (y / 100 % 10) as u8);
        buf.push(b'0' + (y / 10 % 10) as u8);
        buf.push(b'0' + (y % 10) as u8);
    } else {
        use std::io::Write;
        let mut tmp = [0u8; 24];
        let mut w = &mut tmp[..];
        let _ = write!(w, "{:04}", y);
        let used = 24 - w.len();
        buf.extend_from_slice(&tmp[..used]);
    }
    buf.push(b'-');
    push_2(buf, m);
    buf.push(b'-');
    push_2(buf, d);
}

/// The `fgjfmt` layout code, if this is a prepared time vector.
#[inline]
pub(crate) unsafe fn fgj_fmt_code(x: libR_sys::SEXP) -> Option<u32> {
    let sym = libR_sys::Rf_install(b"fgjfmt\0".as_ptr() as *const c_char);
    let a = libR_sys::Rf_getAttrib(x, sym);
    if a == libR_sys::R_NilValue || sexp_len(a) < 1 {
        return None;
    }
    match typeof_sexp(a) {
        t if t == libR_sys::SEXPTYPE::INTSXP as u32 => {
            let v = *libR_sys::INTEGER(a);
            if is_na_int(v) {
                None
            } else {
                Some(v as u32)
            }
        }
        t if t == libR_sys::SEXPTYPE::REALSXP as u32 => {
            let v = *libR_sys::REAL(a);
            if v.is_nan() {
                None
            } else {
                Some(v as u32)
            }
        }
        _ => None,
    }
}

/// `digits = Inf`: shortest round-trip. Out of the range of a real `digits`,
/// which jsonlite caps at 17 significant digits.
pub(crate) const DIGITS_SHORTEST: u8 = u8::MAX;

/// The shortest decimal that round-trips to exactly `v`.
///
/// Whole numbers still print as integers, matching how every other numeric
/// path in this crate renders them; ryu would write "1.0".
#[inline]
pub(crate) fn write_shortest_f64(buf: &mut Vec<u8>, v: f64) {
    if v.fract() == 0.0 && v.abs() < 9.007_199_254_740_992e15 {
        let mut tmp = itoa::Buffer::new();
        buf.extend_from_slice(tmp.format(v as i64).as_bytes());
        return;
    }
    buf.reserve(24);
    let len = buf.len();
    unsafe {
        let ptr = buf.as_mut_ptr().add(len);
        let written = ryu::raw::format64(v, ptr);
        buf.set_len(len + written);
    }
}

/// "00010203...9899": the two decimal digits of every value below 100, so a
/// number can be consumed in base 100 rather than one digit per division.
pub(crate) const DIGIT_PAIRS: [u8; 200] = *b"00010203040506070809101112131415161718192021222324252627282930313233343536373839404142434445464748495051525354555657585960616263646566676869707172737475767778798081828384858687888990919293949596979899";

pub(crate) const POW10_F: [f64; 10] = [
    1.0, 10.0, 100.0, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
];
pub(crate) const POW10_U: [u32; 10] = [
    1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000,
];

/// `d` digits after the decimal point, trailing zeros removed.
///
/// A direct port of the `modp_dtoa2` that jsonlite calls on this path, so the
/// output is byte-identical to jsonlite *by construction* rather than by
/// coincidence -- including its particular half-way rule, which rounds up only
/// when the last kept digit is odd. Going through `core::fmt` instead was both
/// far slower (this is the hottest formatting path there is: every double in
/// every column and every coordinate) and not guaranteed to agree in the
/// half-way cases.
///
/// The caller guarantees `0 <= d < 10` and `1e-5 < |v| < 2^31`.
#[inline]
pub(crate) fn write_fixed_decimals(buf: &mut Vec<u8>, v: f64, d: usize) {
    let prec = d.min(9);
    let neg = v < 0.0;
    let value = if neg { -v } else { v };

    let mut whole = value as i64;
    let tmp = (value - whole as f64) * POW10_F[prec];
    let mut frac = tmp as u32;
    let diff = tmp - frac as f64;
    let p10 = POW10_U[prec];

    if diff > 0.5 {
        frac += 1;
        if frac >= p10 {
            frac = 0;
            whole += 1;
        }
    } else if diff == 0.5
        && ((prec > 0 && frac & 1 == 1) || (prec == 0 && whole & 1 == 1))
    {
        frac += 1;
        if frac >= p10 {
            frac = 0;
            whole += 1;
        }
    }

    // Trailing fractional zeros are dropped, so 100 at prec 4 is "100".
    let mut count = prec;
    if prec > 0 {
        while count > 0 && frac % 10 == 0 {
            count -= 1;
            frac /= 10;
        }
    }

    // Digits come out least-significant first, so build reversed then flip.
    //
    // Two digits at a time through DIGIT_PAIRS, which halves the divisions.
    // This is the hottest function in the package -- every double in every
    // column and every ordinate of every geometry -- and at four decimals it
    // was doing eight or nine divisions per value.
    let mut rev = [0u8; 32];
    let mut k = 0usize;
    let has_dec = count > 0;
    while count >= 2 {
        let r = (frac % 100) as usize * 2;
        frac /= 100;
        rev[k] = DIGIT_PAIRS[r + 1];
        rev[k + 1] = DIGIT_PAIRS[r];
        k += 2;
        count -= 2;
    }
    if count == 1 {
        rev[k] = b'0' + (frac % 10) as u8;
        frac /= 10;
        k += 1;
    }
    if frac > 0 {
        whole += 1;
    }
    if has_dec {
        rev[k] = b'.';
        k += 1;
    }
    while whole >= 100 {
        let r = (whole % 100) as usize * 2;
        whole /= 100;
        rev[k] = DIGIT_PAIRS[r + 1];
        rev[k + 1] = DIGIT_PAIRS[r];
        k += 2;
    }
    if whole >= 10 {
        let r = whole as usize * 2;
        rev[k] = DIGIT_PAIRS[r + 1];
        rev[k + 1] = DIGIT_PAIRS[r];
        k += 2;
    } else {
        // Also the `whole == 0` case, which must still write a leading zero.
        rev[k] = b'0' + whole as u8;
        k += 1;
    }
    if neg {
        rev[k] = b'-';
        k += 1;
    }

    // resize() zero-fills and then every byte is overwritten, which is
    // twice the stores on the hottest formatting path in the package.
    // Reserve and write through the spare capacity instead.
    buf.reserve(k);
    unsafe {
        let dst = buf.as_mut_ptr().add(buf.len());
        for i in 0..k {
            *dst.add(i) = rev[k - 1 - i];
        }
        buf.set_len(buf.len() + k);
    }
}

impl JsonWriter {
    #[inline]
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self { buf: Vec::with_capacity(cap) }
    }
    #[inline(always)]
    pub(crate) fn push_u8(&mut self, b: u8) {
        self.buf.push(b);
    }
    #[inline(always)]
    pub(crate) fn push_bytes(&mut self, s: &[u8]) {
        self.buf.extend_from_slice(s);
    }
    #[inline(always)]
    pub(crate) fn push_i32(&mut self, v: i32) {
        let mut tmp = itoa::Buffer::new();
        self.push_bytes(tmp.format(v).as_bytes());
    }
    #[inline(always)]
    pub(crate) fn push_f64_cfg(&mut self, v: f64, config: SerializerConfig) {
        write_f64_json(&mut self.buf, v, config.digits, config.always_decimal);
    }
    #[inline(always)]
    pub(crate) fn push_bool(&mut self, v: bool) {
        if v { self.push_bytes(b"true"); } else { self.push_bytes(b"false"); }
    }
}

/// Index of the first byte that needs escaping, or `None` if the run is clean.
///
/// Unrolled eight at a time. `chunks_exact` gives fixed-length slices, so the
/// per-byte bounds checks disappear and the loop overhead is amortised. This
/// is yyjson's approach: probe a table over a block, and if the whole block is
/// clean, bulk-copy it rather than moving a byte at a time.
#[inline]
pub(crate) fn find_escape(bytes: &[u8]) -> Option<usize> {
    let mut base = 0usize;
    let mut it = bytes.chunks_exact(8);
    for c in it.by_ref() {
        if ESCAPE_LUT[c[0] as usize] != ESC_NONE { return Some(base); }
        if ESCAPE_LUT[c[1] as usize] != ESC_NONE { return Some(base + 1); }
        if ESCAPE_LUT[c[2] as usize] != ESC_NONE { return Some(base + 2); }
        if ESCAPE_LUT[c[3] as usize] != ESC_NONE { return Some(base + 3); }
        if ESCAPE_LUT[c[4] as usize] != ESC_NONE { return Some(base + 4); }
        if ESCAPE_LUT[c[5] as usize] != ESC_NONE { return Some(base + 5); }
        if ESCAPE_LUT[c[6] as usize] != ESC_NONE { return Some(base + 6); }
        if ESCAPE_LUT[c[7] as usize] != ESC_NONE { return Some(base + 7); }
        base += 8;
    }
    for (k, &b) in it.remainder().iter().enumerate() {
        if ESCAPE_LUT[b as usize] != ESC_NONE {
            return Some(base + k);
        }
    }
    None
}

#[inline]
pub(crate) fn escape_json_string_into(out: &mut Vec<u8>, bytes: &[u8]) {
    // Overwhelmingly the common case: nothing to escape, so reserve once and
    // emit the whole string with a single copy.
    let first = match find_escape(bytes) {
        None => {
            out.reserve(bytes.len() + 2);
            out.push(b'"');
            out.extend_from_slice(bytes);
            out.push(b'"');
            return;
        }
        Some(i) => i,
    };

    // Slow path. Reserve for the clean part plus a little slack; the loop
    // grows the buffer if a run of escapes needs more.
    out.reserve(bytes.len() + 16);
    out.push(b'"');
    let len = bytes.len();
    let mut start = 0usize;
    let mut next = first;
    loop {
        if next > start {
            out.extend_from_slice(&bytes[start..next]);
        }
        let b = bytes[next];
        let mut advance = 1usize;
        match ESCAPE_LUT[b as usize] {
            ESC_QUOTE => out.extend_from_slice(br#"\""#),
            ESC_BACKSLASH => out.extend_from_slice(br#"\\"#),
            ESC_B => out.extend_from_slice(br#"\b"#),
            ESC_T => out.extend_from_slice(br#"\t"#),
            ESC_N => out.extend_from_slice(br#"\n"#),
            ESC_F => out.extend_from_slice(br#"\f"#),
            ESC_R => out.extend_from_slice(br#"\r"#),
            ESC_LT => {
                // jsonlite escapes the solidus only when it follows '<', so a
                // payload containing "</script>" cannot terminate an enclosing
                // <script> block. See jsonlite's src/escape_chars.c. Emitting
                // both bytes here keeps '/' off the scan table entirely.
                out.push(b'<');
                if next + 1 < len && bytes[next + 1] == b'/' {
                    out.extend_from_slice(br#"\/"#);
                    advance = 2;
                }
            }
            ESC_UNICODE => {
                out.extend_from_slice(br#"\u00"#);
                out.push(HEX_DIGITS[(b >> 4) as usize]);
                out.push(HEX_DIGITS[(b & 0x0F) as usize]);
            }
            _ => out.push(b),
        }
        start = next + advance;
        if start >= len {
            break;
        }
        match find_escape(&bytes[start..]) {
            Some(i) => next = start + i,
            None => {
                out.extend_from_slice(&bytes[start..]);
                break;
            }
        }
    }
    out.push(b'"');
}

#[inline]
pub(crate) fn build_escaped_key_bytes(name: &[u8]) -> Vec<u8> {
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
pub(crate) unsafe fn utf8_names(x: libR_sys::SEXP, n: usize) -> Option<Vec<Vec<u8>>> {
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

