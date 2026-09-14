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
    // Keeps the cast and the `+ 719468` below far away from overflow. R's
    // real cutoff is |d| < 8e11, checked again on the year.
    if !(-1.0e15..=1.0e15).contains(&v) {
        return DateCell::Na;
    }
    // `format()` truncates towards -Inf, so -0.5 is 1969-12-31, not
    // 1970-01-01. `f64::floor` is a CRT call on the SSE2 baseline (roundsd
    // needs SSE4.1), and it was the single largest item in a Date cell. Inside
    // +-1e15 < 2^52 the integer cast truncates toward zero exactly, and a
    // negative value with a fractional part needs one more step down.
    let t = v as i64;
    let d = t - ((t as f64 > v) as i64);
    let (y, m, dd) = civil_from_days(d);
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
            if (0..=9999).contains(&y) {
                // The overwhelmingly common case, as one store: the twelve
                // bytes of "YYYY-MM-DD" with its quotes are built in a stack
                // array from the digit-pair table and copied with a single
                // 16-byte write into reserved space. It was twelve pushes,
                // each with its own capacity check.
                let y = y as usize;
                let (m, d) = (m as usize, d as usize);
                let mut t = [b'"'; 16];
                t[1] = DIGIT_PAIRS[(y / 100) * 2];
                t[2] = DIGIT_PAIRS[(y / 100) * 2 + 1];
                t[3] = DIGIT_PAIRS[(y % 100) * 2];
                t[4] = DIGIT_PAIRS[(y % 100) * 2 + 1];
                t[5] = b'-';
                t[6] = DIGIT_PAIRS[m * 2];
                t[7] = DIGIT_PAIRS[m * 2 + 1];
                t[8] = b'-';
                t[9] = DIGIT_PAIRS[d * 2];
                t[10] = DIGIT_PAIRS[d * 2 + 1];
                // t[11] is the closing quote from the fill; 12..16 are padding
                // that is written into the reserved tail and not counted.
                buf.reserve(16);
                unsafe {
                    let dst = buf.as_mut_ptr().add(buf.len());
                    std::ptr::copy_nonoverlapping(t.as_ptr(), dst, 16);
                    buf.set_len(buf.len() + 12);
                }
            } else {
                buf.push(b'"');
                write_year_month_day(buf, y, m, d);
                buf.push(b'"');
            }
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

/// Is this timestamp cell missing, by the same rules `write_time_cell`
/// applies?
///
/// A true NA, or an instant `format()` cannot render -- outside the writer's
/// +-3e17 s range, or a year past what `strftime`'s `int tm_year` holds. R
/// returns NA for those, and jsonlite then treats the cell as missing: the key
/// is dropped in row mode. The Date column already answers this through
/// `date_cell`; the timestamp column only tested for NA_real_, so an absurd
/// instant came out as `"t":null` where `toJSON()` omitted it.
///
/// Anything within 6e16 s of the epoch is a year inside +-1.9e9 and cannot be
/// out of range, so ordinary values pay one comparison.
#[inline]
pub(crate) fn time_cell_is_missing(v: f64) -> bool {
    if v.is_nan() {
        return unsafe { is_na_real(v) };
    }
    if v.abs() <= 6.0e16 {
        return false;
    }
    if !(-3.0e17..=3.0e17).contains(&v) {
        return true;
    }
    let t = v as i64;
    let secs = t - ((t as f64 > v) as i64);
    let (y, _, _) = civil_from_days(secs.div_euclid(86_400));
    y > DATE_YEAR_MAX || y < DATE_YEAR_MIN
}

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
    // Range-checked on `v` before any cast so the arithmetic below cannot
    // overflow. Checking `v` rather than its floor admits nothing new: above
    // 2^52 every double is an integer, so the two agree there, and below it
    // the floor is at most one less, which the year check rejects anyway.
    if !(-3.0e17..=3.0e17).contains(&v) {
        if na == NaMode::String {
            buf.extend_from_slice(b"\"NA\"");
        } else {
            buf.extend_from_slice(b"null");
        }
        return;
    }
    // Truncation is towards -Inf here too, so -0.5 is 23:59:59 the day
    // before. `f64::floor` is a CRT call on the SSE2 baseline; the cast
    // truncates toward zero exactly (inside 2^52 by construction, and above
    // it there is no fraction to lose), and a negative value with a fraction
    // needs one step down.
    let t = v as i64;
    let secs = t - ((t as f64 > v) as i64);
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

    if (0..=9999).contains(&y) {
        // The common case as one store: up to 22 bytes -- quote, date,
        // separator, time, optional Z, quote -- built in a 24-byte stack
        // array from the digit-pair table and copied in one go. It was up to
        // twenty-two pushes, each with its own capacity check.
        let y = y as usize;
        let (mo, d) = (mo as usize, d as usize);
        let mut t = [b'"'; 24];
        t[1] = DIGIT_PAIRS[(y / 100) * 2];
        t[2] = DIGIT_PAIRS[(y / 100) * 2 + 1];
        t[3] = DIGIT_PAIRS[(y % 100) * 2];
        t[4] = DIGIT_PAIRS[(y % 100) * 2 + 1];
        t[5] = b'-';
        t[6] = DIGIT_PAIRS[mo * 2];
        t[7] = DIGIT_PAIRS[mo * 2 + 1];
        t[8] = b'-';
        t[9] = DIGIT_PAIRS[d * 2];
        t[10] = DIGIT_PAIRS[d * 2 + 1];
        let mut n = 11usize; // index of the closing quote for date-only
        if fmt != TFMT_DATE {
            t[11] = if fmt == TFMT_SPACE { b' ' } else { b'T' };
            let h = (sod / 3600) as usize;
            let mi = (sod / 60 % 60) as usize;
            let se = (sod % 60) as usize;
            t[12] = DIGIT_PAIRS[h * 2];
            t[13] = DIGIT_PAIRS[h * 2 + 1];
            t[14] = b':';
            t[15] = DIGIT_PAIRS[mi * 2];
            t[16] = DIGIT_PAIRS[mi * 2 + 1];
            t[17] = b':';
            t[18] = DIGIT_PAIRS[se * 2];
            t[19] = DIGIT_PAIRS[se * 2 + 1];
            n = 20;
            if fmt == TFMT_TZ {
                t[20] = b'Z';
                n = 21;
            }
        }
        // t[n] is the closing quote from the fill; past it is padding written
        // into the reserved tail and not counted.
        t[n] = b'"';
        buf.reserve(24);
        unsafe {
            let dst = buf.as_mut_ptr().add(buf.len());
            std::ptr::copy_nonoverlapping(t.as_ptr(), dst, 24);
            buf.set_len(buf.len() + n + 1);
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
/// path in this crate renders them; ryu would write "1.0". The shortcut
/// covers every whole value below 1e16, where ryu switches to exponent form
/// ("1e16") on its own: `v as i64` is exact up to 2^63, and the bound used to
/// be 2^53, which left 2^53 itself and everything up to 1e16 printing as
/// "9007199254740992.0".
#[inline]
pub(crate) fn write_shortest_f64(buf: &mut Vec<u8>, v: f64) {
    buf.reserve(SHORTEST_MAX);
    let len = buf.len();
    unsafe {
        let written = write_shortest_raw(buf.as_mut_ptr().add(len), v);
        buf.set_len(len + written);
    }
}

/// The most bytes `write_shortest_raw` writes: ryu's 24, itoa's 20 plus a
/// sign for the integral path.
pub(crate) const SHORTEST_MAX: usize = 24;

/// `write_shortest_f64` into raw storage, for loops that have reserved for
/// a whole row and write through a pointer rather than pushing byte by byte.
///
/// # Safety
///
/// `dst` must be writable for `SHORTEST_MAX` bytes.
#[inline]
pub(crate) unsafe fn write_shortest_raw(dst: *mut u8, v: f64) -> usize {
    if v.fract() == 0.0 && v.abs() < 1.0e16 {
        // `v as i64` is 0 for both zeros, and this mode's whole promise is
        // that the text reads back as the same double -- which -0.0 and 0.0
        // are not. ryu keeps the sign; this integral shortcut did not, so
        // as_json(-0.0, digits = Inf) came back as 0.
        let mut n = 0;
        if v == 0.0 && v.is_sign_negative() {
            *dst = b'-';
            n = 1;
        }
        let mut tmp = itoa::Buffer::new();
        let s = tmp.format(v as i64).as_bytes();
        std::ptr::copy_nonoverlapping(s.as_ptr(), dst.add(n), s.len());
        return n + s.len();
    }
    shortest_layout(v, dst)
}

/// The shortest decimal that reads back as `v`, laid out as `ryu` lays it
/// out, with the digits from Żmij (Victor Zverovich's algorithm, David
/// Tolnay's Rust port). Żmij's own layout is ryu's to the byte except that it
/// writes the exponent sign in both directions, `1e+16` for ryu's `1e16`;
/// the `+` is dropped here. Verified byte for byte against ryu in the tests
/// below, over millions of doubles. Digits alone: 24.8 ns per double for ryu
/// against 20.4 on the polygon benchmark's coordinates, and 35 against 21.5
/// on full-range random doubles.
///
/// # Safety
///
/// `dst` must be writable for 24 bytes; `v` must be finite.
///
/// Kept out of line: inlined, a large body sat in every loop that also has
/// the whole-number shortcut above it, and the whole-number column shape
/// slowed by 7% without ever calling this.
#[inline(never)]
pub(crate) unsafe fn shortest_layout(v: f64, dst: *mut u8) -> usize {
    let mut b = zmij::Buffer::new();
    let s = b.format_finite(v).as_bytes();
    let n = s.len();
    // The exponent form, the only one with a sign to drop, appears exactly
    // when the magnitude is outside [1e-5, 1e16); everything inside copies
    // straight through with no scan.
    let a = v.abs();
    if a >= 1e16 || a < 1e-5 {
        if let Some(e) = s.iter().position(|&c| c == b'e') {
            if e + 1 < n && s[e + 1] == b'+' {
                std::ptr::copy_nonoverlapping(s.as_ptr(), dst, e + 1);
                std::ptr::copy_nonoverlapping(s.as_ptr().add(e + 2), dst.add(e + 1), n - e - 2);
                return n - 1;
            }
        }
    }
    std::ptr::copy_nonoverlapping(s.as_ptr(), dst, n);
    n
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
/// The caller guarantees `0 <= d < 10` and `1e-5 < |v| < FIXED_MAX[d]`,
/// which is `10^(17-d)` -- the point past which fixed notation would carry
/// more significant digits than the `%g` fallback's 17-digit cap allows.
/// modp_dtoa2 itself stops at 2^31, where its `(int)` cast overflows; this
/// port uses an i64 and so reaches the real limit.
#[inline]
pub(crate) fn write_fixed_decimals(buf: &mut Vec<u8>, v: f64, d: usize) {
    let prec = d.min(9);
    let neg = v < 0.0;
    let value = if neg { -v } else { v };

    // `as` casts on floats saturate, and the clamping is not free: `value as
    // i64` costs six instructions past the conversion (a compare against
    // 9.22e18, a movabs, a cmova, then a NaN check and a second cmov), and
    // `tmp as u32` costs a maxsd and a minsd -- eight cycles of pure latency
    // sitting directly on the dependency chain, which a disassembly of the
    // built library put at the largest single item in this function.
    //
    // The caller's guard makes both ranges certain, so neither clamp can ever
    // fire: `value` is finite and under FIXED_MAX[d] <= 1e17, well inside i64;
    // and `value - whole` is in [0, 1) because the truncation is toward zero
    // and `value` is positive, so `tmp` is in [0, 10^prec) <= [0, 1e9), well
    // inside u32.
    let mut whole = unsafe { value.to_int_unchecked::<i64>() };
    let tmp = (value - whole as f64) * POW10_F[prec];
    let mut frac = unsafe { tmp.to_int_unchecked::<u32>() };
    let diff = tmp - frac as f64;
    let p10 = POW10_U[prec];

    // modp_dtoa2's rounding -- up when past the half, and on an exact half
    // only to even -- written as one boolean rather than an if / else-if. The
    // truth table is unchanged: up = (diff > 0.5) | (diff == 0.5 & odd), with
    // `odd` the last fractional digit at prec > 0 and the whole part at
    // prec == 0 (where frac is always 0 and p10 is 1, so the carry below is
    // what increments the whole part). On real data `diff > 0.5` is a coin
    // flip, and the branch it compiled to mispredicted half the time; as a
    // select it is 5-6 ns/value cheaper at four decimals.
    //
    // Guarded on `diff != 0.0`: a value exactly representable at this
    // precision -- every whole number, 1.5 at one decimal -- cannot round, and
    // on a column of them the select cost 5 ns/value that the old, perfectly
    // predicted branch did not. The guard is itself a branch, but one that
    // goes the same way for a whole column, so it predicts; only a column
    // mixing exact and inexact values would pay for it.
    if diff != 0.0 {
        let odd = if prec > 0 { frac & 1 } else { (whole & 1) as u32 };
        let up = (diff > 0.5) as u32 | ((diff == 0.5) as u32 & odd);
        frac += up;
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
    // 24 bytes covers the widest this can produce: a sign, 17 whole digits
    // (FIXED_MAX[1] is 1e16), a point and one decimal. Zeroed because the
    // reversal below reads a fixed eight bytes from the low end whatever `k`
    // turns out to be.
    //
    // Every store goes through a raw pointer. `k` is bounded by the digit
    // counts above and LLVM cannot see that, so it was emitting a length
    // check and a panic branch per digit PAIR.
    let mut rev = [0u8; 24];
    let mut k = 0usize;
    let has_dec = count > 0;
    let rp = rev.as_mut_ptr();
    // SAFETY for every `put` below: `k` reaches at most
    // d + 1 (point) + digits(FIXED_MAX[d]) + 1 (rounding carry) + 1 (sign).
    // FIXED_MAX trades the first two off against each other, so working it
    // through per `d`:
    //
    //   d = 0   2^31  ->  10 digits + carry           = 11
    //   d = 1   1e16  ->  16 + carry + point + 1 + -  = 20
    //   d = 2   1e15  ->  15 + carry + point + 2 + -  = 20
    //     ...                              (each 20)
    //   d = 7   1e10  ->  10 + carry + point + 7 + -  = 20
    //   d = 8   2^31  ->  10 + carry + point + 8 + -  = 21
    //   d = 9   2^31  ->  10 + carry + point + 9 + -  = 22
    //
    // so 22, and the pair stores reach index 22. `rev` is 24. An earlier
    // version of this comment said 21, having dropped the carry; the crate's
    // own test caught it on the first run.
    macro_rules! put {
        ($i:expr, $b:expr) => {
            unsafe { *rp.add($i) = $b }
        };
    }
    while count >= 2 {
        let r = (frac % 100) as usize * 2;
        frac /= 100;
        put!(k, DIGIT_PAIRS[r + 1]);
        put!(k + 1, DIGIT_PAIRS[r]);
        k += 2;
        count -= 2;
    }
    if count == 1 {
        put!(k, b'0' + (frac % 10) as u8);
        frac /= 10;
        k += 1;
    }
    // modp_dtoa2 carries here, but by this point it cannot fire: the trim
    // leaves `frac` with exactly `count` digits (or zero, when the trim ran
    // `prec` times and `frac` started below 10^prec), and the loops above
    // consume exactly `count` of them. The two `frac >= p10` carries in the
    // rounding above are what actually handles a rollover. Asserted rather
    // than branched on, so a debug build still checks the reasoning.
    debug_assert_eq!(frac, 0);
    if has_dec {
        put!(k, b'.');
        k += 1;
    }
    while whole >= 100 {
        let r = (whole % 100) as usize * 2;
        whole /= 100;
        put!(k, DIGIT_PAIRS[r + 1]);
        put!(k + 1, DIGIT_PAIRS[r]);
        k += 2;
    }
    if whole >= 10 {
        let r = whole as usize * 2;
        put!(k, DIGIT_PAIRS[r + 1]);
        put!(k + 1, DIGIT_PAIRS[r]);
        k += 2;
    } else {
        // Also the `whole == 0` case, which must still write a leading zero.
        put!(k, b'0' + whole as u8);
        k += 1;
    }
    // Written unconditionally and counted only when negative: on mixed-sign
    // data the branch here was an unpredictable jump on the hottest path. The
    // store into rev[k] is harmless when the value is positive -- k is not
    // advanced, so the byte is never reversed into the output. `neg` stays
    // `v < 0.0`, which is false for -0.0, so negative zero still prints as "0"
    // exactly as jsonlite does; a to_bits sign test would break that.
    put!(k, b'-');
    k += neg as usize;

    // resize() zero-fills and then every byte is overwritten, which is
    // twice the stores on the hottest formatting path in the package.
    // Reserve and write through the spare capacity instead.
    //
    // The reversal itself was a byte loop, and every store depended on the
    // one before it: nine or ten of them for a coordinate like -120.1234,
    // which is why a geometry ordinate cost about 38 ns against 21 for a
    // number in (0, 1). A byte-swapped u64 reverses eight at a time, so the
    // whole of a typical value becomes one load, one bswap, one shift and one
    // store. The shift discards the bytes past `k`, and only `k` are counted,
    // so the reserved-but-unused tail is written and ignored.
    if k <= 16 {
        buf.reserve(16);
        unsafe {
            let dst = buf.as_mut_ptr().add(buf.len());
            // `rev` is 24 bytes, so both reads are inside it for any k <= 16.
            let lo = u64::from_le_bytes(*(rev.as_ptr() as *const [u8; 8]));
            if k <= 8 {
                let w = (lo.swap_bytes() >> ((8 - k) * 8)).to_le_bytes();
                std::ptr::copy_nonoverlapping(w.as_ptr(), dst, 8);
            } else {
                let hi = u64::from_le_bytes(*(rev.as_ptr().add(k - 8) as *const [u8; 8]));
                let w = hi.swap_bytes().to_le_bytes();
                std::ptr::copy_nonoverlapping(w.as_ptr(), dst, 8);
                let w = (lo.swap_bytes() >> ((16 - k) * 8)).to_le_bytes();
                std::ptr::copy_nonoverlapping(w.as_ptr(), dst.add(8), 8);
            }
            buf.set_len(buf.len() + k);
        }
        return;
    }
    // Asserted against the array rather than the number, so the bound above
    // being wrong again cannot become an out-of-bounds write. Every `put!`
    // stores at an index below the final `k`, including the pair form, so
    // `k <= rev.len()` is what keeps them inside.
    debug_assert!(k <= rev.len(), "digit scratch overrun: k = {}", k);
    // Wider than sixteen bytes needs ten whole digits and nine decimals at
    // once, which the fast path's own range test very nearly excludes.
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
        Self { buf: Vec::with_capacity(cap), scratch: Vec::new() }
    }
    #[inline(always)]
    pub(crate) fn push_u8(&mut self, b: u8) {
        self.buf.push(b);
    }
    #[inline(always)]
    pub(crate) fn push_bytes(&mut self, s: &[u8]) {
        self.buf.extend_from_slice(s);
    }
    /// Appends a key, at a fixed width when it is short enough.
    ///
    /// `push_bytes` is `extend_from_slice` with a runtime length, which lowers
    /// to a call to memcpy; for a four-byte key the call costs more than the
    /// bytes do. That is the whole difference between the row-oriented writer,
    /// which emits the key once per row and pays 9 ns of envelope, and the
    /// column-oriented one, which emits it once per column and pays 1.8.
    ///
    /// Two eight-byte copies with constant lengths become two unaligned
    /// stores. Both reads are inside `k.bytes`, which `Key::new` padded to
    /// `KEY_PAD`; both writes are inside the reservation; and the length
    /// advances by the real key length, so the padding is never emitted.
    #[inline(always)]
    pub(crate) fn push_key(&mut self, k: &Key) {
        if k.len > KEY_PAD {
            self.buf.extend_from_slice(k.as_slice());
            return;
        }
        self.buf.reserve(KEY_PAD);
        unsafe {
            let src = k.bytes.as_ptr();
            let n = self.buf.len();
            let dst = self.buf.as_mut_ptr().add(n);
            std::ptr::copy_nonoverlapping(src, dst, 8);
            std::ptr::copy_nonoverlapping(src.add(8), dst.add(8), 8);
            self.buf.set_len(n + k.len);
        }
    }
    #[inline(always)]
    pub(crate) fn push_i32(&mut self, v: i32) {
        let mut tmp = itoa::Buffer::new();
        self.push_bytes(tmp.format(v).as_bytes());
    }
    #[inline(always)]
    pub(crate) fn push_f64_cfg(&mut self, v: f64, config: SerializerConfig) {
        write_f64_json(&mut self.buf, v, config);
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

/// Widens latin1 bytes to UTF-8, appending to `out`.
///
/// Only ever called for a cell the prepass cleared, which means one holding
/// no byte in `0x80..0xA0`. That range is the ambiguous one -- R renders it
/// as CP1252 on this platform and a strict ISO-8859-1 iconv renders it as the
/// C1 controls, 27 of the 256 bytes disagreeing -- so those cells go through
/// `Rf_translateCharUTF8` and get the platform's own answer. Everything else
/// is its own code point and widens with no table at all.
#[inline]
pub(crate) fn widen_latin1_into(out: &mut Vec<u8>, src: &[u8]) {
    out.reserve(src.len() * 2);
    for &b in src {
        if b < 0x80 {
            out.push(b);
        } else {
            out.push(0xC0 | (b >> 6));
            out.push(0x80 | (b & 0x3F));
        }
    }
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

/// How wide a key has to be for `push_key` to copy it at a fixed width.
pub(crate) const KEY_PAD: usize = 16;

/// An escaped `"name":` key, zero-padded to `KEY_PAD` when it is short.
///
/// The padding exists so `JsonWriter::push_key` can read sixteen bytes from it
/// without leaving the allocation. `len` is how many of them are the key.
pub(crate) struct Key {
    pub(crate) bytes: Vec<u8>,
    pub(crate) len: usize,
}

impl Key {
    pub(crate) fn new(escaped: Vec<u8>) -> Self {
        let len = escaped.len();
        let mut bytes = escaped;
        if len <= KEY_PAD {
            bytes.resize(KEY_PAD, 0);
        }
        Key { bytes, len }
    }
    pub(crate) fn from_name(name: &[u8]) -> Self {
        Key::new(build_escaped_key_bytes(name))
    }
    #[inline(always)]
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

/// Above this many names, a repeat is looked for with a set rather than by
/// comparing every pair.
///
/// Below it the quadratic scan is the cheaper of the two -- it touches only
/// the names vector's own memory and allocates nothing, and a three-name list
/// costs three comparisons. The crossover is where one allocation starts to
/// be worth avoiding n^2/2 pointer compares.
pub(crate) const NAME_SCAN_MAX: usize = 32;

/// Does any of `n` names need rewriting -- is one of them empty, NA, or a
/// repeat of another?
///
/// For wide objects. A narrow one is checked as it is written, which costs
/// nothing; see `escaped_keys` and the named-list branch of the serializer.
pub(crate) unsafe fn wide_names_need_fixing(names: libR_sys::SEXP, n: usize) -> bool {
    let np = libR_sys::STRING_PTR_RO(names);
    let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::with_capacity(n);
    for i in 0..n {
        let cs = *np.add(i);
        // R interns its strings, so two equal names are the same CHARSXP and
        // a repeat is a pointer comparison.
        if is_na_string(cs) || libR_sys::Rf_xlength(cs) == 0 || !seen.insert(cs as usize) {
            return true;
        }
    }
    false
}

/// jsonlite's rule for the names of a list or a frame, which is R's own.
///
/// An empty or NA name becomes the element's 1-based index, and then
/// `make.unique` appends `.1`, `.2` and so on until the name is one no other
/// element already carries -- looking at the WHOLE set, not just the names
/// already emitted, which is why `c("a", "a", "a.1")` becomes
/// `a`, `a.2`, `a.1` rather than `a`, `a.1`, `a.1`.
///
/// Only called once something has been found to need rewriting. Without it a
/// list like `list(a = 1, 2)` emitted an empty key, and `c("a", "a")` emitted
/// the same key twice.
pub(crate) unsafe fn mangled_names(names: libR_sys::SEXP, n: usize) -> Vec<Vec<u8>> {
    let np = libR_sys::STRING_PTR_RO(names);

    // The index stands in for a name that is not there.
    let mut base: Vec<Vec<u8>> = Vec::with_capacity(n);
    for i in 0..n {
        let cs = *np.add(i);
        if is_na_string(cs) || libR_sys::Rf_xlength(cs) == 0 {
            let mut tmp = itoa::Buffer::new();
            base.push(tmp.format(i + 1).as_bytes().to_vec());
        } else {
            // Copied straight away: a translated CHARSXP lives on R's vmax
            // stack and is only valid until the next allocation.
            base.push(charsxp_to_utf8_bytes(cs).unwrap_or(b"").to_vec());
        }
    }

    let mut taken: std::collections::HashSet<Vec<u8>> = base.iter().cloned().collect();
    let mut used: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::with_capacity(n);
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(n);
    for b in base.into_iter() {
        if used.insert(b.clone()) {
            out.push(b);
            continue;
        }
        let mut k: u64 = 1;
        loop {
            let mut cand = b.clone();
            cand.push(b'.');
            let mut tmp = itoa::Buffer::new();
            cand.extend_from_slice(tmp.format(k).as_bytes());
            if taken.insert(cand.clone()) {
                used.insert(cand.clone());
                out.push(cand);
                break;
            }
            k += 1;
        }
    }
    out
}

/// `mangled_names` as pre-escaped keys.
pub(crate) unsafe fn mangled_keys(names: libR_sys::SEXP, n: usize) -> Vec<Key> {
    mangled_names(names, n).iter().map(|b| Key::from_name(b)).collect()
}

/// Pre-escaped `"name":` keys for each element of `x`'s `names` attribute.
///
/// The column builder used to take names and escape them itself, which meant
/// two allocations per column -- the name, then the key -- for a value it uses
/// exactly once. A frame of two columns paid five allocations before writing a
/// byte, and a list of 200 small frames paid a thousand.
pub(crate) unsafe fn escaped_keys(x: libR_sys::SEXP, n: usize) -> Option<Vec<Key>> {
    let names_sexp = libR_sys::Rf_getAttrib(x, libR_sys::R_NamesSymbol);
    if names_sexp == libR_sys::R_NilValue
        || typeof_sexp(names_sexp) != libR_sys::SEXPTYPE::STRSXP as u32
        || sexp_len(names_sexp) != n
    {
        return None;
    }
    // An empty, NA or repeated name has to be rewritten the way R does it.
    // A wide frame is checked up front; a narrow one is checked as the keys
    // are built, so the ordinary frame pays only the pointer comparisons
    // against the names it has already seen.
    let wide = n > NAME_SCAN_MAX;
    if wide && wide_names_need_fixing(names_sexp, n) {
        return Some(mangled_keys(names_sexp, n));
    }
    let np = libR_sys::STRING_PTR_RO(names_sexp);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let cs = *np.add(i);
        let bytes: &[u8] = if is_na_string(cs) {
            b""
        } else {
            charsxp_to_utf8_bytes(cs).unwrap_or(b"")
        };
        if !wide && (bytes.is_empty() || (0..i).any(|j| *np.add(j) == cs)) {
            return Some(mangled_keys(names_sexp, n));
        }
        out.push(Key::from_name(bytes));
    }
    Some(out)
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

// ------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn esc(s: &[u8]) -> String {
        let mut b = Vec::new();
        escape_json_string_into(&mut b, s);
        String::from_utf8(b).unwrap()
    }

    #[test]
    fn escaping_matches_jsonlite_rules() {
        // Short escapes for the five named ones, lowercase \u00xx for the rest
        // of the control range, and NO escaping of the solidus or DEL -- except
        // a solidus that follows '<', which is how an embedded </script> is
        // kept from closing the enclosing block.
        assert_eq!(esc(b"plain"), "\"plain\"");
        assert_eq!(esc(b"a\"b"), "\"a\\\"b\"");
        assert_eq!(esc(b"a\\b"), "\"a\\\\b\"");
        assert_eq!(esc(b"a\nb"), "\"a\\nb\"");
        assert_eq!(esc(b"a\tb"), "\"a\\tb\"");
        assert_eq!(esc(b"a\rb"), "\"a\\rb\"");
        assert_eq!(esc(&[b'a', 0x08, b'b']), "\"a\\bb\"");
        assert_eq!(esc(&[b'a', 0x0C, b'b']), "\"a\\fb\"");
        assert_eq!(esc(&[b'a', 0x01, b'b']), "\"a\\u0001b\"");
        assert_eq!(esc(&[b'a', 0x1F, b'b']), "\"a\\u001fb\"");
        assert_eq!(esc(&[b'a', 0x7F, b'b']), "\"a\x7fb\"");
        assert_eq!(esc(b"a/b"), "\"a/b\"");
        assert_eq!(esc(b"</script>"), "\"<\\/script>\"");
        assert_eq!(esc(b"a<b"), "\"a<b\"");
        assert_eq!(esc(b""), "\"\"");
    }

    #[test]
    fn latin1_widens_to_the_same_code_point() {
        // Every byte the fast path is allowed to see, against Rust's own
        // UTF-8 encoder for the code point of the same number -- which is
        // what "latin1 byte n is code point n" means.
        for b in 0u8..=0xFF {
            if (0x80..0xA0).contains(&b) {
                continue; // ambiguous; the prepass sends these to R
            }
            let mut got = Vec::new();
            widen_latin1_into(&mut got, &[b]);
            let mut want = [0u8; 4];
            let want = char::from_u32(b as u32).unwrap().encode_utf8(&mut want);
            assert_eq!(got, want.as_bytes(), "byte 0x{:02x}", b);
            assert!(std::str::from_utf8(&got).is_ok(), "byte 0x{:02x} is not UTF-8", b);
        }
    }

    #[test]
    fn widening_then_escaping_is_the_same_as_escaping_utf8() {
        // The writer widens into scratch and escapes out of it, so the two
        // steps have to compose: a latin1 string must produce exactly what
        // the same text held as UTF-8 would. The cases put high bytes next to
        // every escape, including either side of the '<' '/' pair, which is
        // the one rule that looks at more than one byte.
        let highs: [u8; 4] = [0xA0, 0xC9, 0xE9, 0xFF];
        let specials: [u8; 8] = [b'<', b'/', b'"', b'\\', 0x08, 0x0A, 0x1F, b'x'];
        for &h in &highs {
            for &a in &specials {
                for &c in &specials {
                    let src = [a, h, c, h, a, c];
                    let mut wide = Vec::new();
                    widen_latin1_into(&mut wide, &src);
                    let mut via_latin1 = Vec::new();
                    escape_json_string_into(&mut via_latin1, &wide);
                    // The same text, already UTF-8.
                    let text: String = src
                        .iter()
                        .map(|&b| char::from_u32(b as u32).unwrap())
                        .collect();
                    let mut via_utf8 = Vec::new();
                    escape_json_string_into(&mut via_utf8, text.as_bytes());
                    assert_eq!(
                        via_latin1, via_utf8,
                        "{:?}", src
                    );
                }
            }
        }
    }

    #[test]
    fn find_escape_agrees_with_a_byte_scan() {
        // Unrolled eight at a time, so the seam between the blocks and the
        // remainder is where an off-by-one would hide.
        for len in 0..40usize {
            for pos in 0..len {
                let mut v = vec![b'a'; len];
                v[pos] = b'\n';
                assert_eq!(find_escape(&v), Some(pos), "len {} pos {}", len, pos);
            }
            let clean = vec![b'a'; len];
            assert_eq!(find_escape(&clean), None, "len {}", len);
        }
    }


    #[test]
    fn timestamps_render_in_every_layout() {
        // Local civil seconds, already shifted by R, so the writer is plain
        // arithmetic. 1577872800 is 2020-01-01 10:00:00.
        let t = |v: f64, fmt: u32| {
            let mut b = Vec::new();
            write_time_cell(&mut b, v, fmt, NaMode::Null);
            String::from_utf8(b).unwrap()
        };
        assert_eq!(t(1577872800.0, TFMT_DATE), "\"2020-01-01\"");
        assert_eq!(t(1577872800.0, TFMT_SPACE), "\"2020-01-01 10:00:00\"");
        assert_eq!(t(1577872800.0, TFMT_T), "\"2020-01-01T10:00:00\"");
        assert_eq!(t(1577872800.0, TFMT_TZ), "\"2020-01-01T10:00:00Z\"");
        assert_eq!(t(0.0, TFMT_SPACE), "\"1970-01-01 00:00:00\"");
        // Before the epoch, where a truncating division would give the wrong
        // day and a negative hour.
        assert_eq!(t(-1.0, TFMT_SPACE), "\"1969-12-31 23:59:59\"");
        assert_eq!(t(-86400.0, TFMT_SPACE), "\"1969-12-31 00:00:00\"");
        assert_eq!(t(-86401.0, TFMT_SPACE), "\"1969-12-30 23:59:59\"");
        // The last second of a day, and a leap day.
        assert_eq!(t(86399.0, TFMT_SPACE), "\"1970-01-01 23:59:59\"");
        assert_eq!(t(1582934400.0, TFMT_DATE), "\"2020-02-29\"");
    }

    #[test]
    fn a_timestamp_that_is_not_an_instant() {
        let t = |v: f64, na: NaMode| {
            let mut b = Vec::new();
            write_time_cell(&mut b, v, TFMT_SPACE, na);
            String::from_utf8(b).unwrap()
        };
        assert_eq!(t(na_real(), NaMode::Null), "null");
        assert_eq!(t(na_real(), NaMode::String), "\"NA\"");
        assert_eq!(t(f64::NAN, NaMode::Null), "\"NaN\"");
        assert_eq!(t(f64::INFINITY, NaMode::Null), "\"Inf\"");
        assert_eq!(t(f64::NEG_INFINITY, NaMode::Null), "\"-Inf\"");
    }

    #[test]
    fn seconds_and_days_stay_consistent_with_each_other() {
        // A timestamp at midnight must name the same day the Date writer does
        // for the matching day number, or a POSIXct and a Date would disagree.
        for day in [-25567i64, -1, 0, 1, 11016, 18262, 18321, 50000] {
            let mut a = Vec::new();
            write_date_cell(&mut a, date_cell(day as f64), NaMode::Null);
            let mut b = Vec::new();
            write_time_cell(&mut b, (day * 86400) as f64, TFMT_DATE, NaMode::Null);
            assert_eq!(a, b, "day {}", day);
        }
    }

    #[test]
    fn keys_survive_the_padding_boundary() {
        // push_key copies sixteen bytes whatever the real length, so the
        // padding has to be there and the length must still be the real one.
        for len in 0..40usize {
            let name: Vec<u8> = std::iter::repeat(b'k').take(len).collect();
            let k = Key::from_name(&name);
            let mut w = JsonWriter::with_capacity(0);
            w.push_key(&k);
            w.push_bytes(b"1");
            let got = String::from_utf8(w.buf).unwrap();
            let want = format!("{}{}{}:1", '"', String::from_utf8(name).unwrap(), '"');
            assert_eq!(got, want, "key of {} bytes", len);
        }
    }

    /// R's NA_real_: a quiet NaN whose low-order word is 1954, which is how
    /// R itself tells NA from an ordinary NaN. `from_bits` is not const on
    /// the pinned 1.65 toolchain, so this is a function.
    fn na_real() -> f64 {
        f64::from_bits(0x7FF0_0000_0000_07A2)
    }

    #[test]
    fn dates_match_the_civil_calendar() {
        let d = |days: f64| {
            let mut b = Vec::new();
            write_date_cell(&mut b, date_cell(days), NaMode::Null);
            String::from_utf8(b).unwrap()
        };
        assert_eq!(d(0.0), "\"1970-01-01\"");
        assert_eq!(d(-1.0), "\"1969-12-31\"");
        assert_eq!(d(18262.0), "\"2020-01-01\"");
        // 2020 and 2000 are leap years; 1900 was not.
        assert_eq!(d(18321.0), "\"2020-02-29\"");
        assert_eq!(d(11016.0), "\"2000-02-29\"");
        assert_eq!(d(-25567.0), "\"1900-01-01\"");
        // %Y pads to four characters, sign included.
        assert_eq!(d(-719162.0), "\"0001-01-01\"");
    }

    #[test]
    fn a_date_that_is_not_a_day_keeps_its_own_spelling() {
        // NA and NaN are both NaNs and both satisfy is.na(), but they format
        // differently, and only NA answers to the `na` argument. A NaN Date
        // used to format as an ordinary day number.
        assert!(unsafe { is_na_real(na_real()) });
        assert!(!unsafe { is_na_real(f64::NAN) });
        let d = |v: f64, na: NaMode| {
            let mut b = Vec::new();
            write_date_cell(&mut b, date_cell(v), na);
            String::from_utf8(b).unwrap()
        };
        assert_eq!(d(na_real(), NaMode::Null), "null");
        assert_eq!(d(na_real(), NaMode::String), "\"NA\"");
        assert_eq!(d(f64::NAN, NaMode::Null), "\"NaN\"");
        assert_eq!(d(f64::NAN, NaMode::String), "\"NaN\"");
        assert_eq!(d(f64::INFINITY, NaMode::Null), "\"Inf\"");
        assert_eq!(d(f64::NEG_INFINITY, NaMode::Null), "\"-Inf\"");
    }
}
