// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// JSON WRITER
// ------------------------------------------------------------------

pub(crate) struct JsonWriter {
    pub(crate) buf: Vec<u8>,
}

// ------------------------------------------------------------------
// FLOAT FORMATTING (digits support)
// ------------------------------------------------------------------

#[inline(always)]
pub(crate) fn write_f64_json(buf: &mut Vec<u8>, v: f64, digits: Option<u8>, always_decimal: bool) {
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
pub(crate) fn write_g_format(buf: &mut Vec<u8>, v: f64, precision: i32) {
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
    //
    // Both of these used to be format!(), i.e. two heap allocations for every
    // double on this path. They write into stack buffers instead: 32 bytes is
    // always enough for {:e} (sign, digit, point, 17 digits, e, sign, 3
    // exponent digits) and 384 covers {:.*} at the widest, which is a value
    // near 1e308 with 18 decimals.
    let mut sci_buf = [0u8; 32];
    let sci = match fmt_into(&mut sci_buf, format_args!("{:.*e}", p - 1, v)) {
        Some(t) => t,
        None => return fallback_g(buf, v, p),
    };
    let (mantissa, exp) = match sci.split_once('e') {
        Some((m, e)) => (m, e.parse::<i32>().unwrap_or(0)),
        None => (sci, 0),
    };

    if exp >= -4 && (exp as i64) < p as i64 {
        // Fixed notation, built by moving the point in the mantissa we already
        // have rather than formatting v a second time.
        //
        // Rounding to p significant digits puts the last kept digit at decimal
        // place p - 1 - exp, so it is the same rounding %.*f would do at that
        // precision -- and core::fmt's float conversion is the expensive part
        // of this function, so not doing it twice is most of the cost.
        if !write_fixed_from_mantissa(buf, mantissa, exp) {
            return fallback_g(buf, v, p);
        }
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

/// Writes `%f`-style fixed notation from a normalised `%e` mantissa.
///
/// `mantissa` is `[-]D[.DDD]` as produced by `{:.*e}`, already correctly
/// rounded, and `exp` its decimal exponent. Returns false if the mantissa is
/// wider than expected, which no finite f64 produces at the precisions used
/// here.
pub(crate) fn write_fixed_from_mantissa(buf: &mut Vec<u8>, mantissa: &str, exp: i32) -> bool {
    let m = mantissa.as_bytes();
    if m.is_empty() {
        return false;
    }
    let (neg, m) = if m[0] == b'-' {
        (true, &m[1..])
    } else {
        (false, m)
    };
    let mut digits = [0u8; 32];
    let mut nd = 0usize;
    for &c in m {
        if c == b'.' {
            continue;
        }
        if !c.is_ascii_digit() || nd == digits.len() {
            return false;
        }
        digits[nd] = c;
        nd += 1;
    }
    if nd == 0 {
        return false;
    }
    if neg {
        buf.push(b'-');
    }
    if exp >= 0 {
        let int_len = (exp as usize + 1).min(nd);
        buf.extend_from_slice(&digits[..int_len]);
        // Zero-pad when the exponent runs past the digits we have. Only
        // reachable if a caller passes a precision below exp + 1, which the
        // %g branch condition rules out.
        for _ in nd..(exp as usize + 1) {
            buf.push(b'0');
        }
        // Trailing zeros go, but only fractional ones.
        let mut end = nd;
        while end > int_len && digits[end - 1] == b'0' {
            end -= 1;
        }
        if end > int_len {
            buf.push(b'.');
            buf.extend_from_slice(&digits[int_len..end]);
        }
    } else {
        let mut end = nd;
        while end > 1 && digits[end - 1] == b'0' {
            end -= 1;
        }
        buf.push(b'0');
        buf.push(b'.');
        for _ in 0..(-exp - 1) {
            buf.push(b'0');
        }
        buf.extend_from_slice(&digits[..end]);
    }
    true
}

/// Renders `args` into a stack buffer, returning `None` if it does not fit.
///
/// core::fmt's float paths allocate through `format!`; this keeps the same
/// formatting but with no allocator traffic at all.
#[inline]
pub(crate) fn fmt_into<'a>(buf: &'a mut [u8], args: std::fmt::Arguments) -> Option<&'a str> {
    use std::fmt::Write;
    struct Sink<'b> {
        buf: &'b mut [u8],
        n: usize,
    }
    impl std::fmt::Write for Sink<'_> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            let b = s.as_bytes();
            let end = self.n + b.len();
            if end > self.buf.len() {
                return Err(std::fmt::Error);
            }
            self.buf[self.n..end].copy_from_slice(b);
            self.n = end;
            Ok(())
        }
    }
    let mut sink = Sink { buf, n: 0 };
    sink.write_fmt(args).ok()?;
    let n = sink.n;
    // core::fmt only ever writes valid UTF-8, and every format used here is
    // ASCII anyway.
    Some(unsafe { std::str::from_utf8_unchecked(&buf[..n]) })
}

/// The allocating path, for the pathological widths the stack buffers reject.
/// Never reached for a finite f64 at the precisions this crate uses.
#[cold]
pub(crate) fn fallback_g(buf: &mut Vec<u8>, v: f64, p: usize) {
    let sci = format!("{:.*e}", p.saturating_sub(1), v);
    let (mantissa, exp) = match sci.split_once('e') {
        Some((m, e)) => (m, e.parse::<i32>().unwrap_or(0)),
        None => (sci.as_str(), 0),
    };
    if exp >= -4 && (exp as i64) < p as i64 {
        let decimals = (p as i32 - 1 - exp).max(0) as usize;
        let t = format!("{:.*}", decimals, v);
        buf.extend_from_slice(trim_fixed(&t).as_bytes());
    } else {
        buf.extend_from_slice(trim_fixed(mantissa).as_bytes());
        buf.push(b'e');
        buf.push(if exp < 0 { b'-' } else { b'+' });
        let a = exp.unsigned_abs();
        if a < 10 {
            buf.push(b'0');
        }
        let mut tmp = itoa::Buffer::new();
        buf.extend_from_slice(tmp.format(a).as_bytes());
    }
}

/// Drops trailing fractional zeros, and a trailing point if one is left.
pub(crate) fn trim_fixed(s: &str) -> &str {
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
pub(crate) fn write_f64_json_inner(buf: &mut Vec<u8>, v: f64, digits: Option<u8>) {
    match digits {
        // digits = NA: 15 significant digits.
        None => write_g_format(buf, v, 15),
        // digits = Inf: the shortest decimal that reads back as this exact
        // double. Not a jsonlite mode -- jsonlite warns and falls back to NA
        // for a non-integer `digits` -- and the only lossless option here:
        // digits = NA keeps 15 significant digits, which is not enough for
        // about 94% of doubles that came out of real arithmetic.
        Some(DIGITS_SHORTEST) => write_shortest_f64(buf, v),
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

