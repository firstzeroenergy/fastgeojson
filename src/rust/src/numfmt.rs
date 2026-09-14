// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// JSON WRITER
// ------------------------------------------------------------------

pub(crate) struct JsonWriter {
    pub(crate) buf: Vec<u8>,
    /// Reusable staging buffer, owned per writer and so per worker.
    ///
    /// A latin1 cell is widened into this and then escaped out of it, which
    /// keeps the widening out of the escaper and off the hot path. Reused
    /// across cells, so a column of them allocates once rather than per row.
    pub(crate) scratch: Vec<u8>,
}

// ------------------------------------------------------------------
// FLOAT FORMATTING (digits support)
// ------------------------------------------------------------------

#[inline(always)]
pub(crate) fn write_f64_json(buf: &mut Vec<u8>, v: f64, config: SerializerConfig) {
    let mark = buf.len();
    write_f64_json_inner(buf, v, config.digits, config.signif);
    if config.always_decimal {
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
    let mut short_buf = [0u8; 32];
    // ryu first, which answers for most values at a sixth of the cost; it
    // declines when rounding its digits could disagree with rounding v.
    let (mantissa, exp) = match shortest_g_mantissa(v, p, &mut short_buf) {
        Some(me) => me,
        None => {
            let sci = match fmt_into(&mut sci_buf, format_args!("{:.*e}", p - 1, v)) {
                Some(t) => t,
                None => return fallback_g(buf, v, p),
            };
            split_sci(sci)
        }
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

/// How far `D` can sit from `v`, in units of `D`'s last digit, per digit count.
///
/// `D` round-trips, so `|D - v| < ulp(v)/2`, and in units of its last digit
///
///     |D - v| / u_nd = (ulp(v)/v) * (v/u_nd) / 2 < 1.11e-16 * 10^nd
///
/// which is 0.11 units at 15 digits, 1.11 at 16 and 11.1 at 17. Rounded up to
/// the next integer, since the comparison it guards is on integers.
const SHORTEST_SLACK: [u32; 18] = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 12,
];

/// A normalised `d.ddd` mantissa rounded to `p` significant digits and its
/// decimal exponent, taken from ryu's shortest form rather than from
/// `core::fmt`.
///
/// `core::fmt`'s float formatting runs the exact (Dragon4) algorithm whatever
/// precision is asked of it, so `write_g_format` cost about 100 ns per value
/// at `p = 5` and at `p = 15` alike -- the same whether it was serving
/// `digits = NA` or an ordinary `digits = 4` value below 1e-5. ryu's shortest
/// form costs about 15 ns.
///
/// Let `D` be that shortest form, with `nd` significant digits. When
/// `nd <= p` there is nothing to round and its digits ARE the answer, which is
/// the common case for data that came from decimal input. When `nd > p`,
/// rounding `D` to `p` digits gives the same answer as rounding `v` unless the
/// two sit on opposite sides of a half-way point, and that needs `D`'s
/// discarded tail to be within `SHORTEST_SLACK` units of exactly one half. The
/// tail is an integer, so that test is exact; failing it returns None and the
/// caller pays for `core::fmt`.
fn shortest_g_mantissa<'a>(v: f64, p: usize, out: &'a mut [u8; 32]) -> Option<(&'a str, i32)> {
    // Past 14 digits this stops paying. Two bounds are at work.
    //
    // 15 is where it stops being CORRECT.
    //
    // When `nd <= p` there is no rounding, but `D` is only the answer if `v`
    // rounded to `p` digits IS `D` padded with zeros, which needs
    // `|D - v| < 0.5 u_p`. That holds when `ulp(v) <= u_p`, and since
    // `ulp(v)/v <= 2.22e-16` while `u_p/v > 10^-p`, it holds exactly while
    // `p <= 15`. It is why `%.17g` of 0.1 is 0.10000000000000001 rather than
    // 0.1. And when `nd > p`, `nd` would have to exceed 17, which no double's
    // shortest form does. So 16 and 17 can never be served here.
    //
    // 14 is where it stops being WORTH IT. The tail test's danger band is
    // `2 * slack / 10^(nd-p)` units wide, so with the 16- and 17-digit
    // shortest forms that full-entropy doubles have, `p = 15` falls back for
    // 24% to 40% of values and pays for ryu before core::fmt anyway: measured
    // at 94.7 -> 101.1 ns on random doubles, against 94.7 -> 57.3 on values
    // rounded to two decimals. `p = 14` falls back for 2.4%, `p <= 13` for
    // 0.24%. digits = NA asks for 15, so it keeps core::fmt; an ordinary
    // digits = 4 below 1e-5 asks for 5, and gains 20%.
    if p == 0 || p > 14 {
        return None;
    }
    // Both bounds above rest on ulp(v)/v <= 2.22e-16, which is a property of
    // NORMAL doubles. A subnormal has far coarser relative precision -- for
    // the smallest, 4.9406564584124654e-324, ulp is the value itself -- so its
    // shortest form (ryu writes 5e-324) says nothing about what rounding the
    // exact value to p digits gives (%.2g gives 4.9e-324).
    if !(v.abs() >= f64::MIN_POSITIVE) {
        return None;
    }
    let mut rb = [0u8; 32];
    // Finite and non-zero: write_g_format handles zero, and the callers reject
    // NaN and the infinities before any of this.
    let written = unsafe { ryu::raw::format64(v, rb.as_mut_ptr()) };
    if written == 0 || written > rb.len() {
        return None;
    }
    let s = &rb[..written];

    // ryu emits [-]ddd[.ddd][e[-]ddd].
    let mut digits = [0u8; 24];
    let mut nd = 0usize;
    let mut frac_len = 0i32;
    let mut e10 = 0i32;
    let mut seen_point = false;
    let neg = s[0] == b'-';
    let mut i = if neg { 1 } else { 0 };
    while i < s.len() {
        let c = s[i];
        if c == b'.' {
            seen_point = true;
            i += 1;
            continue;
        }
        if c == b'e' || c == b'E' {
            i += 1;
            if i >= s.len() {
                return None;
            }
            let eneg = s[i] == b'-';
            if eneg || s[i] == b'+' {
                i += 1;
            }
            let mut e = 0i32;
            while i < s.len() {
                if !s[i].is_ascii_digit() {
                    return None;
                }
                e = e * 10 + (s[i] - b'0') as i32;
                i += 1;
            }
            e10 = if eneg { -e } else { e };
            break;
        }
        if !c.is_ascii_digit() || nd == digits.len() {
            return None;
        }
        digits[nd] = c;
        nd += 1;
        if seen_point {
            frac_len += 1;
        }
        i += 1;
    }

    // "0.00012345" arrives with four leading zeros, and "123456.0" with a
    // trailing one; neither is a significant digit.
    let mut lo = 0usize;
    while lo < nd && digits[lo] == b'0' {
        lo += 1;
    }
    if lo == nd {
        return None;
    }
    let mut hi = nd;
    let mut shift = 0i32;
    while hi > lo + 1 && digits[hi - 1] == b'0' {
        hi -= 1;
        shift += 1;
    }
    let d = &mut digits[lo..hi];
    let mut nd = d.len();
    if nd > 17 {
        return None;
    }
    // v = Dint * 10^(e10 - frac_len + shift), and Dint has nd digits, so the
    // leading digit sits at 10^k.
    let mut k = nd as i32 - 1 + e10 - frac_len + shift;

    if nd > p {
        // The discarded tail, counted in units of D's last digit, against the
        // half-way point.
        let mut tail: u64 = 0;
        let mut scale: u64 = 1;
        for &c in &d[p..nd] {
            tail = tail * 10 + (c - b'0') as u64;
            scale *= 10;
        }
        let half = scale / 2;
        let slack = SHORTEST_SLACK[nd] as u64;
        let round_up = if tail >= half + slack {
            true
        } else if tail + slack <= half {
            false
        } else {
            // Too close to the half-way point to decide from D alone.
            return None;
        };
        nd = p;
        if round_up {
            let mut j = nd;
            loop {
                if j == 0 {
                    // All nines: 999 -> 100 at one exponent higher.
                    d[0] = b'1';
                    for c in d[1..nd].iter_mut() {
                        *c = b'0';
                    }
                    k += 1;
                    break;
                }
                if d[j - 1] == b'9' {
                    d[j - 1] = b'0';
                    j -= 1;
                } else {
                    d[j - 1] += 1;
                    break;
                }
            }
        }
        // %g drops trailing zeros, and so does the rendering below, but
        // trimming here keeps the mantissa short for both branches.
        while nd > 1 && d[nd - 1] == b'0' {
            nd -= 1;
        }
    }

    // [-]d[.ddd], which is what `{:.*e}` would have produced bar its padding.
    let mut n = 0usize;
    if neg {
        out[n] = b'-';
        n += 1;
    }
    out[n] = d[0];
    n += 1;
    if nd > 1 {
        out[n] = b'.';
        n += 1;
        out[n..n + nd - 1].copy_from_slice(&d[1..nd]);
        n += nd - 1;
    }
    // Every byte written is ASCII.
    Some((unsafe { std::str::from_utf8_unchecked(&out[..n]) }, k))
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

/// Splits `{:e}` output into its mantissa and exponent.
///
/// `str::split_once` plus `i32::from_str` measured 10.2 ns of the ~90 ns this
/// function costs, which is a lot for finding a byte and reading at most three
/// digits. The exponent is at the tail and is short, so scan backwards.
#[inline]
fn split_sci(sci: &str) -> (&str, i32) {
    let b = sci.as_bytes();
    let mut i = b.len();
    while i > 0 && b[i - 1] != b'e' {
        i -= 1;
    }
    if i == 0 {
        // No exponent, which `{:e}` does not produce, but be safe.
        return (sci, 0);
    }
    let mantissa = &sci[..i - 1];
    let mut rest = &b[i..];
    let neg = matches!(rest.first(), Some(&b'-'));
    if neg || matches!(rest.first(), Some(&b'+')) {
        rest = &rest[1..];
    }
    let mut e: i32 = 0;
    for &d in rest {
        if !d.is_ascii_digit() {
            break;
        }
        e = e * 10 + (d - b'0') as i32;
    }
    (mantissa, if neg { -e } else { e })
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
/// Largest magnitude the fixed-decimal writer is used for, per `digits`.
///
/// Fixed notation carries `int_digits + d` significant digits, while the `%g`
/// fallback below caps its precision at 17, so the two agree only while
/// `int_digits + d <= 17` -- that is, while `|v| < 10^(17-d)`.
///
/// The bound used to be 2^31 for every `d`, which sent entirely ordinary
/// magnitudes down the `%g` path: epoch milliseconds (1.6e12), counts and IDs
/// above two billion, at 122 ns per value against 20 for the fixed writer.
/// Never smaller than the old 2^31, so no range that already matched moves.
/// `digits = 0` keeps the old bound. It is the one precision where the
/// notation rule disagrees: `decimals` is `ceil(log10|v|)`, which for an exact
/// power of ten equals the exponent rather than exceeding it, and `%g` turns
/// scientific as soon as the exponent reaches the precision. So `%.10g` of
/// 1e10 is `1e+10` where fixed notation writes 10000000000. Only exact powers
/// of ten are affected -- 1.5e10 gives `ceil(10.18) = 11` and stays fixed --
/// and screening for them per value costs more than the mode is worth.
pub(crate) static FIXED_MAX: [f64; 10] = [
    2_147_483_647.0, 1e16, 1e15, 1e14, 1e13, 1e12, 1e11, 1e10,
    2_147_483_647.0, 2_147_483_647.0,
];

#[inline(always)]
pub(crate) fn write_f64_json_inner(buf: &mut Vec<u8>, v: f64, digits: Option<u8>, signif: bool) {
    // `digits = I(n)` asks for n SIGNIFICANT digits, which jsonlite renders
    // with sprintf("%.*g") -- exactly what write_g_format is. It used to be
    // handled in R instead, by signif()-ing the whole object and then
    // formatting the rounded values at 15 digits. That was wrong twice over:
    // signif() rounds the binary value half-to-even where %g rounds the
    // decimal expansion, and rendering the result at 15 digits never picks
    // scientific notation, so 12345 at I(4) came out as 12340 where jsonlite
    // writes 1.234e+04.
    if signif {
        if let Some(d) = digits {
            if d != DIGITS_SHORTEST {
                write_g_format(buf, v, d as i32);
                return;
            }
        }
    }
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
            if d > -1 && d < 10 && v.abs() < FIXED_MAX[d as usize] && v.abs() > 1e-5 {
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

// ------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------
// Pure functions only: no SEXP is touched here, so these run without R even
// being initialised. Everything that reads an R object is covered by the
// R-level suites instead.

#[cfg(test)]
mod tests {
    use super::*;

    /// A deterministic spread of doubles, so a failure is reproducible.
    ///
    /// xorshift64 rather than a dependency, and the bits are used directly so
    /// the sample reaches subnormals, huge exponents and the awkward mantissas
    /// that a uniform draw over a range never produces.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        /// A finite, non-zero, normal double.
        fn finite(&mut self) -> f64 {
            loop {
                let v = f64::from_bits(self.next());
                if v.is_finite() && v != 0.0 && v.abs() >= f64::MIN_POSITIVE {
                    return v;
                }
            }
        }
        /// A double of ordinary magnitude, which is what real data looks like.
        fn ordinary(&mut self) -> f64 {
            let m = (self.next() >> 11) as f64 / (1u64 << 53) as f64;
            let e = (self.next() % 40) as i32 - 20;
            let s = if self.next() & 1 == 0 { 1.0 } else { -1.0 };
            s * m * 10f64.powi(e)
        }
    }

    fn g(v: f64, p: i32) -> String {
        let mut b = Vec::new();
        write_g_format(&mut b, v, p);
        String::from_utf8(b).unwrap()
    }

    fn fixed(v: f64, d: usize) -> String {
        let mut b = Vec::new();
        write_fixed_decimals(&mut b, v, d);
        String::from_utf8(b).unwrap()
    }

    /// `{:.*e}` split the way write_g_format's fallback splits it, with the
    /// mantissa trimmed so it is comparable with the ryu-derived one.
    fn reference_mantissa(v: f64, p: usize) -> (String, i32) {
        let s = format!("{:.*e}", p - 1, v);
        let (m, e) = {
            let b = s.as_bytes();
            let i = b.iter().rposition(|&c| c == b'e').unwrap();
            (&s[..i], s[i + 1..].parse::<i32>().unwrap())
        };
        let t = if m.contains('.') {
            let t = m.trim_end_matches('0');
            t.strip_suffix('.').unwrap_or(t).to_string()
        } else {
            m.to_string()
        };
        (t, e)
    }

    // ---- the ryu-derived mantissa ---------------------------------

    #[test]
    fn shortest_mantissa_agrees_with_core_fmt() {
        // The theorem this rests on: D round-trips, so |D - v| < ulp(v)/2,
        // which in units of D's last digit is under 1.11e-16 * 10^nd. Whenever
        // shortest_g_mantissa accepts, rounding D to p digits must therefore
        // give exactly what rounding v to p digits gives. Checked here rather
        // than argued: a mismatch is a counterexample to the theorem.
        let mut rng = Rng(0x2545F4914F6CDD1D);
        let mut accepted = 0usize;
        let mut total = 0usize;
        for i in 0..400_000 {
            let v = if i % 2 == 0 { rng.finite() } else { rng.ordinary() };
            if !v.is_finite() || v == 0.0 || v.abs() < f64::MIN_POSITIVE {
                continue;
            }
            for p in 1..=14usize {
                total += 1;
                let mut buf = [0u8; 32];
                if let Some((m, e)) = shortest_g_mantissa(v, p, &mut buf) {
                    accepted += 1;
                    let (rm, re) = reference_mantissa(v, p);
                    assert_eq!(
                        (m, e),
                        (rm.as_str(), re),
                        "v = {:?} ({:#x}) at p = {}",
                        v,
                        v.to_bits(),
                        p
                    );
                }
            }
        }
        // If it stopped accepting anything the test would pass vacuously.
        assert!(
            accepted * 4 > total,
            "fast path accepted only {} of {}",
            accepted,
            total
        );
    }

    #[test]
    fn shortest_mantissa_declines_where_it_must() {
        let mut buf = [0u8; 32];
        // Past 15 digits it cannot be correct: with nd <= p there is no
        // rounding, but D is only the answer when v rounded to p digits is D
        // padded with zeros, and that needs p <= 15. %.17g of 0.1 is
        // 0.10000000000000001, not 0.1.
        for p in [15usize, 16, 17, 18, 30] {
            assert!(
                shortest_g_mantissa(0.1, p, &mut buf).is_none(),
                "accepted p = {}",
                p
            );
        }
        assert!(shortest_g_mantissa(0.1, 0, &mut buf).is_none());
        // Subnormals break the premise: ulp/v is 1 for the smallest, so its
        // shortest form says nothing about %.2g. ryu writes 5e-324 where
        // %.2g is 4.9e-324.
        for v in [5e-324f64, 1e-320, 1e-310, f64::MIN_POSITIVE / 2.0] {
            assert!(
                shortest_g_mantissa(v, 2, &mut buf).is_none(),
                "accepted subnormal {:?}",
                v
            );
        }
        // The smallest normal is fine.
        assert!(shortest_g_mantissa(f64::MIN_POSITIVE, 2, &mut buf).is_some());
    }

    #[test]
    fn g_format_matches_printf_on_known_values() {
        // Values whose %g output is easy to state, including the two places
        // the notation rule turns over: exponent < -4, and exponent >= p.
        for &(v, p, want) in &[
            (0.0f64, 4i32, "0"),
            (-0.0, 4, "-0"),
            (1.0, 4, "1"),
            (1.5, 4, "1.5"),
            (0.1, 4, "0.1"),
            (1e-4, 4, "0.0001"),
            (1e-5, 4, "1e-05"),
            (9.999e-5, 4, "9.999e-05"),
            (12345.0, 4, "1.234e+04"),
            (1234.0, 4, "1234"),
            (1e20, 4, "1e+20"),
            (1e100, 3, "1e+100"),
            (-1234.5678, 6, "-1234.57"),
            (100.0, 1, "1e+02"),
            (100.0, 3, "100"),
        ] {
            assert_eq!(g(v, p), want, "v = {:?} at p = {}", v, p);
        }
    }

    // ---- the fixed-decimal writer ---------------------------------


    // ---- %g's rendering, not just its digits ----------------------

    fn trim(s: &str) -> String {
        if s.contains('.') {
            let t = s.trim_end_matches('0');
            t.strip_suffix('.').unwrap_or(t).to_string()
        } else {
            s.to_string()
        }
    }

    /// `%.{p}g`, written independently and slowly.
    ///
    /// C's rule: scientific when the exponent of the rounded value is below -4
    /// or at least the precision, fixed otherwise; trailing zeros removed
    /// either way; the exponent signed and at least two digits.
    fn reference_g(v: f64, p: usize) -> String {
        if v == 0.0 {
            return if v.is_sign_negative() { "-0".into() } else { "0".into() };
        }
        let sci = format!("{:.*e}", p - 1, v);
        let i = sci.rfind('e').unwrap();
        let exp: i32 = sci[i + 1..].parse().unwrap();
        if exp < -4 || exp >= p as i32 {
            format!(
                "{}e{}{:02}",
                trim(&sci[..i]),
                if exp < 0 { '-' } else { '+' },
                exp.abs()
            )
        } else {
            let decimals = (p as i32 - 1 - exp).max(0) as usize;
            trim(&format!("{:.*}", decimals, v))
        }
    }

    #[test]
    fn g_format_matches_an_independent_reference() {
        // The known values above pin the corners; this checks the whole
        // rendering -- notation choice, point placement, zero trimming,
        // exponent padding -- over a wide spread, and it is the check that
        // covers the ryu fast path and the core::fmt fallback together, since
        // write_g_format picks between them internally.
        let mut rng = Rng(0xDEADBEEFCAFEF00D);
        for i in 0..120_000 {
            let v = if i % 3 == 0 { rng.finite() } else { rng.ordinary() };
            if !v.is_finite() {
                continue;
            }
            for p in 1..=17usize {
                assert_eq!(
                    g(v, p as i32),
                    reference_g(v, p),
                    "v = {:?} ({:#x}) at p = {}",
                    v,
                    v.to_bits(),
                    p
                );
            }
        }
    }

    #[test]
    fn g_format_handles_the_values_that_have_no_digits() {
        assert_eq!(g(0.0, 1), "0");
        assert_eq!(g(-0.0, 6), "-0");
        // Precision below one is treated as one, as printf does.
        assert_eq!(g(1.5, 0), reference_g(1.5, 1));
        assert_eq!(g(1.5, -3), reference_g(1.5, 1));
    }

    #[test]
    fn splitting_scientific_notation_finds_the_exponent() {
        assert_eq!(split_sci("1.5e10"), ("1.5", 10));
        assert_eq!(split_sci("1.5e-10"), ("1.5", -10));
        assert_eq!(split_sci("-1.5e+10"), ("-1.5", 10));
        assert_eq!(split_sci("7e0"), ("7", 0));
        assert_eq!(split_sci("1.5e308"), ("1.5", 308));
        // No exponent at all: the whole string is the mantissa.
        assert_eq!(split_sci("1.5"), ("1.5", 0));
    }

    #[test]
    fn trimming_only_touches_a_fraction() {
        assert_eq!(trim_fixed("1.500"), "1.5");
        assert_eq!(trim_fixed("1.000"), "1");
        assert_eq!(trim_fixed("100"), "100");
        assert_eq!(trim_fixed("0"), "0");
        assert_eq!(trim_fixed("-1.10"), "-1.1");
    }

    #[test]
    fn fmt_into_reports_an_overflow_rather_than_truncating() {
        let mut small = [0u8; 4];
        assert!(fmt_into(&mut small, format_args!("{}", 123456789)).is_none());
        let mut big = [0u8; 32];
        assert_eq!(fmt_into(&mut big, format_args!("{}", 1234)), Some("1234"));
    }

    #[test]
    fn shortest_layout_is_byte_identical_to_ryu() {
        // Dragonbox digits in ryu's layout must be what ryu itself writes, on
        // every branch of the layout and across the whole exponent range.
        let mut ours = [0u8; 32];
        let mut theirs = [0u8; 32];
        let mut check = |v: f64| {
            let a = unsafe { crate::shortest_layout(v, ours.as_mut_ptr()) };
            let b = unsafe { ryu::raw::format64(v, theirs.as_mut_ptr()) };
            assert_eq!(
                std::str::from_utf8(&ours[..a]).unwrap(),
                std::str::from_utf8(&theirs[..b]).unwrap(),
                "for {:e} (bits {:#x})", v, v.to_bits()
            );
        };
        for v in [
            0.0, -0.0, 1.0, -1.0, 0.1, 0.5, 1.5, 100.0, 123000.0, 1e15, 1e16, 1e17, 9.999e15,
            1e-4, 1e-5, 1e-6, 1.5e300, 1e300, 1e-300, 5e-324, f64::MIN_POSITIVE, f64::MAX,
            f64::MIN, 9007199254740992.0, 9007199254740993.0, 0.30000000000000004,
            3.141592653589793, 2.5e-300, 1.7976931348623157e308, 2.2250738585072014e-308,
            123456789012345680.0, 0.000123456, 1234.5678, 12.34, 0.001234,
        ] {
            check(v);
        }
        for p in -324..=308 {
            check(format!("1e{p}").parse::<f64>().unwrap());
            check(format!("-3e{p}").parse::<f64>().unwrap());
            check(format!("7.5e{p}").parse::<f64>().unwrap());
        }
        let mut rng = Rng(0x0DDB_A11C_0FFE_E000);
        let mut n = 0;
        while n < 5_000_000 {
            let v = f64::from_bits(rng.next());
            if v.is_finite() {
                check(v);
                n += 1;
            }
        }
        // uniform (0, 1) and coordinate-like values, the common shapes
        for _ in 0..2_000_000 {
            let u = (rng.next() >> 11) as f64 / (1u64 << 53) as f64;
            check(u);
            check(-125.0 + u * 59.0);
            check(u * 1e6);
        }
    }

    #[test]
    fn shortest_round_trips_every_double_it_is_given() {
        // digits = Inf: the shortest decimal that reads back as the same
        // double. Integral values below 2^53 take a separate branch.
        let mut rng = Rng(0x1234_5678_9ABC_DEF0);
        let short = |v: f64| {
            let mut b = Vec::new();
            write_shortest_f64(&mut b, v);
            String::from_utf8(b).unwrap()
        };
        assert_eq!(short(1.0), "1");
        assert_eq!(short(-1.0), "-1");
        assert_eq!(short(0.0), "0");
        assert_eq!(short(1e15), "1000000000000000");
        assert_eq!(short(0.1), "0.1");
        // Every whole double below 1e16 prints as plain digits, including the
        // ones at and past 2^53, which the old 2^53 bound sent to ryu's "d.0".
        // From 1e16 ryu's own exponent form is the shorter text.
        assert_eq!(short(9007199254740991.0), "9007199254740991");
        assert_eq!(short(9007199254740992.0), "9007199254740992");
        assert_eq!(short(-9007199254740992.0), "-9007199254740992");
        assert_eq!(short(9007199254740994.0), "9007199254740994");
        assert_eq!(short(9999999999999998.0), "9999999999999998");
        assert_eq!(short(1e16), "1e16");
        assert_eq!(short(1e17), "1e17");
        // -0.0 and 0.0 are different doubles, and this mode promises the text
        // reads back as the same one. The integral shortcut used to drop the
        // sign; the random sample never reaches it, because a uniform mantissa
        // is never exactly zero. jsonlite's issue #435 is what pointed here.
        assert_eq!(short(-0.0), "-0");
        for v in [
            0.0f64, -0.0, 1.0, -1.0, f64::MIN_POSITIVE, -f64::MIN_POSITIVE,
            5e-324, -5e-324, f64::MAX, f64::MIN, 9007199254740992.0,
            -9007199254740992.0, 1e15, -1e15,
        ] {
            let s = short(v);
            let back: f64 = s.parse().unwrap();
            assert_eq!(back.to_bits(), v.to_bits(), "{:?} did not round-trip as {}", v, s);
        }
        for _ in 0..50_000 {
            for v in [rng.finite(), rng.ordinary()] {
                if !v.is_finite() {
                    continue;
                }
                let s = short(v);
                let back: f64 = s.parse().unwrap_or(f64::NAN);
                assert_eq!(back.to_bits(), v.to_bits(), "{:?} did not round-trip as {}", v, s);
            }
        }
    }

    #[test]
    fn fixed_decimals_known_values() {
        for &(v, d, want) in &[
            (0.5f64, 0usize, "0"),
            (1.5, 0, "2"),
            (2.5, 0, "2"),
            (1.0, 4, "1"),
            (100.0, 4, "100"),
            (0.1, 4, "0.1"),
            (1.23456, 4, "1.2346"),
            (-1.23456, 4, "-1.2346"),
            (9.99999, 4, "10"),
            (-9.99999, 4, "-10"),
            (0.00012, 4, "0.0001"),
            (1234567.891, 2, "1234567.89"),
            (-1234567.891, 2, "-1234567.89"),
        ] {
            assert_eq!(fixed(v, d), want, "v = {:?} at d = {}", v, d);
        }
    }

    #[test]
    fn fixed_decimals_round_half_to_even_at_every_precision() {
        // modp_dtoa2 rounds an exact half to the even neighbour: 0.5 -> 0,
        // 1.5 -> 2, 2.5 -> 2 at d = 0, and likewise on the last kept digit at
        // every other precision. The rounding is one boolean now rather than
        // an if / else-if, and this is the case that boolean exists for -- a
        // random sweep almost never lands on an exact half, so it is walked
        // deliberately. Only values exactly representable in binary are used
        // (k / 2^m), so "exact half" really is exact and not a formatting
        // accident.
        let s = |v: f64, d: usize| {
            let mut b = Vec::new();
            write_fixed_decimals(&mut b, v, d);
            String::from_utf8(b).unwrap()
        };
        // d = 0: n + 0.5 for n in 0..64, then the same negated.
        for n in 0..64i64 {
            let v = n as f64 + 0.5;
            let want = if n % 2 == 0 { n } else { n + 1 };
            assert_eq!(s(v, 0), want.to_string(), "{} at d=0", v);
            assert_eq!(s(-v, 0), format!("-{}", want), "{} at d=0", -v);
        }
        // d = 1..=3: an exact binary half on the last kept digit. At d = 1
        // that is x.x5 with the .05 exactly representable: 0.25, 0.75, 1.25,
        // 1.75 ... (odd multiples of 1/4); at d = 2, odd multiples of 1/8 ->
        // x.xx5 only where the third digit is 5 exactly: 0.125, 0.375, ...;
        // at d = 3, odd multiples of 1/16: 0.0625 is not a half-case (four
        // digits), so use 1/2000-style decimals only where exact -- skip and
        // instead check the general rule with values known exact.
        let cases: &[(f64, usize, &str)] = &[
            (0.25, 1, "0.2"), (0.75, 1, "0.8"), (1.25, 1, "1.2"), (1.75, 1, "1.8"),
            (2.25, 1, "2.2"), (-0.25, 1, "-0.2"), (-0.75, 1, "-0.8"),
            (0.125, 2, "0.12"), (0.375, 2, "0.38"), (0.625, 2, "0.62"), (0.875, 2, "0.88"),
            (1.125, 2, "1.12"), (-0.125, 2, "-0.12"), (-0.375, 2, "-0.38"),
            // carry across the whole part on an exact half rounding up
            (0.95, 1, "1"), (9.5, 0, "10"), (99.5, 0, "100"), (0.995, 2, "1"),
            // and one that must NOT carry (even)
            (8.5, 0, "8"), (98.5, 0, "98"),
        ];
        for &(v, d, want) in cases {
            assert_eq!(s(v, d), want, "{} at d={}", v, d);
        }
    }

    #[test]
    fn fixed_decimals_output_is_well_formed() {
        // The reversal writes eight or sixteen bytes at a time and advances the
        // length by the real count; the whole and fraction runs each walk a
        // cursor down to the start of their own field. A slip in any of that
        // shows up as a stray byte, a lost digit or a trailing zero.
        let mut rng = Rng(0x9E3779B97F4A7C15);
        for _ in 0..200_000 {
            let v = rng.ordinary();
            for d in 0..=9usize {
                // The caller's guard, which this function documents as a
                // precondition: feeding it wider values than FIXED_MAX[d]
                // admits overruns the digit scratch, and an earlier version of
                // this test did exactly that.
                if !(v.abs() > 1e-5) || !(v.abs() < FIXED_MAX[d]) {
                    continue;
                }
                let s = fixed(v, d);
                assert!(!s.is_empty());
                assert!(
                    s.bytes().all(|c| c.is_ascii_digit() || c == b'-' || c == b'.'),
                    "stray byte in {:?} for v = {:?} d = {}",
                    s,
                    v,
                    d
                );
                assert_eq!(s.matches('.').count() <= 1, true, "two points in {:?}", s);
                assert!(!s.ends_with('.'), "bare point in {:?}", s);
                if s.contains('.') {
                    assert!(!s.ends_with('0'), "untrimmed zero in {:?}", s);
                }
                let back: f64 = s.parse().unwrap();
                // Rounding to d places cannot move a value by more than half a
                // unit in the last place, plus the double's own slack.
                let tol = 0.5 * 10f64.powi(-(d as i32)) + v.abs() * 1e-12;
                assert!(
                    (back - v).abs() <= tol,
                    "{:?} is not {:?} to {} places",
                    s,
                    v,
                    d
                );
            }
        }
    }

    #[test]
    fn fixed_decimals_spans_the_reversal_boundaries() {
        // The reversal has three shapes -- one word, two words, and a byte
        // loop -- with seams at eight and sixteen bytes of output. These land
        // on both sides of each.
        for (v, d) in [
            (1.0f64, 0usize),           // 1
            (1234567.0, 0),             // 7
            (12345678.0, 0),            // 8
            (123456789.0, 0),           // 9
            (1.2345678, 7),             // 9
            (123456.1234567, 7),        // 14
            (1234567.1234567, 7),       // 15
            (12345678.1234567, 7),      // 16
            (123456789.1234567, 7),     // 17
            (-123456789.1234567, 7),    // 18
            (-1234567890.123456789, 9), // the widest the range test admits
        ] {
            let s = fixed(v, d);
            let back: f64 = s.parse().unwrap();
            let tol = 0.5 * 10f64.powi(-(d as i32)) + v.abs() * 1e-9;
            assert!((back - v).abs() <= tol, "{:?} for v = {:?} d = {}", s, v, d);
        }
    }

    #[test]
    fn fixed_max_bounds_the_significant_digits() {
        // Fixed notation carries int_digits + d significant digits while the
        // %g fallback caps at 17, so the two agree only below 10^(17-d). The
        // table is what encodes that; digits 0, 8 and 9 keep the older 2^31
        // bound because 10^(17-d) is smaller there.
        for d in 0..10usize {
            let want = 10f64.powi(17 - d as i32);
            let got = FIXED_MAX[d];
            assert!(
                got == want || got == 2_147_483_647.0,
                "FIXED_MAX[{}] = {:?}",
                d,
                got
            );
            assert!(got >= 2_147_483_647.0, "FIXED_MAX[{}] shrank to {:?}", d, got);
        }
    }

    // ---- the writer's own dispatch --------------------------------

    #[test]
    fn always_decimal_only_touches_plain_integers() {
        let cfg = |always: bool, digits: Option<u8>| SerializerConfig {
            df: DfMode::Rows,
            na: NaMode::Null,
            null: NullMode::List,
            factor: FactorMode::String,
            auto_unbox: false,
            digits,
            matrix_colmajor: false,
            always_decimal: always,
            signif: false,
            json_verbatim: false,
            rownames: ROWNAMES_REAL,
        };
        let render = |v: f64, c: SerializerConfig| {
            let mut b = Vec::new();
            write_f64_json(&mut b, v, c);
            String::from_utf8(b).unwrap()
        };
        assert_eq!(render(1.0, cfg(true, Some(4))), "1.0");
        assert_eq!(render(1.0, cfg(false, Some(4))), "1");
        assert_eq!(render(1.5, cfg(true, Some(4))), "1.5");
        assert_eq!(render(-2.0, cfg(true, Some(4))), "-2.0");
        // Already carrying an exponent, so nothing is appended.
        assert_eq!(render(1e-20, cfg(true, Some(4))), "1e-20");
    }

    #[test]
    fn signif_routes_to_g_format() {
        let c = SerializerConfig {
            df: DfMode::Rows,
            na: NaMode::Null,
            null: NullMode::List,
            factor: FactorMode::String,
            auto_unbox: false,
            digits: Some(4),
            matrix_colmajor: false,
            always_decimal: false,
            signif: true,
            json_verbatim: false,
            rownames: ROWNAMES_REAL,
        };
        let mut b = Vec::new();
        write_f64_json(&mut b, 12345.0, c);
        // Significant digits, so %g's notation rule applies: exponent 4
        // reaches the precision, which selects scientific.
        assert_eq!(String::from_utf8(b).unwrap(), "1.234e+04");
    }
}
