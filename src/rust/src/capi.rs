// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// C-API HELPERS
// ------------------------------------------------------------------

/// One attribute, read by walking the pairlist directly.
///
/// `Rf_getAttrib` is a cross-DLL call that also sets NOT_MUTABLE on whatever
/// it returns, which is a write to a shared SEXP header and so cannot be done
/// from a worker. This is three pure pointer reads per link and touches
/// nothing, which is what makes the geometry pass movable off the R thread.
#[inline]
pub(crate) unsafe fn attrib_by_tag(
    x: libR_sys::SEXP,
    tag: libR_sys::SEXP,
) -> libR_sys::SEXP {
    let mut a = ATTRIB(x);
    while a != libR_sys::R_NilValue {
        if libR_sys::TAG(a) == tag {
            return libR_sys::CAR(a);
        }
        a = libR_sys::CDR(a);
    }
    libR_sys::R_NilValue
}

/// Element pointer for a list, or `None` if `x` is not one.
///
/// The type check is a header read, so this cannot raise the R error
/// `VECTOR_PTR_RO` would on a non-list.
#[inline]
pub(crate) unsafe fn list_elems(x: libR_sys::SEXP) -> Option<*const libR_sys::SEXP> {
    if typeof_sexp(x) == libR_sys::SEXPTYPE::VECSXP as u32 && sexp_len(x) > 0 {
        Some(VECTOR_PTR_RO(x))
    } else {
        None
    }
}

#[inline(always)]
pub(crate) unsafe fn sexp_len(x: libR_sys::SEXP) -> usize {
    let n = libR_sys::Rf_xlength(x); 
    if n < 0 { return 0; }
    n as usize
}

#[inline(always)]
pub(crate) unsafe fn typeof_sexp(x: libR_sys::SEXP) -> u32 {
    libR_sys::TYPEOF(x) as u32
}

#[inline(always)]
pub(crate) unsafe fn is_na_int(v: i32) -> bool { v == i32::MIN }
// R's NA_real_ is a quiet NaN whose low-order 32-bit word is 1954, which is
// exactly how R_IsNA/R_IsNaN distinguish the two. Calling into R for this was
// a cross-DLL indirect call PER ELEMENT in the hottest loops -- measured at
// roughly 100-160ns per value on the recursive path, which is why a plain
// double vector serialised at 31 MB/s while the equivalent data.frame column
// (which happens to test is_finite first) managed 481 MB/s.
//
// `to_bits() as u32` takes the low 32 bits of the payload, which is the
// low-order word on both endiannesses, matching R's `word[lw]`.
pub(crate) const NA_REAL_LOW_WORD: u32 = 1954;

#[inline(always)]
pub(crate) unsafe fn is_na_real(v: f64) -> bool {
    v.is_nan() && (v.to_bits() as u32) == NA_REAL_LOW_WORD
}
#[inline(always)]
pub(crate) unsafe fn is_nan_real(v: f64) -> bool {
    v.is_nan() && (v.to_bits() as u32) != NA_REAL_LOW_WORD
}
#[inline(always)]
pub(crate) unsafe fn is_na_string(sexp: libR_sys::SEXP) -> bool { sexp == libR_sys::R_NaString }

// Per-call state for string handling. Only ever touched from the R thread
// (every CHARSXP read is hoisted out of the rayon regions), so a plain Cell
// is sufficient.
pub(crate) const STR_NON_ASCII: u8 = 1;
pub(crate) const STR_ENC_ERROR: u8 = 2;
pub(crate) const STR_DEPTH_ERROR: u8 = 4;

thread_local! {
    static STR_STATE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

#[inline]
pub(crate) fn str_state_reset() {
    STR_STATE.with(|c| c.set(0));
}
#[inline]
pub(crate) fn str_state_mark(bit: u8) {
    STR_STATE.with(|c| c.set(c.get() | bit));
}
#[inline]
pub(crate) fn str_state_has(bit: u8) -> bool {
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
pub(crate) unsafe fn charsxp_to_utf8_bytes(charsxp: libR_sys::SEXP) -> Option<&'static [u8]> {
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
/// The errors the writers record rather than raising, because a worker cannot
/// longjmp over Rust frames. Checked once, on the R thread, before a result is
/// handed back.
pub(crate) fn check_str_state() -> PResult<()> {
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
    Ok(())
}

/// Hands `buf` back as an R raw vector.
///
/// This exists because R charges about 1 GB/s to create a character vector: it
/// hashes every byte to intern the string in its global CHARSXP cache. For a
/// 49 MB result that is 52 ms, which measured as 74% of the total time for a
/// million-row frame, against 4 ms for the serialization itself. A raw vector
/// is one memcpy at memory bandwidth.
///
/// No UTF-8 validation, because a raw vector carries no encoding.
pub(crate) fn finish_json_raw(buf: Vec<u8>) -> PResult<Robj> {
    check_str_state()?;
    Ok(Raw::from_bytes(&buf).into())
}

pub(crate) fn finish_json_string(buf: Vec<u8>) -> PResult<String> {
    check_str_state()?;
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
pub(crate) unsafe fn get_df_nrows(sexp: libR_sys::SEXP) -> usize {
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

