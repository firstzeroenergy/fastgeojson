// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;
// `c_void` is not in the glob above; decls.rs imports it for the same reason.
use std::os::raw::c_void;

// ------------------------------------------------------------------
// C-API HELPERS
// ------------------------------------------------------------------

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
/// A failure with a message of its own, recorded by a writer that has no way
/// to return one: the recursive serializer writes into a buffer and cannot
/// propagate a `Result`, and a worker cannot longjmp. The text is kept in
/// `STR_ERR_MSG` and raised by `check_str_state` on the R thread.
pub(crate) const STR_OTHER_ERROR: u8 = 8;

thread_local! {
    static STR_STATE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
    static STR_ERR_MSG: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

#[inline]
pub(crate) fn str_state_reset() {
    STR_STATE.with(|c| c.set(0));
    STR_ERR_MSG.with(|m| *m.borrow_mut() = None);
}

/// Records a failure the caller cannot return. The first message wins, since
/// it is the one nearest the cause.
pub(crate) fn str_state_error(msg: String) {
    STR_STATE.with(|c| c.set(c.get() | STR_OTHER_ERROR));
    STR_ERR_MSG.with(|m| {
        let mut m = m.borrow_mut();
        if m.is_none() {
            *m = Some(msg);
        }
    });
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
    if str_state_has(STR_OTHER_ERROR) {
        if let Some(msg) = STR_ERR_MSG.with(|m| m.borrow().clone()) {
            return Err(msg);
        }
    }
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

/// Has the user asked to interrupt?
///
/// R-exts 6.13 says plainly that "No part of R can be interrupted whilst
/// running long computations in compiled code, so programmers should make
/// provision for the code to be interrupted at suitable points", and
/// `R_CheckUserInterrupt` is how. But it signals an error when it fires,
/// which longjmps -- straight past every Rust destructor between the check
/// and the entry point, leaking the chunk buffers and whatever else is live.
///
/// So it is called inside `R_ToplevelExec`, which runs it in a fresh
/// top-level context and reports a jump by returning FALSE rather than
/// propagating it (R-exts 6.12). Nothing unwinds through Rust; the caller
/// simply learns that an interrupt is pending and returns an error the
/// ordinary way.
///
/// The R thread only. A worker may not touch the R API at all, and a jump out
/// of a rayon closure would be worse than the thing this avoids.
pub(crate) fn interrupt_pending() -> bool {
    unsafe extern "C" fn probe(_: *mut c_void) {
        unsafe { R_CheckUserInterrupt() };
    }
    unsafe { R_ToplevelExec(probe, std::ptr::null_mut()) == 0 }
}

/// The message for a run the user stopped.
pub(crate) fn interrupted_msg() -> String {
    "interrupted".to_string()
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
/// Hands `buf` back as an R raw vector.
///
/// Copies with `simd_copy` rather than letting extendr's `Raw::from_bytes`
/// run the C runtime's `memcpy`, which takes its slow path whenever source
/// and destination differ modulo 4 KB -- and they always do here, R placing
/// a large vector's bytes 48 into a page and the allocator placing ours at
/// 0. See `simd_copy`.
pub(crate) fn finish_json_raw(buf: Vec<u8>) -> PResult<Robj> {
    check_str_state()?;
    let len = buf.len();
    unsafe {
        let out = libR_sys::Rf_allocVector(libR_sys::SEXPTYPE::RAWSXP, len as libR_sys::R_xlen_t);
        libR_sys::Rf_protect(out);
        crate::exports::simd_copy(libR_sys::RAW(out), buf.as_ptr(), len);
        libR_sys::Rf_unprotect(1);
        Ok(Robj::from_sexp(out))
    }
}

/// Hands `buf` back as an R character vector of length one.
///
/// Built with `Rf_mkCharLenCE` straight from the bytes, marked `CE_UTF8`, and
/// NOT validated as UTF-8 -- deliberately, because `toJSON()` does not
/// validate either. A string R holds in its native encoding whose bytes are
/// not valid UTF-8 is emitted by jsonlite unchanged:
///
/// ```r
/// toJSON(rawToChar(as.raw(0xe9)))
/// #> ["\xe9"]      and Encoding() on that says UTF-8, which it is not
/// ```
///
/// That is not valid JSON and the mark on it is a lie, but reproducing
/// jsonlite byte for byte is the contract, and refusing is not parity: we used
/// to raise "serializer produced invalid UTF-8" for input `toJSON()` accepts.
/// The one case jsonlite does refuse is a `"bytes"`-marked string, and
/// `check_str_state` still raises exactly its message for that.
///
/// Going through `Rf_mkCharLenCE` rather than a Rust `String` is what makes it
/// possible at all, since `String` cannot hold invalid UTF-8, and it also
/// drops a validation pass over the whole output for any input carrying a
/// non-ASCII byte.
pub(crate) fn finish_json_string(buf: Vec<u8>) -> PResult<Robj> {
    check_str_state()?;
    // R's own ceiling on a CHARSXP. The entry points guard before getting
    // here; this is the backstop, since mkCharLenCE takes an int.
    if buf.len() > i32::MAX as usize {
        return Err(format!(
            "Result is {} bytes; an R character string is limited to {} (2 GB). \
             Use as_bytes = TRUE to get the same output as a raw vector, which has no such limit.",
            buf.len(),
            i32::MAX
        ));
    }
    unsafe {
        // The STRSXP first, so that only one thing needs protecting: the
        // CHARSXP goes straight into it and mkCharLenCE is the last
        // allocation either of them makes.
        let out = libR_sys::Rf_allocVector(libR_sys::SEXPTYPE::STRSXP, 1);
        libR_sys::Rf_protect(out);
        let cs = libR_sys::Rf_mkCharLenCE(
            buf.as_ptr() as *const c_char,
            buf.len() as i32,
            libR_sys::cetype_t::CE_UTF8,
        );
        libR_sys::SET_STRING_ELT(out, 0, cs);
        libR_sys::Rf_unprotect(1);
        Ok(Robj::from_sexp(out))
    }
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

