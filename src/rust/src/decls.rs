// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;
// `c_char` comes in through the glob above; `c_void` does not.
use std::os::raw::c_void;

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
    pub(crate) fn Rf_translateCharUTF8(x: libR_sys::SEXP) -> *const c_char;
    /// `cetype_t`, a C enum, hence `c_int`.
    pub(crate) fn Rf_getCharCE(x: libR_sys::SEXP) -> i32;
    /// Non-zero when `x`'s bytes are to be read as UTF-8.
    ///
    /// Not the same question as `Rf_getCharCE(x) == CE_UTF8`, which is why R
    /// added this in 4.5.0 and why R-exts 6.10 says to prefer it: "when
    /// needed, it is better to use it in preference of Rf_getCharCE, as it is
    /// safer against future changes in the semantics of encoding marks and
    /// covers strings internally represented in the native encoding".
    ///
    /// The native-encoding case is the one that matters here. `readLines()`
    /// and `rawToChar()` return strings marked CE_NATIVE even in a UTF-8
    /// locale -- which Windows R has used since 4.2 -- so testing the mark
    /// alone sent every non-ASCII value from a text file down the serial
    /// arena. It answers from the header bits and the locale and does not
    /// look at the bytes, so the UTF-8 validation after it still has to run.
    pub(crate) fn Rf_charIsUTF8(x: libR_sys::SEXP) -> i32;
    /// Non-zero when `x` is marked latin1.
    ///
    /// Its bytes then need widening rather than translating, which the
    /// workers can do themselves; see `CD_LATIN1`.
    pub(crate) fn Rf_charIsLatin1(x: libR_sys::SEXP) -> i32;
    /// The attribute pairlist, or `R_NilValue` when the object has none.
    ///
    /// One call here replaces the two `Rf_getAttrib` walks (class, then dim)
    /// that the recursive serializer performed for every node, including the
    /// plain unattributed vectors that make up the bulk of nested data.
    /// Read-only element pointer for a VECSXP, so list traversal indexes
    /// memory instead of making a `VECTOR_ELT` call per element.
    pub(crate) fn VECTOR_PTR_RO(x: libR_sys::SEXP) -> *const libR_sys::SEXP;
    /// Non-zero if `x` has any attributes at all.
    ///
    /// The API-approved way to ask the question `ATTRIB(x) != R_NilValue` was
    /// asking. `ATTRIB` is exported by R.dll on every version, which is what
    /// this file's declarations were originally checked against -- but R-exts
    /// 6.21.6 says being exported is not the test: "The low-level functions
    /// ATTRIB and SET_ATTRIB reveal this representation and are therefore not
    /// part of the API", and `R CMD check --as-cran` reads the package's
    /// undefined symbols and reports any that are not on R's own API list.
    ///
    /// Added in R 4.5.0, which `Depends: R (>= 4.5)` already requires for
    /// `VECTOR_PTR_RO`.
    pub(crate) fn ANY_ATTRIB(x: libR_sys::SEXP) -> i32;
    /// Top of R's `R_alloc` stack.
    ///
    /// `Rf_translateCharUTF8` allocates its result there when it has to
    /// re-encode, and R-exts 6.5 says that memory "persists to the end of the
    /// .Call/.External call unless vmaxset is used". A column of a million
    /// re-encoded strings therefore held a second copy of itself for the rest
    /// of the call. Taking the mark before a translate loop and restoring it
    /// after each string has been copied out reclaims it as we go, which is
    /// what the manual's own showArgs example does.
    ///
    /// # Safety
    ///
    /// Nothing borrowed from R's vmax stack may be live across the restore --
    /// which is exactly the slice `charsxp_to_utf8_bytes` returns.
    pub(crate) fn vmaxget() -> *mut c_void;
    pub(crate) fn vmaxset(x: *const c_void);
    /// Branches to R's error signalling if the user has asked to interrupt.
    ///
    /// Longjmps when it fires, which would walk straight past every Rust
    /// destructor between here and the entry point, so it is never called
    /// directly -- only inside `R_ToplevelExec`. See `interrupt_pending`.
    pub(crate) fn R_CheckUserInterrupt();
    /// Runs `fun` in a fresh top-level context, catching any non-local exit.
    ///
    /// Returns non-zero if `fun` returned normally and zero if it jumped. That
    /// is what makes a safe interrupt poll possible: the jump is absorbed
    /// here instead of unwinding through Rust.
    pub(crate) fn R_ToplevelExec(
        fun: unsafe extern "C" fn(*mut c_void),
        data: *mut c_void,
    ) -> i32;
    /// Non-zero if `x` is an ALTREP object.
    ///
    /// Needed before `REAL`: on an ALTREP vector that materialises the data,
    /// which allocates and can run R code, so it must not happen in a worker.
    pub(crate) fn ALTREP(x: libR_sys::SEXP) -> i32;
}

// `Rf_getCharCE` returns `cetype_t` as a plain int here, so the one value we
// compare against is spelled out. The UTF-8 member is not: `Rf_mkCharLenCE`
// takes the enum, and extendr-ffi already declares it.
pub(crate) const CE_BYTES: i32 = 3;

