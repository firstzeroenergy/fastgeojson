// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

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
    /// The attribute pairlist, or `R_NilValue` when the object has none.
    ///
    /// One call here replaces the two `Rf_getAttrib` walks (class, then dim)
    /// that the recursive serializer performed for every node, including the
    /// plain unattributed vectors that make up the bulk of nested data.
    pub(crate) fn ATTRIB(x: libR_sys::SEXP) -> libR_sys::SEXP;
    /// Read-only element pointer for a VECSXP, so list traversal indexes
    /// memory instead of making a `VECTOR_ELT` call per element.
    pub(crate) fn VECTOR_PTR_RO(x: libR_sys::SEXP) -> *const libR_sys::SEXP;
    /// Non-zero if `x` is an ALTREP object.
    ///
    /// Needed before `REAL`: on an ALTREP vector that materialises the data,
    /// which allocates and can run R code, so it must not happen in a worker.
    pub(crate) fn ALTREP(x: libR_sys::SEXP) -> i32;
}

pub(crate) const CE_UTF8: i32 = 1;
pub(crate) const CE_BYTES: i32 = 3;

