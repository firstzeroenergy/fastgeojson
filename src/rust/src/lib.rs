//! Fast JSON and GeoJSON serialization for R.
//!
//! R objects are read through raw pointers into their own storage, described
//! once on the R thread, then written to JSON bytes by a pool of workers that
//! never touch the R API. Output matches `jsonlite::toJSON()` byte for byte.
//!
//! Set `FASTGEOJSON_PROFILE=1` to have each call print its phase timings; see
//! `PhaseTimer` in `exports`.

use extendr_api::prelude::*;
use extendr_ffi as libR_sys;
use rayon::prelude::*;
use std::ffi::{c_char, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;

// ------------------------------------------------------------------
// MODULES
//
// Split at the section boundaries this file already carried, so the history
// of each part stays readable. `lto = "fat"` with `codegen-units = 1` means
// the boundaries cost nothing: the crate is still optimised as one unit and
// inlining crosses them freely.
//
//   decls      extern declarations for the R entry points extendr omits
//   pool       the explicitly sized rayon pool, and work-aware chunking
//   config     SerializerConfig, the column and class descriptors
//   numfmt     the JSON writer, and float formatting matched to jsonlite
//   datetime   Date and POSIXct, formatted without going through R
//   capi       CHARSXP handling, encodings, names, attributes
//   serialize  the recursive writer for lists, vectors and matrices
//   columns    per-column descriptors and the cell writers
//   dfwrite    the data.frame body both entry points share
//   geometry   sfc flattening and the coordinate writers
//   exports    the #[extendr] entry points, assembly, and pretty printing
//
// Each is glob-imported, so every name resolves exactly where it did when
// this was a single file.
// ------------------------------------------------------------------
mod capi;
mod columns;
mod config;
mod datetime;
mod decls;
mod dfwrite;
mod exports;
mod geometry;
mod numfmt;
mod pool;
mod serialize;

#[allow(unused_imports)]
pub(crate) use capi::*;
pub(crate) use columns::*;
pub(crate) use config::*;
pub(crate) use datetime::*;
pub(crate) use decls::*;
pub(crate) use dfwrite::*;
pub(crate) use geometry::*;
pub(crate) use numfmt::*;
pub(crate) use pool::*;
pub(crate) use serialize::*;
