\## Test environments

\* local Windows 11 install, R 4.5.0

\* R-universe (Windows, Mac, Linux)

\* win-builder (devel and release)



\## R CMD check results

0 errors | 0 warnings | 1 note



1\. New submission

2\. Possibly misspelled words in DESCRIPTION: 'GeoJSON'

&nbsp;  \* 'GeoJSON' is the correct capitalization for the geospatial data interchange format.



\## Compiled code note: `\_abort` symbol (macOS only)



This package includes Rust code via the `extendr` interface, which builds

a static library (`libfastgeojson.a`) that is linked into the final

shared object.



On \*\*macOS only\*\*, R CMD check reports that the compiled library contains

references to `\_abort`. Investigation shows these originate from Rust’s

LLVM `compiler\_builtins` runtime — not from any function exported to R.



No `\_abort` or `panic!()` paths are invoked by package code. Rust

functions exported to R:



\* use `panic = "unwind"` to prevent panics escaping the FFI boundary

\* do not call `abort()` explicitly

\* are linked with `-Wl,-dead\_strip` to remove unused code on macOS



The symbol persists only as \*\*unreferenced toolchain support code\*\*, and

does not execute in practice. This warning does \*\*not\*\* appear on Linux

or Windows builds.



\## Downstream dependencies



None — this is a new submission.

