// We need to forward routine registration from C to Rust
// to avoid the linker removing the static library.

#include <R_ext/Rdynload.h>

void R_init_fastgeojson_extendr(void *dll);
void fastgeojson_release_pool(void);
void fastgeojson_quiet_panics(void);

void R_init_fastgeojson(DllInfo *dll) {
    R_init_fastgeojson_extendr(dll);

    // Registered symbols only. extendr already calls
    // R_useDynamicSymbols(FALSE), which is what R-exts recommends, but it
    // also calls R_forceSymbols(FALSE), which the manual describes as
    // usually not what you want: with TRUE, `.Call` will not accept an entry
    // point named by a character string, so a typo is a load-time error
    // rather than a call-time one. Must come AFTER the extendr call, which
    // sets it the other way. Safe here because the generated wrappers were
    // built with use_symbols = TRUE and pass the symbol object.
    R_forceSymbols(dll, TRUE);

    // extendr raises an R error by panicking and catching it at the boundary,
    // so Rust's default hook printed a panic trace ahead of every ordinary
    // error. See fastgeojson_quiet_panics.
    fastgeojson_quiet_panics();
}

// R calls this when the DLL is unloaded. The worker pool is a process-wide
// static holding live threads; without this, dyn.unload() would unmap the
// code they are running.
void R_unload_fastgeojson(DllInfo *dll) {
    (void) dll;
    fastgeojson_release_pool();
}
