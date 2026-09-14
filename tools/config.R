# tools/config.R -- generate src/Makevars (Unix) or src/Makevars.win (Windows)
#
# Placeholders substituted into src/Makevars.in / src/Makevars.win.in:
#   @TARGET@      cargo target triple, or "" to build for the native host
#   @TARGET_FLAG@ "--target=<triple>", or "" when @TARGET@ is empty
#   @LIBDIR@      path under rust/target where libfastgeojson.a lands
#   @PROFILE@     "--release" or "" (debug)
#   @CRAN_FLAGS@  "-j 2 --offline --locked" when building offline from vendor.tar.xz
#   @CLEAN_TARGET@ the target dir to delete after linking (empty for debug builds)
#   @EXTRA_LIBS@  platform-specific extra linker flags

source("tools/msrv.R")

env_debug    <- Sys.getenv("DEBUG")
env_not_cran <- Sys.getenv("NOT_CRAN")

vendor_exists <- file.exists("src/rust/vendor.tar.xz")
is_debug      <- nzchar(env_debug)
is_not_cran   <- nzchar(env_not_cran) || is_debug

if (is_debug) message("Creating DEBUG build.")
if (!is_not_cran) message("Building for CRAN.")

is_windows <- .Platform[["OS.type"]] == "windows"
webr_target <- "wasm32-unknown-emscripten"
is_wasm <- identical(R.version$platform, webr_target)
if (is_wasm) message("Building for WebR.")

# ---------------------------------------------------------------
# Target triple
# ---------------------------------------------------------------
# Honour CARGO_BUILD_TARGET when the build environment sets it (r-universe
# does this for its aarch64 Windows runners). Otherwise ask R what it is.
# Never derive the triple from make's $(WIN), which is undefined on aarch64.
target <- Sys.getenv("CARGO_BUILD_TARGET")

if (!nzchar(target)) {
  if (is_wasm) {
    target <- webr_target
  } else if (is_windows) {
    arch <- R.version$arch
    target <- switch(
      arch,
      "x86_64"  = "x86_64-pc-windows-gnu",
      "aarch64" = "aarch64-pc-windows-gnullvm",
      "arm64"   = "aarch64-pc-windows-gnullvm",
      "i386"    = "i686-pc-windows-gnu",
      "i686"    = "i686-pc-windows-gnu",
      stop(
        "fastgeojson: unsupported Windows architecture '", arch, "'.\n",
        "Set CARGO_BUILD_TARGET to a suitable Rust target triple and retry."
      )
    )
  } else {
    # Unix-alikes: build for the native host. Letting cargo pick the host
    # triple keeps macOS/Linux behaviour identical to previous releases.
    target <- ""
  }
}

message("fastgeojson: cargo target = ", if (nzchar(target)) target else "<native host>")

.target      <- target
.target_flag <- if (nzchar(target)) paste0("--target=", target) else ""

cfg <- if (is_debug) "debug" else "release"
.libdir <- paste(c("rust/target", if (nzchar(target)) target, cfg), collapse = "/")

.profile       <- if (is_debug) "" else "--release"
.clean_target  <- if (is_debug) "" else "rust/target"
.cran_flags    <- if (!is_not_cran && vendor_exists) "-j 2 --offline --locked" else ""

# ---------------------------------------------------------------
# Platform-specific link flags
# ---------------------------------------------------------------
.extra_libs <- if (is_windows) {
  "-lws2_32 -ladvapi32 -luserenv -lbcrypt -lntdll"
} else if (identical(Sys.info()[["sysname"]], "Darwin")) {
  "-Wl,-dead_strip -Wl,-x"
} else {
  "-lpthread -ldl"
}

# ---------------------------------------------------------------
# Write the Makevars
# ---------------------------------------------------------------
mv_in  <- if (is_windows) "src/Makevars.win.in" else "src/Makevars.in"
mv_out <- if (is_windows) "src/Makevars.win"    else "src/Makevars"

if (!file.exists(mv_in)) stop("fastgeojson: missing template '", mv_in, "'.")

if (file.exists(mv_out)) {
  message("Cleaning previous `", mv_out, "`.")
  invisible(file.remove(mv_out))
}

txt <- readLines(mv_in)
subs <- c(
  "@TARGET@"       = .target,
  "@TARGET_FLAG@"  = .target_flag,
  "@LIBDIR@"       = .libdir,
  "@PROFILE@"      = .profile,
  "@CRAN_FLAGS@"   = .cran_flags,
  "@CLEAN_TARGET@" = .clean_target,
  "@EXTRA_LIBS@"   = .extra_libs
)
for (k in names(subs)) txt <- gsub(k, subs[[k]], txt, fixed = TRUE)

message("Writing `", mv_out, "`.")
con <- file(mv_out, open = "wb")
writeLines(txt, con, sep = "\n")
close(con)

message("`tools/config.R` has finished.")
