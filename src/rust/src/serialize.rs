// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// RECURSIVE SERIALIZER
// ------------------------------------------------------------------

pub(crate) unsafe fn serialize_sexp_to_json_buffer(x: libR_sys::SEXP, buf: &mut Vec<u8>, config: SerializerConfig, depth: u32) {
    if depth > MAX_DEPTH {
        // Unbounded recursion here killed the process outright rather than
        // raising a condition R could catch.
        str_state_mark(STR_DEPTH_ERROR);
        buf.extend_from_slice(b"null");
        return;
    }
    if x == libR_sys::R_NilValue {
        if config.null == NullMode::Null { buf.extend_from_slice(b"null"); }
        else { buf.extend_from_slice(b"{}"); }
        return;
    }
    // A plain vector inside a list has no attributes at all, so one ATTRIB
    // check skips both the class walk and the dim walk below.
    let has_attrs = ATTRIB(x) != libR_sys::R_NilValue;
    let cls = if has_attrs { classify(x) } else { 0 };

    // 0. Geometry columns.
    //
    // An `sfc` renders as an array of typed GeoJSON geometry objects wherever
    // it appears -- bare, in a list, at any nesting depth, or inside a
    // data.frame list column -- not only as an sf object's designated
    // geometry column. Without this it degraded to bare coordinate arrays
    // like [0,0] and the geometry type was lost. Its length is unrelated to
    // any enclosing frame's row count.
    if cls & CLS_SFC != 0 {
        let n = sexp_len(x);
        buf.push(b'[');
        for i in 0..n {
            if i > 0 {
                buf.push(b',');
            }
            render_geometry_to_bytes(libR_sys::VECTOR_ELT(x, i as isize), buf, config, 0);
        }
        buf.push(b']');
        return;
    }

    // 1. Array / Matrix Handling
    if has_attrs && cls & CLS_DATA_FRAME == 0 {
        let dim_attr = libR_sys::Rf_getAttrib(x, libR_sys::R_DimSymbol);
        if dim_attr != libR_sys::R_NilValue && sexp_len(dim_attr) > 0 {
            let dim_sexp = libR_sys::INTEGER(dim_attr);
            let n_dims = sexp_len(dim_attr);
            let mut dims = Vec::with_capacity(n_dims);
            let mut strides = Vec::with_capacity(n_dims);
            let mut current_stride = 1;
            for i in 0..n_dims {
                let d = *dim_sexp.add(i) as usize;
                dims.push(d);
                strides.push(current_stride);
                current_stride *= d;
            }
            if config.matrix_colmajor {
                // jsonlite's matrix = "columnmajor" nests by the last
                // dimension first: a 2x3 matrix becomes [[1,2],[3,4],[5,6]]
                // instead of [[1,3,5],[2,4,6]].
                dims.reverse();
                strides.reverse();
            }

            unsafe fn write_recursive(
                buf: &mut Vec<u8>, 
                dims: &[usize], 
                strides: &[usize], 
                depth: usize, 
                offset: usize,
                write_fn: &impl Fn(usize, &mut Vec<u8>)
            ) {
                let n = dims[depth];
                let stride = strides[depth];
                buf.push(b'[');
                for i in 0..n {
                    if i > 0 { buf.push(b','); }
                    let next_offset = offset + i * stride;
                    if depth == dims.len() - 1 {
                        write_fn(next_offset, buf);
                    } else {
                        write_recursive(buf, dims, strides, depth + 1, next_offset, write_fn);
                    }
                }
                buf.push(b']');
            }

            if cls & CLS_FACTOR != 0 && typeof_sexp(x) == libR_sys::SEXPTYPE::INTSXP as u32 && config.factor == FactorMode::String {
                let levels_sexp = levels_of(x);
                if levels_sexp != libR_sys::R_NilValue {
                    let n_levels = sexp_len(levels_sexp);
                    let p = libR_sys::INTEGER(x);
                    let writer = |idx: usize, b: &mut Vec<u8>| {
                        let v = *p.add(idx);
                        if is_na_int(v) || v < 1 {
                            if config.na == NaMode::String { b.extend_from_slice(b"\"NA\""); }
                            else { b.extend_from_slice(b"null"); }
                        } else {
                            let lvl_idx = (v - 1) as usize;
                            if lvl_idx < n_levels {
                                let s = libR_sys::STRING_ELT(levels_sexp, lvl_idx as isize);
                                if let Some(bytes) = charsxp_to_utf8_bytes(s) { escape_json_string_into(b, bytes); }
                                else { b.extend_from_slice(b"null"); }
                            } else { b.extend_from_slice(b"null"); }
                        }
                    };
                    write_recursive(buf, &dims, &strides, 0, 0, &writer);
                    return;
                }
            }

            let r_type = typeof_sexp(x);
            match r_type {
                 t if t == libR_sys::SEXPTYPE::INTSXP as u32 => {
                     let p = libR_sys::INTEGER(x);
                     let writer = |idx: usize, b: &mut Vec<u8>| {
                         let v = *p.add(idx);
                         if is_na_int(v) {
                             if config.na == NaMode::String || config.na == NaMode::Smart { b.extend_from_slice(b"\"NA\""); }
                             else { b.extend_from_slice(b"null"); }
                         } else {
                             let mut tmp = itoa::Buffer::new();
                             b.extend_from_slice(tmp.format(v).as_bytes());
                         }
                     };
                     write_recursive(buf, &dims, &strides, 0, 0, &writer);
                     return;
                 },
                 t if t == libR_sys::SEXPTYPE::REALSXP as u32 => {
                     let p = libR_sys::REAL(x);
                     let writer = |idx: usize, b: &mut Vec<u8>| {
                         let v = *p.add(idx);
                         // `is_na_real || is_nan_real` is just `is_nan`, so the whole guard
                // reduces to one is_finite test -- and the common path then does
                // no NA inspection at all.
                if !v.is_finite() {
                             if config.na == NaMode::String || config.na == NaMode::Smart {
                                 if v == f64::INFINITY { b.extend_from_slice(b"\"Inf\""); }
                                 else if v == f64::NEG_INFINITY { b.extend_from_slice(b"\"-Inf\""); }
                                 else if is_nan_real(v) { b.extend_from_slice(b"\"NaN\""); }
                                 else { b.extend_from_slice(b"\"NA\""); }
                             } else { b.extend_from_slice(b"null"); }
                         } else {
                             write_f64_json(b, v, config.digits, config.always_decimal);
                         }
                     };
                     write_recursive(buf, &dims, &strides, 0, 0, &writer);
                     return;
                 },
                 t if t == libR_sys::SEXPTYPE::LGLSXP as u32 => {
                     let p = libR_sys::LOGICAL(x);
                     let writer = |idx: usize, b: &mut Vec<u8>| {
                         let v = *p.add(idx);
                         if is_na_int(v) {
                             if config.na == NaMode::String { b.extend_from_slice(b"\"NA\""); }
                             else { b.extend_from_slice(b"null"); }
                         } else if v != 0 { b.extend_from_slice(b"true"); }
                         else { b.extend_from_slice(b"false"); }
                     };
                     write_recursive(buf, &dims, &strides, 0, 0, &writer);
                     return;
                 },
                 t if t == libR_sys::SEXPTYPE::STRSXP as u32 => {
                     let writer = |idx: usize, b: &mut Vec<u8>| {
                         let s_sexp = libR_sys::STRING_ELT(x, idx as isize);
                         if is_na_string(s_sexp) {
                             if config.na == NaMode::String { b.extend_from_slice(b"\"NA\""); }
                             else { b.extend_from_slice(b"null"); }
                         } else if let Some(bytes) = charsxp_to_utf8_bytes(s_sexp) {
                             escape_json_string_into(b, bytes);
                         } else { b.extend_from_slice(b"null"); }
                     };
                     write_recursive(buf, &dims, &strides, 0, 0, &writer);
                     return;
                 },
                 t if t == libR_sys::SEXPTYPE::VECSXP as u32 => {
                     let writer = |idx: usize, b: &mut Vec<u8>| {
                         let val_sexp = libR_sys::VECTOR_ELT(x, idx as isize);
                         serialize_sexp_to_json_buffer(val_sexp, b, config, depth + 1);
                     };
                     write_recursive(buf, &dims, &strides, 0, 0, &writer);
                     return;
                 },
                 _ => {}
            }
        }
    }

    // 2. Linear Vector / Data Frame Handling
    if cls & CLS_FACTOR != 0 && typeof_sexp(x) == libR_sys::SEXPTYPE::INTSXP as u32 {
        if config.factor == FactorMode::String {
            let do_unbox = config.auto_unbox && sexp_len(x) == 1 && cls & CLS_ASIS == 0;
            let levels_sexp = levels_of(x);
            if levels_sexp != libR_sys::R_NilValue {
                let n_levels = sexp_len(levels_sexp);
                let n = sexp_len(x);
                let p = libR_sys::INTEGER(x);
                if !do_unbox { buf.push(b'['); }
                for i in 0..n {
                    if i > 0 { buf.push(b','); }
                    let v = *p.add(i);
                    if is_na_int(v) || v < 1 { 
                        if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                        else { buf.extend_from_slice(b"null"); }
                    } else {
                        let idx = (v - 1) as usize;
                        if idx < n_levels {
                            let level_charsxp = libR_sys::STRING_ELT(levels_sexp, idx as isize);
                            if let Some(bytes) = charsxp_to_utf8_bytes(level_charsxp) { escape_json_string_into(buf, bytes); }
                            else { buf.extend_from_slice(b"null"); }
                        } else { buf.extend_from_slice(b"null"); }
                    }
                }
                if !do_unbox { buf.push(b']'); }
                return;
            }
        }
    }

    if cls & CLS_DATE != 0 {
        // Formatted here rather than by R. Reached for a bare Date vector and
        // for a Date nested anywhere inside a list, which is why it has to
        // honour auto_unbox and AsIs the same way the atomic arms below do.
        let r_type = typeof_sexp(x);
        if r_type == libR_sys::SEXPTYPE::REALSXP as u32
            || r_type == libR_sys::SEXPTYPE::INTSXP as u32
        {
            let n = sexp_len(x);
            let do_unbox = config.auto_unbox && n == 1 && cls & CLS_ASIS == 0;
            if !do_unbox {
                buf.push(b'[');
            }
            if r_type == libR_sys::SEXPTYPE::REALSXP as u32 {
                let p = libR_sys::REAL(x);
                for i in 0..n {
                    if i > 0 {
                        buf.push(b',');
                    }
                    write_date_cell(buf, date_cell(*p.add(i)), config.na);
                }
            } else {
                let p = libR_sys::INTEGER(x);
                for i in 0..n {
                    if i > 0 {
                        buf.push(b',');
                    }
                    write_date_cell(buf, date_cell_i32(*p.add(i)), config.na);
                }
            }
            if !do_unbox {
                buf.push(b']');
            }
            return;
        }
    }

    if cls & CLS_FGJTIME != 0 && typeof_sexp(x) == libR_sys::SEXPTYPE::REALSXP as u32 {
        let fmt = fgj_fmt_code(x).unwrap_or(TFMT_SPACE);
        let n = sexp_len(x);
        let do_unbox = config.auto_unbox && n == 1 && cls & CLS_ASIS == 0;
        let p = libR_sys::REAL(x);
        if !do_unbox {
            buf.push(b'[');
        }
        for i in 0..n {
            if i > 0 {
                buf.push(b',');
            }
            write_time_cell(buf, *p.add(i), fmt, config.na);
        }
        if !do_unbox {
            buf.push(b']');
        }
        return;
    }

    if cls & CLS_POSIXT != 0 {
        // POSIXt is pre-encoded in R, because resolving a time zone needs R's
        // own database; this is the defensive path, so paying for an Robj here
        // costs nothing measurable.
        if let Ok(char_robj) = call!("format", Robj::from_sexp(x)) {
            serialize_sexp_to_json_buffer(char_robj.get(), buf, config, depth + 1);
            return;
        }
    }

    let r_type = typeof_sexp(x);
    // `sexp_len` of a data.frame is its COLUMN count, so a one-column frame
    // used to satisfy this and lose the brackets around its row array,
    // emitting `{"a":1},{"a":2},{"a":3}` with no enclosing `[]` -- structurally
    // invalid JSON. jsonlite never unboxes a data.frame, only the `scalar`
    // class, so exclude them outright.
    let do_unbox = config.auto_unbox
        && sexp_len(x) == 1
        && cls & CLS_ASIS == 0
        && cls & CLS_DATA_FRAME == 0;

    if r_type == libR_sys::SEXPTYPE::INTSXP as u32 {
        let n = sexp_len(x);
        let p = libR_sys::INTEGER(x);
        if !do_unbox { buf.push(b'['); }
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let v = *p.add(i);
            if is_na_int(v) { 
                if config.na == NaMode::String || config.na == NaMode::Smart { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            } else {
                let mut tmp = itoa::Buffer::new();
                buf.extend_from_slice(tmp.format(v).as_bytes());
            }
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }

    if r_type == libR_sys::SEXPTYPE::REALSXP as u32 {
        let n = sexp_len(x);
        let p = libR_sys::REAL(x);
        if !do_unbox { buf.push(b'['); }
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let v = *p.add(i);
            // `is_na_real || is_nan_real` is just `is_nan`, so the whole guard
                // reduces to one is_finite test -- and the common path then does
                // no NA inspection at all.
                if !v.is_finite() { 
                if config.na == NaMode::String || config.na == NaMode::Smart {
                    if v == f64::INFINITY { buf.extend_from_slice(b"\"Inf\""); }
                    else if v == f64::NEG_INFINITY { buf.extend_from_slice(b"\"-Inf\""); }
                    else if is_nan_real(v) { buf.extend_from_slice(b"\"NaN\""); }
                    else { buf.extend_from_slice(b"\"NA\""); }
                } else { buf.extend_from_slice(b"null"); }
            } else {
                write_f64_json(buf, v, config.digits, config.always_decimal);
            }
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }

    if r_type == libR_sys::SEXPTYPE::LGLSXP as u32 {
        let n = sexp_len(x);
        let p = libR_sys::LOGICAL(x);
        if !do_unbox { buf.push(b'['); }
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let v = *p.add(i);
            if is_na_int(v) { 
                if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            }
            else if v != 0 { buf.extend_from_slice(b"true"); }
            else { buf.extend_from_slice(b"false"); }
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }

    // [MODIFICATION: Passthrough Support for Generic Vectors]
    if r_type == libR_sys::SEXPTYPE::STRSXP as u32 {
        let is_json = cls & CLS_JSON != 0;
        let n = sexp_len(x);
        // jsonlite's asJSON("json") returns the text verbatim and never
        // collapses it into an array, so a length-one `json` value must not
        // gain surrounding brackets.
        let json_scalar = is_json && n == 1;
        let do_unbox = do_unbox || json_scalar;
        if !do_unbox { buf.push(b'['); }
        for i in 0..n {
            if i > 0 { buf.push(b','); }
            let s_sexp = libR_sys::STRING_ELT(x, i as isize);
            if is_na_string(s_sexp) { 
                if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            }
            else if let Some(bytes) = charsxp_to_utf8_bytes(s_sexp) { 
                if is_json { buf.extend_from_slice(bytes); } // Passthrough
                else { escape_json_string_into(buf, bytes); } // Escape
            }
            else { buf.extend_from_slice(b"null"); }
        }
        if !do_unbox { buf.push(b']'); }
        return;
    }

    if r_type == libR_sys::SEXPTYPE::VECSXP as u32 {
        if cls & CLS_DATA_FRAME != 0 {
            let n_cols = sexp_len(x);
            if config.df == DfMode::Columns {
                buf.push(b'{');
                let names_sym = libR_sys::R_NamesSymbol;
                let names_sexp = libR_sys::Rf_getAttrib(x, names_sym);
                let has_names = names_sexp != libR_sys::R_NilValue && sexp_len(names_sexp) == n_cols;
                let mut first = true;
                for c in 0..n_cols {
                    if !first { buf.push(b','); }
                    if has_names {
                        let key_charsxp = libR_sys::STRING_ELT(names_sexp, c as isize);
                        if !is_na_string(key_charsxp) {
                            if let Some(key_bytes) = charsxp_to_utf8_bytes(key_charsxp) { escape_json_string_into(buf, key_bytes); }
                            else { buf.extend_from_slice(b"\"\""); }
                        } else { buf.extend_from_slice(b"\"\""); }
                    } else { buf.extend_from_slice(b"\"\""); }
                    buf.push(b':');
                    let col_sexp = libR_sys::VECTOR_ELT(x, c as isize);
                    buf.push(b'[');
                    let n_rows_inner = sexp_len(col_sexp);
                    for r in 0..n_rows_inner {
                        if r > 0 { buf.push(b','); }
                        serialize_element_at_index(col_sexp, r, buf, config, depth + 1);
                    }
                    buf.push(b']');
                    first = false;
                }
                buf.push(b'}');
                return;
            }

            let n_rows = get_df_nrows(x);
            let names_sym = libR_sys::R_NamesSymbol;
            let names_sexp = libR_sys::Rf_getAttrib(x, names_sym);
            let has_names = names_sexp != libR_sys::R_NilValue && sexp_len(names_sexp) == n_cols;
            let rn_sexp = libR_sys::Rf_getAttrib(x, libR_sys::R_RowNamesSymbol);

            if !do_unbox { buf.push(b'['); }
            for r in 0..n_rows {
                if r > 0 { buf.push(b','); }
                buf.push(b'{');
                let mut needs_comma = false;
                for c in 0..n_cols {
                    let col_sexp = libR_sys::VECTOR_ELT(x, c as isize);
                    let col_len = sexp_len(col_sexp);
                    if r < col_len {
                        if needs_comma { buf.push(b','); }
                        if has_names {
                            let key_charsxp = libR_sys::STRING_ELT(names_sexp, c as isize);
                            if !is_na_string(key_charsxp) {
                                if let Some(key_bytes) = charsxp_to_utf8_bytes(key_charsxp) { escape_json_string_into(buf, key_bytes); }
                                else { buf.extend_from_slice(b"\"\""); }
                            } else { buf.extend_from_slice(b"\"\""); }
                        } else { buf.extend_from_slice(b"\"\""); }
                        buf.push(b':');
                        serialize_element_at_index(col_sexp, r, buf, config, depth + 1);
                        needs_comma = true;
                    }
                }
                // A nested data.frame carries its row names too, exactly as a
                // top-level one does. Omitting this dropped `_row` for any
                // frame that was not the outermost object.
                if !is_default_rownames(rn_sexp) && r < sexp_len(rn_sexp) {
                    if needs_comma { buf.push(b','); }
                    buf.extend_from_slice(br#""_row":"#);
                    write_rowname_at(rn_sexp, r, buf);
                }
                buf.push(b'}');
            }
            if !do_unbox { buf.push(b']'); }
            return;
        }

        let n = sexp_len(x);
        let names_sym = libR_sys::R_NamesSymbol;
        let names_sexp = libR_sys::Rf_getAttrib(x, names_sym);
        let has_names = names_sexp != libR_sys::R_NilValue && sexp_len(names_sexp) == n;
        // One base pointer for the whole list instead of a VECTOR_ELT call
        // per element.
        let elems = VECTOR_PTR_RO(x);

        if has_names {
            buf.push(b'{');
            for i in 0..n {
                if i > 0 { buf.push(b','); }
                let key_charsxp = libR_sys::STRING_ELT(names_sexp, i as isize);
                if !is_na_string(key_charsxp) {
                    if let Some(key_bytes) = charsxp_to_utf8_bytes(key_charsxp) { escape_json_string_into(buf, key_bytes); }
                    else { buf.extend_from_slice(b"\"\""); }
                } else { buf.extend_from_slice(b"\"\""); }
                buf.push(b':');
                serialize_sexp_to_json_buffer(*elems.add(i), buf, config, depth + 1);
            }
            buf.push(b'}');
        } else {
            buf.push(b'[');
            for i in 0..n {
                if i > 0 { buf.push(b','); }
                serialize_sexp_to_json_buffer(*elems.add(i), buf, config, depth + 1);
            }
            buf.push(b']');
        }
        return;
    }
    buf.extend_from_slice(b"{}");
}

pub(crate) unsafe fn serialize_element_at_index(col: libR_sys::SEXP, idx: usize, buf: &mut Vec<u8>, config: SerializerConfig, depth: u32) {
    let r_type = typeof_sexp(col);
    // Was one Robj construction per CELL.
    let cls = classify(col);
    
    if cls & CLS_FACTOR != 0 && r_type == libR_sys::SEXPTYPE::INTSXP as u32 {
        if config.factor == FactorMode::String {
            let levels_sexp = levels_of(col);
            if levels_sexp != libR_sys::R_NilValue {
                let p = libR_sys::INTEGER(col);
                let v = *p.add(idx);
                if is_na_int(v) || v < 1 { 
                    if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                    else { buf.extend_from_slice(b"null"); }
                } else {
                    let lvl_idx = (v - 1) as usize;
                    if lvl_idx < sexp_len(levels_sexp) {
                        let s = libR_sys::STRING_ELT(levels_sexp, lvl_idx as isize);
                        if let Some(bytes) = charsxp_to_utf8_bytes(s) { escape_json_string_into(buf, bytes); }
                        else { buf.extend_from_slice(b"null"); }
                    } else { buf.extend_from_slice(b"null"); }
                }
                return;
            }
        }
    }

    match r_type {
        t if t == libR_sys::SEXPTYPE::INTSXP as u32 => {
            let v = *libR_sys::INTEGER(col).add(idx);
            if is_na_int(v) { 
                if config.na == NaMode::String || config.na == NaMode::Smart { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            } else {
                let mut tmp = itoa::Buffer::new();
                buf.extend_from_slice(tmp.format(v).as_bytes());
            }
        },
        t if t == libR_sys::SEXPTYPE::REALSXP as u32 => {
            let v = *libR_sys::REAL(col).add(idx);
            // `is_na_real || is_nan_real` is just `is_nan`, so the whole guard
                // reduces to one is_finite test -- and the common path then does
                // no NA inspection at all.
                if !v.is_finite() { 
                if config.na == NaMode::String || config.na == NaMode::Smart {
                    if v == f64::INFINITY { buf.extend_from_slice(b"\"Inf\""); }
                    else if v == f64::NEG_INFINITY { buf.extend_from_slice(b"\"-Inf\""); }
                    else if is_nan_real(v) { buf.extend_from_slice(b"\"NaN\""); }
                    else { buf.extend_from_slice(b"\"NA\""); }
                } else { buf.extend_from_slice(b"null"); }
            } else {
                write_f64_json(buf, v, config.digits, config.always_decimal);
            }
        },
        t if t == libR_sys::SEXPTYPE::LGLSXP as u32 => {
            let v = *libR_sys::LOGICAL(col).add(idx);
            if is_na_int(v) { 
                if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            }
            else if v != 0 { buf.extend_from_slice(b"true"); }
            else { buf.extend_from_slice(b"false"); }
        },
        // [MODIFICATION: Passthrough Support for Rows]
        t if t == libR_sys::SEXPTYPE::STRSXP as u32 => {
            let s = libR_sys::STRING_ELT(col, idx as isize);
            if is_na_string(s) { 
                if config.na == NaMode::String { buf.extend_from_slice(b"\"NA\""); }
                else { buf.extend_from_slice(b"null"); }
            }
            else if let Some(bytes) = charsxp_to_utf8_bytes(s) { 
                if cls & CLS_JSON != 0 { buf.extend_from_slice(bytes); } // Passthrough
                else { escape_json_string_into(buf, bytes); } // Escape
            }
            else { buf.extend_from_slice(b"null"); }
        },
        t if t == libR_sys::SEXPTYPE::VECSXP as u32 => {
             let val = libR_sys::VECTOR_ELT(col, idx as isize);
             serialize_sexp_to_json_buffer(val, buf, config, depth + 1);
        },
        _ => buf.extend_from_slice(b"null"),
    }
}

