// Split out of lib.rs; see the module list there. Nothing here is
// changed from the single-file version beyond crate visibility.
#![allow(clippy::all)]

use crate::*;

// ------------------------------------------------------------------
// GEOMETRY & WORKER FUNCTIONS
// ------------------------------------------------------------------

pub(crate) fn detect_sfc_type_sexp(geom_col_sexp: libR_sys::SEXP) -> SfcType {
    unsafe {
        let classes = libR_sys::Rf_getAttrib(geom_col_sexp, libR_sys::R_ClassSymbol);
        if classes != libR_sys::R_NilValue {
            let n = sexp_len(classes);
            for i in 0..n {
                let s = libR_sys::STRING_ELT(classes, i as isize);
                let p = libR_sys::R_CHAR(s);
                let c_str = CStr::from_ptr(p as *const c_char);
                match c_str.to_str().unwrap_or("") {
                    "sfc_POINT" => return SfcType::Point,
                    "sfc_MULTIPOINT" => return SfcType::MultiPoint,
                    "sfc_LINESTRING" => return SfcType::LineString,
                    "sfc_MULTILINESTRING" => return SfcType::MultiLineString,
                    "sfc_POLYGON" => return SfcType::Polygon,
                    "sfc_MULTIPOLYGON" => return SfcType::MultiPolygon,
                    "sfc_GEOMETRY" | "sfc_GEOMETRYCOLLECTION" => return SfcType::GeometryCollection,
                    _ => continue,
                }
            }
        }
    }
    SfcType::Unknown
}

pub(crate) fn get_row_sfg_type(sfg: libR_sys::SEXP) -> SfcType {
    unsafe {
        let classes = libR_sys::Rf_getAttrib(sfg, libR_sys::R_ClassSymbol);
        if classes != libR_sys::R_NilValue {
            let n = sexp_len(classes);
            for i in 0..n {
                let s = libR_sys::STRING_ELT(classes, i as isize);
                let p = libR_sys::R_CHAR(s);
                let c_str = CStr::from_ptr(p as *const c_char);
                match c_str.to_str().unwrap_or("") {
                    "POINT" => return SfcType::Point,
                    "MULTIPOINT" => return SfcType::MultiPoint,
                    "LINESTRING" => return SfcType::LineString,
                    "MULTILINESTRING" => return SfcType::MultiLineString,
                    "POLYGON" => return SfcType::Polygon,
                    "MULTIPOLYGON" => return SfcType::MultiPolygon,
                    // Without this arm a GEOMETRYCOLLECTION fell through to
                    // Unknown and its geometry was silently emitted as null.
                    "GEOMETRYCOLLECTION" => return SfcType::GeometryCollection,
                    _ => continue,
                }
            }
        }
    }
    SfcType::Unknown
}

/// Is `x` a plain double vector whose data pointer can simply be read?
///
/// `REAL()` raises an R error on any other type, and on an ALTREP vector it
/// materialises the data, which allocates and can run R code. Both would be
/// fatal in a worker, and the error longjmps over Rust frames even on the R
/// thread -- it leaked about 1.8 KB per malformed geometry before this.
#[inline]
pub(crate) unsafe fn is_plain_real(x: libR_sys::SEXP) -> bool {
    typeof_sexp(x) == libR_sys::SEXPTYPE::REALSXP as u32 && ALTREP(x) == 0
}

/// `read_coord_ptr` for a geometry known to carry no `dim`: a POINT, whose
/// ordinates are a plain numeric vector.
#[inline]
pub(crate) unsafe fn read_point_coord_ptr(x: libR_sys::SEXP) -> Option<CoordPtr> {
    if !is_plain_real(x) {
        return None;
    }
    let len = sexp_len(x);
    Some(CoordPtr { ptr: libR_sys::REAL(x) as usize, len, ncol: len.max(1) })
}

/// Reads an sfg coordinate matrix (or bare vector) into a thread-safe pointer,
/// or `None` if it is not a plain double vector.
///
/// `ncol` comes from the `dim` attribute, so XYZ/XYM/XYZM geometries carry all
/// their ordinates. A bare vector (as POINT uses) reports ncol equal to its
/// length, i.e. a single row.
pub(crate) unsafe fn read_coord_ptr(x: libR_sys::SEXP) -> Option<CoordPtr> {
    if !is_plain_real(x) {
        return None;
    }
    let len = sexp_len(x);
    let dim = attrib_by_tag(x, libR_sys::R_DimSymbol);
    let ncol = if dim != libR_sys::R_NilValue && sexp_len(dim) == 2 {
        let d = libR_sys::INTEGER(dim);
        let c = *d.add(1);
        if c > 0 { c as usize } else { 1 }
    } else {
        // No dim: treat the whole vector as one row.
        len.max(1)
    };
    Some(CoordPtr { ptr: libR_sys::REAL(x) as usize, len, ncol })
}

/// Renders a GEOMETRYCOLLECTION (or any geometry) to bytes on the R thread.
///
/// GEOMETRYCOLLECTION uses a recursive `geometries` array in place of
/// `coordinates`, and walking it needs `VECTOR_ELT`, so it cannot be done from
/// a rayon worker. These are rare enough that pre-rendering costs nothing.
pub(crate) unsafe fn render_geometry_to_bytes(
    sfg: libR_sys::SEXP,
    buf: &mut Vec<u8>,
    config: SerializerConfig,
    depth: u32,
) {
    if sfg == libR_sys::R_NilValue || depth > 64 {
        buf.extend_from_slice(b"null");
        return;
    }
    let typ = get_row_sfg_type(sfg);
    match typ {
        SfcType::GeometryCollection => {
            buf.extend_from_slice(br#"{"type":"GeometryCollection","geometries":["#);
            let n = sexp_len(sfg);
            for i in 0..n {
                if i > 0 {
                    buf.push(b',');
                }
                render_geometry_to_bytes(
                    libR_sys::VECTOR_ELT(sfg, i as isize),
                    buf,
                    config,
                    depth + 1,
                );
            }
            buf.extend_from_slice(b"]}");
        }
        SfcType::Point => {
            buf.extend_from_slice(br#"{"type":"Point","coordinates":"#);
            match read_coord_ptr(sfg) {
                Some(cp) => write_point_coords(buf, &cp, config),
                // Not a double vector. jsonlite serialises the coordinate
                // object with its generic writer, so integers stay integers
                // and strings stay strings; match that rather than erroring.
                None => serialize_sexp_to_json_buffer(sfg, buf, config, depth + 1),
            }
            buf.push(b'}');
        }
        SfcType::MultiPoint | SfcType::LineString => {
            buf.extend_from_slice(if typ == SfcType::MultiPoint {
                br#"{"type":"MultiPoint","coordinates":"#
            } else {
                br#"{"type":"LineString","coordinates":"#
            });
            match read_coord_ptr(sfg) {
                Some(cp) => write_coord_matrix(buf, &cp, config),
                None => serialize_sexp_to_json_buffer(sfg, buf, config, depth + 1),
            }
            buf.push(b'}');
        }
        SfcType::MultiLineString | SfcType::Polygon => {
            buf.extend_from_slice(if typ == SfcType::Polygon {
                br#"{"type":"Polygon","coordinates":["#
            } else {
                br#"{"type":"MultiLineString","coordinates":["#
            });
            let n = sexp_len(sfg);
            for i in 0..n {
                if i > 0 {
                    buf.push(b',');
                }
                let el = libR_sys::VECTOR_ELT(sfg, i as isize);
                match read_coord_ptr(el) {
                    Some(cp) => write_coord_matrix(buf, &cp, config),
                    None => serialize_sexp_to_json_buffer(el, buf, config, depth + 1),
                }
            }
            buf.extend_from_slice(b"]}");
        }
        SfcType::MultiPolygon => {
            buf.extend_from_slice(br#"{"type":"MultiPolygon","coordinates":["#);
            let n_polys = sexp_len(sfg);
            for i in 0..n_polys {
                if i > 0 {
                    buf.push(b',');
                }
                let poly = libR_sys::VECTOR_ELT(sfg, i as isize);
                buf.push(b'[');
                let n_rings = sexp_len(poly);
                for k in 0..n_rings {
                    if k > 0 {
                        buf.push(b',');
                    }
                    let el = libR_sys::VECTOR_ELT(poly, k as isize);
                    match read_coord_ptr(el) {
                        Some(cp) => write_coord_matrix(buf, &cp, config),
                        None => serialize_sexp_to_json_buffer(el, buf, config, depth + 1),
                    }
                }
                buf.push(b']');
            }
            buf.extend_from_slice(b"]}");
        }
        SfcType::Unknown => buf.extend_from_slice(b"null"),
    }
}

/// Merges the per-chunk descriptors into one set, rebasing their indices.
///
/// Each chunk's `FastGeom` entries index that chunk's own `coords`, `counts`
/// and `raw`, so concatenating the arrays means shifting the indices by the
/// preceding chunks' lengths. The point of doing it is that the serialization
/// pass can then choose its own row boundaries, independent of the ones
/// extraction used.
pub(crate) fn merge_chunk_geoms(
    chunks: Vec<(usize, usize, usize, ChunkGeoms)>,
) -> (GeometryBatch, Vec<FastGeom>) {
    let n: usize = chunks.iter().map(|(_, _, _, cg)| cg.geoms.len()).sum();
    let mut batch = GeometryBatch {
        coords: Vec::with_capacity(chunks.iter().map(|(_, _, _, c)| c.batch.coords.len()).sum()),
        counts: Vec::with_capacity(chunks.iter().map(|(_, _, _, c)| c.batch.counts.len()).sum()),
        raw: Vec::with_capacity(chunks.iter().map(|(_, _, _, c)| c.batch.raw.len()).sum()),
    };
    let mut geoms = Vec::with_capacity(n);
    for (_, _, _, cg) in chunks {
        let cb = batch.coords.len() as u32;
        let ctb = batch.counts.len() as u32;
        let rb = batch.raw.len() as u32;
        batch.coords.extend(cg.batch.coords);
        batch.counts.extend(cg.batch.counts);
        batch.raw.extend(cg.batch.raw);
        for g in cg.geoms {
            geoms.push(match g {
                FastGeom::FlatList { start, len, typ } => FastGeom::FlatList {
                    start: start + cb,
                    len,
                    typ,
                },
                FastGeom::MultiPolygon { coords_start, counts_start, n_polys } => {
                    FastGeom::MultiPolygon {
                        coords_start: coords_start + cb,
                        counts_start: counts_start + ctb,
                        n_polys,
                    }
                }
                FastGeom::Prerendered { start, len } => FastGeom::Prerendered {
                    start: start + rb,
                    len,
                },
                other => other,
            });
        }
    }
    (batch, geoms)
}

/// Ordinates a geometry will emit, which is what its serialization costs.
pub(crate) fn geom_ordinates(g: &FastGeom, batch: &GeometryBatch) -> usize {
    match g {
        FastGeom::Null => 1,
        FastGeom::Point(cp) | FastGeom::Single(cp, _) => cp.len.max(1),
        FastGeom::FlatList { start, len, .. } => {
            let s = *start as usize;
            let e = (s + *len as usize).min(batch.coords.len());
            batch.coords[s.min(e)..e].iter().map(|c| c.len).sum::<usize>().max(1)
        }
        FastGeom::MultiPolygon { coords_start, counts_start, n_polys } => {
            let ns = *counts_start as usize;
            let ne = (ns + *n_polys as usize).min(batch.counts.len());
            let rings: usize = batch.counts[ns.min(ne)..ne].iter().sum();
            let cs = *coords_start as usize;
            let ce = (cs + rings).min(batch.coords.len());
            batch.coords[cs.min(ce)..ce].iter().map(|c| c.len).sum::<usize>().max(1)
        }
        // Already bytes; divide by roughly the bytes an ordinate occupies so
        // the two kinds of work are on one scale.
        FastGeom::Prerendered { len, .. } => (*len as usize / 8).max(1),
    }
}

/// Row ranges carrying roughly equal work, as `(id, start, end)`.
///
/// Equal-row chunks assume every feature costs the same. A layer with a few
/// large geometries among many small ones breaks that badly: uniform polygons
/// scaled 15.9x on 32 workers while 9990 small polygons plus 10 large ones
/// scaled 1.6x, because the large ones sat in one chunk. Splitting on
/// cumulative ordinates instead gives a very large geometry a chunk of its
/// own.
pub(crate) fn weighted_ranges(work: &[usize], target_chunks: usize) -> Vec<(usize, usize, usize)> {
    let n = work.len();
    if n == 0 {
        return Vec::new();
    }
    let target_chunks = target_chunks.max(1).min(n);
    let total: usize = work.iter().sum();
    let per = (total / target_chunks).max(1);
    let mut out: Vec<(usize, usize, usize)> = Vec::with_capacity(target_chunks + 1);
    let mut start = 0usize;
    let mut acc = 0usize;
    for i in 0..n {
        acc += work[i];
        // Close the chunk as soon as it carries its share, with no condition
        // on how many rows remain. An earlier version refused to close a
        // chunk unless enough rows were left to fill the remaining chunk
        // count, which is precisely wrong for this: the large geometries in
        // the test layers sit at the end of the vector, so that guard swept
        // all of them into one final chunk and the partition did nothing.
        if acc >= per {
            out.push((out.len(), start, i + 1));
            start = i + 1;
            acc = 0;
        }
    }
    if start < n {
        out.push((out.len(), start, n));
    }
    out
}

/// Descriptors for one chunk of an sfc.
///
/// `pending` holds the chunk-local indices of geometries that could not be
/// described from pure reads: GEOMETRYCOLLECTION, an unrecognised class, or
/// coordinates that are not plain double vectors. Rendering those needs
/// `Rf_translateCharUTF8`, which allocates on R's vmax stack, so the caller
/// finishes them on the R thread. Everything else touches only SEXP headers,
/// attribute pairlists and data pointers, which is what lets this whole pass
/// run in the worker pool.
pub(crate) struct ChunkGeoms {
    pub(crate) batch: GeometryBatch,
    pub(crate) geoms: Vec<FastGeom>,
    pub(crate) pending: Vec<usize>,
}

pub(crate) fn extract_geometries_chunk(
    geom_col: libR_sys::SEXP,
    sfc_type: SfcType,
    start: usize,
    end: usize,
    config: SerializerConfig,
) -> ChunkGeoms {
    // `config` is unused now that nothing is rendered here, but keeping it in
    // the signature keeps the call sites stable.
    let _ = config;
    let capacity_est = end - start;
    let mut batch = GeometryBatch {
        coords: Vec::with_capacity(capacity_est * 2),
        counts: Vec::with_capacity(capacity_est),
        raw: Vec::new(),
    };
    let mut out = Vec::with_capacity(capacity_est);
    let mut pending: Vec<usize> = Vec::new();
    // Defence in depth: `VECTOR_ELT` past the end of the list dereferences a
    // wild SEXP and segfaults the R process. The caller also rejects such
    // objects up front, but never index out of range from here.
    let geom_len = unsafe { sexp_len(geom_col) };
    // One call for the whole column instead of a VECTOR_ELT per feature. At a
    // million points those calls were most of this pass.
    let sfc_elems = unsafe { list_elems(geom_col) };

    for i in start..end {
        if i >= geom_len {
            out.push(FastGeom::Null);
            continue;
        }
        let sfg = match sfc_elems {
            Some(p) => unsafe { *p.add(i) },
            None => unsafe { libR_sys::VECTOR_ELT(geom_col, i as isize) },
        };
        if sfg == unsafe { libR_sys::R_NilValue } {
            out.push(FastGeom::Null);
            continue;
        }
        // The sfc's own class is only a hint: a homogeneous sfc_POINT column
        // can still be indexed per element, and sfc_GEOMETRY holds mixed
        // types. jsonlite reads class(sfg)[2] for every element, so match that
        // whenever the column class is not a reliable single type.
        let row_type = if sfc_type == SfcType::GeometryCollection || sfc_type == SfcType::Unknown {
            get_row_sfg_type(sfg)
        } else {
            sfc_type
        };

        // Anything whose coordinates are not plain double vectors is handed
        // to the recursive writer instead, which is what jsonlite does with
        // them, and is the only way to avoid REAL() raising an R error. That
        // writer needs the R thread, so record the row and move on.
        macro_rules! prerender {
            () => {{
                pending.push(out.len());
                out.push(FastGeom::Null);
                continue;
            }};
        }

        match row_type {
            SfcType::Point => {
                // A POINT sfg is a bare numeric vector of ordinates with no
                // `dim`, so the attribute lookup read_coord_ptr does can be
                // skipped -- and Rf_getAttrib walks the attribute pairlist,
                // which for a million points is a million walks.
                match unsafe { read_point_coord_ptr(sfg) } {
                    // Including zero ordinates, which emits
                    // {"type":"Point","coordinates":[]} as jsonlite does.
                    // Reporting it as a null geometry instead was wrong: a
                    // real POINT EMPTY from st_point() is two NAs, not an
                    // empty vector, so this only ever fired for a
                    // hand-assembled coordinate vector.
                    Some(cp) => out.push(FastGeom::Point(cp)),
                    None => prerender!(),
                }
            }
            SfcType::MultiPoint | SfcType::LineString => {
                match unsafe { read_coord_ptr(sfg) } {
                    Some(cp) => out.push(FastGeom::Single(cp, row_type)),
                    None => prerender!(),
                }
            }
            SfcType::MultiLineString | SfcType::Polygon => {
                let n = unsafe { sexp_len(sfg) };
                let start_idx = batch.coords.len();
                let rings = unsafe { list_elems(sfg) };
                let mut ok = true;
                for j in 0..n {
                    let ring = match rings {
                        Some(r) => unsafe { *r.add(j) },
                        None => unsafe { libR_sys::VECTOR_ELT(sfg, j as isize) },
                    };
                    match unsafe { read_coord_ptr(ring) } {
                        Some(cp) => batch.coords.push(cp),
                        None => {
                            ok = false;
                            break;
                        }
                    }
                }
                if !ok {
                    batch.coords.truncate(start_idx);
                    prerender!();
                }
                out.push(FastGeom::FlatList {
                    start: start_idx as u32,
                    len: n as u32,
                    typ: row_type,
                });
            }
            SfcType::MultiPolygon => {
                let n_polys = unsafe { sexp_len(sfg) };
                let counts_start = batch.counts.len();
                let coords_start = batch.coords.len();
                let polys = unsafe { list_elems(sfg) };
                let mut ok = true;
                'polys: for j in 0..n_polys {
                    let poly_sfg = match polys {
                        Some(p) => unsafe { *p.add(j) },
                        None => unsafe { libR_sys::VECTOR_ELT(sfg, j as isize) },
                    };
                    let n_rings = unsafe { sexp_len(poly_sfg) };
                    batch.counts.push(n_rings);
                    let rings = unsafe { list_elems(poly_sfg) };
                    for k in 0..n_rings {
                        let ring = match rings {
                            Some(r) => unsafe { *r.add(k) },
                            None => unsafe { libR_sys::VECTOR_ELT(poly_sfg, k as isize) },
                        };
                        match unsafe { read_coord_ptr(ring) } {
                            Some(cp) => batch.coords.push(cp),
                            None => {
                                ok = false;
                                break 'polys;
                            }
                        }
                    }
                }
                if !ok {
                    batch.coords.truncate(coords_start);
                    batch.counts.truncate(counts_start);
                    prerender!();
                }
                out.push(FastGeom::MultiPolygon {
                    coords_start: coords_start as u32,
                    counts_start: counts_start as u32,
                    n_polys: n_polys as u32,
                });
            }
            // GEOMETRYCOLLECTION and anything unrecognised.
            _ => prerender!(),
        }
    }
    ChunkGeoms { batch, geoms: out, pending }
}

/// Renders the geometries a worker had to leave alone. Must run on the R
/// thread: `render_geometry_to_bytes` reaches string translation, which
/// allocates on R's vmax stack.
pub(crate) unsafe fn finish_pending_geoms(
    cg: &mut ChunkGeoms,
    geom_col: libR_sys::SEXP,
    start: usize,
    config: SerializerConfig,
) {
    if cg.pending.is_empty() {
        return;
    }
    let sfc_elems = list_elems(geom_col);
    let geom_len = sexp_len(geom_col);
    for &local in &cg.pending {
        let i = start + local;
        if i >= geom_len {
            continue;
        }
        let sfg = match sfc_elems {
            Some(p) => *p.add(i),
            None => libR_sys::VECTOR_ELT(geom_col, i as isize),
        };
        let s = cg.batch.raw.len() as u32;
        render_geometry_to_bytes(sfg, &mut cg.batch.raw, config, 0);
        let l = cg.batch.raw.len() as u32 - s;
        cg.geoms[local] = FastGeom::Prerendered { start: s, len: l };
    }
}

/// Writes one coordinate, honouring `digits` and the `na` mode.
///
/// sf represents an empty or missing ordinate as NA_real_/NaN, and jsonlite
/// keeps those distinct: NA -> "NA", NaN -> "NaN", Inf -> "Inf". Previously
/// these went through ryu unchecked and emitted values like
/// 1.797693134863096e308.
#[inline(always)]
pub(crate) fn write_coord_value(buf: &mut Vec<u8>, v: f64, config: SerializerConfig) {
    if v.is_finite() {
        write_f64_json(buf, v, config.digits, config.always_decimal);
    } else if config.na == NaMode::Null {
        buf.extend_from_slice(b"null");
    } else if v == f64::INFINITY {
        buf.extend_from_slice(b"\"Inf\"");
    } else if v == f64::NEG_INFINITY {
        buf.extend_from_slice(b"\"-Inf\"");
    } else if unsafe { is_nan_real(v) } {
        buf.extend_from_slice(b"\"NaN\"");
    } else {
        buf.extend_from_slice(b"\"NA\"");
    }
}

/// A bare coordinate vector, as POINT stores it: `[x, y]` / `[x, y, z]`.
pub(crate) fn write_point_coords(buf: &mut Vec<u8>, cp: &CoordPtr, config: SerializerConfig) {
    let p = cp.ptr as *const f64;
    buf.reserve(cp.len * 26 + 2);
    buf.push(b'[');
    for i in 0..cp.len {
        if i > 0 {
            buf.push(b',');
        }
        write_coord_value(buf, unsafe { *p.add(i) }, config);
    }
    buf.push(b']');
}

/// An nrow x ncol column-major coordinate matrix as an array of rows.
pub(crate) fn write_coord_matrix(buf: &mut Vec<u8>, cp: &CoordPtr, config: SerializerConfig) {
    let ncol = cp.ncol.max(1);
    let nrow = cp.len / ncol;
    let p = cp.ptr as *const f64;
    // One reservation for the whole ring: a 200-vertex polygon otherwise
    // grows the buffer repeatedly inside the hottest sf loop.
    buf.reserve(nrow * (ncol * 26 + 3) + 2);
    buf.push(b'[');
    for i in 0..nrow {
        if i > 0 {
            buf.push(b',');
        }
        buf.push(b'[');
        for j in 0..ncol {
            if j > 0 {
                buf.push(b',');
            }
            // Column-major: element (i, j) lives at i + j * nrow.
            write_coord_value(buf, unsafe { *p.add(i + j * nrow) }, config);
        }
        buf.push(b']');
    }
    buf.push(b']');
}

pub(crate) fn write_geometry_parallel(
    out: &mut JsonWriter,
    geom: &FastGeom,
    batch: &GeometryBatch,
    config: SerializerConfig,
) {
    match geom {
        FastGeom::Point(cp) => {
            out.push_bytes(br#"{"type":"Point","coordinates":"#);
            write_point_coords(&mut out.buf, cp, config);
            out.push_u8(b'}');
        }
        FastGeom::Single(cp, typ) => {
            match typ {
                SfcType::MultiPoint => out.push_bytes(br#"{"type":"MultiPoint","coordinates":"#),
                SfcType::LineString => out.push_bytes(br#"{"type":"LineString","coordinates":"#),
                _ => {
                    out.push_bytes(b"null");
                    return;
                }
            }
            write_coord_matrix(&mut out.buf, cp, config);
            out.push_u8(b'}');
        }
        FastGeom::FlatList { start, len, typ } => {
            match typ {
                SfcType::MultiLineString => {
                    out.push_bytes(br#"{"type":"MultiLineString","coordinates":["#)
                }
                SfcType::Polygon => out.push_bytes(br#"{"type":"Polygon","coordinates":["#),
                _ => {
                    out.push_bytes(b"null");
                    return;
                }
            }
            let s = *start as usize;
            let l = *len as usize;
            for (i, cp) in batch.coords[s..s + l].iter().enumerate() {
                if i > 0 {
                    out.push_u8(b',');
                }
                write_coord_matrix(&mut out.buf, cp, config);
            }
            out.push_bytes(br#"]}"#);
        }
        FastGeom::MultiPolygon { coords_start, counts_start, n_polys } => {
            out.push_bytes(br#"{"type":"MultiPolygon","coordinates":["#);
            let mut c_idx = *coords_start as usize;
            let cnt_start = *counts_start as usize;
            let cnt_len = *n_polys as usize;
            for (i, &n_rings) in batch.counts[cnt_start..cnt_start + cnt_len].iter().enumerate() {
                if i > 0 {
                    out.push_u8(b',');
                }
                out.push_u8(b'[');
                for k in 0..n_rings {
                    if k > 0 {
                        out.push_u8(b',');
                    }
                    if c_idx < batch.coords.len() {
                        write_coord_matrix(&mut out.buf, &batch.coords[c_idx], config);
                    } else {
                        out.push_bytes(b"[]");
                    }
                    c_idx += 1;
                }
                out.push_u8(b']');
            }
            out.push_bytes(br#"]}"#);
        }
        FastGeom::Prerendered { start, len } => {
            let s = *start as usize;
            let l = *len as usize;
            if s + l <= batch.raw.len() {
                out.push_bytes(&batch.raw[s..s + l]);
            } else {
                out.push_bytes(b"null");
            }
        }
        FastGeom::Null => out.push_bytes(b"null"),
    }
}

pub(crate) fn process_feature_parallel(out: &mut JsonWriter, row: usize, props: &[(Vec<u8>, ThreadSafeColumn)], geom: &FastGeom, batch: &GeometryBatch, config: SerializerConfig) {
    out.push_bytes(FEAT_HEAD);
    let mut needs_comma = false;
    for (key, col) in props {
        // Rewind to here if the field turns out to be omitted, so the
        // separator never survives a skipped key.
        let mark = out.buf.len();
        if needs_comma { out.push_u8(b','); }
        if try_write_kv(out, row, key, col, config) {
            needs_comma = true;
        } else {
            out.buf.truncate(mark);
        }
    }
    out.push_bytes(FEAT_MID);
    write_geometry_parallel(out, geom, batch, config);
    out.push_u8(b'}');
}

/// jsonlite's dataframe = "values": each row is a bare array of its values,
/// with the row name appended as a final element when one is emitted.
pub(crate) fn process_row_values(out: &mut JsonWriter, row: usize, props: &[(Vec<u8>, ThreadSafeColumn)], config: SerializerConfig) {
    out.push_u8(b'[');
    for (i, (_key, col)) in props.iter().enumerate() {
        if i > 0 { out.push_u8(b','); }
        write_col_value(out, row, col, config);
    }
    out.push_u8(b']');
}

pub(crate) fn process_row_generic(out: &mut JsonWriter, row: usize, props: &[(Vec<u8>, ThreadSafeColumn)], config: SerializerConfig) {
    out.push_u8(b'{');
    let mut needs_comma = false;
    for (key, col) in props {
        // Rewind to here if the field turns out to be omitted, so the
        // separator never survives a skipped key.
        let mark = out.buf.len();
        if needs_comma { out.push_u8(b','); }
        if try_write_kv(out, row, key, col, config) {
            needs_comma = true;
        } else {
            out.buf.truncate(mark);
        }
    }
    out.push_u8(b'}');
}

