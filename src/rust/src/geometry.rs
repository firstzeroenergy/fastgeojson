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

/// Is `x` a double vector we may read once we are on the R thread?
///
/// Same as `is_plain_real` but ALTREP is allowed. `REAL()` on an ALTREP vector
/// may materialise it, which allocates and can run R code — fine here, fatal
/// in a worker.
#[inline]
pub(crate) unsafe fn is_real_on_r_thread(x: libR_sys::SEXP) -> bool {
    typeof_sexp(x) == libR_sys::SEXPTYPE::REALSXP as u32
}

/// `read_coord_ptr` for the R thread: accepts an ALTREP vector.
///
/// `st_linestring()` and `st_multipoint()` attach a class to their coordinate
/// matrix, and R wraps a vector of 64 or more elements in an ALTREP wrapper
/// when attributes are set. So every LINESTRING or MULTIPOINT of 32 or more XY
/// points is ALTREP, `is_plain_real` rejects it, and the whole geometry used to
/// be pre-rendered serially. Resolving the pointer here instead is one call per
/// feature, and the coordinates are still written by the workers.
pub(crate) unsafe fn read_coord_ptr_r_thread(x: libR_sys::SEXP) -> Option<CoordPtr> {
    if !is_real_on_r_thread(x) {
        return None;
    }
    let len = sexp_len(x);
    // getAttrib, not an ATTRIB walk: ATTRIB is not part of R's API. Only
    // possible on this side, because getAttrib marks the attribute
    // NOT_MUTABLE, which is a write to a shared header -- see read_coord_ptr.
    let dim = libR_sys::Rf_getAttrib(x, libR_sys::R_DimSymbol);
    let ncol = if dim != libR_sys::R_NilValue && sexp_len(dim) == 2 {
        let d = libR_sys::INTEGER(dim);
        let c = *d.add(1);
        if c > 0 { c as usize } else { 1 }
    } else {
        len.max(1)
    };
    Some(CoordPtr { ptr: libR_sys::REAL(x) as usize, len, ncol })
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

/// Reads an sfg coordinate matrix (or bare vector) into a thread-safe pointer
/// from inside a worker, or `None` if the R thread has to handle it.
///
/// Workers must not read attributes: `Rf_getAttrib` writes NOT_MUTABLE into
/// the attribute's header, which several threads would then do to one shared
/// object at once, and the raw `ATTRIB` walk that avoided the write is not
/// part of R's API (a WARNING under R 4.6). So the column count arrives as
/// `ncol`, read on the R thread from the sfg's class -- `XY`, `XYZ`, `XYM`,
/// `XYZM` -- by `describe_geometries`. Only two header reads happen here:
/// the type and whether any attributes exist at all (`ANY_ATTRIB`, API).
///
/// A vector with no attributes is a bare row of ordinates, as POINT stores
/// them. A matrix whose length is not a multiple of `ncol` -- a dimension its
/// class does not describe -- goes back to the R thread, which reads `dim`
/// itself.
pub(crate) unsafe fn read_coord_ptr(x: libR_sys::SEXP, ncol: u8) -> Option<CoordPtr> {
    if !is_plain_real(x) {
        return None;
    }
    let len = sexp_len(x);
    let ncol = if ANY_ATTRIB(x) == 0 {
        len.max(1)
    } else if ncol >= 2 && len % ncol as usize == 0 {
        ncol as usize
    } else {
        return None;
    };
    Some(CoordPtr { ptr: libR_sys::REAL(x) as usize, len, ncol })
}

/// What a worker needs to know about one feature and may not read for itself:
/// the column count of its coordinate matrices, from the dimension token in
/// its class, and its type where the layer is mixed. Zero means unknown, and
/// the worker leaves that feature to the R thread.
#[derive(Clone, Copy)]
pub(crate) struct GeomInfo {
    pub(crate) ncol: u8,
    pub(crate) typ: SfcType,
}

/// The column count of the first coordinate matrix under `x`, from its `dim`
/// -- read on the R thread with `Rf_getAttrib` -- or 0 if there is none
/// within three levels of nesting (MULTIPOLYGON is two).
unsafe fn first_matrix_ncol(x: libR_sys::SEXP, depth: u32) -> u8 {
    let t = typeof_sexp(x);
    if t == libR_sys::SEXPTYPE::REALSXP as u32 {
        let dim = libR_sys::Rf_getAttrib(x, libR_sys::R_DimSymbol);
        if dim != libR_sys::R_NilValue
            && typeof_sexp(dim) == libR_sys::SEXPTYPE::INTSXP as u32
            && sexp_len(dim) == 2
        {
            let c = *libR_sys::INTEGER(dim).add(1);
            if c > 0 && c < 256 {
                return c as u8;
            }
        }
        return 0;
    }
    if t == libR_sys::SEXPTYPE::VECSXP as u32 && depth < 3 && sexp_len(x) > 0 {
        return first_matrix_ncol(libR_sys::VECTOR_ELT(x, 0), depth + 1);
    }
    0
}

/// One pass over the layer on the R thread, reading each sfg's class with
/// `Rf_getAttrib`. A POINT layer needs nothing from it -- its ordinates are
/// bare vectors -- so it returns empty for one. Measured at about 25 ns per
/// feature, which is what the parallel description used to spend walking the
/// attribute list per ring anyway.
pub(crate) unsafe fn describe_geometries(
    geom_col: libR_sys::SEXP,
    n: usize,
    sfc_type: SfcType,
) -> Vec<GeomInfo> {
    if sfc_type == SfcType::Point {
        return Vec::new();
    }
    let mixed = sfc_type == SfcType::GeometryCollection || sfc_type == SfcType::Unknown;
    let elems = list_elems(geom_col);
    let len = sexp_len(geom_col);
    let mut out = Vec::with_capacity(n);
    for i in 0..n.min(len) {
        let sfg = match elems {
            Some(p) => *p.add(i),
            None => libR_sys::VECTOR_ELT(geom_col, i as isize),
        };
        let mut info = GeomInfo { ncol: 0, typ: sfc_type };
        if sfg != libR_sys::R_NilValue {
            // The first coordinate matrix's own `dim` is the truth for the
            // layout, and every matrix of one sfg shares it; the class token
            // below only stands in when there is no matrix to read (a POINT
            // in a mixed layer, or an empty geometry).
            info.ncol = first_matrix_ncol(sfg, 0);
            // The class is read only when it is needed: for the type in a
            // mixed layer, or for the dimension when there was no matrix to
            // take it from. In the ordinary case that is one attribute read
            // per feature, not two plus three string compares.
            let classes = if mixed || info.ncol == 0 {
                libR_sys::Rf_getAttrib(sfg, libR_sys::R_ClassSymbol)
            } else {
                libR_sys::R_NilValue
            };
            if classes != libR_sys::R_NilValue && typeof_sexp(classes) == libR_sys::SEXPTYPE::STRSXP as u32 {
                let nc = sexp_len(classes);
                for k in 0..nc {
                    let s = libR_sys::STRING_ELT(classes, k as isize);
                    let bytes = CStr::from_ptr(libR_sys::R_CHAR(s) as *const c_char).to_bytes();
                    match bytes {
                        b"XY" if info.ncol == 0 => info.ncol = 2,
                        b"XYZ" | b"XYM" if info.ncol == 0 => info.ncol = 3,
                        b"XYZM" if info.ncol == 0 => info.ncol = 4,
                        b"POINT" if mixed => info.typ = SfcType::Point,
                        b"MULTIPOINT" if mixed => info.typ = SfcType::MultiPoint,
                        b"LINESTRING" if mixed => info.typ = SfcType::LineString,
                        b"MULTILINESTRING" if mixed => info.typ = SfcType::MultiLineString,
                        b"POLYGON" if mixed => info.typ = SfcType::Polygon,
                        b"MULTIPOLYGON" if mixed => info.typ = SfcType::MultiPolygon,
                        b"GEOMETRYCOLLECTION" if mixed => info.typ = SfcType::GeometryCollection,
                        _ => {}
                    }
                }
            }
        }
        out.push(info);
    }
    out
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
            match read_coord_ptr_r_thread(sfg) {
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
            match read_coord_ptr_r_thread(sfg) {
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
                match read_coord_ptr_r_thread(el) {
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
                    match read_coord_ptr_r_thread(el) {
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
    /// Ordinates per row, filled by the worker that described them. Computing
    /// it on the R thread afterwards was a serial 5 ms for a million features.
    pub(crate) work: Vec<usize>,
}

pub(crate) fn extract_geometries_chunk(
    geom_col: libR_sys::SEXP,
    sfc_type: SfcType,
    start: usize,
    end: usize,
    config: SerializerConfig,
    // One entry per feature of this chunk, from `describe_geometries`; empty
    // for a POINT layer.
    infos: &[GeomInfo],
) -> ChunkGeoms {
    // `config` is unused now that nothing is rendered here, but keeping it in
    // the signature keeps the call sites stable.
    let _ = config;
    let capacity_est = end - start;
    // `coords` and `counts` hold the rings of polygons and multi-geometries.
    // A POINT, MULTIPOINT or LINESTRING is one coordinate matrix and goes into
    // the FastGeom itself, so for those the two vectors stay empty -- and
    // reserving them anyway was the single largest cost on the points path:
    // 128 chunks each reserving ~440 KB it never touched was 5 ms of
    // allocation inside "extract" and another 4-5 ms freeing it at return,
    // all on sub-megabyte heap blocks that serialise on the allocator lock.
    let (coord_cap, count_cap) = match sfc_type {
        SfcType::Point | SfcType::MultiPoint | SfcType::LineString => (0, 0),
        _ => (capacity_est * 2, capacity_est),
    };
    let mut batch = GeometryBatch {
        coords: Vec::with_capacity(coord_cap),
        counts: Vec::with_capacity(count_cap),
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
        // types. jsonlite reads class(sfg)[2] for every element; the R thread
        // did that in describe_geometries, and the worker takes its word.
        let info = infos.get(i - start).copied().unwrap_or(GeomInfo { ncol: 0, typ: sfc_type });
        let row_type = if sfc_type == SfcType::GeometryCollection || sfc_type == SfcType::Unknown {
            info.typ
        } else {
            sfc_type
        };
        let ncol = info.ncol;

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
                match unsafe { read_coord_ptr(sfg, ncol) } {
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
                    match unsafe { read_coord_ptr(ring, ncol) } {
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
                        match unsafe { read_coord_ptr(ring, ncol) } {
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
    // Only the rows this worker could describe are weighed here; anything
    // deferred is weighed by finish_pending_geoms once it has a descriptor.
    let work = out
        .iter()
        .map(|g| geom_ordinates(g, &batch))
        .collect::<Vec<usize>>();
    ChunkGeoms { batch, geoms: out, pending, work }
}

/// Renders the geometries a worker had to leave alone. Must run on the R
/// thread: `render_geometry_to_bytes` reaches string translation, which
/// allocates on R's vmax stack.
pub(crate) unsafe fn finish_pending_geoms(
    cg: &mut ChunkGeoms,
    geom_col: libR_sys::SEXP,
    sfc_type: SfcType,
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
        // Most deferrals are only here because the coordinates are ALTREP,
        // which is safe to resolve now that we are on the R thread. Doing so
        // keeps the coordinate writing in the workers; falling through to
        // render_geometry_to_bytes would serialise it.
        let row_type = if sfc_type == SfcType::GeometryCollection
            || sfc_type == SfcType::Unknown
        {
            get_row_sfg_type(sfg)
        } else {
            sfc_type
        };
        let fast = match row_type {
            SfcType::Point => read_coord_ptr_r_thread(sfg).map(FastGeom::Point),
            SfcType::MultiPoint | SfcType::LineString => {
                read_coord_ptr_r_thread(sfg).map(|cp| FastGeom::Single(cp, row_type))
            }
            SfcType::MultiLineString | SfcType::Polygon => {
                let n = sexp_len(sfg);
                let rings = list_elems(sfg);
                let base = cg.batch.coords.len();
                let mut ok = n > 0 || sexp_len(sfg) == 0;
                for j in 0..n {
                    let ring = match rings {
                        Some(r) => *r.add(j),
                        None => libR_sys::VECTOR_ELT(sfg, j as isize),
                    };
                    match read_coord_ptr_r_thread(ring) {
                        Some(cp) => cg.batch.coords.push(cp),
                        None => { ok = false; break; }
                    }
                }
                if ok {
                    Some(FastGeom::FlatList {
                        start: base as u32,
                        len: n as u32,
                        typ: row_type,
                    })
                } else {
                    cg.batch.coords.truncate(base);
                    None
                }
            }
            SfcType::MultiPolygon => {
                let n_polys = sexp_len(sfg);
                let cbase = cg.batch.coords.len();
                let nbase = cg.batch.counts.len();
                let polys = list_elems(sfg);
                let mut ok = true;
                'p: for j in 0..n_polys {
                    let poly = match polys {
                        Some(pp) => *pp.add(j),
                        None => libR_sys::VECTOR_ELT(sfg, j as isize),
                    };
                    let n_rings = sexp_len(poly);
                    cg.batch.counts.push(n_rings);
                    let rings = list_elems(poly);
                    for k in 0..n_rings {
                        let ring = match rings {
                            Some(r) => *r.add(k),
                            None => libR_sys::VECTOR_ELT(poly, k as isize),
                        };
                        match read_coord_ptr_r_thread(ring) {
                            Some(cp) => cg.batch.coords.push(cp),
                            None => { ok = false; break 'p; }
                        }
                    }
                }
                if ok {
                    Some(FastGeom::MultiPolygon {
                        coords_start: cbase as u32,
                        counts_start: nbase as u32,
                        n_polys: n_polys as u32,
                    })
                } else {
                    cg.batch.coords.truncate(cbase);
                    cg.batch.counts.truncate(nbase);
                    None
                }
            }
            _ => None,
        };
        if let Some(g) = fast {
            cg.geoms[local] = g;
            cg.work[local] = geom_ordinates(&g, &cg.batch);
            continue;
        }
        // Genuinely needs the recursive writer: a GEOMETRYCOLLECTION, an
        // unrecognised class, or coordinates that are not doubles at all.
        let s = cg.batch.raw.len() as u32;
        render_geometry_to_bytes(sfg, &mut cg.batch.raw, config, 0);
        let l = cg.batch.raw.len() as u32 - s;
        let g = FastGeom::Prerendered { start: s, len: l };
        cg.geoms[local] = g;
        cg.work[local] = geom_ordinates(&g, &cg.batch);
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
        write_f64_json(buf, v, config);
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
    if cp.len == 2 && config.digits == Some(DIGITS_SHORTEST) && !config.always_decimal {
        let x = unsafe { *p };
        let y = unsafe { *p.add(1) };
        if x.is_finite() && y.is_finite() {
            buf.reserve(2 * SHORTEST_MAX + 3);
            unsafe {
                let base = buf.as_mut_ptr();
                let mut n = buf.len();
                *base.add(n) = b'[';
                n += 1;
                n += write_shortest_raw(base.add(n), x);
                *base.add(n) = b',';
                n += 1;
                n += write_shortest_raw(base.add(n), y);
                *base.add(n) = b']';
                n += 1;
                buf.set_len(n);
            }
            return;
        }
    }
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

/// A coordinate matrix below this many ordinates is written by one thread.
///
/// Above it the matrix is split across the pool. The parallel pass over an
/// sfc splits between GEOMETRIES, which does nothing for a layer whose work
/// is one geometry: 2,000,000 coordinates in a single LineString measured 555
/// ms with one worker busy and thirty-one idle, against 103 ms for the same
/// coordinates spread over 2000 features. The threshold is well above any
/// ordinary ring -- a 200-vertex polygon is 400 ordinates -- so the usual
/// layer never pays for the check.
pub(crate) const MIN_SPLIT_ORDINATES: usize = 1 << 17;

/// One row range of a coordinate matrix, written into its own buffer.
///
/// The piece carries its own punctuation: the first opens the array, every
/// later one starts with the comma that separates it from the piece before,
/// and the last closes. They then concatenate with nothing in between.
fn write_coord_rows(
    out: &mut Vec<u8>,
    cp: &CoordPtr,
    nrow: usize,
    ncol: usize,
    start: usize,
    end: usize,
    config: SerializerConfig,
) {
    let p = cp.ptr as *const f64;
    if ncol == 2 && config.digits == Some(DIGITS_SHORTEST) && !config.always_decimal {
        write_coord_rows_xy_shortest(out, cp, nrow, start, end, config);
        return;
    }
    out.reserve((end - start) * (ncol * 26 + 3) + 2);
    if start == 0 {
        out.push(b'[');
    }
    for i in start..end {
        if i > 0 {
            out.push(b',');
        }
        out.push(b'[');
        for j in 0..ncol {
            if j > 0 {
                out.push(b',');
            }
            // Column-major: element (i, j) lives at i + j * nrow.
            write_coord_value(out, unsafe { *p.add(i + j * nrow) }, config);
        }
        out.push(b']');
    }
    if end == nrow {
        out.push(b']');
    }
}

/// `write_coord_rows` for the common case: two columns, lossless digits, no
/// `always_decimal`. One reservation for the whole range, then every byte
/// goes through a pointer -- no per-byte capacity checks, no per-value
/// dispatch on `digits`, no second `reserve` inside the number writer. A
/// non-finite ordinate hands that one row to the generic writer, which
/// produces exactly what this loop would have, so the output is the same
/// byte for byte either way. In isolation the loop runs at 24 ns per
/// ordinate against ryu's own 22, so the number formatting is nearly all of
/// it; the README polygons went from 227 to 218 ms single-threaded.
fn write_coord_rows_xy_shortest(
    out: &mut Vec<u8>,
    cp: &CoordPtr,
    nrow: usize,
    start: usize,
    end: usize,
    config: SerializerConfig,
) {
    // Per row: ",[" + number + "," + number + "]", plus the outer brackets.
    const ROW_MAX: usize = 2 * SHORTEST_MAX + 4;
    let p = cp.ptr as *const f64;
    out.reserve((end - start) * ROW_MAX + 2);
    unsafe {
        let mut base = out.as_mut_ptr();
        let mut n = out.len();
        if start == 0 {
            *base.add(n) = b'[';
            n += 1;
        }
        for i in start..end {
            let x = *p.add(i);
            let y = *p.add(i + nrow);
            if !(x.is_finite() && y.is_finite()) {
                // The generic row for this one, through the Vec's own API;
                // then back to the pointer, re-reserved in case it moved.
                out.set_len(n);
                if i > 0 {
                    out.push(b',');
                }
                out.push(b'[');
                write_coord_value(out, x, config);
                out.push(b',');
                write_coord_value(out, y, config);
                out.push(b']');
                out.reserve((end - i - 1) * ROW_MAX + 2);
                base = out.as_mut_ptr();
                n = out.len();
                continue;
            }
            if i > 0 {
                *base.add(n) = b',';
                n += 1;
            }
            *base.add(n) = b'[';
            n += 1;
            n += write_shortest_raw(base.add(n), x);
            *base.add(n) = b',';
            n += 1;
            n += write_shortest_raw(base.add(n), y);
            *base.add(n) = b']';
            n += 1;
        }
        if end == nrow {
            *base.add(n) = b']';
            n += 1;
        }
        out.set_len(n);
    }
}

/// `write_coord_matrix` for a matrix big enough to be worth spreading.
///
/// Called from the R thread when the layer is one huge geometry, and from
/// inside a worker when it is a handful of large ones; rayon work-steals
/// either way, so the idle workers pick the pieces up in both cases.
fn write_coord_matrix_split(
    buf: &mut Vec<u8>,
    cp: &CoordPtr,
    nrow: usize,
    ncol: usize,
    config: SerializerConfig,
) {
    let ordinates = nrow * ncol;
    let pieces = (ordinates / (MIN_SPLIT_ORDINATES / 2))
        .clamp(2, desired_threads().max(1))
        .min(nrow);
    if pieces < 2 {
        write_coord_rows(buf, cp, nrow, ncol, 0, nrow, config);
        return;
    }
    // Contiguous row ranges; the last ends at nrow, which is what closes the
    // array.
    let per = (nrow + pieces - 1) / pieces;
    let ranges: Vec<(usize, usize)> = (0..pieces)
        .map(|k| (k * per, ((k + 1) * per).min(nrow)))
        .filter(|&(s, e)| s < e)
        .collect();
    let cpc = *cp;
    let parts: Vec<Vec<u8>> = with_pool(|| {
        ranges
            .par_iter()
            .map(|&(s, e)| {
                let mut b = Vec::new();
                write_coord_rows(&mut b, &cpc, nrow, ncol, s, e, config);
                b
            })
            .collect()
    });

    // Offsets are a prefix sum over the piece lengths, so the destination
    // ranges are disjoint and the copies need no synchronisation. Serial
    // concatenation of the pieces would be another pass over the whole
    // geometry, which is the cost this is here to avoid.
    let mut offs: Vec<usize> = Vec::with_capacity(parts.len());
    let at = buf.len();
    let mut acc = at;
    for pt in &parts {
        offs.push(acc);
        acc += pt.len();
    }
    let total = acc - at;
    buf.reserve(total);
    unsafe {
        struct Dst(*mut u8);
        unsafe impl Send for Dst {}
        unsafe impl Sync for Dst {}
        impl Dst {
            /// A method so the closure captures `&Dst`, which is Sync, rather
            /// than the bare pointer, which is not.
            #[inline]
            unsafe fn write(&self, at: usize, src: &[u8]) {
                std::ptr::copy_nonoverlapping(src.as_ptr(), self.0.add(at), src.len());
            }
        }
        let base = Dst(buf.as_mut_ptr());
        if total < (1 << 22) || desired_threads() <= 1 {
            for (pt, &o) in parts.iter().zip(offs.iter()) {
                base.write(o, pt);
            }
        } else {
            with_pool(|| {
                parts
                    .par_iter()
                    .zip(offs.par_iter())
                    .for_each(|(pt, &o)| base.write(o, pt))
            });
        }
        buf.set_len(at + total);
    }
}

/// An nrow x ncol column-major coordinate matrix as an array of rows.
pub(crate) fn write_coord_matrix(buf: &mut Vec<u8>, cp: &CoordPtr, config: SerializerConfig) {
    let ncol = cp.ncol.max(1);
    let nrow = cp.len / ncol;
    if cp.len >= MIN_SPLIT_ORDINATES && nrow >= 2 && desired_threads() > 1 {
        write_coord_matrix_split(buf, cp, nrow, ncol, config);
        return;
    }
    // The whole matrix as one row range, which also selects the two-column
    // fast path for the ordinary ring.
    write_coord_rows(buf, cp, nrow, ncol, 0, nrow, config);
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

pub(crate) fn process_feature_parallel(out: &mut JsonWriter, row: usize, props: &[(Key, ThreadSafeColumn)], geom: &FastGeom, batch: &GeometryBatch, config: SerializerConfig) {
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
pub(crate) fn process_row_values(out: &mut JsonWriter, row: usize, props: &[(Key, ThreadSafeColumn)], config: SerializerConfig) {
    out.push_u8(b'[');
    for (i, (_key, col)) in props.iter().enumerate() {
        if i > 0 { out.push_u8(b','); }
        write_col_value(out, row, col, config);
    }
    out.push_u8(b']');
}

pub(crate) fn process_row_generic(out: &mut JsonWriter, row: usize, props: &[(Key, ThreadSafeColumn)], config: SerializerConfig) {
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

// ------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_ranges_cover_everything_once() {
        for work in [
            vec![],
            vec![1usize],
            vec![1, 1, 1, 1],
            vec![1, 1, 1, 1, 1, 1, 1, 100],
            vec![100, 1, 1, 1, 1, 1, 1, 1],
            vec![5; 1000],
        ] {
            for target in [1usize, 2, 4, 32] {
                let r = weighted_ranges(&work, target);
                if work.is_empty() {
                    assert!(r.is_empty());
                    continue;
                }
                assert_eq!(r[0].1, 0, "first range does not start at zero");
                assert_eq!(r[r.len() - 1].2, work.len(), "last range does not end");
                for w in r.windows(2) {
                    assert_eq!(w[0].2, w[1].1, "ranges are not contiguous");
                }
                for (i, x) in r.iter().enumerate() {
                    assert_eq!(x.0, i, "ids are not sequential");
                    assert!(x.1 < x.2, "empty range");
                }
            }
        }
    }


    // ---- the coordinate writers -----------------------------------

    fn cfg4() -> SerializerConfig {
        SerializerConfig {
            df: DfMode::Rows,
            na: NaMode::Null,
            null: NullMode::List,
            factor: FactorMode::String,
            auto_unbox: false,
            digits: Some(4),
            matrix_colmajor: false,
            always_decimal: false,
            signif: false,
            json_verbatim: false,
            rownames: ROWNAMES_REAL,
        }
    }

    fn cp(v: &[f64], ncol: usize) -> CoordPtr {
        CoordPtr { ptr: v.as_ptr() as usize, len: v.len(), ncol }
    }

    fn render(g: &FastGeom, batch: &GeometryBatch) -> String {
        let mut w = JsonWriter::with_capacity(0);
        write_geometry_parallel(&mut w, g, batch, cfg4());
        String::from_utf8(w.buf).unwrap()
    }

    fn empty_batch() -> GeometryBatch {
        GeometryBatch { coords: Vec::new(), counts: Vec::new(), raw: Vec::new() }
    }

    #[test]
    fn a_point_is_a_bare_coordinate_vector() {
        let v = vec![1.5f64, 2.5];
        let g = FastGeom::Point(cp(&v, 2));
        assert_eq!(
            render(&g, &empty_batch()),
            r#"{"type":"Point","coordinates":[1.5,2.5]}"#
        );
        let z = vec![1.0f64, 2.0, 3.0];
        let g = FastGeom::Point(cp(&z, 3));
        assert_eq!(
            render(&g, &empty_batch()),
            r#"{"type":"Point","coordinates":[1,2,3]}"#
        );
    }

    #[test]
    fn a_coordinate_matrix_is_read_column_major() {
        // sf stores a ring as an nrow x ncol column-major matrix, so the three
        // XY points below are (1,4), (2,5), (3,6).
        let m = vec![1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0];
        let g = FastGeom::Single(cp(&m, 2), SfcType::LineString);
        assert_eq!(
            render(&g, &empty_batch()),
            r#"{"type":"LineString","coordinates":[[1,4],[2,5],[3,6]]}"#
        );
        let g = FastGeom::Single(cp(&m, 2), SfcType::MultiPoint);
        assert_eq!(
            render(&g, &empty_batch()),
            r#"{"type":"MultiPoint","coordinates":[[1,4],[2,5],[3,6]]}"#
        );
        // Three ordinates per point, which is how XYZ arrives.
        let m3 = vec![1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0];
        let g = FastGeom::Single(cp(&m3, 3), SfcType::LineString);
        assert_eq!(
            render(&g, &empty_batch()),
            r#"{"type":"LineString","coordinates":[[1,3,5],[2,4,6]]}"#
        );
    }

    #[test]
    fn coordinate_pieces_concatenate_to_the_whole_matrix() {
        // The split writer gives each piece its own punctuation: the first
        // opens the array, every later one starts with the comma that
        // separates it from the piece before, and the last closes. Get any of
        // that wrong and the seam carries a doubled or a missing separator,
        // which only shows up at a piece boundary. So: every row count, cut
        // at every possible place, against one unsplit write.
        let c = cfg4();
        for ncol in 1..=4usize {
            for nrow in 0..12usize {
                let v: Vec<f64> = (0..nrow * ncol).map(|k| k as f64 * 1.5 - 3.0).collect();
                let p = cp(&v, ncol);

                let mut whole = Vec::new();
                write_coord_matrix(&mut whole, &p, c);

                // Writes the pieces the way the splitter does: empty ranges
                // are dropped, because a piece that spans no rows would open
                // and close the array by itself.
                let join = |cuts: &[usize]| -> Vec<u8> {
                    let mut out = Vec::new();
                    let mut at = 0usize;
                    for &k in cuts.iter().chain(std::iter::once(&nrow)) {
                        if k > at {
                            write_coord_rows(&mut out, &p, nrow, ncol, at, k, c);
                            at = k;
                        }
                    }
                    if out.is_empty() {
                        write_coord_rows(&mut out, &p, nrow, ncol, 0, nrow, c);
                    }
                    out
                };

                // Every way of cutting the rows into two, then into three.
                for a in 0..=nrow {
                    assert_eq!(
                        join(&[a]), whole,
                        "{}x{} cut at {}: {:?} vs {:?}",
                        nrow, ncol, a,
                        String::from_utf8_lossy(&join(&[a])),
                        String::from_utf8_lossy(&whole)
                    );
                    for b in a..=nrow {
                        assert_eq!(
                            join(&[a, b]), whole,
                            "{}x{} cut at {} and {}", nrow, ncol, a, b
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn only_a_big_matrix_is_worth_splitting() {
        // The threshold exists so an ordinary ring never pays for the check:
        // a 200-vertex polygon is 400 ordinates, four hundred times under it.
        assert!(MIN_SPLIT_ORDINATES >= 1 << 16, "too low to leave small rings alone");
        let small: Vec<f64> = vec![1.0; 400];
        assert!(small.len() < MIN_SPLIT_ORDINATES);
        // And a piece count never exceeds the rows available, or two pieces
        // would claim the same row and the last would never close the array.
        for nrow in [1usize, 2, 3, 1000, 65_536, 2_000_000] {
            for ncol in 1..=4usize {
                let pieces = ((nrow * ncol) / (MIN_SPLIT_ORDINATES / 2)).clamp(2, 32).min(nrow);
                assert!(pieces <= nrow.max(1), "{} rows asked for {} pieces", nrow, pieces);
                let per = (nrow + pieces - 1) / pieces;
                let ranges: Vec<(usize, usize)> = (0..pieces)
                    .map(|k| (k * per, ((k + 1) * per).min(nrow)))
                    .filter(|&(a, b)| a < b)
                    .collect();
                assert_eq!(ranges.first().map(|r| r.0), Some(0), "{} rows", nrow);
                assert_eq!(ranges.last().map(|r| r.1), Some(nrow), "{} rows", nrow);
                for w in ranges.windows(2) {
                    assert_eq!(w[0].1, w[1].0, "{} rows: a gap or an overlap", nrow);
                }
            }
        }
    }

    #[test]
    fn non_finite_ordinates_keep_their_spelling() {
        let na = f64::from_bits(0x7FF0_0000_0000_07A2);
        let v = vec![na, f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
        let mut c = cfg4();
        let one = |v: f64, c: SerializerConfig| {
            let mut b = Vec::new();
            write_coord_value(&mut b, v, c);
            String::from_utf8(b).unwrap()
        };
        for &x in &v {
            assert_eq!(one(x, c), "null", "na = null must flatten everything");
        }
        c.na = NaMode::String;
        assert_eq!(one(v[0], c), "\"NA\"");
        assert_eq!(one(v[1], c), "\"NaN\"");
        assert_eq!(one(v[2], c), "\"Inf\"");
        assert_eq!(one(v[3], c), "\"-Inf\"");
    }

    #[test]
    fn a_polygon_reads_its_rings_from_the_batch() {
        let outer = vec![0.0f64, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0];
        let inner = vec![0.2f64, 0.4, 0.4, 0.2, 0.2, 0.2, 0.2, 0.4, 0.4, 0.2];
        let batch = GeometryBatch {
            coords: vec![cp(&outer, 2), cp(&inner, 2)],
            counts: vec![2],
            raw: Vec::new(),
        };
        let g = FastGeom::FlatList { start: 0, len: 2, typ: SfcType::Polygon };
        let s = render(&g, &batch);
        assert!(s.starts_with(r#"{"type":"Polygon","coordinates":[[["#), "{}", s);
        assert_eq!(s.matches("],[[").count(), 1, "two rings expected in {}", s);

        let g = FastGeom::MultiPolygon { coords_start: 0, counts_start: 0, n_polys: 1 };
        let s = render(&g, &batch);
        assert!(s.starts_with(r#"{"type":"MultiPolygon","coordinates":[[[["#), "{}", s);
    }

    #[test]
    fn a_prerendered_geometry_is_spliced_verbatim() {
        let batch = GeometryBatch {
            coords: Vec::new(),
            counts: Vec::new(),
            raw: br#"{"type":"GeometryCollection","geometries":[]}"#.to_vec(),
        };
        let g = FastGeom::Prerendered { start: 0, len: batch.raw.len() as u32 };
        assert_eq!(render(&g, &batch), r#"{"type":"GeometryCollection","geometries":[]}"#);
        assert_eq!(render(&FastGeom::Null, &empty_batch()), "null");
    }

    #[test]
    fn ordinate_counts_drive_the_split() {
        // geom_ordinates is what weighted_ranges partitions on, so it has to
        // reflect the work a geometry actually emits.
        let a = vec![0.0f64; 10];
        let b = vec![0.0f64; 100];
        let batch = GeometryBatch {
            coords: vec![cp(&a, 2), cp(&b, 2)],
            counts: vec![2],
            raw: vec![0u8; 80],
        };
        assert_eq!(geom_ordinates(&FastGeom::Null, &batch), 1);
        assert_eq!(geom_ordinates(&FastGeom::Point(cp(&a, 2)), &batch), 10);
        assert_eq!(
            geom_ordinates(&FastGeom::Single(cp(&b, 2), SfcType::LineString), &batch),
            100
        );
        assert_eq!(
            geom_ordinates(
                &FastGeom::FlatList { start: 0, len: 2, typ: SfcType::Polygon },
                &batch
            ),
            110
        );
        // Prerendered bytes are divided by roughly the bytes an ordinate takes,
        // so the two kinds of work land on one scale.
        assert_eq!(geom_ordinates(&FastGeom::Prerendered { start: 0, len: 80 }, &batch), 10);
    }

    #[test]
    fn ordinate_counts_refuse_to_read_past_the_batch() {
        // A descriptor that points outside its batch must be clamped rather
        // than indexed, since it would be read in a worker.
        let a = vec![0.0f64; 4];
        let batch = GeometryBatch { coords: vec![cp(&a, 2)], counts: vec![1], raw: Vec::new() };
        let _ = geom_ordinates(&FastGeom::FlatList { start: 0, len: 99, typ: SfcType::Polygon }, &batch);
        let _ = geom_ordinates(
            &FastGeom::MultiPolygon { coords_start: 0, counts_start: 0, n_polys: 99 },
            &batch,
        );
        let _ = geom_ordinates(&FastGeom::Prerendered { start: 0, len: 9999 }, &batch);
    }

    #[test]
    fn a_heavy_tail_is_split_off() {
        // The failure this exists for: an earlier version refused to close a
        // chunk unless enough rows remained, which swept the large geometries
        // at the end of the vector into one final chunk and made the whole
        // partition pointless.
        let mut work = vec![1usize; 9990];
        work.extend([100_000usize; 10]);
        let r = weighted_ranges(&work, 32);
        assert!(r.len() > 1, "heavy tail was not split at all");
        let last = r[r.len() - 1];
        assert!(
            last.2 - last.1 < 10,
            "the ten heavy rows landed in one chunk of {}",
            last.2 - last.1
        );
    }
}
