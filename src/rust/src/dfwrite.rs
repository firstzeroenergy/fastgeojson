// Shared data.frame body writer.
//
// A data.frame is serialised the same way wherever it sits: build one
// descriptor per column, then walk rows (or columns) in the worker pool. The
// top-level entry point had that machinery; the recursive serializer had its
// own per-cell loop instead, and that loop was both slower and wrong.
//
// It reclassified the column on every cell -- typeof, a class-attribute walk,
// and for a factor a `levels` lookup -- and re-escaped the column's name once
// per row, so a 100000 x 8 frame did 800000 class inspections and escaped 8
// names 800000 times. On one worker that cost 38.4 ms against 29.2 ms for the
// same frame at top level, and it never entered the pool at all, so against
// the top-level path's 9.9 ms it was 3.9x slower.
//
// It was also missing most of what a column can be. Nine defects, all of them
// reachable from `as_json(list(d = df))`:
//
//   * a Date column emitted its epoch day number, 18262 for "2020-01-01"
//   * a POSIXct column emitted local civil seconds, 1577872800
//   * `na` left at its default emitted "NA" and null where jsonlite omits the
//     key, so `{}` came out as {"a":"NA","b":null}
//   * a matrix column emitted only the row's first element, 1 for [1,3]
//   * an sfc column lost its type, [1,2] for a Point
//   * `dataframe = "columns"` was ignored -- the argument never reached the
//     recursive entry point at all
//   * `dataframe = "values"` likewise
//   * a data.frame-valued column (tidyr::nest) repeated the Date and matrix
//     defects, being rendered by the same per-cell writer
//   * a list column holding a frame did too
//
// All of them are properties of the per-cell writer, not of nesting, so the
// fix is to stop having two implementations. `write_df_into` is what the
// recursive serializer calls; `df_row_chunks` and `df_col_chunks` are the
// middle that both paths share. Assembly stays separate, because the
// top-level path writes its chunks straight into the R object and must not
// gain a copy on the way.

use crate::*;
// `exports` is not glob-imported by lib.rs, so these two are named.
use crate::exports::{with_pool_if, PhaseTimer};

/// Writes rows `start..end`, comma separated, with no enclosing brackets.
#[inline]
pub(crate) fn write_rows(
    w: &mut JsonWriter,
    start: usize,
    end: usize,
    props: &[(Key, ThreadSafeColumn)],
    config: SerializerConfig,
) {
    let values = config.df == DfMode::Values;
    for i in start..end {
        if i > start {
            w.push_u8(b',');
        }
        if values {
            process_row_values(w, i, props, config);
        } else {
            process_row_generic(w, i, props, config);
        }
    }
}

/// Serialised row chunks for a prepared frame, in row order.
///
/// `ph`, when given, records the same phases the top-level path always
/// reported.
pub(crate) fn df_row_chunks(
    props: &[(Key, ThreadSafeColumn)],
    n_rows: usize,
    // Row count alone is the wrong measure -- a 2000-row x 200-column frame is
    // more work than a 200000-row x 1-column one -- so the caller supplies
    // `estimate_row_work(props)`, which it has usually already computed to
    // decide whether to come here at all.
    row_work: usize,
    config: SerializerConfig,
    mut ph: Option<&mut PhaseTimer>,
) -> PResult<Vec<Vec<u8>>> {
    let chunk_size = rows_per_chunk(n_rows, row_work);
    // estimate_row_work is roughly a quarter of the bytes a row occupies, so
    // this sizes each chunk buffer from the actual columns instead of a flat
    // 128 bytes per row. Being 20x out in either direction costs either
    // untouched pages or a realloc-and-copy of the whole chunk.
    let est_row_bytes = (row_work * 8).clamp(16, 1 << 16);
    let num_chunks = (n_rows + chunk_size - 1) / chunk_size;
    let ranges: Vec<(usize, usize, usize)> = (0..num_chunks)
        .map(|id| {
            (
                id,
                id * chunk_size,
                (id * chunk_size + chunk_size).min(n_rows),
            )
        })
        .collect();
    if let Some(p) = ph.as_deref_mut() {
        p.lap("plan chunks");
    }

    let par = ranges.len() > 1 && desired_threads() > 1;
    let parts_res: Vec<PResult<(usize, Vec<u8>)>> = with_pool_if(par, || {
        ranges
            .into_par_iter()
            .map(|(chunk_id, start, end)| {
                let rr = catch_unwind(AssertUnwindSafe(|| {
                    let mut w = JsonWriter::with_capacity((end - start) * est_row_bytes);
                    write_rows(&mut w, start, end, props, config);
                    (chunk_id, w.buf)
                }));
                match rr {
                    Ok(v) => Ok(v),
                    Err(p) => Err(format!("Worker panic: {}", panic_message(p))),
                }
            })
            .collect()
    });
    if let Some(p) = ph.as_deref_mut() {
        p.lap("serialize (parallel)");
    }

    let mut parts: Vec<(usize, Vec<u8>)> = Vec::with_capacity(parts_res.len());
    for r in parts_res {
        parts.push(r?);
    }
    parts.sort_by_key(|(id, _)| *id);
    Ok(parts.into_iter().map(|(_, v)| v).collect())
}

/// Serialised `"key":[...]` parts for `dataframe = "columns"`, in column order.
pub(crate) fn df_col_chunks(
    props: &[(Key, ThreadSafeColumn)],
    n_rows: usize,
    config: SerializerConfig,
    mut ph: Option<&mut PhaseTimer>,
) -> PResult<Vec<Vec<u8>>> {
    // One task per column leaves most of the machine idle whenever there are
    // fewer columns than workers, and a one-column frame entirely serial: a
    // 250000 x 1 numeric frame measured 5.31 ms of `serialize (parallel)` on
    // 32 workers, which is one worker's work. Columns that are big enough on
    // their own are split by rows as well.
    //
    // The row split is decided by `rows_per_chunk` on that column's own work,
    // so the same thresholds that keep a small frame serial apply here, and
    // the number of pieces is capped so that columns x pieces stays near the
    // worker count rather than flooding the pool.
    let threads = desired_threads();
    let cap = if props.len() >= threads {
        1
    } else {
        (threads + props.len() - 1) / props.len()
    };
    let mut tasks: Vec<(usize, usize, usize, usize)> = Vec::with_capacity(props.len());
    for (ci, (_, col)) in props.iter().enumerate() {
        let pieces = if cap <= 1 || n_rows == 0 || matches!(col.kind, ColumnType::JsonWhole) {
            1
        } else {
            let w = estimate_row_work(std::slice::from_ref(&props[ci]));
            let cs = rows_per_chunk(n_rows, w).max(1);
            ((n_rows + cs - 1) / cs).clamp(1, cap)
        };
        let _ = col;
        let per = (n_rows + pieces - 1) / pieces;
        let mut start = 0usize;
        while start < n_rows || (start == 0 && n_rows == 0) {
            let end = (start + per).min(n_rows);
            tasks.push((ci, tasks.len(), start, end));
            if n_rows == 0 {
                break;
            }
            start = end;
        }
    }

    let par = tasks.len() > 1 && threads > 1;
    let frags_res: Vec<PResult<(usize, usize, Vec<u8>)>> = with_pool_if(par, || {
        tasks
            .par_iter()
            .map(|&(ci, seq, start, end)| {
                // The row path has always converted a worker panic into an R
                // error; this one used to let it unwind across the boundary.
                let rr = catch_unwind(AssertUnwindSafe(|| {
                    let (key, col) = &props[ci];
                    let mut w = JsonWriter::with_capacity((end - start) * 16 + 32);
                    // A whole-column blob replaces the usual bracketed run of
                    // cells; it is never split, so this is its only piece.
                    if matches!(col.kind, ColumnType::JsonWhole) {
                        w.push_key(key);
                        write_col_value(&mut w, 0, col, config);
                        return (ci, seq, w.buf);
                    }
                    if start == 0 {
                        w.push_key(key);
                        w.push_u8(b'[');
                    } else {
                        // Continuing a column, so the separator belongs here
                        // rather than between the pieces.
                        w.push_u8(b',');
                    }
                    for r in start..end {
                        if r > start {
                            w.push_u8(b',');
                        }
                        write_col_value(&mut w, r, col, config);
                    }
                    if end == n_rows {
                        w.push_u8(b']');
                    }
                    (ci, seq, w.buf)
                }));
                match rr {
                    Ok(v) => Ok(v),
                    Err(p) => Err(format!("Worker panic: {}", panic_message(p))),
                }
            })
            .collect()
    });
    if let Some(p) = ph.as_deref_mut() {
        p.lap("serialize (parallel)");
    }

    let mut frags: Vec<(usize, usize, Vec<u8>)> = Vec::with_capacity(frags_res.len());
    for r in frags_res {
        frags.push(r?);
    }
    frags.sort_by_key(|&(ci, seq, _)| (ci, seq));

    // Back to one part per column, which is what the caller joins with commas
    // inside `{}`. A column that was not split hands its buffer straight over.
    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(props.len());
    let mut i = 0usize;
    while i < frags.len() {
        let ci = frags[i].0;
        let mut j = i + 1;
        while j < frags.len() && frags[j].0 == ci {
            j += 1;
        }
        if j == i + 1 {
            parts.push(std::mem::take(&mut frags[i].2));
        } else {
            let total: usize = frags[i..j].iter().map(|(_, _, b)| b.len()).sum();
            let mut out: Vec<u8> = Vec::with_capacity(total);
            for (_, _, b) in &frags[i..j] {
                out.extend_from_slice(b);
            }
            parts.push(out);
        }
        i = j;
    }
    Ok(parts)
}

/// Writes a data.frame's body into `buf` with the column machinery.
///
/// Returns false, having written nothing, for the two shapes that are
/// literals rather than work -- no columns, or no rows -- which the caller
/// emits itself.
///
/// `depth` is the recursion depth of the enclosing serializer and is passed on
/// to the column builder, whose list-column and nested-frame branches recurse
/// back into it. The builder previously hardcoded 0 there, which was harmless
/// only because a nested frame never reached it; delegating without threading
/// the depth through would have made a chain of frames joined by list columns
/// recurse without any bound.
pub(crate) unsafe fn write_df_into(
    x: libR_sys::SEXP,
    buf: &mut Vec<u8>,
    config: SerializerConfig,
    depth: u32,
) -> bool {
    let n_cols = sexp_len(x);
    let n_rows = get_df_nrows(x);
    if n_cols == 0 || n_rows == 0 {
        return false;
    }
    let keys = match escaped_keys(x, n_cols) {
        Some(v) => v,
        None => return false,
    };

    let cols_mode = config.df == DfMode::Columns;
    let props = match build_thread_safe_cols(x, keys, usize::MAX, n_rows, config, depth) {
        Ok(p) => p,
        Err(e) => {
            // No Result to return from inside a buffer writer, so the message
            // goes through the same channel the encoding and depth errors use
            // and the entry point raises it.
            str_state_error(format!("{:?}", e));
            buf.extend_from_slice(b"null");
            return true;
        }
    };
    let (open, close) = if cols_mode { (b'{', b'}') } else { (b'[', b']') };

    // Nothing to spread: write straight into the caller's buffer. That skips
    // the chunk vector, the parts vector, the join and a full copy of the
    // output -- about ten allocations, which is most of the cost of a small
    // frame and all of it for 200 of them in a list.
    let row_work = if cols_mode { 0 } else { estimate_row_work(&props) };
    let single = if cols_mode {
        props.len() <= 1 || desired_threads() <= 1
    } else {
        rows_per_chunk(n_rows, row_work) >= n_rows || desired_threads() <= 1
    };
    if single {
        let mut w = JsonWriter { buf: std::mem::take(buf), scratch: Vec::new() };
        w.push_u8(open);
        if cols_mode {
            for (i, (key, col)) in props.iter().enumerate() {
                if i > 0 {
                    w.push_u8(b',');
                }
                w.push_key(key);
                w.push_u8(b'[');
                for r in 0..n_rows {
                    if r > 0 {
                        w.push_u8(b',');
                    }
                    write_col_value(&mut w, r, col, config);
                }
                w.push_u8(b']');
            }
        } else {
            write_rows(&mut w, 0, n_rows, &props, config);
        }
        w.push_u8(close);
        *buf = w.buf;
        return true;
    }

    let res = if cols_mode {
        df_col_chunks(&props, n_rows, config, None)
    } else {
        df_row_chunks(&props, n_rows, row_work, config, None)
    };
    let chunks = match res {
        Ok(c) => c,
        Err(msg) => {
            str_state_error(msg);
            buf.extend_from_slice(b"null");
            return true;
        }
    };

    let total: usize = chunks.iter().map(|c| c.len()).sum();
    buf.reserve(total + chunks.len() + 2);
    buf.push(open);
    let mut first = true;
    for c in &chunks {
        if c.is_empty() {
            continue;
        }
        if !first {
            buf.push(b',');
        }
        buf.extend_from_slice(c);
        first = false;
    }
    buf.push(close);
    true
}
