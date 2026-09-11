//! Clone density — heap allocations per delivered row on the hydrate and
//! advance paths, counted with dhat (deterministic, unlike wall time).
//!
//! The IVM operators are a 1:1 port, so their per-row work is fixed by TS;
//! what Rust adds on top is allocation — a `.clone()` of a `String`, a
//! `Vec<String>` key, a `SourceSchema` tree or a `Node` on every row or every
//! child fetch. This harness pins that overhead: each shape asserts an upper
//! bound on `allocations / delivered row`, set just above the measured value
//! after the per-row clones were removed, so re-introducing one (a schema
//! clone per parent node, a column map clone per fetch) fails here.
//!
//! Report the numbers: `cargo test -p rust-ivm --test clone_density_test -- --nocapture`

#[path = "support/clone_density_fixture.rs"]
mod fixture;

use fixture::{Fixture, SourceKind, exists_ast, related_ast, take_ast};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const N_ISSUES: usize = 1_000;

struct Stat {
    rows: usize,
    blocks: u64,
    bytes: u64,
    micros: u128,
}

impl Stat {
    fn blocks_per_row(&self) -> f64 {
        self.blocks as f64 / self.rows.max(1) as f64
    }
    fn bytes_per_row(&self) -> f64 {
        self.bytes as f64 / self.rows.max(1) as f64
    }
}

fn measure(label: &str, f: impl FnOnce() -> usize) -> Stat {
    let before = dhat::HeapStats::get();
    let t = std::time::Instant::now();
    let rows = f();
    let micros = t.elapsed().as_micros();
    let after = dhat::HeapStats::get();
    let s = Stat {
        rows,
        blocks: after.total_blocks - before.total_blocks,
        bytes: after.total_bytes - before.total_bytes,
        micros,
    };
    println!(
        "{label:<32} rows={:>7} allocs={:>9} bytes={:>11}  allocs/row={:>7.1} bytes/row={:>8.0}  {:>7.1} ms",
        s.rows,
        s.blocks,
        s.bytes,
        s.blocks_per_row(),
        s.bytes_per_row(),
        s.micros as f64 / 1000.0
    );
    s
}

/// One (shape, source) cell of the report. `limit` is the allocations-per-row
/// ceiling for the hydrate; `advance_limit` for the new-issue advance.
fn run(kind: SourceKind) -> Vec<(String, Stat)> {
    let mut out = Vec::new();
    let mut fx = Fixture::build(kind, N_ISSUES);
    let tag = format!("{kind:?}");

    let s = measure(&format!("{tag}/related hydrate"), || {
        fx.hydrate("related", related_ast())
    });
    out.push((format!("{tag}/related hydrate"), s));
    let s = measure(&format!("{tag}/related advance+issues"), || {
        fx.advance_new_issues(200)
    });
    out.push((format!("{tag}/related advance+issues"), s));
    let s = measure(&format!("{tag}/related advance edit"), || {
        fx.advance_edit_comments(200)
    });
    out.push((format!("{tag}/related advance edit"), s));
    fx.remove("related");

    let s = measure(&format!("{tag}/exists hydrate"), || {
        fx.hydrate("exists", exists_ast())
    });
    out.push((format!("{tag}/exists hydrate"), s));
    let s = measure(&format!("{tag}/exists advance+issues"), || {
        fx.advance_new_issues(200)
    });
    out.push((format!("{tag}/exists advance+issues"), s));
    fx.remove("exists");

    let s = measure(&format!("{tag}/take hydrate"), || {
        fx.hydrate("take", take_ast(100))
    });
    out.push((format!("{tag}/take hydrate"), s));
    let s = measure(&format!("{tag}/take advance edit"), || {
        fx.advance_edit_comments(50)
    });
    out.push((format!("{tag}/take advance edit"), s));
    fx.remove("take");
    out
}

/// Attribution run: `CLONE_DENSITY_SHAPE="Sqlite/exists hydrate" \
/// CLONE_DENSITY_OUT=/path/dhat.json cargo test --test clone_density_test \
/// -- --ignored clone_density_profile` writes a dhat profile of that one
/// shape (view it at https://nnethercote.github.io/dh_view/dh_view.html, or
/// aggregate the allocation sites by frame).
#[test]
#[ignore = "attribution harness; run explicitly with --ignored and CLONE_DENSITY_SHAPE"]
fn clone_density_profile() {
    let shape = std::env::var("CLONE_DENSITY_SHAPE").expect("CLONE_DENSITY_SHAPE");
    let out = std::env::var("CLONE_DENSITY_OUT").unwrap_or_else(|_| "dhat-heap.json".to_string());
    let (kind, rest) = shape.split_once('/').expect("<Memory|Sqlite>/<shape>");
    let kind = match kind {
        "Memory" => SourceKind::Memory,
        "Sqlite" => SourceKind::Sqlite,
        other => panic!("unknown source kind {other}"),
    };
    let mut fx = Fixture::build(kind, N_ISSUES);
    let (query, ast) = match rest.split(' ').next().unwrap_or("") {
        "related" => ("related", related_ast()),
        "exists" => ("exists", exists_ast()),
        "take" => ("take", take_ast(100)),
        other => panic!("unknown shape {other}"),
    };
    let hydrate_only = rest.ends_with("hydrate");
    if !hydrate_only {
        fx.hydrate(query, ast.clone());
    }
    // Profile only the measured phase: the fixture build and any priming
    // hydrate stay outside the profiler window.
    let profiler = dhat::Profiler::builder().file_name(&out).build();
    let rows = if hydrate_only {
        fx.hydrate(query, ast)
    } else if rest.ends_with("advance edit") {
        fx.advance_edit_comments(200)
    } else {
        fx.advance_new_issues(200)
    };
    drop(profiler);
    println!("{shape}: rows={rows} profile written to {out}");
    assert!(rows > 0);
}

/// Allocations-per-delivered-row ceilings: the value measured after the
/// per-row clones were removed, plus ten percent of slack for allocator-
/// independent variation (dhat counts every allocation, so the measurement
/// itself is deterministic). A shape over its ceiling means a clone came back
/// on a per-row or per-fetch path; the failure message prints the measured
/// value. Lower a ceiling when a change removes more; never raise one to make
/// a regression pass.
const CEILINGS: &[(&str, f64)] = &[
    ("Memory/related hydrate", 15.0),
    ("Memory/related advance+issues", 86.7),
    ("Memory/related advance edit", 145.2),
    ("Memory/exists hydrate", 45.2),
    ("Memory/exists advance+issues", 136.4),
    ("Memory/take hydrate", 15.8),
    ("Memory/take advance edit", 152.9),
    ("Sqlite/related hydrate", 20.5),
    ("Sqlite/related advance+issues", 80.6),
    ("Sqlite/related advance edit", 139.7),
    ("Sqlite/exists hydrate", 50.2),
    ("Sqlite/exists advance+issues", 133.1),
    ("Sqlite/take hydrate", 23.8),
    ("Sqlite/take advance edit", 147.4),
];

#[test]
fn clone_density_report() {
    let _profiler = dhat::Profiler::builder().testing().build();
    let mut all = run(SourceKind::Memory);
    all.extend(run(SourceKind::Sqlite));
    let mut over = Vec::new();
    for (label, s) in &all {
        assert!(
            s.rows > 0,
            "{label}: delivered no rows — the fixture is broken"
        );
        let ceiling = CEILINGS
            .iter()
            .find(|(l, _)| l == label)
            .map(|(_, c)| *c)
            .unwrap_or_else(|| panic!("{label}: no ceiling registered in CEILINGS"));
        if s.blocks_per_row() > ceiling {
            over.push(format!(
                "{label}: {:.1} allocations/row, ceiling {ceiling}",
                s.blocks_per_row()
            ));
        }
    }
    assert!(
        over.is_empty(),
        "clone density regressed on {} shape(s):\n  {}\nA per-row or per-fetch clone came back; \
         profile it with clone_density_profile before touching the ceiling.",
        over.len(),
        over.join("\n  ")
    );
}
