//! Shared fixture for the clone-density harnesses (`tests/clone_density_test.rs`
//! counts allocations per delivered row; `benches/hydrate_bench.rs` times the
//! same shapes). Included by `#[path]` from both, so the two measure one
//! workload.
//!
//! Workload: `user` (50) ← `issue` (N, `ownerId`) ← `comment` (3 per issue,
//! `issueId`). Three query shapes cover the operators the hydrate perf trace
//! attributed the cost to — a `related` join (join.fetch_lazy / child fetch),
//! a correlated EXISTS (exists + child fetch) and a `related` + `limit`
//! (take + join) — over either source kind, so operator-graph overhead
//! (MemorySource) and the SQLite fetch path (TableSource) are measured apart.

#![allow(dead_code)]

use rust_ivm::ivm::data::RowMap;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rusqlite::Connection;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, OrderPart, RelatedSubquery, SimpleCondition,
    ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::{SourceChange, make_source_change_add, make_source_change_edit};
use rust_ivm::sqlite::table_source::TableSource;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceKind {
    Memory,
    Sqlite,
}

pub const COMMENTS_PER_ISSUE: usize = 3;
const USERS: usize = 50;

const TABLES: [(&str, &[&str]); 3] = [
    ("user", &["id", "name"]),
    ("issue", &["id", "ownerId", "title"]),
    ("comment", &["id", "issueId", "body"]),
];

pub struct Fixture {
    pub engine: Engine,
    pub kind: SourceKind,
    pub n_issues: usize,
    next_issue: usize,
    memory: HashMap<String, Rc<RefCell<MemorySource>>>,
    /// Plays the replication stream for the SQLite kind: each pushed change
    /// is written AFTER its advance, so the pinned snapshot shows the
    /// pre-change state during the push (TS validates and fetches against
    /// that) and the next advance sees the row in the database.
    writer: Option<Connection>,
    db_path: Option<String>,
}

fn columns(cols: &[&str]) -> HashMap<String, ColumnType> {
    cols.iter()
        .map(|c| (c.to_string(), ColumnType::String { optional: false }))
        .collect()
}

pub fn row(pairs: &[(&str, &str)]) -> Row {
    let map: RowMap = pairs
        .iter()
        .map(|(k, v)| (Arc::from(*k), Value::Str((*v).into())))
        .collect();
    Arc::new(map)
}

fn user_id(i: usize) -> String {
    format!("u{i:04}")
}
fn issue_id(i: usize) -> String {
    format!("i{i:08}")
}
fn comment_id(issue: usize, k: usize) -> String {
    format!("c{issue:08}-{k}")
}

fn issue_rows(i: usize) -> (Row, Vec<Row>) {
    let iid = issue_id(i);
    let issue = row(&[
        ("id", &iid),
        ("ownerId", &user_id(i % USERS)),
        ("title", &format!("issue {i}")),
    ]);
    let comments = (0..COMMENTS_PER_ISSUE)
        .map(|k| {
            row(&[
                ("id", &comment_id(i, k)),
                ("issueId", &iid),
                ("body", &format!("comment {k} on {i}")),
            ])
        })
        .collect();
    (issue, comments)
}

impl Fixture {
    pub fn build(kind: SourceKind, n_issues: usize) -> Fixture {
        let pks: HashMap<String, Vec<String>> = TABLES
            .iter()
            .map(|(t, _)| (t.to_string(), vec!["id".to_string()]))
            .collect();
        let mut engine = Engine::new(pks);
        let mut memory = HashMap::new();
        let mut writer = None;
        let mut db_path = None;

        match kind {
            SourceKind::Memory => {
                for (t, cols) in TABLES {
                    let src = Rc::new(RefCell::new(MemorySource::new(
                        t,
                        columns(cols),
                        vec!["id".to_string()],
                    )));
                    engine.register_source(src.clone());
                    memory.insert(t.to_string(), src);
                }
                let add = |m: &HashMap<String, Rc<RefCell<MemorySource>>>, t: &str, r: Row| {
                    m[t].borrow_mut().add_row((*r).clone());
                };
                for u in 0..USERS {
                    add(
                        &memory,
                        "user",
                        row(&[("id", &user_id(u)), ("name", &format!("user {u}"))]),
                    );
                }
                for i in 0..n_issues {
                    let (issue, comments) = issue_rows(i);
                    add(&memory, "issue", issue);
                    for c in comments {
                        add(&memory, "comment", c);
                    }
                }
            }
            SourceKind::Sqlite => {
                let path = format!(
                    "{}/rust-ivm-clone-density-{}.db",
                    std::env::temp_dir().display(),
                    std::process::id()
                );
                for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
                    let _ = std::fs::remove_file(format!("{path}{suffix}"));
                }
                let w = Connection::open(&path).expect("open fixture db");
                w.execute_batch(
                    r#"
                    CREATE TABLE user (id TEXT PRIMARY KEY, name TEXT NOT NULL);
                    CREATE TABLE issue (id TEXT PRIMARY KEY, ownerId TEXT NOT NULL, title TEXT NOT NULL);
                    CREATE TABLE comment (id TEXT PRIMARY KEY, issueId TEXT NOT NULL, body TEXT NOT NULL);
                    CREATE INDEX issue_owner ON issue (ownerId);
                    CREATE INDEX comment_issue ON comment (issueId);
                    "#,
                )
                .expect("create fixture tables");
                w.execute_batch("BEGIN").unwrap();
                for u in 0..USERS {
                    w.execute(
                        "INSERT INTO user VALUES (?,?)",
                        rusqlite::params![user_id(u), format!("user {u}")],
                    )
                    .unwrap();
                }
                for i in 0..n_issues {
                    let (issue, comments) = issue_rows(i);
                    insert(&w, "issue", &["id", "ownerId", "title"], &issue);
                    for c in comments {
                        insert(&w, "comment", &["id", "issueId", "body"], &c);
                    }
                }
                w.execute_batch("COMMIT").unwrap();

                let reader = Rc::new(RefCell::new(Connection::open(&path).expect("open reader")));
                for (t, cols) in TABLES {
                    let src =
                        TableSource::new(reader.clone(), t, columns(cols), vec!["id".to_string()]);
                    engine.register_source(Rc::new(RefCell::new(src)));
                }
                writer = Some(w);
                db_path = Some(path);
            }
        }
        for (t, _) in TABLES {
            engine.set_unique_keys(t, vec![vec!["id".to_string()]]);
        }
        Fixture {
            engine,
            kind,
            n_issues,
            next_issue: n_issues,
            memory,
            writer,
            db_path,
        }
    }

    /// Hydrate `ast` under `query_id`; returns the delivered row count.
    pub fn hydrate(&mut self, query_id: &str, ast: Ast) -> usize {
        let mut rows = 0usize;
        self.engine.add_queries_streaming(
            &[QuerySpec {
                query_id: query_id.to_string(),
                ast,
            }],
            |_rc| rows += 1,
        );
        rows
    }

    pub fn remove(&mut self, query_id: &str) {
        self.engine.remove_query(query_id);
    }

    /// Advance with `count` new issues, each with its comments (one change
    /// per row, like the replication stream); returns the delivered row count.
    pub fn advance_new_issues(&mut self, count: usize) -> usize {
        let mut delivered = 0usize;
        for _ in 0..count {
            let i = self.next_issue;
            self.next_issue += 1;
            let (issue, comments) = issue_rows(i);
            let mut changes = vec![("issue".to_string(), make_source_change_add(issue))];
            for c in comments {
                changes.push(("comment".to_string(), make_source_change_add(c)));
            }
            for change in changes {
                delivered += self.advance_one(change);
            }
        }
        delivered
    }

    /// Advance with an edit to the first comment of `count` existing issues.
    pub fn advance_edit_comments(&mut self, count: usize) -> usize {
        let mut delivered = 0usize;
        for i in 0..count.min(self.n_issues) {
            let (_, comments) = issue_rows(i);
            let old = comments[0].clone();
            let mut edited = (*old).clone();
            edited.insert("body".into(), Value::Str(format!("edited {i}").into()));
            let new: Row = Arc::new(edited);
            delivered +=
                self.advance_one(("comment".to_string(), make_source_change_edit(old, new)));
        }
        delivered
    }

    fn advance_one(&mut self, change: (String, SourceChange)) -> usize {
        let mut rows = 0usize;
        self.engine
            .advance_streaming(std::slice::from_ref(&change), |_rc| rows += 1);
        if let Some(w) = &self.writer {
            let (table, change) = &change;
            let cols: &[&str] = TABLES
                .iter()
                .find(|(t, _)| t == table)
                .map(|(_, c)| *c)
                .expect("known table");
            match change {
                SourceChange::Add { row } => insert(w, table, cols, row),
                SourceChange::Edit { row, .. } => {
                    let sets: Vec<String> = cols[1..].iter().map(|c| format!("{c}=?")).collect();
                    let mut vals: Vec<String> = cols[1..]
                        .iter()
                        .map(|c| match row.get(*c) {
                            Some(Value::Str(s)) => s.to_string(),
                            other => panic!("fixture column {c} is not a string: {other:?}"),
                        })
                        .collect();
                    vals.push(match row.get("id") {
                        Some(Value::Str(s)) => s.to_string(),
                        other => panic!("fixture id is not a string: {other:?}"),
                    });
                    w.execute(
                        &format!("UPDATE {table} SET {} WHERE id=?", sets.join(",")),
                        rusqlite::params_from_iter(vals.iter()),
                    )
                    .expect("update fixture row");
                }
                SourceChange::Remove { row } => {
                    let id = match row.get("id") {
                        Some(Value::Str(s)) => s.to_string(),
                        other => panic!("fixture id is not a string: {other:?}"),
                    };
                    w.execute(
                        &format!("DELETE FROM {table} WHERE id=?"),
                        rusqlite::params![id],
                    )
                    .expect("delete fixture row");
                }
            }
        }
        rows
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(path) = &self.db_path {
            for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
                let _ = std::fs::remove_file(format!("{path}{suffix}"));
            }
        }
    }
}

fn insert(w: &Connection, table: &str, cols: &[&str], r: &Row) {
    let sql = format!(
        "INSERT INTO {table} VALUES ({})",
        cols.iter().map(|_| "?").collect::<Vec<_>>().join(",")
    );
    let vals: Vec<String> = cols
        .iter()
        .map(|c| match r.get(*c) {
            Some(Value::Str(s)) => s.to_string(),
            other => panic!("fixture column {c} is not a string: {other:?}"),
        })
        .collect();
    w.execute(&sql, rusqlite::params_from_iter(vals.iter()))
        .expect("insert fixture row");
}

fn order_by_id() -> Option<Vec<OrderPart>> {
    Some(vec![OrderPart {
        column: "id".to_string(),
        direction: "asc".to_string(),
    }])
}

fn comments_related() -> RelatedSubquery {
    RelatedSubquery {
        subquery: Box::new(Ast {
            table: "comment".to_string(),
            alias: Some("comments".to_string()),
            order_by: order_by_id(),
            ..Default::default()
        }),
        relationship_name: "comments".to_string(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["issueId".to_string()],
        hidden: false,
        system: None,
    }
}

/// `issue.related('comments').orderBy('id')`
pub fn related_ast() -> Ast {
    Ast {
        table: "issue".to_string(),
        related: vec![comments_related()],
        order_by: order_by_id(),
        ..Default::default()
    }
}

/// `issue.related('comments').orderBy('id').limit(limit)`
pub fn take_ast(limit: usize) -> Ast {
    Ast {
        limit: Some(limit),
        ..related_ast()
    }
}

/// `issue.whereExists('owner', q => q.where('name', '!=', ''))`
pub fn exists_ast() -> Ast {
    Ast {
        table: "issue".to_string(),
        where_clause: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: RelatedSubquery {
                subquery: Box::new(Ast {
                    table: "user".to_string(),
                    alias: Some("owner".to_string()),
                    where_clause: Some(Condition::Simple(SimpleCondition {
                        op: "!=".to_string(),
                        left: ValuePosition::Column {
                            name: "name".to_string(),
                        },
                        right: ValuePosition::Literal {
                            value: Value::Str("".into()),
                        },
                    })),
                    ..Default::default()
                }),
                relationship_name: "owner".to_string(),
                parent_key: vec!["ownerId".to_string()],
                child_key: vec!["id".to_string()],
                hidden: false,
                system: None,
            },
            op: "EXISTS".to_string(),
            flip: Some(false),
            scalar: false,
            plan_id: None,
        })),
        order_by: order_by_id(),
        ..Default::default()
    }
}
