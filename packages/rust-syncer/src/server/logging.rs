//! Port of `zero-cache/src/server/logging.ts` (`createLogContext`: the
//! `{worker, workerIndex}` context every worker's lines carry) and the
//! `shared/src/logging.ts` it wraps (`createLogContext`: `pid`;
//! `consoleJsonLogSink`: the JSON envelope).
//!
//! TS (`shared/src/logging.ts:96-119`) writes one object per line:
//!
//! ```text
//! {"level": LEVEL, ...context, ...lastObj, "message": args.join(' ')}
//! ```
//!
//! `level` first, `message` LAST, and the context (`pid`, `worker`,
//! `workerIndex`, every `withContext` key) flattened at the top level. The
//! stock `tracing_subscriber` JSON format instead nests every field under
//! `"fields"` and carries no `pid`/`worker`/`workerIndex`, so a log pipeline
//! keyed on `.message` read 4,810 of 4,956 rust INFO lines as empty (sandbox
//! pod `xyne-spaces-zero-…-98c9b4446-bssjz`, 2026-09-09; the rust image bakes
//! `ZERO_LOG_FORMAT=json`, so this is the production shape).
//!
//! Rust-only adapter: tracing has no `LogContext`, so an event's structured
//! fields play the `withContext` keys. One rust-only key, `target` (the
//! tracing module path `RUST_LOG` filters on — TS has no twin), sits before
//! `message`. Span fields are not rendered: the three crates declare no spans
//! (0 `info_span!`/`#[instrument]` sites), and TS has no span concept.

use std::fmt;

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// TS `consoleJsonLogSink` (`shared/src/logging.ts:96`): the JSON envelope,
/// carrying the `{pid, worker, workerIndex}` context `createLogContext`
/// (`zero-cache/src/server/logging.ts:27`, `shared/src/logging.ts:92`) bakes
/// into every line.
pub struct ConsoleJsonLogSink {
    pid: u32,
    worker: String,
    worker_index: u32,
}

impl ConsoleJsonLogSink {
    pub fn new(worker: &str, worker_index: u32) -> Self {
        Self {
            pid: std::process::id(),
            worker: worker.to_string(),
            worker_index,
        }
    }
}

/// Collects an event's fields into the envelope; `message` is held back so it
/// can be written last, where TS puts it.
struct EnvelopeVisitor<'a> {
    fields: &'a mut Map<String, Value>,
    message: &'a mut Option<String>,
}

impl EnvelopeVisitor<'_> {
    fn put(&mut self, field: &Field, value: Value) {
        if field.name() == "message" {
            *self.message = Some(match value {
                Value::String(s) => s,
                other => other.to_string(),
            });
        } else {
            self.fields.insert(field.name().to_string(), value);
        }
    }
}

impl Visit for EnvelopeVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, Value::from(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, Value::from(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, Value::from(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, Value::from(value));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, Value::from(value));
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, Value::from(value.to_string()));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // `format_args!` (the message) and `%display` values both arrive here;
        // `Debug` for `fmt::Arguments` renders the formatted text unquoted.
        self.put(field, Value::from(format!("{value:?}")));
    }
}

impl<S, N> FormatEvent<S, N> for ConsoleJsonLogSink
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        // `serde_json` is built with `preserve_order`, so insertion order is
        // the wire order: level, pid, worker, workerIndex, fields, target, message.
        let mut obj = Map::new();
        obj.insert("level".into(), Value::from(meta.level().to_string()));
        obj.insert("pid".into(), Value::from(self.pid));
        obj.insert("worker".into(), Value::from(self.worker.as_str()));
        obj.insert("workerIndex".into(), Value::from(self.worker_index));
        let mut message = None;
        event.record(&mut EnvelopeVisitor {
            fields: &mut obj,
            message: &mut message,
        });
        obj.insert("target".into(), Value::from(meta.target()));
        if let Some(message) = message {
            obj.insert("message".into(), Value::from(message));
        }
        let line = serde_json::to_string(&obj).map_err(|_| fmt::Error)?;
        writeln!(writer, "{line}")
    }
}

/// Port of `createLogContext` (`zero-cache/src/server/logging.ts:20`): install
/// the process-wide subscriber for this `worker`/`workerIndex`. Filter
/// precedence: `RUST_LOG` (rust-native targeting syntax) else `ZERO_LOG_LEVEL`
/// (the zero-cache config's level, forwarded by rust-syncer-bridge) else
/// `info`. `ZERO_LOG_FORMAT=json` selects the TS envelope above — REQUIRED in
/// deployments whose log pipeline parses the container stream as JSON (the
/// parent zero-cache forwards this binary's stdout verbatim). The plaintext
/// path is tracing's default line format. ANSI is never emitted: stdout is a
/// pipe to the parent, never a tty.
pub fn create_log_context(worker: &str, worker_index: u32) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| {
            std::env::var("ZERO_LOG_LEVEL").map(|l| tracing_subscriber::EnvFilter::new(l.trim()))
        })
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let json_logs =
        std::env::var("ZERO_LOG_FORMAT").is_ok_and(|f| f.trim().eq_ignore_ascii_case("json"));
    if json_logs {
        tracing_subscriber::fmt()
            .event_format(ConsoleJsonLogSink::new(worker, worker_index))
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .init();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone)]
    struct CapWriter(Arc<Mutex<Vec<u8>>>);
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapWriter {
        type Writer = CapWriter;
        fn make_writer(&'a self) -> CapWriter {
            self.clone()
        }
    }
    impl std::io::Write for CapWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// TS `shared/src/logging.ts:96-119`: `level` first, the `{pid, worker,
    /// workerIndex}` context and the event fields flattened at the top level,
    /// `message` LAST, nothing nested. NON-VACUOUS: point the builder at the
    /// stock `tracing_subscriber::fmt::format().json()` main.rs used until this
    /// commit and `line["message"]` is null (it lives under `"fields"`),
    /// `pid`/`worker`/`workerIndex` are absent, and the key order differs.
    #[test]
    fn json_lines_carry_the_ts_envelope_with_message_last() {
        let buf = CapWriter(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .event_format(ConsoleJsonLogSink::new("syncer", 3))
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(
                client_id = "c1",
                rows = 7u64,
                "closing connection with error: {}",
                "boom"
            );
        });
        let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let line: Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(line["level"], "WARN");
        assert_eq!(line["pid"], std::process::id());
        assert_eq!(line["worker"], "syncer");
        assert_eq!(line["workerIndex"], 3);
        assert_eq!(line["client_id"], "c1");
        assert_eq!(line["rows"], 7);
        assert_eq!(line["message"], "closing connection with error: boom");
        assert!(line.get("fields").is_none(), "no tracing `fields` nesting");
        let keys: Vec<&str> = line
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "level",
                "pid",
                "worker",
                "workerIndex",
                "client_id",
                "rows",
                "target",
                "message"
            ],
            "TS order: level first, message last"
        );
    }
}
