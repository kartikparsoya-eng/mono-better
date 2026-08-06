use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use rust_cvr::row_record_cache::{
    CatchupCursor, ExecuteResult, FlushMode, RowRecord, RowRecordCache,
};
use rust_cvr::row_key::RowID;
use rust_cvr::version::CVRVersion;
use std::sync::Arc;
use tokio::sync::Mutex;

#[napi(object)]
pub struct FlushMetrics {
  pub rows: u32,
  pub elapsed_ms: f64,
}

/// Input entry for apply/executeRowUpdates: a RowID paired with an optional RowRecord.
#[napi(object)]
pub struct RowUpdateEntry {
  pub id: serde_json::Value,
  pub record: Option<serde_json::Value>,
}

/// The structured result when executeRowUpdates does not defer.
#[napi(object)]
pub struct RowsVersionRowJs {
  pub client_group_id: String,
  pub version: String,
}

#[napi(object)]
pub struct RowKeyRefJs {
  pub schema: String,
  pub table: String,
  pub row_key: serde_json::Value,
}

#[napi(object)]
pub struct RowUpdateStatementsJs {
  pub rows_version: RowsVersionRowJs,
  pub deletes: Vec<RowKeyRefJs>,
  pub inserts: Vec<serde_json::Value>,
  pub total_count: u32,
}

/// Result of executeRowUpdates.
#[napi(object)]
pub struct ExecuteResultJs {
  /// "defer" or "execute"
  pub kind: String,
  /// Present when kind = "execute"
  pub statements: Option<RowUpdateStatementsJs>,
}

#[napi]
pub struct RowRecordCacheHandle {
  inner: RowRecordCache,
}

#[napi]
impl RowRecordCacheHandle {
  #[napi(constructor)]
  pub fn new(
    pg_uri: String,
    schema: String,
    cvr_id: String,
    threshold: Option<u32>,
    #[napi(ts_arg_type = "(err: string) => void")]
    on_fail: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled>,
    #[napi(ts_arg_type = "((metrics: FlushMetrics) => void) | null | undefined")]
    on_metrics: Option<ThreadsafeFunction<FlushMetrics, ErrorStrategy::CalleeHandled>>,
  ) -> Result<Self> {
    let pool = sqlx::postgres::PgPoolOptions::new()
      .connect_lazy(&pg_uri)
      .map_err(|e| {
        Error::new(
          Status::InvalidArg,
          format!("Failed to create PgPool: {}", e),
        )
      })?;

    let fail_service: rust_cvr::row_record_cache::FailCallback =
      Arc::new(move |err: String| {
        let _ = on_fail.call(Ok(err), ThreadsafeFunctionCallMode::NonBlocking);
      });

    let metrics_callback = on_metrics.map(|cb| {
      let metrics_cb: rust_cvr::row_record_cache::MetricsCallback =
        Arc::new(move |rows: usize, elapsed_ms: f64| {
          let _ = cb.call(
            Ok(FlushMetrics {
              rows: rows as u32,
              elapsed_ms,
            }),
            ThreadsafeFunctionCallMode::NonBlocking,
          );
        });
      metrics_cb
    });

    let inner = RowRecordCache::new(
      pool,
      schema,
      cvr_id,
      threshold.unwrap_or(100) as usize,
      fail_service,
      metrics_callback,
    );

    Ok(RowRecordCacheHandle { inner })
  }

  /// Loads all row records from the CVR store. Returns the count.
  #[napi]
  pub async fn load(&self) -> Result<u32> {
    let count = self
      .inner
      .load()
      .await
      .map_err(|e| Error::new(Status::GenericFailure, format!("load failed: {}", e)))?;
    Ok(count as u32)
  }

  /// Returns all cached row records as a JSON object keyed by rowIDString.
  #[napi]
  pub async fn get_row_records(&self) -> Result<serde_json::Value> {
    let records = self.inner.get_row_records().await;
    let map: serde_json::Map<String, serde_json::Value> = records
      .into_iter()
      .map(|(k, v)| {
        let val = serde_json::to_value(&v).unwrap_or(serde_json::Value::Null);
        (k, val)
      })
      .collect();
    Ok(serde_json::Value::Object(map))
  }

  /// Applies row updates to the cache. Returns the cache size.
  #[napi]
  pub async fn apply(
    &self,
    row_records: Vec<RowUpdateEntry>,
    rows_version: serde_json::Value,
    flushed: bool,
  ) -> Result<u32> {
    let version: CVRVersion = serde_json::from_value(rows_version).map_err(|e| {
      Error::new(
        Status::InvalidArg,
        format!("invalid rowsVersion: {}", e),
      )
    })?;

    let mut entries: Vec<(RowID, Option<RowRecord>)> = Vec::with_capacity(row_records.len());
    for entry in row_records {
      let id: RowID = serde_json::from_value(entry.id).map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid RowID: {}", e))
      })?;
      let record: Option<RowRecord> = match entry.record {
        Some(v) => Some(serde_json::from_value(v).map_err(|e| {
          Error::new(Status::InvalidArg, format!("invalid RowRecord: {}", e))
        })?),
        None => None,
      };
      entries.push((id, record));
    }

    let count = self
      .inner
      .apply(entries, version, flushed)
      .await
      .map_err(|e| Error::new(Status::GenericFailure, e))?;
    Ok(count as u32)
  }

  /// Returns true if there are pending (unflushed) row updates.
  #[napi]
  pub fn has_pending_updates_sync(&self) -> bool {
    self.inner.is_flushing.load(std::sync::atomic::Ordering::SeqCst)
  }

  /// Returns true if there are pending (unflushed) row updates.
  #[napi]
  pub async fn has_pending_updates(&self) -> Result<bool> {
    Ok(self.inner.has_pending_updates().await)
  }

  /// Waits until all pending row records have been flushed to the CVR store.
  #[napi]
  pub async fn flushed(&self) -> Result<()> {
    self
      .inner
      .flushed()
      .await
      .map_err(|e| Error::new(Status::GenericFailure, e))
  }

  /// Clears the in-memory cache (preserves pending writes).
  #[napi]
  pub async fn clear(&self) -> Result<()> {
    self.inner.clear().await;
    Ok(())
  }

  /// Decides whether to defer or execute row updates.
  /// Returns structured data for the TS wrapper to build SQL.
  #[napi]
  pub fn execute_row_updates(
    &self,
    version: serde_json::Value,
    row_updates: Vec<RowUpdateEntry>,
    mode: String,
  ) -> Result<ExecuteResultJs> {
    let version: CVRVersion = serde_json::from_value(version).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid version: {}", e))
    })?;

    let flush_mode = if mode == "force" {
      FlushMode::Force
    } else {
      FlushMode::AllowDefer
    };

    let mut entries: Vec<(RowID, Option<RowRecord>)> = Vec::with_capacity(row_updates.len());
    for entry in row_updates {
      let id: RowID = serde_json::from_value(entry.id).map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid RowID: {}", e))
      })?;
      let record: Option<RowRecord> = match entry.record {
        Some(v) => Some(serde_json::from_value(v).map_err(|e| {
          Error::new(Status::InvalidArg, format!("invalid RowRecord: {}", e))
        })?),
        None => None,
      };
      entries.push((id, record));
    }

    let result = self.inner.execute_row_updates(&version, &entries, flush_mode);

    match result {
      ExecuteResult::Defer => Ok(ExecuteResultJs {
        kind: "defer".to_string(),
        statements: None,
      }),
      ExecuteResult::Execute(stmts) => {
        let inserts_json: Vec<serde_json::Value> = stmts
          .inserts
          .iter()
          .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
          .collect();

        Ok(ExecuteResultJs {
          kind: "execute".to_string(),
          statements: Some(RowUpdateStatementsJs {
            rows_version: RowsVersionRowJs {
              client_group_id: stmts.rows_version.client_group_id,
              version: stmts.rows_version.version,
            },
            deletes: stmts
              .deletes
              .iter()
              .map(|d| RowKeyRefJs {
                schema: d.schema.clone(),
                table: d.table.clone(),
                row_key: d.row_key.clone(),
              })
              .collect(),
            inserts: inserts_json,
            total_count: stmts.total_count as u32,
          }),
        })
      }
    }
  }

  /// Starts a catchup cursor for streaming row patches.
  #[napi]
  pub async fn catchup_row_patches(
    &self,
    after_version: Option<serde_json::Value>,
    up_to_version: serde_json::Value,
    current: serde_json::Value,
    exclude_query_hashes: Vec<String>,
  ) -> Result<CatchupCursorHandle> {
    let after: Option<CVRVersion> = match after_version {
      Some(v) => Some(serde_json::from_value(v).map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid afterVersion: {}", e))
      })?),
      None => None,
    };

    let up_to: CVRVersion = serde_json::from_value(up_to_version).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid upToVersion: {}", e))
    })?;

    let current: CVRVersion = serde_json::from_value(current).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid current: {}", e))
    })?;

    let cursor = self
      .inner
      .catchup_row_patches(after, &up_to, &current, &exclude_query_hashes)
      .await
      .map_err(|e| Error::new(Status::GenericFailure, format!("catchup: {}", e)))?;

    Ok(CatchupCursorHandle {
      inner: Mutex::new(Some(cursor)),
    })
  }
}

#[napi]
pub struct CatchupCursorHandle {
  inner: Mutex<Option<CatchupCursor>>,
}

#[napi]
impl CatchupCursorHandle {
  /// Pulls the next page of rows (up to 10000). Returns null when done.
  #[napi]
  pub async fn next_page(&self) -> Result<Option<Vec<serde_json::Value>>> {
    let mut guard = self.inner.lock().await;
    let cursor = match &mut *guard {
      Some(c) => c,
      None => return Ok(None),
    };

    match cursor.next_page().await {
      Ok(Some(rows)) => {
        let json_rows: Vec<serde_json::Value> = rows
          .iter()
          .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
          .collect();
        Ok(Some(json_rows))
      }
      Ok(None) => {
        *guard = None;
        Ok(None)
      }
      Err(e) => Err(Error::new(Status::GenericFailure, e)),
    }
  }
}

// ════════════════════════════════════════════════════════════════════════════
// Phase C napi: CVR Updaters
// ════════════════════════════════════════════════════════════════════════════

use rust_cvr::updater::{CVRConfigDrivenUpdater, CVRQueryDrivenUpdater};
use rust_cvr::types::{CVR, ShardID, DesiredQuerySpec};

#[napi]
pub struct CVRConfigDrivenUpdaterHandle {
  inner: tokio::sync::Mutex<CVRConfigDrivenUpdater>,
  store: Arc<tokio::sync::Mutex<CVRStoreHandle>>,
}

#[napi]
impl CVRConfigDrivenUpdaterHandle {
  #[napi(constructor)]
  pub fn new(
    cvr_json: serde_json::Value,
    shard_json: serde_json::Value,
    store: &CVRStoreNapiHandle,
  ) -> Result<Self> {
    let cvr: CVR = serde_json::from_value(cvr_json).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid cvr: {}", e))
    })?;
    let shard: ShardID = serde_json::from_value(shard_json).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid shard: {}", e))
    })?;
    Ok(Self {
      inner: tokio::sync::Mutex::new(CVRConfigDrivenUpdater::new(cvr, shard)),
      store: store.clone_inner(),
    })
  }

  /// Internal helper: drain StoreOps from the updater and apply them to the store.
  /// All in Rust — zero napi boundary crossing.
  async fn drain_and_apply(&self, inner: &mut CVRConfigDrivenUpdater) -> Result<()> {
    let ops = inner.base.drain_store_ops();
    if ops.is_empty() {
      return Ok(());
    }
    let mut store = self.store.lock().await;
    store.apply_store_ops(ops);
    Ok(())
  }

  #[napi]
  pub async fn ensure_client(&self, id: String) -> Result<serde_json::Value> {
    let mut inner = self.inner.lock().await;
    inner.ensure_client(&id);
    self.drain_and_apply(&mut inner).await?;
    let client = inner.base.cvr.clients.get(&id).ok_or_else(|| {
      Error::new(Status::GenericFailure, "ensure_client failed")
    })?;
    serde_json::to_value(client).map_err(|e| {
      Error::new(Status::GenericFailure, format!("serialize: {}", e))
    })
  }

  #[napi]
  pub async fn set_client_schema(&self, schema_json: serde_json::Value) -> Result<()> {
    let mut inner = self.inner.lock().await;
    let schema: rust_cvr::types::ClientSchema = serde_json::from_value(schema_json)
      .map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid schema: {}", e))
      })?;
    inner.set_client_schema(schema).map_err(|e| {
      Error::new(Status::GenericFailure, e)
    })?;
    self.drain_and_apply(&mut inner).await
  }

  #[napi]
  pub async fn set_profile_id(&self, profile_id: String) -> Result<()> {
    let mut inner = self.inner.lock().await;
    inner.set_profile_id(&profile_id);
    self.drain_and_apply(&mut inner).await
  }

  #[napi]
  pub async fn put_desired_queries(
    &self,
    client_id: String,
    desired_queries_json: serde_json::Value,
  ) -> Result<Vec<serde_json::Value>> {
    let mut inner = self.inner.lock().await;
    let specs: Vec<DesiredQuerySpec> = serde_json::from_value(desired_queries_json)
      .map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid desiredQueries: {}", e))
      })?;
    let patches = inner.put_desired_queries(&client_id, &specs);
    self.drain_and_apply(&mut inner).await?;
    Ok(patches.iter().map(|p| serde_json::to_value(p).unwrap_or(Value::Null)).collect())
  }

  #[napi]
  pub async fn delete_desired_queries(
    &self,
    client_id: String,
    query_ids: Vec<String>,
  ) -> Result<Vec<serde_json::Value>> {
    let mut inner = self.inner.lock().await;
    let patches = inner.delete_desired_queries(&client_id, &query_ids);
    self.drain_and_apply(&mut inner).await?;
    Ok(patches.iter().map(|p| serde_json::to_value(p).unwrap_or(Value::Null)).collect())
  }

  #[napi]
  pub async fn mark_desired_queries_as_inactive(
    &self,
    client_id: String,
    query_ids: Vec<String>,
    ttl_clock: i64,
  ) -> Result<()> {
    let mut inner = self.inner.lock().await;
    inner.mark_desired_queries_as_inactive(&client_id, &query_ids, ttl_clock);
    self.drain_and_apply(&mut inner).await
  }

  #[napi]
  pub async fn clear_desired_queries(&self, client_id: String) -> Result<Vec<serde_json::Value>> {
    let mut inner = self.inner.lock().await;
    let patches = inner.clear_desired_queries(&client_id);
    self.drain_and_apply(&mut inner).await?;
    Ok(patches.iter().map(|p| serde_json::to_value(p).unwrap_or(Value::Null)).collect())
  }

  #[napi]
  pub async fn delete_client(&self, client_id: String, ttl_clock: i64) -> Result<Vec<serde_json::Value>> {
    let mut inner = self.inner.lock().await;
    let patches = inner.delete_client(&client_id, ttl_clock);
    self.drain_and_apply(&mut inner).await?;
    Ok(patches.iter().map(|p| serde_json::to_value(p).unwrap_or(Value::Null)).collect())
  }

  #[napi]
  pub async fn flush(&self, last_connect_time: i64, last_active: i64, ttl_clock: i64) -> Result<serde_json::Value> {
    let mut inner = self.inner.lock().await;
    let (version, patches) = inner.flush(last_connect_time, last_active, ttl_clock);
    self.drain_and_apply(&mut inner).await?;
    Ok(serde_json::json!({
      "version": serde_json::to_value(&version).unwrap_or(Value::Null),
      "patches": patches.iter().map(|p| serde_json::to_value(p).unwrap_or(Value::Null)).collect::<Vec<_>>(),
    }))
  }

  #[napi]
  pub async fn get_cvr(&self) -> Result<serde_json::Value> {
    let inner = self.inner.lock().await;
    serde_json::to_value(&inner.base.cvr).map_err(|e| {
      Error::new(Status::GenericFailure, format!("serialize: {}", e))
    })
  }

  #[napi]
  pub async fn get_version(&self) -> Result<serde_json::Value> {
    let mut inner = self.inner.lock().await;
    serde_json::to_value(&inner.base.cvr.version).map_err(|e| {
      Error::new(Status::GenericFailure, format!("serialize: {}", e))
    })
  }
}

#[napi]
pub struct CVRQueryDrivenUpdaterHandle {
  inner: tokio::sync::Mutex<CVRQueryDrivenUpdater>,
  store: Arc<tokio::sync::Mutex<CVRStoreHandle>>,
}

#[napi]
impl CVRQueryDrivenUpdaterHandle {
  #[napi(constructor)]
  pub fn new(
    cvr_json: serde_json::Value,
    state_version: String,
    replica_version: String,
    store: &CVRStoreNapiHandle,
  ) -> Result<Self> {
    let cvr: CVR = serde_json::from_value(cvr_json).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid cvr: {}", e))
    })?;
    Ok(Self {
      inner: tokio::sync::Mutex::new(CVRQueryDrivenUpdater::new(cvr, state_version, replica_version, None)),
      store: store.clone_inner(),
    })
  }

  /// Internal helper: drain StoreOps from the updater and apply them to the store.
  /// All in Rust — zero napi boundary crossing.
  async fn drain_and_apply(&self, inner: &mut CVRQueryDrivenUpdater) -> Result<()> {
    let ops = inner.base.drain_store_ops();
    if ops.is_empty() {
      return Ok(());
    }
    let mut store = self.store.lock().await;
    store.apply_store_ops(ops);
    Ok(())
  }

  #[napi]
  pub async fn updated_version(&self) -> Result<serde_json::Value> {
    let inner = self.inner.lock().await;
    serde_json::to_value(&inner.updated_version()).map_err(|e| {
      Error::new(Status::GenericFailure, format!("serialize: {}", e))
    })
  }

  #[napi]
  pub async fn track_queries(
    &self,
    executed_json: serde_json::Value,
    removed_json: serde_json::Value,
  ) -> Result<serde_json::Value> {
    let mut inner = self.inner.lock().await;
    // executed is an array of [queryID, transformationHash] pairs
    let executed_raw: Vec<(String, String)> = serde_json::from_value(executed_json)
      .map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid executed: {}", e))
      })?;
    let executed_refs: Vec<(&str, &str)> = executed_raw.iter()
      .map(|(a, b)| (a.as_str(), b.as_str()))
      .collect();
    let removed: Vec<String> = serde_json::from_value(removed_json)
      .map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid removed: {}", e))
      })?;
    let removed_refs: Vec<&str> = removed.iter().map(|s| s.as_str()).collect();
    let (version, patches) = inner.track_queries(&executed_refs, &removed_refs);
    self.drain_and_apply(&mut inner).await?;
    Ok(serde_json::json!({
      "version": serde_json::to_value(&version).unwrap_or(Value::Null),
      "patches": patches.iter().map(|p| serde_json::to_value(p).unwrap_or(Value::Null)).collect::<Vec<_>>(),
    }))
  }

  #[napi]
  pub async fn received(
    &self,
    rows_json: serde_json::Value,
    existing_rows_json: serde_json::Value,
  ) -> Result<Vec<serde_json::Value>> {
    let mut inner = self.inner.lock().await;
    // rows is a map of rowIDString -> {id: RowID, update: RowUpdate}
    #[derive(Deserialize)]
    struct RowEntry {
      id: rust_cvr::row_key::RowID,
      update: rust_cvr::types::RowUpdate,
    }
    let rows: std::collections::HashMap<String, RowEntry> =
      serde_json::from_value(rows_json).map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid rows: {}", e))
      })?;
    let rows: std::collections::HashMap<String, (rust_cvr::row_key::RowID, rust_cvr::types::RowUpdate)> =
      rows.into_iter().map(|(k, v)| (k, (v.id, v.update))).collect();
    let existing_rows: std::collections::HashMap<String, rust_cvr::types::RowRecord> =
      serde_json::from_value(existing_rows_json).map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid existingRows: {}", e))
      })?;
    let patches = inner.received(&rows, &existing_rows);
    self.drain_and_apply(&mut inner).await?;
    Ok(patches.iter().map(|p| serde_json::to_value(p).unwrap_or(Value::Null)).collect())
  }

  #[napi]
  pub async fn delete_unreferenced_rows(
    &self,
    existing_rows_json: serde_json::Value,
  ) -> Result<Vec<serde_json::Value>> {
    let mut inner = self.inner.lock().await;
    let existing_rows: Vec<rust_cvr::types::RowRecord> =
      serde_json::from_value(existing_rows_json).map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid existingRows: {}", e))
      })?;
    let patches = inner.delete_unreferenced_rows(&existing_rows);
    self.drain_and_apply(&mut inner).await?;
    Ok(patches.iter().map(|p| serde_json::to_value(p).unwrap_or(Value::Null)).collect())
  }

  #[napi]
  pub async fn flush(&self, last_connect_time: i64, last_active: i64, ttl_clock: i64) -> Result<serde_json::Value> {
    let mut inner = self.inner.lock().await;
    let (version, patches) = inner.flush(last_connect_time, last_active, ttl_clock);
    self.drain_and_apply(&mut inner).await?;
    Ok(serde_json::json!({
      "version": serde_json::to_value(&version).unwrap_or(Value::Null),
      "patches": patches.iter().map(|p| serde_json::to_value(p).unwrap_or(Value::Null)).collect::<Vec<_>>(),
    }))
  }

  #[napi]
  pub async fn get_cvr(&self) -> Result<serde_json::Value> {
    let inner = self.inner.lock().await;
    serde_json::to_value(&inner.base.cvr).map_err(|e| {
      Error::new(Status::GenericFailure, format!("serialize: {}", e))
    })
  }
}

// ════════════════════════════════════════════════════════════════════════════
// Phase D napi: ClientHandler + PokeHandler
// ════════════════════════════════════════════════════════════════════════════

use rust_cvr::client_handler::{ClientHandler, PokeHandler, WebSocketSink};
use serde::Deserialize;
use serde_json::Value;

struct NapiWebSocketSink {
  push_fn: ThreadsafeFunction<serde_json::Value, ErrorStrategy::CalleeHandled>,
  fail_fn: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled>,
  cancel_fn: ThreadsafeFunction<bool, ErrorStrategy::CalleeHandled>,
}

impl WebSocketSink for NapiWebSocketSink {
  fn push(&self, msg: Value) -> std::result::Result<(), String> {
    let status = self.push_fn.call(Ok(msg), ThreadsafeFunctionCallMode::Blocking);
    if status == Status::Ok || status == Status::Closing {
      Ok(())
    } else {
      Err(format!("TSFN push failed: {:?}", status))
    }
  }

  fn fail(&self, e: String) {
    let _ = self.fail_fn.call(Ok(e), ThreadsafeFunctionCallMode::NonBlocking);
  }

  fn cancel(&self) {
    let _ = self.cancel_fn.call(Ok(true), ThreadsafeFunctionCallMode::NonBlocking);
  }
}

#[napi]
pub struct ClientHandlerHandle {
  inner: Arc<ClientHandler>,
}

#[napi]
impl ClientHandlerHandle {
  #[napi(constructor)]
  pub fn new(
    client_group_id: String,
    client_id: String,
    ws_id: String,
    shard_json: serde_json::Value,
    base_cookie: Option<String>,
    #[napi(ts_arg_type = "(msg: unknown) => void")]
    push_fn: ThreadsafeFunction<serde_json::Value, ErrorStrategy::CalleeHandled>,
    #[napi(ts_arg_type = "(err: string) => void")]
    fail_fn: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled>,
    #[napi(ts_arg_type = "() => void")]
    cancel_fn: ThreadsafeFunction<bool, ErrorStrategy::CalleeHandled>,
  ) -> Result<Self> {
    let shard: ShardID = serde_json::from_value(shard_json).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid shard: {}", e))
    })?;
    let sink = Arc::new(NapiWebSocketSink {
      push_fn,
      fail_fn,
      cancel_fn,
    });
    let inner = Arc::new(ClientHandler::new(
      &client_group_id,
      &client_id,
      &ws_id,
      &shard,
      base_cookie.as_deref(),
      sink,
    ));
    Ok(Self { inner })
  }

  #[napi]
  pub fn version(&self) -> Result<serde_json::Value> {
    let v = self.inner.version();
    serde_json::to_value(&v).map_err(|e| {
      Error::new(Status::GenericFailure, format!("serialize: {}", e))
    })
  }

  #[napi]
  pub fn fail(&self, e: String) {
    self.inner.fail(&e);
  }

  #[napi]
  pub fn close(&self, reason: String) {
    self.inner.close(&reason);
  }

  #[napi]
  pub fn start_poke(&self, tentative_version: serde_json::Value) -> Result<PokeHandlerHandle> {
    let version: CVRVersion = serde_json::from_value(tentative_version).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid version: {}", e))
    })?;
    let poke = self.inner.start_poke(version);
    Ok(PokeHandlerHandle { inner: poke })
  }

  #[napi]
  pub fn send_delete_clients(&self, client_ids: Vec<String>, client_group_ids: Vec<String>) -> Result<()> {
    self
      .inner
      .send_delete_clients(client_ids, client_group_ids)
      .map_err(|e| Error::new(Status::GenericFailure, e))
  }

  #[napi]
  pub fn send_query_transform_application_errors(
    &self,
    errors_json: serde_json::Value,
  ) -> Result<()> {
    let errors: Vec<Value> = serde_json::from_value(errors_json).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid errors: {}", e))
    })?;
    self
      .inner
      .send_query_transform_application_errors(errors)
      .map_err(|e| Error::new(Status::GenericFailure, e))
  }

  #[napi]
  pub fn send_inspect_response(&self, response: serde_json::Value) {
    self.inner.send_inspect_response(response);
  }
}

#[napi]
pub struct PokeHandlerHandle {
  inner: PokeHandler,
}

#[napi]
impl PokeHandlerHandle {
  #[napi]
  pub fn add_patch(&self, patch_json: serde_json::Value) -> Result<()> {
    let patch: rust_cvr::types::PatchToVersion = serde_json::from_value(patch_json)
      .map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid patch: {}", e))
      })?;
    self
      .inner
      .add_patch(&patch)
      .map_err(|e| Error::new(Status::GenericFailure, e))
  }

  #[napi]
  pub fn cancel(&self) -> Result<()> {
    self
      .inner
      .cancel()
      .map_err(|e| Error::new(Status::GenericFailure, e))
  }

  #[napi]
  pub fn end(&self, final_version: serde_json::Value) -> Result<()> {
    let version: CVRVersion = serde_json::from_value(final_version).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid version: {}", e))
    })?;
    self
      .inner
      .end(version)
      .map_err(|e| Error::new(Status::GenericFailure, e))
  }
}

// ════════════════════════════════════════════════════════════════════════════
// Phase E napi: CVRStore
// ════════════════════════════════════════════════════════════════════════════

use rust_cvr::store::{CVRStoreHandle, CVRFlushStats as RustCVRFlushStats};

#[napi(object)]
pub struct CVRFlushStatsJs {
  pub clients: u32,
  pub queries: u32,
  pub rows: u32,
  pub desires: u32,
  pub instances: u32,
}

impl From<RustCVRFlushStats> for CVRFlushStatsJs {
  fn from(s: RustCVRFlushStats) -> Self {
    Self {
      clients: s.clients as u32,
      queries: s.queries as u32,
      rows: s.rows as u32,
      desires: s.desires as u32,
      instances: s.instances as u32,
    }
  }
}

#[napi]
pub struct CVRStoreNapiHandle {
  inner: Arc<tokio::sync::Mutex<CVRStoreHandle>>,
}

#[napi]
impl CVRStoreNapiHandle {
  #[napi(constructor)]
  pub fn new(
    pg_uri: String,
    schema: String,
    cvr_id: String,
    task_id: String,
  ) -> Result<Self> {
    let pool = sqlx::postgres::PgPoolOptions::new()
      .connect_lazy(&pg_uri)
      .map_err(|e| {
        Error::new(
          Status::InvalidArg,
          format!("Failed to create PgPool: {}", e),
        )
      })?;
    Ok(Self {
      inner: Arc::new(tokio::sync::Mutex::new(CVRStoreHandle::new(pool, schema, cvr_id, task_id))),
    })
  }

  /// Returns a clone of the inner Arc for sharing with updater handles.
  /// Called internally by the updater constructor.
  pub fn clone_inner(&self) -> Arc<tokio::sync::Mutex<CVRStoreHandle>> {
    self.inner.clone()
  }

  #[napi]
  pub async fn has_pending_writes(&self) -> Result<bool> {
    let mut inner = self.inner.lock().await;
    Ok(inner.has_pending_writes())
  }

  #[napi]
  pub async fn row_count(&self) -> Result<u32> {
    let mut inner = self.inner.lock().await;
    Ok(inner.row_count() as u32)
  }

  #[napi]
  pub async fn flush(
    &self,
    version_json: serde_json::Value,
    cvr_json: serde_json::Value,
    last_connect_time: f64,
  ) -> Result<CVRFlushStatsJs> {
    let version: CVRVersion = serde_json::from_value(version_json).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid version: {}", e))
    })?;
    let cvr: CVR = serde_json::from_value(cvr_json).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid cvr: {}", e))
    })?;
    // We need to move the inner out, flush, and move back.
    // Use try_lock pattern.
    let mut inner_guard = self.inner.lock().await;
    let stats = inner_guard
      .flush(&version, &cvr, last_connect_time)
      .await
      .map_err(|e| Error::new(Status::GenericFailure, format!("{}", e)))?;
    match stats {
      Some(s) => Ok(s.into()),
      None => Ok(CVRFlushStatsJs { clients: 0, queries: 0, rows: 0, desires: 0, instances: 0 }),
    }
  }

  #[napi]
  pub async fn load(&self, last_connect_time: f64) -> Result<serde_json::Value> {
    let mut inner = self.inner.lock().await;
    let result = inner
      .load(last_connect_time)
      .await
      .map_err(|e| Error::new(Status::GenericFailure, format!("{}", e)))?;
    serde_json::to_value(&result).map_err(|e| {
      Error::new(Status::GenericFailure, format!("serialize: {}", e))
    })
  }

  #[napi]
  pub async fn catchup_config_patches(
    &self,
    after_version: Option<serde_json::Value>,
    up_to_version: serde_json::Value,
  ) -> Result<Vec<serde_json::Value>> {
    let after: Option<CVRVersion> = match after_version {
      Some(v) => Some(serde_json::from_value(v).map_err(|e| {
        Error::new(Status::InvalidArg, format!("invalid afterVersion: {}", e))
      })?),
      None => None,
    };
    let up_to: CVRVersion = serde_json::from_value(up_to_version).map_err(|e| {
      Error::new(Status::InvalidArg, format!("invalid upToVersion: {}", e))
    })?;
    let mut inner = self.inner.lock().await;
    let patches = inner
      .catchup_config_patches(after, &up_to, &up_to)
      .await
      .map_err(|e| Error::new(Status::GenericFailure, format!("{}", e)))?;
    Ok(patches.iter().map(|p| serde_json::to_value(p).unwrap_or(Value::Null)).collect())
  }
}
