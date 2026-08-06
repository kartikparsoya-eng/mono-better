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
