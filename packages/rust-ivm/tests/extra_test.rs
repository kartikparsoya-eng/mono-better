//! Tests for the newly ported query utilities and ZQLite extras.

use std::collections::HashMap;
use std::sync::Arc;

use rust_ivm::builder::error::{NotImplementedError, QueryParseError};
use rust_ivm::builder::escape_like::escape_like;
use rust_ivm::builder::metrics_delegate::{Metric, MetricsDelegate, NullMetricsDelegate};
use rust_ivm::builder::named::SyncedQuery;
use rust_ivm::builder::query::{Cardinality, RelationshipSpec};
use rust_ivm::builder::query_registry::CustomQuery;
use rust_ivm::builder::schema_query::{SchemaQuery, create_builder};
use rust_ivm::builder::validate_input::validate_input;
use rust_ivm::ivm::data::Value;

fn make_schema() -> SchemaQuery {
    let mut schema: SchemaQuery = HashMap::new();

    let mut user_rels: HashMap<String, RelationshipSpec> = HashMap::new();
    user_rels.insert(
        "posts".to_string(),
        RelationshipSpec {
            source_field: vec!["id".to_string()],
            dest_field: vec!["author_id".to_string()],
            dest_table: "posts".to_string(),
            cardinality: Cardinality::Many,
        },
    );
    schema.insert("users".to_string(), user_rels);

    let mut post_rels: HashMap<String, RelationshipSpec> = HashMap::new();
    post_rels.insert(
        "author".to_string(),
        RelationshipSpec {
            source_field: vec!["author_id".to_string()],
            dest_field: vec!["id".to_string()],
            dest_table: "users".to_string(),
            cardinality: Cardinality::One,
        },
    );
    schema.insert("posts".to_string(), post_rels);

    schema
}

// ===========================================================================
// escape_like
// ===========================================================================

#[test]
fn test_escape_like_percent() {
    assert_eq!(escape_like("100%done"), "100\\%done");
}

#[test]
fn test_escape_like_underscore() {
    assert_eq!(escape_like("hello_world"), "hello\\_world");
}

#[test]
fn test_escape_like_both() {
    assert_eq!(escape_like("a_b%c"), "a\\_b\\%c");
}

#[test]
fn test_escape_like_none() {
    assert_eq!(escape_like("plain"), "plain");
}

// ===========================================================================
// Error types
// ===========================================================================

#[test]
fn test_query_parse_error() {
    let err = QueryParseError::new(None);
    assert!(err.message.contains("Failed to parse"));
    let err2 = QueryParseError::new(Some(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "bad input",
    ))));
    assert!(err2.message.contains("bad input"));
}

#[test]
fn test_not_implemented_error() {
    let err = NotImplementedError::new("junction limit");
    assert!(err.to_string().contains("junction limit"));
}

// ===========================================================================
// Metrics delegate
// ===========================================================================

#[test]
fn test_metric_names() {
    assert_eq!(
        Metric::QueryMaterializationClient.name(),
        "query-materialization-client"
    );
    assert_eq!(
        Metric::QueryMaterializationEndToEnd.name(),
        "query-materialization-end-to-end"
    );
    assert_eq!(
        Metric::QueryMaterializationServer.name(),
        "query-materialization-server"
    );
    assert_eq!(Metric::QueryUpdateClient.name(), "query-update-client");
    assert_eq!(Metric::QueryUpdateServer.name(), "query-update-server");
}

#[test]
fn test_metric_is_client_server() {
    assert!(Metric::QueryMaterializationClient.is_client_metric());
    assert!(Metric::QueryMaterializationEndToEnd.is_client_metric());
    assert!(Metric::QueryUpdateClient.is_client_metric());
    assert!(Metric::QueryMaterializationServer.is_server_metric());
    assert!(Metric::QueryUpdateServer.is_server_metric());
    assert!(!Metric::QueryMaterializationClient.is_server_metric());
}

#[test]
fn test_null_metrics_delegate() {
    let delegate = NullMetricsDelegate;
    delegate.add_metric(Metric::QueryUpdateClient, 5.0, "q1", None);
    // Should not panic
}

// ===========================================================================
// validate_input
// ===========================================================================

#[test]
fn test_validate_input_no_validator() {
    let input = Value::F64(42.0);
    let result = validate_input("myquery", &input, None, "query");
    assert!(result.is_ok());
    match result {
        Ok(Value::F64(n)) => assert_eq!(n, 42.0),
        _ => panic!("Expected F64 value"),
    }
}

#[test]
fn test_validate_input_with_validator_pass() {
    let validator = Box::new(|v: &Value| match v {
        Value::F64(_) => Ok(v.clone()),
        _ => Err(vec!["expected number".to_string()]),
    });
    let input = Value::F64(42.0);
    let result = validate_input("myquery", &input, Some(&validator), "query");
    assert!(result.is_ok());
}

#[test]
fn test_validate_input_with_validator_fail() {
    let validator = Box::new(|v: &Value| match v {
        Value::F64(_) => Ok(v.clone()),
        _ => Err(vec!["expected number".to_string()]),
    });
    let input = Value::Str("hello".into());
    let result = validate_input("myquery", &input, Some(&validator), "query");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.message.contains("Validation failed"));
    assert!(err.issues.contains(&"expected number".to_string()));
}

// ===========================================================================
// schema_query / create_builder
// ===========================================================================

#[test]
fn test_create_builder() {
    let schema = make_schema();
    let q = create_builder(&schema, "users");
    assert_eq!(q.ast().table, "users");
}

// ===========================================================================
// CustomQuery / QueryRequest
// ===========================================================================

#[test]
fn test_custom_query_call() {
    let schema = make_schema();
    let cq = CustomQuery::new("getUsers", None, move |_input| {
        create_builder(&schema, "users")
    });

    let request = cq.call(Value::Null);
    assert_eq!(request.custom_query.query_name, "getUsers");
    let result = request.run();
    assert!(result.is_ok());
    let q = result.unwrap();
    assert_eq!(q.ast().table, "users");
}

#[test]
fn test_custom_query_with_validator() {
    let _schema = make_schema();
    let validator = Arc::new(|v: &Value| match v {
        Value::F64(_) => Ok(v.clone()),
        _ => Err(vec!["expected number".to_string()]),
    });

    let cq = CustomQuery::new("getById", Some(validator), move |input| {
        let schema = make_schema();
        let mut q = create_builder(&schema, "users");
        // Use the validated input value
        if let Value::F64(n) = input {
            q = q.where_eq("id", Value::F64(*n));
        }
        q
    });

    // Valid input
    let request = cq.call(Value::F64(42.0));
    let result = request.run();
    assert!(result.is_ok());
    let q = result.unwrap();
    assert!(q.ast().where_clause.is_some());

    // Invalid input
    let request2 = cq.call(Value::Str("bad".into()));
    let result2 = request2.run();
    assert!(result2.is_err());
}

// ===========================================================================
// SyncedQuery
// ===========================================================================

#[test]
fn test_synced_query_basic() {
    let schema = make_schema();
    let sq = SyncedQuery::new(
        "myQuery",
        None,
        Box::new(move |_args| create_builder(&schema, "users")),
    );
    assert_eq!(sq.query_name, "myQuery");
    assert!(!sq.takes_context);

    let result = sq.call(None, &[]);
    assert!(result.is_ok());
    let q = result.unwrap();
    assert_eq!(q.ast().table, "users");
}
