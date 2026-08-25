//! Query error — port of `zql/src/query/error.ts`.

use std::fmt;

/// Error parsing query arguments.
/// Port of TS `QueryParseError` (error.ts:1).
#[derive(Debug)]
pub struct QueryParseError {
    pub message: String,
    pub cause: Option<Box<dyn std::error::Error>>,
}

impl QueryParseError {
    pub fn new(cause: Option<Box<dyn std::error::Error>>) -> Self {
        let message = match &cause {
            Some(e) => format!("Failed to parse arguments for query: {}", e),
            None => "Failed to parse arguments for query".to_string(),
        };
        QueryParseError { message, cause }
    }
}

impl fmt::Display for QueryParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for QueryParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_deref().map(|e| e as &dyn std::error::Error)
    }
}

/// Error raised when a feature is not yet implemented.
/// Port of TS `NotImplementedError` (from error.ts in the framework).
#[derive(Debug)]
pub struct NotImplementedError {
    pub message: String,
}

impl NotImplementedError {
    pub fn new(msg: &str) -> Self {
        NotImplementedError {
            message: msg.to_string(),
        }
    }
}

impl fmt::Display for NotImplementedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Not implemented: {}", self.message)
    }
}

impl std::error::Error for NotImplementedError {}
