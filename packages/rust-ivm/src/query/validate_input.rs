//! Validate input — port of `zql/src/query/validate-input.ts`.
//!
//! Validates input using a validator function if provided.

use crate::ivm::data::Value;

/// Error from input validation.
/// Port of TS `InputValidationError` (validate-input.ts:5).
#[derive(Debug)]
pub struct InputValidationError {
    pub message: String,
    pub issues: Vec<String>,
}

impl std::fmt::Display for InputValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.message, self.issues.join(", "))
    }
}

impl std::error::Error for InputValidationError {}

/// A validator function that validates input and returns either the validated
/// value or an error with issue messages.
pub type Validator = Box<dyn Fn(&Value) -> Result<Value, Vec<String>>>;

/// Validate input using a validator function if provided.
/// Port of TS `validateInput` (validate-input.ts:37).
/// Accepts a raw `&dyn Fn` trait object so it works with both Box and Rc.
pub fn validate_input(
    name: &str,
    input: &Value,
    validator: Option<&dyn Fn(&Value) -> Result<Value, Vec<String>>>,
    kind: &str,
) -> Result<Value, InputValidationError> {
    match validator {
        None => Ok(input.clone()),
        Some(v) => match v(input) {
            Ok(val) => Ok(val),
            Err(issues) => Err(InputValidationError {
                message: format!("Validation failed for {} {}", kind, name),
                issues,
            }),
        },
    }
}
