//! Query registry — port of `zql/src/query/query-registry.ts`.
//!
//! defineQuery, QueryRequest, CustomQuery.

use std::sync::Arc;

use crate::builder::named::CustomQueryID;
use crate::builder::query::Query;
use crate::builder::validate_input::{validate_input, InputValidationError, Validator};
use crate::ivm::data::Value;

/// A query function: produces a Query from a validated input value.
pub type QueryFn = Arc<dyn Fn(&Value) -> Query>;

/// A validator backed by Rc (clonable).
pub type ValidatorRc = Arc<dyn Fn(&Value) -> Result<Value, Vec<String>>>;

/// A custom query: a named, callable query with optional input validation.
pub struct CustomQuery {
    pub query_name: String,
    pub validator: Option<ValidatorRc>,
    pub fn_impl: QueryFn,
}

/// A query request: a custom query instance with bound args.
pub struct QueryRequest {
    pub custom_query: CustomQuery,
    pub args: Value,
}

impl QueryRequest {
    pub fn custom_query_id(&self) -> CustomQueryID {
        CustomQueryID {
            name: self.custom_query.query_name.clone(),
            args: vec![self.args.clone()],
        }
    }

    pub fn run(&self) -> Result<Query, InputValidationError> {
        let validator: Option<&dyn Fn(&Value) -> Result<Value, Vec<String>>> = self.custom_query.validator.as_deref();
        let validated = validate_input(
            &self.custom_query.query_name,
            &self.args,
            validator,
            "query",
        )?;
        Ok((self.custom_query.fn_impl)(&validated))
    }
}

impl CustomQuery {
    pub fn new(
        name: &str,
        validator: Option<ValidatorRc>,
        fn_impl: impl Fn(&Value) -> Query + 'static,
    ) -> Self {
        CustomQuery {
            query_name: name.to_string(),
            validator,
            fn_impl: Arc::new(fn_impl),
        }
    }

    pub fn call(&self, args: Value) -> QueryRequest {
        QueryRequest {
            custom_query: CustomQuery {
                query_name: self.query_name.clone(),
                validator: self.validator.clone(),
                fn_impl: self.fn_impl.clone(),
            },
            args,
        }
    }
}
