//! `auth/` — port of `zero-cache/src/auth/`: JWT verification (`jwt.rs`) and the
//! read-permission query transform (`read_authorizer.rs`). The connection-level
//! auth/context state (`auth.ts`) lives with the ConnectionContextManager port.
pub mod jwt;
pub mod load_permissions;
pub mod read_authorizer;
