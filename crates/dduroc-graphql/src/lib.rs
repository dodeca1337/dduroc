//! Transport-neutral, bounded GraphQL access to dduroc readers.
//! The host supplies a Reader; requests never select filesystem paths.
#![forbid(unsafe_code)]
mod config;
pub mod error;
pub mod input;
mod schema;
mod service;
mod session;
mod subscription;
pub mod types;
pub use config::ServiceConfig;
pub use error::ServiceError;
pub use service::Service;
pub use session::{
    CountedSessionOpened, SessionOpened, SessionPage, SessionSeekPage, SessionSeekResult,
};
pub use types::{ConversionLimits, Presentation, Projection};

pub use schema::{LogSchema, SchemaLimits, build_schema};
pub use subscription::FollowStream;
