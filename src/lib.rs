//! Official Rust SDK for the [Jobcelis](https://jobcelis.com) Event Infrastructure Platform.
//!
//! All API calls go to `https://jobcelis.com` by default — you only need your API key.
//!
//! # Quick Start
//!
//! ```no_run
//! use jobcelis::JobcelisClient;
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), jobcelis::JobcelisError> {
//!     let client = JobcelisClient::new("your_api_key");
//!     let event = client.send_event("order.created", json!({"order_id": "123"})).await?;
//!     println!("{:?}", event);
//!     Ok(())
//! }
//! ```

mod client;
mod error;
mod retry;
mod webhook;

#[cfg(test)]
mod retry_tests;

pub use client::JobcelisClient;
pub use error::JobcelisError;
pub use retry::DEFAULT_MAX_RETRIES;
pub use webhook::verify_webhook_signature;
