//! unituner: tuner sharing and abstraction server with a minimal
//! mirakurun-compatible HTTP API.
//!
//! Phase 4/7: axum server, TS streaming and the channel scan API.

pub mod api;
pub mod config;
pub mod pipeline;
pub mod scan;
pub mod services;
pub mod session;
pub mod si;
pub mod tsfilter;
pub mod tsframe;
pub mod webui;
