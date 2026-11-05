//! unituner: tuner sharing and abstraction server with a minimal
//! mirakurun-compatible HTTP API.
//!
//! Phase 4/6/7: axum server, TS streaming, the channel scan API and the
//! embedded management UI (`webui`).

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
