//! unituner: tuner sharing and abstraction server with a minimal
//! mirakurun-compatible HTTP API.
//!
//! Phase 1 implements configuration loading/validation and logging only.
//! The remaining modules are stubs matching the layout in SPEC §11.

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
