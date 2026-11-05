//! Session management: reservation, sharing, preemption, fanout, idle release
//! and backpressure (SPEC §6, §7).
//!
//! TODO(phase 3): keep one shared process tree per (tuner, type, channel) and
//! one bounded mpsc per subscriber.
