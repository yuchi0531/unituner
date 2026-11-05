//! Session management: reservation, sharing, preemption, fan-out, idle release
//! and backpressure (SPEC §4, §6, §7).
//!
//! # One process tree per `(type, channel)`
//!
//! A session owns one tuner process plus the type's pipeline stages. It is
//! registered under the internal key `(tuner, type, channel)` — the tuner is
//! fixed when the session starts — but **join search is by `(type, channel)`**
//! (SPEC §6.2): at most one session exists for a channel, so two requests can
//! never spawn two processes for it.
//!
//! # Selection and takeover
//!
//! [`SessionManager::acquire`] implements SPEC §6.2:
//!
//! 1. an existing session for `(type, channel)` is joined regardless of
//!    priority (for a pinned request, only if it already runs on that tuner);
//! 2. otherwise a completely free tuner supporting the type is used (pinned:
//!    only the named tuner);
//! 3. otherwise the weakest session that may be preempted is replaced (SPEC
//!    §6.1: requester priority `>= 0`, victim's **max** subscriber priority
//!    strictly lower, `128` grabs unconditionally, negative priority and the
//!    scan priority `-1000` never take over);
//! 4. otherwise [`AcquireError::Busy`] / [`AcquireError::NoTuner`].
//!
//! # Readiness and takeover ordering
//!
//! Pick → reserve → spawn happens under one registry mutex: a `Starting`
//! placeholder is inserted, so the tuner is invisible to other requests while
//! the spawn itself runs outside the lock. The placeholder is what concurrent
//! requests wait on, which keeps every `await` out of the critical section.
//!
//! A victim with subscribers is **never** killed before the replacement has
//! produced its first TS data (SPEC §6.2). The replacement is spawned first,
//! its first chunk is awaited, and only then is the victim removed from the
//! registry, its broadcast closed (all its subscribers see EOF) and its process
//! tree stopped. The tradeoff is explicit: during that window both process
//! trees are started on the same device. If the device is exclusive and the
//! second `open()` fails, the spawn fails, the victim is left untouched and the
//! request gets a 503-equivalent error. That is the order SPEC §6.2 requires.
//!
//! A 0-user (idle) victim is the documented exception: it has no stream to
//! protect and its device may still be held, so it is retired *before* the
//! spawn — SPEC §6.2 explicitly accepts the SIGTERM latency of "kill +
//! restart" for a different channel. The takeover is logged with the old and
//! new priority either way.
//!
//! # Raw vs filtered
//!
//! Every session broadcasts the **post-pipeline** TS (SPEC §4). [`SessionTap`]
//! is the per-subscriber seam for phase 5: [`SessionTap::Raw`] is the channel
//! stream, [`SessionTap::Filtered`] the service stream where a per-subscriber
//! `TSFilter` is inserted. Until phase 5 both deliver the same bytes; the hook
//! exists so `api.rs` and `scan.rs` do not have to change again.
//!
//! # Fan-out and backpressure
//!
//! One task per session reads the last stage's stdout and broadcasts
//! [`Bytes`] chunks (at most `server.chunk_size_bytes` each). Every subscriber
//! owns a bounded mpsc and a pump task from broadcast to mpsc. The pump uses
//! `try_send`: a full queue or a `Lagged` broadcast drops **that subscriber
//! only**, and the broadcast is never blocked by a slow client (SPEC §7).
//!
//! # Idle release
//!
//! When the last subscriber leaves, the process tree is kept for
//! `server.idle_timeout_secs` and then stopped. A new subscriber invalidates
//! the pending timer through a per-session epoch counter, so a cancelled timer
//! can never kill a session that has users again (SPEC §7).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::ChildStdout;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex as AsyncMutex, Notify};

use crate::config::{Config, TunerConfig};
use crate::pipeline::{
    spawn_pipeline, spawn_tuner_command, CommandVars, PipelineCtx, PipelineHandle, PipelineSpec,
    TunerProcess,
};
use crate::services::ServiceTable;
use crate::tsframe::TsNormalizer;

/// Number of recent chunks replayed to a newly joined subscriber.
///
/// The broadcast only delivers chunks sent after `subscribe()`, so without a
/// replay the readiness chunk (broadcast a moment before the first subscriber
/// attaches) would be lost. Two chunks (128 KiB with the defaults) is enough to
/// cover the gap between readiness and `attach` while keeping the replay
/// bounded and small for a late joiner.
pub const PRELUDE_CHUNKS: usize = 2;

/// Capacity of a session's broadcast ring, in chunks.
///
/// SPEC §7 requires the broadcast capacity to be part of the memory budget.
/// A session holds at most this many chunks here **in addition to**
/// `server.subscriber_buffer_chunks` chunks per subscriber: with the defaults
/// (64 KiB chunks) that is ≈1 MiB of broadcast buffer plus ≈64 MiB per
/// subscriber. A subscriber whose pump falls this many chunks behind is
/// dropped; in practice the pump never waits on the client, so this only
/// happens if the runtime itself is starved.
pub const BROADCAST_CAPACITY_CHUNKS: usize = 16;

/// How long [`SessionManager::acquire`] waits for the first pipeline output
/// before treating the spawn as failed (SPEC §6.2 readiness).
///
/// Not specified by SPEC; a tuner that produces no byte at all in this window
/// is treated as broken so a request cannot hang forever. With a working tuner
/// the first packet arrives in milliseconds (a 204-framed source needs two
/// packets for framing detection, still well under a second).
pub const READINESS_TIMEOUT: Duration = Duration::from_secs(10);

/// Re-check interval while waiting for a concurrent spawn.
///
/// The session's `Notify` wakes the waiter immediately; this tick only bounds
/// the damage of a wakeup that was consumed by another waiter before this one
/// polled, so a request never sleeps out a long timeout.
const STARTUP_POLL: Duration = Duration::from_millis(250);

/// Scan sessions run at this priority and never preempt (SPEC §6.1, §8).
pub const SCAN_PRIORITY: i32 = -1000;

/// mirakc's GRAB priority: unconditional takeover (SPEC §6.1).
pub const GRAB_PRIORITY: i32 = 128;

/// One user of a tuner, as supplied by the HTTP layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TunerUser {
    pub id: String,
    /// Request priority (SPEC §6.1). Defaults to 0 at the API layer.
    pub priority: i32,
    /// `X-Mirakurun-Tuner-User-Provider`, if any.
    pub provider: Option<String>,
    /// `X-Mirakurun-Tuner-User-Agent`, if any.
    pub agent: Option<String>,
}

impl TunerUser {
    /// A user with only an id and a priority.
    pub fn new(id: impl Into<String>, priority: i32) -> Self {
        Self {
            id: id.into(),
            priority,
            provider: None,
            agent: None,
        }
    }

    /// Sets the provider header value.
    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }

    /// Sets the user-agent header value.
    pub fn with_agent(mut self, agent: impl Into<String>) -> Self {
        self.agent = Some(agent.into());
        self
    }
}

/// Why a request could not get a session.
///
/// The variants map to SPEC §6.2/§6.3: [`NoTuner`](Self::NoTuner),
/// [`Unavailable`](Self::Unavailable) and [`Busy`](Self::Busy) are 503,
/// [`UnknownTuner`](Self::UnknownTuner) and
/// [`UnsupportedType`](Self::UnsupportedType) are 400, and
/// [`ShuttingDown`](Self::ShuttingDown) is 503.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AcquireError {
    /// No configured tuner supports the requested type (SPEC §6.2).
    #[error("no tuner supports this channel type")]
    NoTuner,
    /// Every candidate failed to start, or the pinned tuner failed to start.
    #[error("tuner is unavailable")]
    Unavailable,
    /// `X-Mirakc-Tuner` / `X-Mirakurun-Tuner` names no enabled tuner.
    #[error("unknown tuner {0:?}")]
    UnknownTuner(String),
    /// The pinned tuner does not support the channel type.
    #[error("tuner does not support channel type {0:?}")]
    UnsupportedType(String),
    /// All suitable tuners are busy and preemption is not allowed.
    #[error("all tuners are busy")]
    Busy,
    /// The manager is shutting down.
    #[error("server is shutting down")]
    ShuttingDown,
}

/// What a subscriber receives from the shared post-pipeline broadcast.
///
/// This is the phase 5 seam (SPEC §4): [`Raw`](Self::Raw) is the channel
/// stream, [`Filtered`](Self::Filtered) the service stream where a
/// per-subscriber `TSFilter` is instantiated. Until the filter is implemented
/// both variants deliver the post-pipeline TS unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTap {
    /// Channel stream: post-pipeline TS, delivered as-is.
    Raw,
    /// Service stream: post-pipeline TS; one `TSFilter` per subscriber later.
    ///
    /// `service_id` is the requested service (phase 5 builds the filter from
    /// it). It is `None` when the caller only knows `raw = false`.
    Filtered { service_id: Option<u16> },
}

/// A live subscription to a session's post-pipeline TS.
///
/// Dropping the subscription closes the mpsc receiver, which ends the
/// subscriber pump and, when it was the last one, arms the idle timer.
pub struct Subscription {
    /// Chunks of post-pipeline TS, at most `server.chunk_size_bytes` each.
    pub rx: mpsc::Receiver<Bytes>,
    /// Stable id of the shared session.
    pub session_id: u64,
    /// Tuner the session runs on.
    pub tuner: String,
    /// Channel type of the session.
    pub type_name: String,
    /// Channel of the session.
    pub channel: String,
    /// Id of this subscriber within the session.
    pub subscriber_id: u64,
    /// The per-subscriber tap this subscription was created with.
    pub tap: SessionTap,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("session_id", &self.session_id)
            .field("tuner", &self.tuner)
            .field("type_name", &self.type_name)
            .field("channel", &self.channel)
            .field("subscriber_id", &self.subscriber_id)
            .field("tap", &self.tap)
            .finish()
    }
}

/// One subscriber attached to a session.
#[derive(Clone, Debug)]
struct SubscriberEntry {
    id: u64,
    user: TunerUser,
    started_at: SystemTime,
    /// Phase 5's per-subscriber TSFilter is selected from this.
    #[allow(dead_code)]
    tap: SessionTap,
}

/// Subscriber information in [`TunerStatus`] (SPEC §9 `/api/tuners`).
#[derive(Clone, Debug)]
pub struct TunerUserStatus {
    pub id: String,
    pub priority: i32,
    pub provider: Option<String>,
    pub agent: Option<String>,
    pub started_at: SystemTime,
}

/// One configured tuner, as exposed by `GET /api/tuners`.
#[derive(Clone, Debug)]
pub struct TunerStatus {
    pub index: usize,
    pub name: String,
    pub types: Vec<String>,
    /// `true` for a configured, enabled tuner: mirakurun's `isAvailable` means
    /// "not faulty", not "idle". `is_fault` is always `false`.
    pub is_available: bool,
    /// `true` while no session occupies the tuner. A 0-user session still holds
    /// the device until its idle timer fires, so it is *not* free.
    pub is_free: bool,
    /// A session (possibly idle) is attached to this tuner.
    pub is_using: bool,
    /// Always `false`: SPEC §1/§9 has no tuner fault detection.
    pub is_fault: bool,
    /// `(type, channel)` currently tuned, if any.
    pub channel: Option<(String, String)>,
    pub users: Vec<TunerUserStatus>,
}

/// Lifecycle phase of a session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionPhase {
    /// Reserved; the process tree is being spawned and readiness awaited.
    Starting,
    /// Streaming; subscribers may join.
    Ready,
    /// Being stopped; the registry no longer exposes it.
    Stopping,
    /// The stream ended (EOF, spawn failure) and cleanup is running.
    Failed,
}

const PHASE_STARTING: u8 = 0;
const PHASE_READY: u8 = 1;
const PHASE_STOPPING: u8 = 2;
const PHASE_FAILED: u8 = 3;

impl SessionPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            PHASE_STARTING => Self::Starting,
            PHASE_READY => Self::Ready,
            PHASE_STOPPING => Self::Stopping,
            _ => Self::Failed,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::Starting => PHASE_STARTING,
            Self::Ready => PHASE_READY,
            Self::Stopping => PHASE_STOPPING,
            Self::Failed => PHASE_FAILED,
        }
    }
}

/// One session, as exposed for diagnostics (not part of the mirakurun API).
#[derive(Clone, Debug)]
pub struct SessionStatus {
    pub id: u64,
    /// Tuner the session was started on.
    pub tuner: String,
    pub type_name: String,
    pub channel: String,
    pub phase: SessionPhase,
    pub users: usize,
    /// Tuner PID followed by the pipeline stage PIDs.
    pub pids: Vec<u32>,
    pub started_at: SystemTime,
}

/// Internal session identity. The tuner is fixed at spawn time (SPEC §6.2).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SessionKey {
    tuner: String,
    type_name: String,
    channel: String,
}

/// The tuner process plus the pipeline stages it feeds.
struct ProcessTree {
    tuner: TunerProcess,
    pipeline: Option<PipelineHandle>,
}

impl ProcessTree {
    /// Stops both halves concurrently; stopping the whole tree takes at most
    /// one grace period.
    async fn stop(&self) {
        let pipeline = async {
            if let Some(pipeline) = &self.pipeline {
                pipeline.stop().await;
            }
        };
        tokio::join!(self.tuner.stop(), pipeline);
    }
}

/// One session: one process tree, one broadcast, one subscriber list.
struct Session {
    id: u64,
    key: SessionKey,
    started_at: SystemTime,
    phase: AtomicU8,
    /// Notified on every phase change. `notify_one` so a change that happens
    /// between a state check and the wait is not lost.
    changed: Notify,
    users: Mutex<Vec<SubscriberEntry>>,
    broadcaster: Mutex<Option<broadcast::Sender<Bytes>>>,
    /// Rolling window of the most recent chunks, replayed to a new subscriber
    /// so the readiness chunk (broadcast while the session was still
    /// `Starting`) is never lost. Guarded together with `subscribe` in
    /// [`attach`], so a chunk is delivered either from here or from the
    /// broadcast, never twice.
    prelude: Mutex<VecDeque<Bytes>>,
    next_subscriber_id: AtomicU64,
    /// Invalidates a pending idle timer; bumped on every new subscriber.
    idle_epoch: AtomicU64,
    /// A takeover is being prepared for this session; new joins wait.
    takeover_pending: AtomicBool,
    /// The stream ended before the session was promoted to `Ready`; the
    /// promoter removes the session instead of leaving a dead `Ready` entry.
    ended: AtomicBool,
    process: AsyncMutex<Option<ProcessTree>>,
    pids: Mutex<Vec<u32>>,
}

impl Session {
    fn new(id: u64, key: SessionKey) -> Self {
        Self {
            id,
            key,
            started_at: SystemTime::now(),
            phase: AtomicU8::new(PHASE_STARTING),
            changed: Notify::new(),
            users: Mutex::new(Vec::new()),
            broadcaster: Mutex::new(None),
            prelude: Mutex::new(VecDeque::new()),
            next_subscriber_id: AtomicU64::new(0),
            idle_epoch: AtomicU64::new(0),
            takeover_pending: AtomicBool::new(false),
            ended: AtomicBool::new(false),
            process: AsyncMutex::new(None),
            pids: Mutex::new(Vec::new()),
        }
    }

    fn phase(&self) -> SessionPhase {
        SessionPhase::from_u8(self.phase.load(Ordering::SeqCst))
    }

    fn set_phase(&self, phase: SessionPhase) {
        self.phase.store(phase.as_u8(), Ordering::SeqCst);
        self.wake();
    }

    /// Wakes every waiter and leaves a permit for one that has not polled yet.
    /// `notify_waiters` alone loses a wakeup that happens between a waiter's
    /// state check and its first poll, and `notify_one` alone wakes only one of
    /// several concurrent acquires.
    fn wake(&self) {
        self.changed.notify_waiters();
        self.changed.notify_one();
    }

    fn users_len(&self) -> usize {
        lock(&self.users).len()
    }

    fn users_is_empty(&self) -> bool {
        lock(&self.users).is_empty()
    }

    fn max_priority(&self) -> Option<i32> {
        lock(&self.users)
            .iter()
            .map(|entry| entry.user.priority)
            .max()
    }

    fn user_statuses(&self) -> Vec<TunerUserStatus> {
        lock(&self.users)
            .iter()
            .map(|entry| TunerUserStatus {
                id: entry.user.id.clone(),
                priority: entry.user.priority,
                provider: entry.user.provider.clone(),
                agent: entry.user.agent.clone(),
                started_at: entry.started_at,
            })
            .collect()
    }

    /// Records `chunk` in the replay window and broadcasts it.
    ///
    /// The prelude lock is held across both the record and the `send`, and
    /// [`attach`] holds it across its snapshot and `subscribe`, so every
    /// subscriber receives each chunk exactly once: from the broadcast if it
    /// subscribed in time, or from the replay window if it joined later.
    fn emit(&self, sender: &broadcast::Sender<Bytes>, chunk: Bytes) -> bool {
        let mut prelude = lock(&self.prelude);
        let first = prelude.is_empty();
        prelude.push_back(chunk.clone());
        while prelude.len() > PRELUDE_CHUNKS {
            prelude.pop_front();
        }
        // No receivers is fine: the stream is still running.
        let _ = sender.send(chunk);
        first
    }

    fn take_broadcaster(&self) -> Option<broadcast::Sender<Bytes>> {
        lock(&self.broadcaster).take()
    }

    /// Drops the session's broadcast sender; receivers observe `Closed` once
    /// the pump's sender is gone too.
    fn close_broadcast(&self) {
        let _ = self.take_broadcaster();
        self.wake();
    }

    async fn stop_process(&self) {
        let tree = self.process.lock().await.take();
        if let Some(tree) = tree {
            tree.stop().await;
        }
    }
}

#[derive(Default)]
struct State {
    /// At most one session per `(type, channel)`; also at most one per tuner,
    /// because a session reserves its tuner from spawn until stop.
    sessions: HashMap<SessionKey, Arc<Session>>,
}

/// Shared state behind [`SessionManager`].
struct Inner {
    config: Arc<Config>,
    services: ServiceTable,
    state: Mutex<State>,
    shutting_down: AtomicBool,
    /// Spawns currently running outside the registry lock. `shutdown` waits for
    /// this to reach zero so a tree that is being spawned is stopped before
    /// shutdown returns.
    spawns_in_flight: AtomicUsize,
    spawn_finished: Notify,
    next_session_id: AtomicU64,
}

impl Inner {
    fn session_id(&self) -> u64 {
        self.next_session_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Finds the session for `(type, channel)`, whatever its tuner.
    fn session_for_channel<'a>(
        state: &'a State,
        type_name: &str,
        channel: &str,
    ) -> Option<&'a Arc<Session>> {
        state
            .sessions
            .values()
            .find(|session| session.key.type_name == type_name && session.key.channel == channel)
    }

    fn tuner_occupied(&self, state: &State, tuner: &str) -> bool {
        state
            .sessions
            .values()
            .any(|session| session.key.tuner == tuner)
    }

    fn tuner(&self, name: &str) -> Option<&TunerConfig> {
        self.config.tuners.iter().find(|tuner| tuner.name == name)
    }

    fn supports(&self, tuner: &TunerConfig, type_name: &str) -> bool {
        !tuner.disabled && tuner.types.iter().any(|name| name == type_name)
    }
}

/// Tuner sessions for one server.
#[derive(Clone)]
pub struct SessionManager {
    inner: Arc<Inner>,
}

impl SessionManager {
    /// Creates a manager for `config`. `services` is stored for phase 5/7
    /// (service streams and scanning read the same table).
    pub fn new(config: Arc<Config>, services: ServiceTable) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                services,
                state: Mutex::new(State::default()),
                shutting_down: AtomicBool::new(false),
                spawns_in_flight: AtomicUsize::new(0),
                spawn_finished: Notify::new(),
                next_session_id: AtomicU64::new(0),
            }),
        }
    }

    /// The configuration the manager was built with.
    pub fn config(&self) -> &Arc<Config> {
        &self.inner.config
    }

    /// The shared service table.
    pub fn services(&self) -> &ServiceTable {
        &self.inner.services
    }

    /// Joins or starts the session for `(type_name, channel)`.
    ///
    /// `pin` is the tuner name from `X-Mirakc-Tuner` / `X-Mirakurun-Tuner`
    /// (empty values are ignored, SPEC §6.3). `raw` selects the channel stream
    /// (`true`) or the service stream (`false`); both currently deliver the
    /// post-pipeline TS, see [`SessionTap`].
    pub async fn acquire(
        &self,
        type_name: &str,
        channel: &str,
        user: TunerUser,
        pin: Option<String>,
        raw: bool,
    ) -> Result<Subscription, AcquireError> {
        let tap = if raw {
            SessionTap::Raw
        } else {
            SessionTap::Filtered { service_id: None }
        };
        self.acquire_with_tap(type_name, channel, user, pin, tap)
            .await
    }

    /// Like [`acquire`](Self::acquire) with an explicit [`SessionTap`].
    ///
    /// This is the entry point phase 5 uses for service streams, which also
    /// know the requested `service_id`.
    pub async fn acquire_with_tap(
        &self,
        type_name: &str,
        channel: &str,
        user: TunerUser,
        pin: Option<String>,
        tap: SessionTap,
    ) -> Result<Subscription, AcquireError> {
        let inner = Arc::clone(&self.inner);
        let pin = pin.and_then(|name| {
            let name = name.trim();
            if name.is_empty() {
                None
            } else {
                Some(name.to_string())
            }
        });

        if inner.shutting_down.load(Ordering::SeqCst) {
            return Err(AcquireError::ShuttingDown);
        }
        // Answer 400-style errors without a spawn.
        if let Some(name) = &pin {
            self.validate_pin(name, type_name)?;
        }

        // Tuners whose spawn failed during this request. Kept per request so a
        // broken tuner cannot loop the pool (SPEC §6.2 fall-through is ON), but
        // a later request may try it again.
        let mut failed: HashSet<String> = HashSet::new();
        // Waiting is only for concurrent spawns; bound the whole wait so a
        // wedged spawn cannot hold a request open indefinitely.
        let wait_deadline =
            tokio::time::Instant::now() + READINESS_TIMEOUT + Duration::from_secs(5);

        loop {
            if inner.shutting_down.load(Ordering::SeqCst) {
                return Err(AcquireError::ShuttingDown);
            }

            let selection = {
                let mut state = lock(&inner.state);
                select(
                    &inner,
                    &mut state,
                    type_name,
                    channel,
                    &user,
                    pin.as_deref(),
                    &failed,
                )
            };

            match selection {
                Selection::Join(session) => {
                    if let Some(subscription) = join(&inner, &session, &user, tap) {
                        return Ok(subscription);
                    }
                    // The session changed between selection and join. Wait for
                    // the next change instead of spinning on a repeated miss.
                    if !wait_before_retry(&session, wait_deadline, type_name, channel).await {
                        return Err(AcquireError::Unavailable);
                    }
                }
                Selection::Wait(session) => {
                    if !wait_before_retry(&session, wait_deadline, type_name, channel).await {
                        return Err(AcquireError::Unavailable);
                    }
                    if session.phase() == SessionPhase::Failed {
                        // Do not retry a tuner that just failed to start.
                        failed.insert(session.key.tuner.clone());
                    }
                }
                Selection::Spawn { session, victim } => {
                    match spawn_and_promote(&inner, session, victim, &user, tap).await {
                        Ok(subscription) => return Ok(subscription),
                        Err(SpawnFailure::ShuttingDown) => return Err(AcquireError::ShuttingDown),
                        Err(SpawnFailure::Failed { tuner }) => {
                            // Pinned requests never fall back to another tuner.
                            if pin.is_some() {
                                return Err(AcquireError::Unavailable);
                            }
                            failed.insert(tuner);
                        }
                    }
                }
                Selection::Fail(error) => {
                    let broke = matches!(error, AcquireError::Busy | AcquireError::NoTuner);
                    return Err(if broke && !failed.is_empty() {
                        // A candidate was tried and failed before this: 503
                        // unavailable is more accurate than 503 busy.
                        AcquireError::Unavailable
                    } else {
                        error
                    });
                }
            }
        }
    }

    /// Validates a pinned tuner name (SPEC §6.3).
    ///
    /// A `disabled: true` tuner is reported as unknown: it cannot be selected,
    /// which is what the 400 must convey.
    fn validate_pin(&self, name: &str, type_name: &str) -> Result<(), AcquireError> {
        let Some(tuner) = self.inner.tuner(name) else {
            return Err(AcquireError::UnknownTuner(name.to_string()));
        };
        if tuner.disabled {
            return Err(AcquireError::UnknownTuner(name.to_string()));
        }
        if !tuner.types.iter().any(|name| name == type_name) {
            return Err(AcquireError::UnsupportedType(type_name.to_string()));
        }
        Ok(())
    }

    /// Status of every configured tuner (SPEC §9 `/api/tuners`).
    pub fn tuners_snapshot(&self) -> Vec<TunerStatus> {
        let sessions: Vec<Arc<Session>> = {
            let state = lock(&self.inner.state);
            state.sessions.values().cloned().collect()
        };
        self.inner
            .config
            .tuners
            .iter()
            .enumerate()
            .map(|(index, tuner)| {
                let session = sessions
                    .iter()
                    .find(|session| session.key.tuner == tuner.name);
                let using = session.is_some();
                let enabled = !tuner.disabled;
                TunerStatus {
                    index,
                    name: tuner.name.clone(),
                    types: tuner.types.clone(),
                    is_available: enabled,
                    is_free: enabled && !using,
                    is_using: using,
                    is_fault: false,
                    channel: session.map(|session| {
                        (session.key.type_name.clone(), session.key.channel.clone())
                    }),
                    users: session
                        .map(|session| session.user_statuses())
                        .unwrap_or_default(),
                }
            })
            .collect()
    }

    /// Status of every live session, including reservations still starting.
    pub fn sessions_snapshot(&self) -> Vec<SessionStatus> {
        let mut sessions: Vec<Arc<Session>> = {
            let state = lock(&self.inner.state);
            state.sessions.values().cloned().collect()
        };
        sessions.sort_by_key(|session| session.id);
        sessions
            .iter()
            .map(|session| SessionStatus {
                id: session.id,
                tuner: session.key.tuner.clone(),
                type_name: session.key.type_name.clone(),
                channel: session.key.channel.clone(),
                phase: session.phase(),
                users: session.users_len(),
                pids: lock(&session.pids).clone(),
                started_at: session.started_at,
            })
            .collect()
    }

    /// Stops every session and waits for all process trees to exit.
    ///
    /// The registry is cleared first, so no new request can join, and every
    /// tree is signalled concurrently. No child process survives.
    pub async fn shutdown(&self) {
        self.inner.shutting_down.store(true, Ordering::SeqCst);

        // A spawn in progress is not visible to the stop pass below if it has
        // not stored its process tree yet. `SpawnGuard` publishes it, and the
        // readiness wait aborts on the shutdown flag, so this settles quickly.
        while self.inner.spawns_in_flight.load(Ordering::SeqCst) > 0 {
            tokio::select! {
                _ = self.inner.spawn_finished.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }

        let sessions: Vec<Arc<Session>> = {
            let mut state = lock(&self.inner.state);
            let sessions: Vec<Arc<Session>> = state.sessions.values().cloned().collect();
            state.sessions.clear();
            sessions
        };
        for session in &sessions {
            session.set_phase(SessionPhase::Stopping);
            session.close_broadcast();
        }

        let mut tasks = Vec::with_capacity(sessions.len());
        for session in sessions {
            tasks.push(tokio::spawn(async move {
                session.stop_process().await;
            }));
        }
        for task in tasks {
            if let Err(error) = task.await {
                tracing::warn!(%error, "session stop task failed during shutdown");
            }
        }
    }
}

/// Marks a spawn as in flight for the lifetime of the operation, so `shutdown`
/// can wait for it to finish and stop its tree.
struct SpawnGuard(Arc<Inner>);

impl SpawnGuard {
    fn new(inner: &Arc<Inner>) -> Self {
        inner.spawns_in_flight.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(inner))
    }
}

impl Drop for SpawnGuard {
    fn drop(&mut self) {
        if self.0.spawns_in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            // Wake every shutdown waiter; leave a permit for one that has not
            // polled yet.
            self.0.spawn_finished.notify_waiters();
            self.0.spawn_finished.notify_one();
        }
    }
}

/// The selection outcome computed under the registry lock.
enum Selection {
    /// Join an existing, ready session.
    Join(Arc<Session>),
    /// Wait for a concurrent spawn / takeover of this session, then retry.
    Wait(Arc<Session>),
    /// A reservation was taken; spawn the tree.
    Spawn {
        session: Arc<Session>,
        victim: Option<Arc<Session>>,
    },
    /// Nothing can satisfy the request.
    Fail(AcquireError),
}

enum SpawnFailure {
    ShuttingDown,
    Failed { tuner: String },
}

/// SPEC §6.1: may `requester` take over a session whose highest subscriber
/// priority is `victim_max`?
fn can_grab(requester: i32, victim_max: i32) -> bool {
    // Negative priorities (including scan at -1000) never take over.
    if requester < 0 {
        return false;
    }
    // mirakc GRAB compatibility: unconditional takeover.
    if requester == GRAB_PRIORITY {
        return true;
    }
    victim_max < requester
}

/// Picks a tuner and atomically reserves it, or explains why it cannot.
fn select(
    inner: &Arc<Inner>,
    state: &mut State,
    type_name: &str,
    channel: &str,
    user: &TunerUser,
    pin: Option<&str>,
    failed: &HashSet<String>,
) -> Selection {
    // A pinned tuner must exist and support the type (SPEC §6.3).
    if let Some(name) = pin {
        match inner.tuner(name) {
            None => return Selection::Fail(AcquireError::UnknownTuner(name.to_string())),
            Some(tuner) if !inner.supports(tuner, type_name) => {
                return Selection::Fail(AcquireError::UnsupportedType(type_name.to_string()));
            }
            Some(_) => {}
        }
    } else if !inner
        .config
        .tuners
        .iter()
        .any(|tuner| inner.supports(tuner, type_name))
    {
        return Selection::Fail(AcquireError::NoTuner);
    }

    // 1. Join an existing session for this (type, channel), if it suits.
    if let Some(existing) = Inner::session_for_channel(state, type_name, channel) {
        let existing = Arc::clone(existing);
        // The channel already streams on another tuner. Joining it would
        // silently ignore the pin, and starting a second session for the same
        // (type, channel) is forbidden by SPEC §6.2, so the request fails
        // instead of falling back. Checked before the phase so a pinned request
        // does not wait for a spawn it can never join.
        if pin.is_some_and(|name| name != existing.key.tuner) {
            return Selection::Fail(AcquireError::Busy);
        }
        return match existing.phase() {
            SessionPhase::Starting => Selection::Wait(existing),
            SessionPhase::Ready => {
                if existing.takeover_pending.load(Ordering::SeqCst) {
                    // A replacement is being prepared; join it afterwards.
                    Selection::Wait(existing)
                } else {
                    Selection::Join(existing)
                }
            }
            // Stopping/failed entries are removed under the same lock; wait for
            // the registry to change instead of racing their cleanup.
            _ => Selection::Wait(existing),
        };
    }

    let supports_type = |tuner: &TunerConfig| inner.supports(tuner, type_name);
    let matches_pin = |tuner: &TunerConfig| match pin {
        Some(name) => tuner.name == name,
        None => true,
    };

    // 2. A completely free, type-compatible tuner.
    let free = inner
        .config
        .tuners
        .iter()
        .filter(|tuner| supports_type(tuner) && matches_pin(tuner))
        .filter(|tuner| !failed.contains(&tuner.name))
        .find(|tuner| !inner.tuner_occupied(state, &tuner.name));
    if let Some(tuner) = free {
        return reserve(state, inner, &tuner.name, type_name, channel, None);
    }

    // 3. The weakest session that may be preempted (SPEC §6.1/§6.2).
    let mut victims: Vec<&Arc<Session>> = state
        .sessions
        .values()
        .filter(|session| session.phase() == SessionPhase::Ready)
        .filter(|session| !session.takeover_pending.load(Ordering::SeqCst))
        .filter(|session| !failed.contains(&session.key.tuner))
        .filter(|session| match pin {
            Some(name) => session.key.tuner == name,
            None => true,
        })
        .filter(|session| {
            inner
                .tuner(&session.key.tuner)
                .is_some_and(|tuner| inner.supports(tuner, type_name))
        })
        .filter(|session| match session.max_priority() {
            Some(victim_max) => can_grab(user.priority, victim_max),
            // A 0-user (idle) session can be reused for another channel by any
            // non-negative request; negative priorities still cannot preempt.
            None => user.priority >= 0,
        })
        .collect();
    // Weakest first; ties are broken by age so the oldest session goes.
    victims.sort_by_key(|session| (session.max_priority(), session.started_at));
    if let Some(victim) = victims.first().map(|session| Arc::clone(session)) {
        // The flag keeps new joins out and stops the idle timer from releasing
        // a session whose replacement is on its way.
        victim.takeover_pending.store(true, Ordering::SeqCst);
        victim.wake();
        let tuner = victim.key.tuner.clone();
        return reserve(state, inner, &tuner, type_name, channel, Some(victim));
    }

    // 4. Nothing available.
    Selection::Fail(AcquireError::Busy)
}

/// Inserts the `Starting` reservation under the lock (pick → reserve atomically).
fn reserve(
    state: &mut State,
    inner: &Arc<Inner>,
    tuner: &str,
    type_name: &str,
    channel: &str,
    victim: Option<Arc<Session>>,
) -> Selection {
    let session = Arc::new(Session::new(
        inner.session_id(),
        SessionKey {
            tuner: tuner.to_string(),
            type_name: type_name.to_string(),
            channel: channel.to_string(),
        },
    ));
    state
        .sessions
        .insert(session.key.clone(), Arc::clone(&session));
    tracing::debug!(
        session_id = session.id,
        tuner,
        type_name,
        channel,
        victim = victim.as_ref().map(|session| session.id),
        "session reserved"
    );
    Selection::Spawn { session, victim }
}

/// Attaches `user` to a ready `session` while the registry lock is held, so the
/// idle timer cannot remove the session between the phase check and the insert.
fn attach(
    inner: &Arc<Inner>,
    state: &State,
    session: &Arc<Session>,
    user: &TunerUser,
    tap: SessionTap,
) -> Option<(Subscription, AttachedPump)> {
    // The reservation must still be the registered session.
    let registered = state
        .sessions
        .get(&session.key)
        .is_some_and(|registered| Arc::ptr_eq(registered, session));
    if !registered || session.phase() != SessionPhase::Ready {
        return None;
    }

    let capacity = inner.config.server.subscriber_buffer_chunks.max(1);
    let (tx, rx) = mpsc::channel(capacity);

    // Snapshot the replay window and subscribe under one lock: a chunk is
    // delivered either from the window or from the broadcast, never both.
    let (broadcast_rx, prelude) = {
        let prelude_guard = lock(&session.prelude);
        let sender = lock(&session.broadcaster).clone()?;
        let broadcast_rx = sender.subscribe();
        // Replay at most one queue's worth, so the replay itself can never
        // overflow the subscriber's bounded mpsc (SPEC §7).
        let skip = prelude_guard.len().saturating_sub(capacity);
        let prelude = prelude_guard.iter().skip(skip).cloned().collect::<Vec<_>>();
        (broadcast_rx, prelude)
    };

    let subscriber_id = session.next_subscriber_id.fetch_add(1, Ordering::Relaxed) + 1;
    lock(&session.users).push(SubscriberEntry {
        id: subscriber_id,
        user: user.clone(),
        started_at: SystemTime::now(),
        tap,
    });
    // A new subscriber cancels any pending idle release.
    session.idle_epoch.fetch_add(1, Ordering::Relaxed);

    tracing::debug!(
        session_id = session.id,
        subscriber_id,
        user = %user.id,
        priority = user.priority,
        ?tap,
        "subscribed"
    );

    let subscription = Subscription {
        rx,
        session_id: session.id,
        tuner: session.key.tuner.clone(),
        type_name: session.key.type_name.clone(),
        channel: session.key.channel.clone(),
        subscriber_id,
        tap,
    };
    Some((
        subscription,
        AttachedPump {
            broadcast_rx,
            tx,
            prelude,
            subscriber_id,
        },
    ))
}

/// Pieces a subscriber pump needs, produced under the registry lock.
struct AttachedPump {
    broadcast_rx: broadcast::Receiver<Bytes>,
    tx: mpsc::Sender<Bytes>,
    prelude: Vec<Bytes>,
    subscriber_id: u64,
}

/// Joins a ready session; `None` means it disappeared and the caller retries.
fn join(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    user: &TunerUser,
    tap: SessionTap,
) -> Option<Subscription> {
    let (subscription, pump) = {
        let state = lock(&inner.state);
        attach(inner, &state, session, user, tap)?
    };
    spawn_subscriber_pump(Arc::clone(inner), Arc::clone(session), pump);
    Some(subscription)
}

/// Spawns the tree for a reservation, waits for readiness, promotes it to
/// `Ready`, and only then stops the victim (SPEC §6.2).
///
/// `session` is the reservation created by [`select`]; the session id is fixed
/// for the whole lifetime of the process tree.
async fn spawn_and_promote(
    inner: &Arc<Inner>,
    session: Arc<Session>,
    victim: Option<Arc<Session>>,
    user: &TunerUser,
    tap: SessionTap,
) -> Result<Subscription, SpawnFailure> {
    let tuner_name = session.key.tuner.clone();
    let label = format!(
        "{}:{}:{}",
        session.key.tuner, session.key.type_name, session.key.channel
    );
    let _guard = SpawnGuard::new(inner);

    // SPEC §6.2: a 0-user (idle) session is killed before the restart, because
    // there is no stream to protect and an exclusive device must be released
    // before the replacement can open it. A session with subscribers keeps the
    // spawn-first ordering below.
    let victim_stopped_early = match victim.as_ref() {
        Some(victim) if victim.users_is_empty() => {
            retire_victim(inner, victim, user);
            victim.stop_process().await;
            true
        }
        _ => false,
    };

    let readiness = match start_process_tree(inner, &session, &label).await {
        Ok(readiness) => readiness,
        Err(error) => {
            tracing::warn!(session_id = session.id, %label, %error, "spawn failed");
            release_reservation(inner, &session, victim.as_ref());
            return Err(SpawnFailure::Failed { tuner: tuner_name });
        }
    };

    let ready = match await_readiness(inner, readiness).await {
        Readiness::Ready => true,
        Readiness::Ended(reason) => {
            tracing::warn!(session_id = session.id, %label, %reason, "no output before EOF");
            false
        }
        Readiness::TimedOut => {
            tracing::warn!(
                session_id = session.id,
                %label,
                timeout_ms = READINESS_TIMEOUT.as_millis(),
                "readiness timed out"
            );
            false
        }
        Readiness::ShuttingDown => {
            tracing::debug!(session_id = session.id, %label, "shutdown during readiness");
            session.stop_process().await;
            release_reservation(inner, &session, victim.as_ref());
            return Err(SpawnFailure::ShuttingDown);
        }
    };
    if !ready {
        session.stop_process().await;
        release_reservation(inner, &session, victim.as_ref());
        return Err(SpawnFailure::Failed { tuner: tuner_name });
    }

    let promoted = {
        let mut state = lock(&inner.state);
        let registered = state
            .sessions
            .get(&session.key)
            .is_some_and(|registered| Arc::ptr_eq(registered, &session));
        if !registered {
            if inner.shutting_down.load(Ordering::SeqCst) {
                Err(SpawnFailure::ShuttingDown)
            } else {
                Err(SpawnFailure::Failed {
                    tuner: tuner_name.clone(),
                })
            }
        } else if inner.shutting_down.load(Ordering::SeqCst) {
            Err(SpawnFailure::ShuttingDown)
        } else if session.phase() != SessionPhase::Starting {
            Err(SpawnFailure::Failed {
                tuner: tuner_name.clone(),
            })
        } else {
            session.set_phase(SessionPhase::Ready);
            match attach(inner, &state, &session, user, tap) {
                None => Err(SpawnFailure::Failed {
                    tuner: tuner_name.clone(),
                }),
                Some(pump) => {
                    // The stream can end between the first chunk and the
                    // promotion (a short recording). Promote, then remove the
                    // session right away so the subscriber sees the replay
                    // window followed by EOF and no request joins a dead
                    // session.
                    if session.ended.load(Ordering::SeqCst) {
                        if state
                            .sessions
                            .get(&session.key)
                            .is_some_and(|registered| Arc::ptr_eq(registered, &session))
                        {
                            state.sessions.remove(&session.key);
                        }
                        session.set_phase(SessionPhase::Failed);
                        session.close_broadcast();
                    }
                    if let Some(victim) = &victim {
                        if !victim_stopped_early {
                            // Remove the victim atomically with the promotion
                            // so no request can join it any more.
                            remove_and_stop_marking(&mut state, victim);
                            tracing::info!(
                                session_id = session.id,
                                victim_session = victim.id,
                                victim_tuner = %victim.key.tuner,
                                victim_priority = victim.max_priority(),
                                new_priority = user.priority,
                                "taking over tuner"
                            );
                        }
                    }
                    Ok(pump)
                }
            }
        }
    };

    let pump = match promoted {
        Ok(pump) => pump,
        Err(error) => {
            session.stop_process().await;
            release_reservation(inner, &session, victim.as_ref());
            return Err(error);
        }
    };

    let (subscription, pump) = pump;
    spawn_subscriber_pump(Arc::clone(inner), Arc::clone(&session), pump);

    if let Some(victim) = victim.filter(|_| !victim_stopped_early) {
        // Readiness is confirmed, so the victim may now be killed. Closing the
        // broadcast ends its subscribers (EOF) as soon as the pump exits.
        victim.close_broadcast();
        victim.stop_process().await;
    }
    Ok(subscription)
}

/// Outcome of waiting for the first pipeline chunk.
enum Readiness {
    Ready,
    /// The stream ended (or the pump died) before any data.
    Ended(String),
    TimedOut,
    ShuttingDown,
}

/// Waits for the first chunk, aborting promptly when shutdown starts so a slow
/// tuner cannot delay `SessionManager::shutdown`.
async fn await_readiness(
    inner: &Arc<Inner>,
    readiness: oneshot::Receiver<Option<String>>,
) -> Readiness {
    let mut readiness = readiness;
    let deadline = tokio::time::Instant::now() + READINESS_TIMEOUT;
    loop {
        tokio::select! {
            result = &mut readiness => match result {
                Ok(None) => return Readiness::Ready,
                Ok(Some(reason)) => return Readiness::Ended(reason),
                Err(_) => return Readiness::Ended("pipeline dropped readiness".to_string()),
            },
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if inner.shutting_down.load(Ordering::SeqCst) {
                    return Readiness::ShuttingDown;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Readiness::TimedOut;
                }
            }
        }
    }
}

/// Removes `victim` from the registry and marks it `Stopping` (under the state
/// lock) so no request can join it any more.
fn remove_and_stop_marking(state: &mut State, victim: &Arc<Session>) {
    if state
        .sessions
        .get(&victim.key)
        .is_some_and(|registered| Arc::ptr_eq(registered, victim))
    {
        state.sessions.remove(&victim.key);
    }
    victim.takeover_pending.store(false, Ordering::SeqCst);
    victim.set_phase(SessionPhase::Stopping);
}

/// Retires an idle victim before its replacement is spawned, logging the
/// takeover with the old and new priority.
fn retire_victim(inner: &Arc<Inner>, victim: &Arc<Session>, user: &TunerUser) {
    {
        let mut state = lock(&inner.state);
        remove_and_stop_marking(&mut state, victim);
        tracing::info!(
            victim_session = victim.id,
            victim_tuner = %victim.key.tuner,
            victim_priority = victim.max_priority(),
            new_priority = user.priority,
            "reusing an idle tuner session for another channel"
        );
    }
    victim.close_broadcast();
}

/// Removes a reservation that never became ready, leaving the victim alone
/// (SPEC §6.2: a failed spawn must not kill a victim).
fn release_reservation(inner: &Arc<Inner>, session: &Arc<Session>, victim: Option<&Arc<Session>>) {
    let mut state = lock(&inner.state);
    if state
        .sessions
        .get(&session.key)
        .is_some_and(|registered| Arc::ptr_eq(registered, session))
    {
        state.sessions.remove(&session.key);
    }
    session.set_phase(SessionPhase::Failed);
    session.close_broadcast();
    if let Some(victim) = victim {
        victim.takeover_pending.store(false, Ordering::SeqCst);
        victim.wake();
        // A pending takeover disarms the idle timer; if the victim has no
        // users left, re-arm it so a failed spawn cannot keep an unused tree
        // alive until shutdown.
        if victim.phase() == SessionPhase::Ready && victim.users_is_empty() {
            let epoch = victim.idle_epoch.fetch_add(1, Ordering::Relaxed) + 1;
            spawn_idle_timer(Arc::clone(inner), Arc::clone(victim), epoch);
        }
    }
}

/// Waits for a concurrent spawn/takeover to change `session`'s phase.
///
/// Returns immediately on a notification, and after [`STARTUP_POLL`] at the
/// latest so a wakeup consumed by another waiter cannot strand this request.
/// `false` means the request-wide deadline passed: the caller gives up.
async fn wait_before_retry(
    session: &Arc<Session>,
    deadline: tokio::time::Instant,
    type_name: &str,
    channel: &str,
) -> bool {
    if tokio::time::Instant::now() >= deadline {
        tracing::warn!(
            type_name,
            channel,
            session_id = session.id,
            "gave up waiting for a concurrent spawn"
        );
        return false;
    }
    let changed = session.changed.notified();
    tokio::select! {
        _ = changed => {}
        _ = tokio::time::sleep(STARTUP_POLL) => {}
    }
    true
}

/// Spawns the tuner and pipeline for a reserved session and starts the pump.
///
/// Returns the readiness receiver: `Ok(())` on the first chunk, or a reason if
/// the stream ended first.
async fn start_process_tree(
    inner: &Arc<Inner>,
    session: &Arc<Session>,
    label: &str,
) -> Result<oneshot::Receiver<Option<String>>, String> {
    let tuner_config = inner
        .tuner(&session.key.tuner)
        .ok_or_else(|| format!("tuner {:?} disappeared from the config", session.key.tuner))?
        .clone();
    let vars = CommandVars::new(&session.key.channel, &session.key.type_name);

    let mut tuner = spawn_tuner_command(&tuner_config.command, &vars)
        .await
        .map_err(|error| format!("tuner spawn failed: {error}"))?;
    let mut tuner_stdout = Some(
        tuner
            .take_stdout()
            .ok_or_else(|| "tuner stdout unavailable".to_string())?,
    );

    // SPEC §3.4: types without a pipeline binding deliver the raw TS.
    let spec = PipelineSpec::for_type(&inner.config, &session.key.type_name);
    let chunk_size = inner.config.server.chunk_size_bytes.max(1);

    let mut pipeline = if spec.is_empty() {
        None
    } else {
        let ctx = PipelineCtx::new(vars, label.to_string());
        match spawn_pipeline(&spec.commands, &ctx).await {
            Ok(pipeline) => Some(pipeline),
            Err(error) => {
                tuner.stop().await;
                return Err(format!("pipeline spawn failed: {error}"));
            }
        }
    };

    // 204 → 188 normalization sits between the tuner and stage 1 (SPEC §4), so
    // it feeds the first stage's stdin. Without a pipeline the pump itself
    // normalizes the tuner stream.
    let pipes = take_pipes(&mut pipeline, &mut tuner_stdout);
    let (source, normalize) = match pipes {
        Ok((Some(stdin), stdout)) => {
            let tuner_stdout = tuner_stdout
                .take()
                .ok_or_else(|| "tuner stdout unavailable".to_string())?;
            spawn_normalizer(tuner_stdout, stdin, label.to_string());
            (stdout, false)
        }
        Ok((None, stdout)) => (stdout, true),
        Err(error) => {
            // A partially built chain must not outlive the failed spawn.
            tuner.stop().await;
            if let Some(pipeline) = &pipeline {
                pipeline.stop().await;
            }
            return Err(error);
        }
    };

    {
        let mut pids = lock(&session.pids);
        pids.clear();
        pids.extend(tuner.pids().iter().copied());
        if let Some(pipeline) = &pipeline {
            pids.extend(pipeline.pids().iter().copied());
        }
    }

    let (sender, _) = broadcast::channel(BROADCAST_CAPACITY_CHUNKS);
    *lock(&session.broadcaster) = Some(sender.clone());
    let (readiness_tx, readiness_rx) = oneshot::channel();

    {
        let mut slot = session.process.lock().await;
        *slot = Some(ProcessTree { tuner, pipeline });
    }
    // Close the race with `shutdown` / `release_reservation`: both set a
    // non-`Starting` phase before draining, so a tree stored after their stop
    // pass is stopped here instead of leaking.
    if session.phase() != SessionPhase::Starting {
        session.stop_process().await;
        return Err("server is shutting down".to_string());
    }

    spawn_pump(
        Arc::clone(inner),
        Arc::clone(session),
        source,
        normalize,
        chunk_size,
        sender,
        readiness_tx,
        label,
    );
    Ok(readiness_rx)
}

/// Takes the pipeline's stdin/stdout, or the tuner's stdout when there is no
/// pipeline, returning a user-facing error instead of a `None` pipe.
fn take_pipes(
    pipeline: &mut Option<PipelineHandle>,
    tuner_stdout: &mut Option<ChildStdout>,
) -> Result<(Option<tokio::process::ChildStdin>, ChildStdout), String> {
    match pipeline {
        Some(pipeline) => {
            let stdin = pipeline
                .take_stdin()
                .ok_or_else(|| "pipeline stdin unavailable".to_string())?;
            let stdout = pipeline
                .take_stdout()
                .ok_or_else(|| "pipeline stdout unavailable".to_string())?;
            Ok((Some(stdin), stdout))
        }
        None => {
            let stdout = tuner_stdout
                .take()
                .ok_or_else(|| "tuner stdout unavailable".to_string())?;
            Ok((None, stdout))
        }
    }
}

/// Reads the tuner stdout, normalizes 204 → 188 and writes it to the first
/// pipeline stage (SPEC §4).
fn spawn_normalizer(
    mut tuner_stdout: ChildStdout,
    mut pipeline_stdin: tokio::process::ChildStdin,
    label: String,
) {
    tokio::spawn(async move {
        let mut normalizer = TsNormalizer::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match tuner_stdout.read(&mut buf).await {
                Ok(0) => break,
                Ok(read) => {
                    let out = normalizer.push(&buf[..read]);
                    if !out.is_empty() && pipeline_stdin.write_all(&out).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    tracing::warn!(session = %label, %error, "tuner read failed");
                    break;
                }
            }
        }
        let tail = normalizer.finish();
        if !tail.is_empty() {
            let _ = pipeline_stdin.write_all(&tail).await;
        }
        let _ = pipeline_stdin.shutdown().await;
        tracing::debug!(
            session = %label,
            packets = normalizer.packets_out,
            discarded = normalizer.bytes_discarded,
            "tuner stream ended; pipeline stdin closed"
        );
    });
}

/// Broadcast pump for one session.
#[allow(clippy::too_many_arguments)]
fn spawn_pump(
    inner: Arc<Inner>,
    session: Arc<Session>,
    mut source: ChildStdout,
    normalize: bool,
    chunk_size: usize,
    sender: broadcast::Sender<Bytes>,
    readiness: oneshot::Sender<Option<String>>,
    label: &str,
) {
    let label = label.to_string();
    tokio::spawn(async move {
        let mut readiness = Some(readiness);
        let mut normalizer = if normalize {
            Some(TsNormalizer::new())
        } else {
            None
        };
        let mut buf = vec![0u8; chunk_size];
        let mut reason = None;

        loop {
            match source.read(&mut buf).await {
                Ok(0) => break,
                Ok(read) => {
                    let chunk = match normalizer.as_mut() {
                        Some(normalizer) => {
                            let out = normalizer.push(&buf[..read]);
                            if out.is_empty() {
                                None
                            } else {
                                Some(Bytes::from(out))
                            }
                        }
                        None => Some(Bytes::copy_from_slice(&buf[..read])),
                    };
                    if let Some(chunk) = chunk {
                        for piece in split_chunks(chunk, chunk_size) {
                            if session.emit(&sender, piece) {
                                if let Some(tx) = readiness.take() {
                                    let _ = tx.send(None);
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    reason = Some(format!("read failed: {error}"));
                    break;
                }
            }
        }

        if let Some(mut normalizer) = normalizer {
            let tail = normalizer.finish();
            for piece in split_chunks(Bytes::from(tail), chunk_size) {
                if session.emit(&sender, piece) {
                    if let Some(tx) = readiness.take() {
                        let _ = tx.send(None);
                    }
                }
            }
        }

        if let Some(tx) = readiness.take() {
            let _ = tx.send(Some(
                reason
                    .clone()
                    .unwrap_or_else(|| "stream ended before any data".to_string()),
            ));
        }

        tracing::debug!(
            session_id = session.id,
            session = %label,
            reason = reason.as_deref().unwrap_or("eof"),
            "session stream ended"
        );
        // Drop this task's sender first: subscribers drain the chunks still in
        // the broadcast and then observe `Closed` (EOF) without waiting for the
        // process tree to finish stopping.
        drop(sender);
        inner
            .stream_ended(Arc::clone(&session), reason.as_deref().unwrap_or("eof"))
            .await;
    });
}

/// Splits `data` into `size`-byte slices, yielding nothing for empty input.
///
/// The `TsNormalizer` can emit more than one read's worth (it needs two packets
/// to detect 188 vs 204), so the pump re-chunks to keep the SPEC §7 promise
/// that a subscriber chunk is at most `server.chunk_size_bytes`.
fn split_chunks(data: Bytes, size: usize) -> impl Iterator<Item = Bytes> {
    let size = size.max(1);
    let mut offset = 0;
    std::iter::from_fn(move || {
        if offset >= data.len() {
            return None;
        }
        let end = (offset + size).min(data.len());
        let piece = data.slice(offset..end);
        offset = end;
        Some(piece)
    })
}

impl Inner {
    /// Handles the end of a session's stream: unregister, close the broadcast
    /// (subscribers see EOF) and stop the process tree.
    async fn stream_ended(self: &Arc<Self>, session: Arc<Session>, reason: &str) {
        // A `Starting` session is left to `spawn_and_promote`, which either
        // promotes it or releases the reservation. The flag tells the promoter
        // that the stream already ended so it does not leave a dead `Ready`
        // entry behind.
        session.ended.store(true, Ordering::SeqCst);
        let removed = {
            let mut state = lock(&self.state);
            let registered = state
                .sessions
                .get(&session.key)
                .is_some_and(|registered| Arc::ptr_eq(registered, &session));
            if registered && session.phase() == SessionPhase::Ready {
                state.sessions.remove(&session.key);
                true
            } else {
                false
            }
        };
        if removed {
            session.set_phase(SessionPhase::Failed);
            session.close_broadcast();
            tracing::info!(
                session_id = session.id,
                tuner = %session.key.tuner,
                type_name = %session.key.type_name,
                channel = %session.key.channel,
                reason,
                "session ended"
            );
        }
        session.stop_process().await;
    }
}

/// One subscriber's broadcast → mpsc pump.
fn spawn_subscriber_pump(inner: Arc<Inner>, session: Arc<Session>, pump: AttachedPump) {
    let AttachedPump {
        mut broadcast_rx,
        tx,
        prelude,
        subscriber_id,
    } = pump;
    tokio::spawn(async move {
        // Replay the chunks the broadcast no longer holds: `subscribe()` only
        // delivers chunks sent after it was called.
        for chunk in prelude {
            if tx.try_send(chunk).is_err() {
                detach(&inner, &session, subscriber_id).await;
                return;
            }
        }
        loop {
            tokio::select! {
                // The client dropped its receiver.
                _ = tx.closed() => break,
                // The session was stopped or its stream ended.
                _ = session.changed.notified() => {
                    // `Stopping` is a takeover/shutdown: stop now. `Failed` is
                    // an ended stream: keep draining what the broadcast still
                    // holds and let `Closed` end the loop.
                    if session.phase() == SessionPhase::Stopping {
                        break;
                    }
                }
                received = broadcast_rx.recv() => match received {
                    Ok(chunk) => match tx.try_send(chunk) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            // SPEC §7: drop the slow client, not the stream.
                            tracing::warn!(
                                session_id = session.id,
                                subscriber_id,
                                "subscriber buffer full; dropping subscriber"
                            );
                            break;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    },
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        // SPEC §7: a lagging subscriber is dropped alone.
                        tracing::warn!(
                            session_id = session.id,
                            subscriber_id,
                            skipped,
                            "subscriber lagged; dropping subscriber"
                        );
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            }
        }
        detach(&inner, &session, subscriber_id).await;
    });
}

/// Removes a subscriber and arms the idle timer when it was the last one.
async fn detach(inner: &Arc<Inner>, session: &Arc<Session>, subscriber_id: u64) {
    let last = {
        let state = lock(&inner.state);
        let mut users = lock(&session.users);
        users.retain(|entry| entry.id != subscriber_id);
        let last = users.is_empty();
        let live = session.phase() == SessionPhase::Ready
            && !inner.shutting_down.load(Ordering::SeqCst)
            && state
                .sessions
                .get(&session.key)
                .is_some_and(|registered| Arc::ptr_eq(registered, session));
        last && live
    };
    tracing::debug!(session_id = session.id, subscriber_id, "unsubscribed");
    if last {
        // Bumped by a new subscriber, which invalidates this timer.
        let epoch = session.idle_epoch.fetch_add(1, Ordering::Relaxed) + 1;
        spawn_idle_timer(Arc::clone(inner), Arc::clone(session), epoch);
    }
}

/// Arms the SPEC §7 idle release for `session`.
fn spawn_idle_timer(inner: Arc<Inner>, session: Arc<Session>, epoch: u64) {
    let timeout = Duration::from_secs(inner.config.server.idle_timeout_secs);
    tokio::spawn(async move {
        if !timeout.is_zero() {
            tokio::time::sleep(timeout).await;
        }
        let stop = {
            let mut state = lock(&inner.state);
            let registered = state
                .sessions
                .get(&session.key)
                .is_some_and(|registered| Arc::ptr_eq(registered, &session));
            if session.idle_epoch.load(Ordering::SeqCst) != epoch
                || session.phase() != SessionPhase::Ready
                || !session.users_is_empty()
                || session.takeover_pending.load(Ordering::SeqCst)
                || !registered
            {
                false
            } else {
                state.sessions.remove(&session.key);
                session.set_phase(SessionPhase::Stopping);
                true
            }
        };
        if stop {
            tracing::info!(
                session_id = session.id,
                tuner = %session.key.tuner,
                type_name = %session.key.type_name,
                channel = %session.key.channel,
                "idle timeout; stopping session"
            );
            session.close_broadcast();
            session.stop_process().await;
        }
    });
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    const PACKET: usize = 188;

    fn packet(seed: u8) -> Vec<u8> {
        let mut packet = vec![seed; PACKET];
        packet[0] = 0x47;
        packet
    }

    fn fixture(dir: &TempDir, name: &str, packets: &[Vec<u8>]) -> PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, packets.concat()).expect("write fixture");
        path
    }

    fn tuner_yaml(name: &str, types: &[&str], command: &str) -> String {
        format!(
            "- name: {name}\n  types: [{}]\n  command: '{}'\n",
            types.join(", "),
            command
        )
    }

    fn channels_yaml(channels: &[(&str, &str)]) -> String {
        channels
            .iter()
            .map(|(channel_type, channel)| {
                format!(
                    "- name: {channel_type}-{channel}\n  type: {channel_type}\n  channel: \"{channel}\"\n"
                )
            })
            .collect()
    }

    fn manager(
        dir: &TempDir,
        tuners: &str,
        channels: &str,
        pipelines: &str,
        idle_secs: u64,
        chunk_size: usize,
        buffer_chunks: usize,
    ) -> SessionManager {
        let server = format!(
            "addr: \":40772\"\nidle_timeout_secs: {idle_secs}\nchunk_size_bytes: {chunk_size}\nsubscriber_buffer_chunks: {buffer_chunks}\n"
        );
        for (name, body) in [
            ("server.yml", server),
            ("tuners.yml", tuners.to_string()),
            ("channels.yml", channels.to_string()),
            ("pipelines.yml", pipelines.to_string()),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write config");
        }
        let config = Config::load(dir.path()).expect("load config");
        SessionManager::new(Arc::new(config), ServiceTable::new())
    }

    /// `sh -c "cat <fixture>; sleep 30"`: streams the fixture and then holds
    /// the pipe open, like a tuner that keeps producing.
    fn streaming_command(path: &Path) -> String {
        format!("sh -c \"cat {}; sleep 30\"", path.display())
    }

    fn pipelines_passthrough() -> String {
        "pipelines:\n  pass:\n    - \"cat\"\ntypes:\n  GR: { pipeline: pass }\n".to_string()
    }

    fn pipelines_none() -> String {
        "pipelines:\n  pass:\n    - \"cat\"\ntypes: {}\n".to_string()
    }

    fn user(id: &str, priority: i32) -> TunerUser {
        TunerUser::new(id, priority)
    }

    fn process_exists(pid: u32) -> bool {
        // SAFETY: signal 0 only checks for existence and permission.
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }

    async fn read_at_least(
        rx: &mut mpsc::Receiver<Bytes>,
        want: usize,
        timeout: Duration,
    ) -> Vec<u8> {
        let mut received = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout;
        while received.len() < want {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(chunk)) => received.extend_from_slice(&chunk),
                Ok(None) | Err(_) => break,
            }
        }
        received
    }

    fn subscriber(id: u64, priority: i32) -> SubscriberEntry {
        SubscriberEntry {
            id,
            user: user(&format!("u{id}"), priority),
            started_at: SystemTime::now(),
            tap: SessionTap::Raw,
        }
    }

    #[tokio::test]
    async fn two_acquires_for_one_channel_share_one_process_tree() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let first = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("first acquire");
        let first_pids = manager.sessions_snapshot()[0].pids.clone();
        let second = manager
            .acquire("GR", "27", user("b", 0), None, true)
            .await
            .expect("second acquire");

        assert_eq!(first.session_id, second.session_id);
        assert_eq!(first.tuner, second.tuner);
        assert_ne!(first.subscriber_id, second.subscriber_id);
        // Joining never spawns: same tuner and pipeline PIDs.
        assert_eq!(manager.sessions_snapshot()[0].pids, first_pids);

        let sessions = manager.sessions_snapshot();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].users, 2);

        let tuners = manager.tuners_snapshot();
        assert_eq!(tuners.len(), 1);
        assert!(tuners[0].is_using);
        assert!(!tuners[0].is_free);
        assert_eq!(
            tuners[0].channel,
            Some(("GR".to_string(), "27".to_string()))
        );
        assert_eq!(tuners[0].users.len(), 2);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_second_channel_without_a_free_tuner_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let _first = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("first acquire");
        let error = manager
            .acquire("GR", "28", user("b", 0), None, true)
            .await
            .expect_err("no free tuner");
        assert_eq!(error, AcquireError::Busy);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_higher_priority_takes_over_and_the_victim_is_killed() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let victim = manager
            .acquire("GR", "27", user("low", 0), None, true)
            .await
            .expect("victim acquire");
        let victim_pids = manager.sessions_snapshot()[0].pids.clone();
        assert!(victim_pids.iter().all(|pid| process_exists(*pid)));

        let winner = manager
            .acquire("GR", "28", user("high", 10), None, true)
            .await
            .expect("takeover");
        drop(victim);

        let sessions = manager.sessions_snapshot();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].channel, "28");
        assert_eq!(sessions[0].id, winner.session_id);
        for pid in victim_pids {
            assert!(
                !process_exists(pid),
                "victim pid {pid} survived the takeover"
            );
        }

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn equal_priority_does_not_take_over() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let _first = manager
            .acquire("GR", "27", user("a", 5), None, true)
            .await
            .expect("first acquire");
        let error = manager
            .acquire("GR", "28", user("b", 5), None, true)
            .await
            .expect_err("equal priority must not take over");
        assert_eq!(error, AcquireError::Busy);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn priority_128_grabs_even_at_equal_priority() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let _victim = manager
            .acquire("GR", "27", user("grabber", GRAB_PRIORITY), None, true)
            .await
            .expect("victim acquire");
        let subscription = manager
            .acquire("GR", "28", user("grabber2", GRAB_PRIORITY), None, true)
            .await
            .expect("grab must succeed");
        assert_eq!(manager.sessions_snapshot()[0].channel, "28");
        drop(subscription);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_negative_priority_never_takes_over() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let _victim = manager
            .acquire("GR", "27", user("victim", 0), None, true)
            .await
            .expect("victim acquire");
        let error = manager
            .acquire("GR", "28", user("scan", SCAN_PRIORITY), None, true)
            .await
            .expect_err("scan priority must not take over");
        assert_eq!(error, AcquireError::Busy);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn idle_release_stops_the_tree_after_the_timeout() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            1,
            188,
            8,
        );

        let subscription = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("acquire");
        let pids = manager.sessions_snapshot()[0].pids.clone();
        assert_eq!(pids.len(), 2, "tuner plus one pipeline stage");

        let dropped_at = std::time::Instant::now();
        drop(subscription);
        // The subscriber pump notices the closed receiver asynchronously.
        let users_zero_deadline = dropped_at + Duration::from_millis(500);
        while manager.sessions_snapshot()[0].users != 0 {
            assert!(
                std::time::Instant::now() < users_zero_deadline,
                "subscriber was never released"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for pid in &pids {
            assert!(process_exists(*pid), "pid {pid} died immediately");
        }

        let deadline = dropped_at + Duration::from_secs(8);
        loop {
            if manager.sessions_snapshot().is_empty()
                && pids.iter().all(|pid| !process_exists(*pid))
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "idle release did not finish in time (sessions: {:?})",
                manager.sessions_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            dropped_at.elapsed() >= Duration::from_millis(900),
            "released before the 1 s idle timeout: {:?}",
            dropped_at.elapsed()
        );

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_new_subscriber_cancels_the_pending_idle_timer() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            1,
            188,
            8,
        );

        let first = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("acquire");
        let session_id = first.session_id;
        drop(first);

        // Re-subscribe inside the idle window.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let second = manager
            .acquire("GR", "27", user("b", 0), None, true)
            .await
            .expect("re-acquire");
        assert_eq!(second.session_id, session_id);

        // Past the original deadline the session must still be alive.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let sessions = manager.sessions_snapshot();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].users, 1);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn subscribers_receive_identical_bytes() {
        let dir = TempDir::new().expect("tempdir");
        let packets: Vec<Vec<u8>> = (0..6).map(|index| packet(index as u8)).collect();
        // Spread the packets over time so the second subscriber joins after the
        // readiness chunk but before the rest of the stream. The first gap is
        // long enough that the replay window still holds only the first chunk.
        let mut command = String::from("sh -c \"");
        for (index, packet) in packets.iter().enumerate() {
            let path = fixture(
                &dir,
                &format!("packet-{}.bin", packet[1]),
                std::slice::from_ref(packet),
            );
            let gap = if index == 0 { 0.5 } else { 0.05 };
            command.push_str(&format!("cat {}; sleep {gap}; ", path.display()));
        }
        command.push_str("sleep 30\"");
        let tuners = tuner_yaml("t0", &["GR"], &command);
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let mut first = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("first acquire");
        let mut second = manager
            .acquire("GR", "27", user("b", 0), None, true)
            .await
            .expect("second acquire");

        let want = packets.len() * PACKET;
        let first_bytes = read_at_least(&mut first.rx, want, Duration::from_secs(5)).await;
        let second_bytes = read_at_least(&mut second.rx, want, Duration::from_secs(5)).await;

        assert_eq!(first_bytes.len(), want);
        assert_eq!(second_bytes, first_bytes, "fan-out must be identical");
        assert_eq!(first_bytes, packets.concat());

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn an_overflowing_subscriber_is_dropped_alone() {
        let dir = TempDir::new().expect("tempdir");
        let manager = manager(
            &dir,
            &tuner_yaml("t0", &["GR"], "true"),
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            64,
        );
        let inner = Arc::clone(&manager.inner);
        let session = Arc::new(Session::new(
            99,
            SessionKey {
                tuner: "t0".to_string(),
                type_name: "GR".to_string(),
                channel: "27".to_string(),
            },
        ));
        session.set_phase(SessionPhase::Ready);
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY_CHUNKS);
        *lock(&session.broadcaster) = Some(sender.clone());

        // A slow subscriber with a full queue and no reader.
        let (slow_tx, _slow_rx) = mpsc::channel(2);
        lock(&session.users).push(subscriber(1, 0));
        spawn_subscriber_pump(
            Arc::clone(&inner),
            Arc::clone(&session),
            AttachedPump {
                broadcast_rx: sender.subscribe(),
                tx: slow_tx,
                prelude: Vec::new(),
                subscriber_id: 1,
            },
        );

        // A fast subscriber that drains everything.
        let (fast_tx, mut fast_rx) = mpsc::channel(64);
        lock(&session.users).push(subscriber(2, 0));
        spawn_subscriber_pump(
            Arc::clone(&inner),
            Arc::clone(&session),
            AttachedPump {
                broadcast_rx: sender.subscribe(),
                tx: fast_tx,
                prelude: Vec::new(),
                subscriber_id: 2,
            },
        );
        let reader = tokio::spawn(async move {
            let mut total = 0usize;
            while let Some(chunk) = fast_rx.recv().await {
                total += chunk.len();
            }
            total
        });

        let chunks = 40;
        for index in 0..chunks {
            session.emit(&sender, Bytes::from(packet(index as u8)));
            // Slow enough that the fast subscriber's reader always keeps up;
            // the slow one still overflows its 2-slot queue.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining: Vec<u64> = lock(&session.users).iter().map(|entry| entry.id).collect();
            if remaining == [2] {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "slow subscriber was not dropped (users: {remaining:?})"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Close the stream and check what the fast subscriber received.
        session.close_broadcast();
        drop(sender);
        let received = reader.await.expect("reader task");
        assert_eq!(received, chunks * PACKET);
    }

    #[tokio::test]
    async fn an_idle_session_is_killed_and_restarted_for_another_channel() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            30,
            188,
            8,
        );

        let first = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("first acquire");
        let first_id = first.session_id;
        let first_pids = manager.sessions_snapshot()[0].pids.clone();
        drop(first);
        // Let the pump detach before the second request sees a 0-user session.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while manager.sessions_snapshot()[0].users != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "subscriber not detached"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // SPEC §6.2: an idle session is reused only for the same channel; a
        // different channel kills it and restarts on the freed tuner.
        let second = manager
            .acquire("GR", "28", user("b", 0), None, true)
            .await
            .expect("idle session reuse");
        assert_ne!(second.session_id, first_id, "restart uses a new session id");
        assert_eq!(manager.sessions_snapshot()[0].channel, "28");
        assert_ne!(manager.sessions_snapshot()[0].pids, first_pids);
        for pid in first_pids {
            assert!(!process_exists(pid), "old pid {pid} survived the restart");
        }

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn the_strongest_subscriber_priority_decides_the_takeover() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let _low = manager
            .acquire("GR", "27", user("low", 0), None, true)
            .await
            .expect("low acquire");
        let _high = manager
            .acquire("GR", "27", user("high", 10), None, true)
            .await
            .expect("join at high priority");

        // 5 > 0 but not > 10: the session's max subscriber priority protects it.
        let error = manager
            .acquire("GR", "28", user("mid", 5), None, true)
            .await
            .expect_err("must not take over a session with a priority-10 user");
        assert_eq!(error, AcquireError::Busy);

        let subscription = manager
            .acquire("GR", "28", user("stronger", 11), None, true)
            .await
            .expect("11 > 10 must take over");
        assert_eq!(manager.sessions_snapshot()[0].channel, "28");
        drop(subscription);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_pinned_existing_session_is_joined_regardless_of_priority() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let first = manager
            .acquire("GR", "27", user("a", 50), Some("t0".to_string()), true)
            .await
            .expect("pinned acquire");
        let second = manager
            .acquire("GR", "27", user("b", 0), Some("t0".to_string()), true)
            .await
            .expect("pinned join");
        assert_eq!(second.session_id, first.session_id);
        assert_eq!(manager.sessions_snapshot()[0].users, 2);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_pinned_unknown_tuner_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let error = manager
            .acquire("GR", "27", user("a", 0), Some("nope".to_string()), true)
            .await
            .expect_err("unknown pin");
        assert_eq!(error, AcquireError::UnknownTuner("nope".to_string()));
        assert!(manager.sessions_snapshot().is_empty());
        assert!(manager.tuners_snapshot()[0].is_free);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_disabled_pinned_tuner_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = format!(
            "- name: t0\n  types: [GR]\n  command: '{}'\n  disabled: true\n",
            streaming_command(&path)
        );
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let error = manager
            .acquire("GR", "27", user("a", 0), Some("t0".to_string()), true)
            .await
            .expect_err("disabled pin");
        assert_eq!(error, AcquireError::UnknownTuner("t0".to_string()));
        assert!(!manager.tuners_snapshot()[0].is_available);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_pinned_tuner_with_the_wrong_type_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["BS"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let error = manager
            .acquire("GR", "27", user("a", 0), Some("t0".to_string()), true)
            .await
            .expect_err("type mismatch");
        assert_eq!(error, AcquireError::UnsupportedType("GR".to_string()));

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_pinned_busy_tuner_without_priority_is_busy_and_never_falls_back() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = format!(
            "{}{}",
            tuner_yaml("t0", &["GR"], &streaming_command(&path)),
            tuner_yaml("t1", &["GR"], &streaming_command(&path))
        );
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let _first = manager
            .acquire("GR", "27", user("a", 0), Some("t0".to_string()), true)
            .await
            .expect("pinned acquire");
        let error = manager
            .acquire("GR", "28", user("b", 0), Some("t0".to_string()), true)
            .await
            .expect_err("busy pinned tuner");
        assert_eq!(error, AcquireError::Busy);

        // t1 is free but pinning must not fall back to it.
        let snapshot = manager.tuners_snapshot();
        let t1 = snapshot
            .iter()
            .find(|tuner| tuner.name == "t1")
            .expect("t1");
        assert!(t1.is_free);
        assert_eq!(manager.sessions_snapshot().len(), 1);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_pin_to_another_tuner_for_a_streaming_channel_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = format!(
            "{}{}",
            tuner_yaml("t0", &["GR"], &streaming_command(&path)),
            tuner_yaml("t1", &["GR"], &streaming_command(&path))
        );
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let _streaming = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("unpinned acquire");
        // The channel is already on t0. Joining it would ignore the pin, and a
        // second session for the channel is forbidden, so this is a conflict.
        let error = manager
            .acquire("GR", "27", user("b", 50), Some("t1".to_string()), true)
            .await
            .expect_err("pinned to another tuner");
        assert_eq!(error, AcquireError::Busy);
        assert_eq!(manager.sessions_snapshot().len(), 1);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_pinned_busy_tuner_is_taken_over_with_enough_priority() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = format!(
            "{}{}",
            tuner_yaml("t0", &["GR"], &streaming_command(&path)),
            tuner_yaml("t1", &["GR"], &streaming_command(&path))
        );
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let _first = manager
            .acquire("GR", "27", user("a", 0), Some("t0".to_string()), true)
            .await
            .expect("pinned acquire");
        let winner = manager
            .acquire("GR", "28", user("b", 50), Some("t0".to_string()), true)
            .await
            .expect("pinned takeover");

        let sessions = manager.sessions_snapshot();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, winner.session_id);
        assert_eq!(sessions[0].tuner, "t0");
        assert_eq!(sessions[0].channel, "28");

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_acquires_of_one_channel_spawn_only_one_tree() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = format!(
            "{}{}",
            tuner_yaml("t0", &["GR"], &streaming_command(&path)),
            tuner_yaml("t1", &["GR"], &streaming_command(&path))
        );
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let (first, second) = tokio::join!(
            manager.acquire("GR", "27", user("a", 0), None, true),
            manager.acquire("GR", "27", user("b", 0), None, true)
        );
        let first = first.expect("first acquire");
        let second = second.expect("second acquire");
        assert_eq!(first.session_id, second.session_id);

        let sessions = manager.sessions_snapshot();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].users, 2);
        assert_eq!(sessions[0].pids.len(), 2);

        let used = manager
            .tuners_snapshot()
            .into_iter()
            .filter(|tuner| tuner.is_using)
            .count();
        assert_eq!(used, 1, "only one tuner may be used");

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_broken_tuner_falls_through_to_the_next_candidate() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = format!(
            "- name: broken\n  types: [GR]\n  command: 'false'\n{}",
            tuner_yaml("good", &["GR"], &streaming_command(&path))
        );
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let subscription = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("fall-through to the good tuner");
        assert_eq!(subscription.tuner, "good");

        let snapshot = manager.tuners_snapshot();
        let broken = snapshot
            .iter()
            .find(|tuner| tuner.name == "broken")
            .expect("broken");
        assert!(broken.is_free);
        assert_eq!(manager.sessions_snapshot()[0].tuner, "good");

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_broken_pinned_tuner_is_unavailable() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = format!(
            "- name: broken\n  types: [GR]\n  command: 'false'\n{}",
            tuner_yaml("good", &["GR"], &streaming_command(&path))
        );
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let error = manager
            .acquire("GR", "27", user("a", 0), Some("broken".to_string()), true)
            .await
            .expect_err("pinned broken tuner");
        assert_eq!(error, AcquireError::Unavailable);
        assert!(manager.sessions_snapshot().is_empty());

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn an_unbound_type_delivers_the_tuner_stream_directly() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_none(),
            5,
            188,
            8,
        );

        let mut subscription = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("acquire");
        // Only the tuner process, no pipeline stage.
        assert_eq!(manager.sessions_snapshot()[0].pids.len(), 1);

        let received =
            read_at_least(&mut subscription.rx, 2 * PACKET, Duration::from_secs(5)).await;
        assert_eq!(received, [packet(0x11), packet(0x22)].concat());

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_stops_every_session() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = format!(
            "{}{}",
            tuner_yaml("t0", &["GR"], &streaming_command(&path)),
            tuner_yaml("t1", &["GR"], &streaming_command(&path))
        );
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let _first = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("first acquire");
        let _second = manager
            .acquire("GR", "28", user("b", 0), None, true)
            .await
            .expect("second acquire");

        let pids: Vec<u32> = manager
            .sessions_snapshot()
            .iter()
            .flat_map(|session| session.pids.clone())
            .collect();
        assert_eq!(pids.len(), 4, "two tuners plus two pipeline stages");

        manager.shutdown().await;

        assert!(manager.sessions_snapshot().is_empty());
        for pid in pids {
            assert!(!process_exists(pid), "pid {pid} survived shutdown");
        }
        // A request after shutdown is refused, not served.
        let error = manager
            .acquire("GR", "27", user("c", 0), None, true)
            .await
            .expect_err("shutting down");
        assert_eq!(error, AcquireError::ShuttingDown);
    }

    #[tokio::test]
    async fn the_filtered_tap_delivers_the_post_pipeline_ts_for_now() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let tuners = tuner_yaml("t0", &["GR"], &streaming_command(&path));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        // `raw = false`: the service stream. Phase 5 inserts a TSFilter per
        // subscriber here; until then the bytes are the session broadcast.
        let mut filtered = manager
            .acquire("GR", "27", user("svc", 0), None, false)
            .await
            .expect("filtered acquire");
        assert_eq!(filtered.tap, SessionTap::Filtered { service_id: None });

        let mut raw = manager
            .acquire("GR", "27", user("raw", 0), None, true)
            .await
            .expect("raw acquire");
        assert_eq!(raw.tap, SessionTap::Raw);

        let want = 2 * PACKET;
        let filtered_bytes = read_at_least(&mut filtered.rx, want, Duration::from_secs(5)).await;
        let raw_bytes = read_at_least(&mut raw.rx, want, Duration::from_secs(5)).await;
        assert_eq!(filtered_bytes, raw_bytes);
        assert_eq!(filtered_bytes, [packet(0x11), packet(0x22)].concat());

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_failed_takeover_leaves_the_victim_running() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        let marker = dir.path().join("fail-now");
        // Fails as soon as the marker exists, without writing anything, so the
        // takeover's spawn can never reach readiness.
        let command = format!(
            "sh -c \"if test -f {}; then exit 1; fi; cat {}; sleep 30\"",
            marker.display(),
            path.display()
        );
        let tuners = tuner_yaml("t0", &["GR"], &command);
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27"), ("GR", "28")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let mut victim = manager
            .acquire("GR", "27", user("low", 0), None, true)
            .await
            .expect("victim acquire");
        let victim_pids = manager.sessions_snapshot()[0].pids.clone();
        let victim_bytes = read_at_least(&mut victim.rx, 2 * PACKET, Duration::from_secs(5)).await;
        assert_eq!(victim_bytes.len(), 2 * PACKET);

        // From now on a new spawn fails before producing any output.
        std::fs::write(&marker, b"fail").expect("marker");

        let error = manager
            .acquire("GR", "28", user("high", 10), None, true)
            .await
            .expect_err("spawn fails during takeover");
        assert_eq!(error, AcquireError::Unavailable);

        // SPEC §6.2: a failed spawn must not kill the victim.
        let sessions = manager.sessions_snapshot();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].channel, "27");
        assert_eq!(sessions[0].pids, victim_pids);
        for pid in &victim_pids {
            assert!(process_exists(*pid), "victim pid {pid} was killed");
        }

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_stops_a_tree_whose_spawn_is_still_waiting_for_readiness() {
        let dir = TempDir::new().expect("tempdir");
        // No output at all: the readiness gate stays open until shutdown.
        let tuners = tuner_yaml("t0", &["GR"], r#"sh -c "sleep 30""#);
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let acquire = tokio::spawn({
            let manager = manager.clone();
            async move { manager.acquire("GR", "27", user("a", 0), None, true).await }
        });

        // Wait until the reservation has spawned its processes.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let pids = loop {
            let pids: Vec<u32> = manager
                .sessions_snapshot()
                .iter()
                .flat_map(|session| session.pids.clone())
                .collect();
            if !pids.is_empty() {
                break pids;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the session never spawned"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        let started = std::time::Instant::now();
        manager.shutdown().await;
        let acquire_result = acquire.await.expect("acquire task");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "shutdown waited for the readiness timeout: {:?}",
            started.elapsed()
        );
        assert!(
            matches!(
                acquire_result,
                Err(AcquireError::ShuttingDown) | Err(AcquireError::Unavailable)
            ),
            "unexpected acquire result: {acquire_result:?}"
        );
        for pid in pids {
            assert!(!process_exists(pid), "pid {pid} survived shutdown");
        }
        assert!(manager.sessions_snapshot().is_empty());
    }

    #[tokio::test]
    async fn a_stream_that_ends_releases_the_session_and_subscribers() {
        let dir = TempDir::new().expect("tempdir");
        let path = fixture(&dir, "stream.bin", &[packet(0x11), packet(0x22)]);
        // `cat` only: the session ends as soon as the file is consumed.
        let tuners = tuner_yaml("t0", &["GR"], &format!("cat {}", path.display()));
        let manager = manager(
            &dir,
            &tuners,
            &channels_yaml(&[("GR", "27")]),
            &pipelines_passthrough(),
            5,
            188,
            8,
        );

        let mut subscription = manager
            .acquire("GR", "27", user("a", 0), None, true)
            .await
            .expect("acquire");

        let mut received = Vec::new();
        while let Ok(Some(chunk)) =
            tokio::time::timeout(Duration::from_secs(5), subscription.rx.recv()).await
        {
            received.extend_from_slice(&chunk);
        }
        assert_eq!(received, [packet(0x11), packet(0x22)].concat());

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !manager.sessions_snapshot().is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "session was not released after its stream ended"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(manager.tuners_snapshot()[0].is_free);

        manager.shutdown().await;
    }
}
