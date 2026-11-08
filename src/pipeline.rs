//! External command pipelines (SPEC §3.5, §4, §6.2).
//!
//! A [`PipelineSpec`] is an ordered list of command strings from
//! `pipelines.yml`. [`spawn_pipeline`] turns each string into argv *without a
//! shell*, spawns the stages chained `stdout -> stdin`, and returns a handle
//! whose stdin (first stage) and stdout (last stage) are the only connections
//! to the outside world.
//!
//! Every child is placed in its own session and process group with `setsid`
//! (through `pre_exec`), so [`PipelineHandle::kill_all`] can signal the whole
//! group — including grandchildren a shell or helper may have started — with
//! `SIGTERM`, wait up to a grace period for the group to exit, and fall back to
//! `SIGKILL`. Waiting is done by polling the children, never with a fixed
//! sleep when they exit early (SPEC §6.2).
//!
//! # Placeholders (SPEC §3.5)
//!
//! `{channel}`, `{type}` and `{extra_args}` are substituted before argv
//! splitting. Any other `{...}` sequence is left **literally** in place
//! instead of being blanked: a typo such as `{chanel}` reaches the command
//! unchanged and fails visibly, rather than silently invoking the tuner without
//! its channel argument. Substitution is textual, so a value must not be able
//! to break out of the argv token it is written into; the `{channel}` / `{type}`
//! values are therefore validated against the same charset the config layer
//! enforces ([`crate::config::validate_channel_value`] /
//! [`crate::config::validate_type_name`]) and contain neither whitespace nor
//! quotes. `{extra_args}` is intentionally **not** validated: it is an argv
//! fragment whose whole point is to expand to several tokens (e.g.
//! `--lnb 15`), and its value comes from trusted config.
//!
//! An empty `{extra_args}` substitutes to nothing. Because argv splitting
//! collapses the surrounding whitespace, an unquoted `cmd {channel}
//! {extra_args}` with an empty `extra_args` yields `["cmd", "<channel>"]` —
//! the token **disappears**. A quoted `"{extra_args}"` yields one empty token,
//! exactly as a shell would. Both behaviors are asserted below.
//!
//! # Why argv splitting, not a shell
//!
//! SPEC §3.5 requires direct `exec` with argv splitting. [`shell_split`]
//! implements the subset of POSIX shell word splitting that a command line
//! needs: whitespace separates words, single quotes keep everything literal,
//! double quotes allow `\` escapes, and a backslash outside single quotes
//! escapes the next character. There is no variable expansion, command
//! substitution, globbing or redirection.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::config::{
    validate_channel_value, validate_type_name, Config, ServerConfig, DEFAULT_CHILD_CWD,
    DEFAULT_CHILD_HOME, DEFAULT_CHILD_PATH, DEFAULT_CHILD_TMPDIR,
};

/// Grace between `SIGTERM` and `SIGKILL` (SPEC §6.2).
pub const KILL_GRACE: Duration = Duration::from_secs(2);

/// How often a process group is polled while waiting for it to exit.
///
/// 10 ms, not a full sleep: the loop checks the group *before* sleeping and
/// returns as soon as the last member is gone, so a child that exits in
/// ~10 ms costs ~0-10 ms of teardown instead of the whole `KILL_GRACE`.
const EXIT_POLL: Duration = Duration::from_millis(10);

/// Upper bound for [`PipelineHandle::wait`] before it escalates to `SIGKILL`
/// and stops waiting. Without a bound, a child stuck in D-state (uninterruptible
/// sleep, e.g. a wedged tuner driver) would hang `wait()` forever, and with it
/// the spawn-failure cleanup path would hang too. 10 s is far above any normal
/// exit latency but keeps a wedged child from wedging the caller.
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to keep reaping after a `SIGKILL` issued by the `wait()` timeout
/// or by [`PipelineHandle::kill_all`]. `SIGKILL` death is normally visible
/// within one poll interval; this is only a ceiling for a D-state child that
/// cannot die until its I/O completes.
///
/// It equals [`KILL_GRACE`] rather than being shorter: a leader in D-state
/// (a wedged tuner driver) can outlive any bound, so shortening the wait after
/// `SIGKILL` would only make it more likely that `kill_all` reports a dead
/// group while the device is still held. The caller is told the truth either
/// way (see the `bool` return).
const POST_KILL_REAP: Duration = KILL_GRACE;

/// Stages of one `pipelines.yml` entry, in stdin-to-stdout order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PipelineSpec {
    pub commands: Vec<String>,
}

impl PipelineSpec {
    /// Builds a spec from the raw command strings of `pipelines.yml`.
    ///
    /// Stages that are exactly the `passthrough` pseudo-command (after
    /// trimming, ASCII case-insensitively) are dropped. A spec made only of
    /// those therefore has no stages and delivers the tuner stream unchanged.
    pub fn new(commands: Vec<String>) -> Self {
        let mut commands = commands;
        let reduced = commands.len();
        commands.retain(|command| !is_passthrough(command));
        if reduced > 0 && commands.is_empty() {
            tracing::debug!("pipeline reduced to passthrough");
        }
        Self { commands }
    }

    /// Whether the pipeline has no stage (the tuner stream is delivered as-is).
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Resolves the pipeline bound to `type_name` in `pipelines.yml`.
    ///
    /// A type without a binding (or one whose binding names a missing pipeline,
    /// which startup validation rejects) yields an empty spec, so the tuner
    /// stream is delivered as-is (SPEC §3.4).
    pub fn for_type(config: &Config, type_name: &str) -> Self {
        let Some(binding) = config.pipelines.types.get(type_name) else {
            return Self::default();
        };
        match config.pipelines.pipelines.get(&binding.pipeline) {
            Some(commands) => Self::new(commands.clone()),
            None => {
                tracing::warn!(
                    type_name,
                    pipeline = %binding.pipeline,
                    "pipeline binding references a missing pipeline; delivering raw TS"
                );
                Self::default()
            }
        }
    }
}

/// The explicit environment and working directory of every child process
/// (SPEC §3.5).
///
/// Children never inherit the server's environment: [`apply`](Self::apply)
/// clears it first, then sets `PATH`, `HOME` and `TMPDIR`, merges
/// [`extra`](Self::extra) (which therefore wins over the three base variables),
/// and applies [`cwd`](Self::cwd) when it exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildEnv {
    /// `PATH` handed to the child.
    pub path: String,
    /// `HOME` handed to the child.
    pub home: String,
    /// `TMPDIR` handed to the child.
    pub tmpdir: String,
    /// Working directory; `None` inherits the server's working directory.
    pub cwd: Option<String>,
    /// Extra variables, merged after the three base ones.
    pub extra: HashMap<String, String>,
}

impl Default for ChildEnv {
    fn default() -> Self {
        Self {
            path: DEFAULT_CHILD_PATH.to_string(),
            home: DEFAULT_CHILD_HOME.to_string(),
            tmpdir: DEFAULT_CHILD_TMPDIR.to_string(),
            cwd: (!DEFAULT_CHILD_CWD.is_empty()).then(|| DEFAULT_CHILD_CWD.to_string()),
            extra: HashMap::new(),
        }
    }
}

impl ChildEnv {
    /// Builds the child environment from `server.yml` (SPEC §3.1, §3.5).
    pub fn from_server(server: &ServerConfig) -> Self {
        Self {
            path: server.child_path.clone(),
            home: server.child_home.clone(),
            tmpdir: server.child_tmpdir.clone(),
            cwd: (!server.child_cwd.is_empty()).then(|| server.child_cwd.clone()),
            extra: server.child_env.clone(),
        }
    }

    /// Applies the explicit environment and `current_dir` to `command`.
    ///
    /// A configured working directory that does not exist is omitted with a
    /// warning per spawned process (the startup pass already warned once), so
    /// the child still starts instead of failing with `ENOENT`.
    fn apply(&self, command: &mut Command, label: &str, stage: usize) {
        command.env_clear();
        command.env("PATH", &self.path);
        command.env("HOME", &self.home);
        command.env("TMPDIR", &self.tmpdir);
        // Applied last, so a user entry overrides one of the base three.
        for (key, value) in &self.extra {
            command.env(key, value);
        }
        if let Some(cwd) = &self.cwd {
            if Path::new(cwd).is_dir() {
                command.current_dir(cwd);
            } else {
                tracing::warn!(
                    session = label,
                    stage,
                    child_cwd = %cwd,
                    "child_cwd does not exist; spawning without current_dir"
                );
            }
        }
    }
}

/// The `{channel}`, `{type}` and `{extra_args}` values used for placeholder
/// substitution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandVars {
    /// The channel value (`27`, `BS13_1`, `C17`, ...). For a routed channel
    /// this is the **effective** channel of the route being tried.
    pub channel: String,
    /// The channel type (`GR`, `BS`, ...). Spelled without the `type` keyword.
    pub type_name: String,
    /// Extra arguments for the tuner command (`--lnb 15`, ...). Empty unless
    /// the channel or a route defines `extra-args`; an empty value substitutes
    /// to nothing (see the module documentation).
    pub extra_args: String,
    /// Explicit environment/working directory for every spawned child.
    pub child_env: ChildEnv,
}

impl CommandVars {
    /// Builds the substitution variables for one session, with no extra args
    /// and the default child environment.
    pub fn new(channel: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            channel: channel.into(),
            type_name: type_name.into(),
            extra_args: String::new(),
            child_env: ChildEnv::default(),
        }
    }

    /// Sets the `{extra_args}` value.
    pub fn with_extra_args(mut self, extra_args: impl Into<String>) -> Self {
        self.extra_args = extra_args.into();
        self
    }

    /// Sets the explicit child environment.
    pub fn with_child_env(mut self, child_env: ChildEnv) -> Self {
        self.child_env = child_env;
        self
    }
}

/// Context for [`spawn_pipeline`].
#[derive(Clone, Debug)]
pub struct PipelineCtx {
    /// Placeholder values for every stage.
    pub vars: CommandVars,
    /// Log label, typically the tuner name and channel.
    pub label: String,
}

impl PipelineCtx {
    /// Builds a context for `label` with the given substitution variables.
    pub fn new(vars: CommandVars, label: impl Into<String>) -> Self {
        Self {
            vars,
            label: label.into(),
        }
    }
}

/// A pipeline failure.
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    /// The command line produced no argv (only whitespace, or `''`).
    #[error("empty command: {command:?}")]
    NoArgv { command: String },
    /// A `{channel}` or `{type}` value failed charset validation.
    #[error("invalid {field} value {value:?}: {reason}")]
    InvalidValue {
        field: &'static str,
        value: String,
        reason: String,
    },
    /// `spawn` failed for one stage.
    #[error("failed to spawn stage {stage} ({argv:?}): {source}")]
    Spawn {
        stage: usize,
        /// The expanded argv that was actually handed to `spawn`, not the
        /// `{channel}` template. Kept to one line by [`argv_summary`].
        argv: String,
        #[source]
        source: io::Error,
    },
}

/// Splits a command line into argv using the shell word-splitting rules from
/// SPEC §3.5: whitespace separation, `'...'`, `"..."` and `\` escapes, with no
/// expansion of any kind.
///
/// Unbalanced quotes are accepted: the rest of the line is kept in the current
/// word, which lets a malformed command fail at `spawn` with the real argv
/// instead of being silently rewritten here.
///
/// ```
/// # use unituner::pipeline::shell_split;
/// assert_eq!(shell_split("cat /tmp/a"), ["cat", "/tmp/a"]);
/// assert_eq!(shell_split("sh -c 'cat a; sleep 1'"), ["sh", "-c", "cat a; sleep 1"]);
/// assert_eq!(shell_split(r"a\ b c\ d"), ["a b", "c d"]);
/// ```
pub fn shell_split(input: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;
    let mut has_token = false;

    for c in input.chars() {
        if escaped {
            current.push(c);
            escaped = false;
            has_token = true;
            continue;
        }
        // A backslash is literal inside single quotes.
        if c == '\\' && !in_single {
            escaped = true;
            has_token = true;
            continue;
        }
        if c == '\'' && !in_double {
            in_single = !in_single;
            has_token = true;
            continue;
        }
        if c == '"' && !in_single {
            in_double = !in_double;
            has_token = true;
            continue;
        }
        if c.is_whitespace() && !in_single && !in_double {
            if has_token {
                args.push(std::mem::take(&mut current));
                has_token = false;
            }
            continue;
        }
        current.push(c);
        has_token = true;
    }
    // A trailing isolated backslash is a literal backslash.
    if escaped {
        current.push('\\');
    }
    if has_token {
        args.push(current);
    }
    args
}

/// Replaces `{channel}`, `{type}` and `{extra_args}` in `command`, leaving every
/// other `{...}` sequence untouched (see the module documentation).
pub fn substitute(command: &str, vars: &CommandVars) -> String {
    let mut out = String::with_capacity(command.len());
    let mut rest = command;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        match after.find('}') {
            Some(close) => {
                let key = &after[..close];
                match key {
                    "channel" => out.push_str(&vars.channel),
                    "type" => out.push_str(&vars.type_name),
                    "extra_args" => out.push_str(&vars.extra_args),
                    // Unknown placeholders stay literal on purpose.
                    _ => {
                        out.push('{');
                        out.push_str(key);
                        out.push('}');
                    }
                }
                rest = &after[close + 1..];
            }
            None => {
                // No closing brace: keep the remainder verbatim.
                out.push_str(&rest[open..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

/// One spawned command chain.
///
/// Dropping the handle does **not** kill the children (`kill_on_drop(false)`):
/// the owner (a session) decides when to stop the tree. Use
/// [`stop`](Self::stop) / [`kill_all`](Self::kill_all).
pub struct PipelineHandle {
    label: String,
    children: Mutex<Vec<Child>>,
    pids: Vec<u32>,
    /// stdin of the first stage, when the chain was spawned with
    /// `stdin_piped`.
    stdin: Option<ChildStdin>,
    /// stdout of the last stage.
    stdout: Option<ChildStdout>,
}

impl PipelineHandle {
    /// Handles a chain with no stage: no children, no pipes.
    pub fn passthrough(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            children: Mutex::new(Vec::new()),
            pids: Vec::new(),
            stdin: None,
            stdout: None,
        }
    }

    /// The log label of this chain.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Takes the stdin of the first stage, if the chain has one.
    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.stdin.take()
    }

    /// Takes the stdout of the last stage, if the chain has one.
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.stdout.take()
    }

    /// PIDs of the spawned stages, in pipeline order.
    pub fn pids(&self) -> &[u32] {
        &self.pids
    }

    /// PID of the first stage.
    pub fn pid(&self) -> Option<u32> {
        self.pids.first().copied()
    }

    /// Whether at least one stage is still running.
    pub fn is_alive(&self) -> bool {
        let mut children = lock(&self.children);
        children
            .iter_mut()
            .any(|child| matches!(child.try_wait(), Ok(None)))
    }

    /// Stops the chain with the SPEC §6.2 grace period.
    pub async fn stop(&self) {
        let _ = self.kill_all(KILL_GRACE).await;
    }

    /// Sends `SIGTERM` to every stage's process group, waits up to `grace` for
    /// the whole group to exit, then `SIGKILL`s whatever is left.
    ///
    /// Waiting checks the group (`killpg(pgid, 0)`), not just our direct
    /// children: a grandchild that outlives its parent but ignores `SIGTERM` is
    /// still killed after the grace period. Returns as soon as the group is
    /// gone, without sleeping out the rest of `grace`.
    ///
    /// The `bool` result says whether **every** group is confirmed gone. A
    /// `false` means a member survived even `SIGKILL` (a child stuck in
    /// D-state), i.e. the device may still be held: the caller must not treat
    /// the tuner as free. The direct parent-child fallback signal is used only
    /// for the initial `SIGTERM`, before anything is reaped, so no recycled PID
    /// can be hit.
    pub async fn kill_all(&self, grace: Duration) -> bool {
        let pids = self.pids.clone();
        if pids.is_empty() {
            return true;
        }
        for pid in &pids {
            signal_group(*pid, libc::SIGTERM, true);
        }

        let deadline = Instant::now() + grace;
        loop {
            // Reap first: a zombie still counts as a process-group member for
            // `killpg(pgid, 0)`, so checking the group before reaping would
            // report "still alive" for a group whose leader already exited.
            reap(&self.children);
            if pids.iter().all(|pid| !group_exists(*pid)) {
                return true;
            }
            if Instant::now() >= deadline {
                // Post-reap: the direct child may already be reaped and its
                // pid recycled, so a `kill(pid)` fallback could hit a
                // stranger. Only the process group is signalled here.
                for pid in &pids {
                    signal_group(*pid, libc::SIGKILL, false);
                }
                // After SIGKILL there is no grace left to spend: reap direct
                // children and confirm the group (incl. grandchildren) is
                // gone, but only up to POST_KILL_REAP. Previously this waited
                // another full `grace` (2 s) even though SIGKILLed children
                // normally die within one poll interval, and it only checked
                // direct children, so a surviving grandchild was neither
                // waited for nor confirmed dead.
                let reap_deadline = Instant::now() + POST_KILL_REAP;
                loop {
                    reap(&self.children);
                    if !any_running(&self.children) && pids.iter().all(|pid| !group_exists(*pid)) {
                        return true;
                    }
                    if Instant::now() >= reap_deadline {
                        reap(&self.children);
                        let alive: Vec<u32> = pids
                            .iter()
                            .copied()
                            .filter(|pid| group_exists(*pid))
                            .collect();
                        tracing::warn!(
                            ?alive,
                            "process groups survived SIGKILL; the device may still be held"
                        );
                        return alive.is_empty();
                    }
                    tokio::time::sleep(EXIT_POLL).await;
                }
            }
            tokio::time::sleep(EXIT_POLL).await;
        }
    }

    /// Waits until every stage has exited and returns their exit statuses in
    /// pipeline order.
    ///
    /// Bounded by [`WAIT_TIMEOUT`]: if a stage is still running after that
    /// (e.g. stuck in D-state, where even `SIGKILL` cannot take effect until
    /// the I/O completes), the process groups are `SIGKILL`ed and a short
    /// bounded reap follows, so this never hangs forever. The statuses of
    /// the stages that did exit are returned; still-running stages contribute
    /// nothing. The poll checks *before* sleeping, so a stage that exits
    /// promptly returns promptly.
    pub async fn wait(&self) -> Vec<ExitStatus> {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        let mut timed_out = false;
        loop {
            let (done, statuses) = {
                let mut children = lock(&self.children);
                let mut statuses = Vec::with_capacity(children.len());
                let mut done = true;
                for child in children.iter_mut() {
                    match child.try_wait() {
                        Ok(Some(status)) => statuses.push(Some(status)),
                        Ok(None) => {
                            done = false;
                            statuses.push(None);
                        }
                        // A `try_wait` error does not mean the stage exited,
                        // so the loop must keep polling instead of returning
                        // early with a short status list.
                        Err(_) => {
                            done = false;
                            statuses.push(None);
                        }
                    }
                }
                (done, statuses)
            };
            if done {
                return statuses.into_iter().flatten().collect();
            }
            if !timed_out && Instant::now() >= deadline {
                timed_out = true;
                for pid in &self.pids {
                    signal_group(*pid, libc::SIGKILL, false);
                }
                // Bounded reap after SIGKILL, so a D-state child cannot hold
                // the caller past POST_KILL_REAP either.
            }
            if timed_out && Instant::now() >= deadline + POST_KILL_REAP {
                reap(&self.children);
                // Re-read the (now reaped) statuses: a stage that exited
                // between the poll above and this return would otherwise lose
                // its status. `std` caches the status after reaping.
                let mut children = lock(&self.children);
                return children
                    .iter_mut()
                    .filter_map(|child| child.try_wait().ok().flatten())
                    .collect();
            }
            tokio::time::sleep(EXIT_POLL).await;
        }
    }
}

impl std::fmt::Debug for PipelineHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineHandle")
            .field("label", &self.label)
            .field("pids", &self.pids)
            .finish()
    }
}

/// A single externally spawned tuner command.
///
/// stdout is piped for the data path; stdin is `/dev/null`; stderr is logged.
/// The process is the leader of its own session/process group, like every
/// pipeline stage.
pub struct TunerProcess {
    inner: PipelineHandle,
}

impl TunerProcess {
    /// PID of the tuner process.
    pub fn pid(&self) -> Option<u32> {
        self.inner.pid()
    }

    /// PIDs of the tuner process (always one).
    pub fn pids(&self) -> &[u32] {
        self.inner.pids()
    }

    /// Takes the tuner's stdout.
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.inner.take_stdout()
    }

    /// Whether the tuner process is still running.
    pub fn is_alive(&self) -> bool {
        self.inner.is_alive()
    }

    /// Stops the tuner with the SPEC §6.2 grace period.
    pub async fn stop(&self) {
        self.inner.stop().await;
    }

    /// See [`PipelineHandle::kill_all`].
    pub async fn kill_all(&self, grace: Duration) {
        self.inner.kill_all(grace).await;
    }

    /// Waits for the tuner to exit.
    pub async fn wait(&self) -> Option<ExitStatus> {
        self.inner.wait().await.into_iter().next()
    }
}

impl std::fmt::Debug for TunerProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TunerProcess")
            .field("pid", &self.pid())
            .finish()
    }
}

/// Spawns a tuner command from `tuners.yml`.
///
/// `{channel}` / `{type}` are substituted, the values are validated against the
/// config charset, and the resulting argv is spawned directly (no shell) in its
/// own process group.
pub async fn spawn_tuner_command(
    command: &str,
    vars: &CommandVars,
) -> Result<TunerProcess, PipelineError> {
    validate_vars(vars)?;

    let expanded = substitute(command, vars);
    let argv = shell_split(&expanded);
    if argv.is_empty() || argv[0].is_empty() {
        return Err(PipelineError::NoArgv {
            command: command.to_string(),
        });
    }

    let plan = [SpawnPlan {
        argv,
        stdin_piped: false,
    }];
    let mut inner = spawn_chain(&plan, "tuner", &vars.child_env).await?;
    // A tuner never receives input.
    inner.stdin = None;
    Ok(TunerProcess { inner })
}

/// Spawns every stage of `commands` and chains `stdout -> stdin`.
///
/// The returned handle owns the first stage's stdin and the last stage's
/// stdout. If any stage fails to spawn, the stages already started are killed
/// before the error is returned.
///
/// Stages that are exactly the `passthrough` pseudo-command (after trimming,
/// ASCII case-insensitively) are dropped, matching [`PipelineSpec::new`]. A
/// list that is empty after the filter yields a pass-through handle with no
/// children (SPEC §3.4: a type without a pipeline binding delivers the raw TS).
pub async fn spawn_pipeline(
    commands: &[String],
    ctx: &PipelineCtx,
) -> Result<PipelineHandle, PipelineError> {
    // Validate even for an empty chain: the values belong to the session, and a
    // bad one must fail consistently whether or not a pipeline is bound.
    validate_vars(&ctx.vars)?;

    let stages: Vec<&String> = commands
        .iter()
        .filter(|command| !is_passthrough(command))
        .collect();
    if stages.is_empty() {
        if !commands.is_empty() {
            tracing::debug!(label = %ctx.label, "pipeline reduced to passthrough");
        }
        return Ok(PipelineHandle::passthrough(ctx.label.clone()));
    }

    let mut plan = Vec::with_capacity(stages.len());
    for command in stages {
        let expanded = substitute(command, &ctx.vars);
        let argv = shell_split(&expanded);
        if argv.is_empty() || argv[0].is_empty() {
            return Err(PipelineError::NoArgv {
                command: command.clone(),
            });
        }
        plan.push(SpawnPlan {
            argv,
            stdin_piped: true,
        });
    }

    spawn_chain(&plan, &ctx.label, &ctx.vars.child_env).await
}

/// Whether `command` is the `passthrough` pseudo-command.
///
/// Only the exact token is special: surrounding whitespace is ignored and the
/// match is ASCII case-insensitive. `passthrough-x`, `/usr/bin/passthrough`
/// and `passthrough something` are ordinary command lines.
fn is_passthrough(command: &str) -> bool {
    command.trim().eq_ignore_ascii_case("passthrough")
}

/// Validates the `{channel}` / `{type}` values used for substitution.
///
/// The config layer already validates every value it produces, but a public
/// caller may hand us any string; refusing one here keeps the invariant local
/// to the module that does the substitution (SPEC §3.5).
fn validate_vars(vars: &CommandVars) -> Result<(), PipelineError> {
    validate_channel_value(&vars.channel).map_err(|reason| PipelineError::InvalidValue {
        field: "channel",
        value: vars.channel.clone(),
        reason,
    })?;
    validate_type_name(&vars.type_name).map_err(|reason| PipelineError::InvalidValue {
        field: "type",
        value: vars.type_name.clone(),
        reason,
    })?;
    Ok(())
}

/// One stage to spawn.
struct SpawnPlan {
    argv: Vec<String>,
    /// Whether stdin is a pipe owned by the caller (`spawn_pipeline`) instead
    /// of `/dev/null` (`spawn_tuner_command`).
    stdin_piped: bool,
}

/// Renders the argv of one stage for a spawn failure.
///
/// The template (`command`) is deliberately not used: it still shows
/// `{channel}`, which hides the program and argument that were actually
/// executed. Control characters are collapsed so the error stays one line, and
/// the result is capped well below the progress line's ~200-char budget (the
/// spawn stage prefix and the OS error are added on top).
fn argv_summary(argv: &[String]) -> String {
    const MAX_CHARS: usize = 140;
    let one_line = argv
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if one_line.chars().count() <= MAX_CHARS {
        return one_line;
    }
    let mut truncated: String = one_line.chars().take(MAX_CHARS - 3).collect();
    truncated.push_str("...");
    truncated
}

async fn spawn_chain(
    plan: &[SpawnPlan],
    label: &str,
    child_env: &ChildEnv,
) -> Result<PipelineHandle, PipelineError> {
    let mut children: Vec<Child> = Vec::with_capacity(plan.len());
    let mut pids = Vec::with_capacity(plan.len());
    let mut previous_stdout: Option<ChildStdout> = None;
    let mut first_stdin: Option<ChildStdin> = None;
    let mut last_stdout: Option<ChildStdout> = None;

    for (stage, stage_plan) in plan.iter().enumerate() {
        let mut command = Command::new(&stage_plan.argv[0]);
        command.args(&stage_plan.argv[1..]);
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        command.kill_on_drop(false);
        child_env.apply(&mut command, label, stage);

        if stage == 0 {
            command.stdin(if stage_plan.stdin_piped {
                Stdio::piped()
            } else {
                Stdio::null()
            });
        } else {
            match previous_stdout.take() {
                Some(stdout) => {
                    // Hand the pipe to the next stage as a blocking `Stdio`.
                    let stdio: Stdio =
                        stdout.try_into().map_err(|source| PipelineError::Spawn {
                            stage,
                            argv: argv_summary(&stage_plan.argv),
                            source,
                        })?;
                    command.stdin(stdio);
                }
                // Unreachable for a well-formed chain; `null` keeps the stage
                // from inheriting our stdin.
                None => {
                    command.stdin(Stdio::null());
                }
            };
        }

        // New session and process group: the group survives the leader, which
        // lets `kill_all` reach grandchildren (SPEC §4, §6.2).
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }

        match command.spawn() {
            Ok(mut child) => {
                if let Some(pid) = child.id() {
                    pids.push(pid);
                    log_spawned_stage(stage, pid, label);
                }
                let stderr = child.stderr.take();
                let stdout = child.stdout.take();
                if stage == 0 {
                    first_stdin = child.stdin.take();
                }
                if let Some(stderr) = stderr {
                    spawn_stderr_logger(stderr, label.to_string(), stage);
                }
                if stage + 1 == plan.len() {
                    last_stdout = stdout;
                } else {
                    previous_stdout = stdout;
                }
                children.push(child);
            }
            Err(source) => {
                let handle = PipelineHandle {
                    label: label.to_string(),
                    children: Mutex::new(children),
                    pids,
                    stdin: None,
                    stdout: None,
                };
                // Best-effort cleanup of the stages that did start. Nothing
                // has been reaped yet here, so the `kill(pid)` fallback is
                // still safe (a child that has not reached `setsid` would
                // otherwise be missed by `killpg`).
                for pid in &handle.pids {
                    signal_group(*pid, libc::SIGKILL, true);
                }
                let _ = handle.wait().await;
                return Err(PipelineError::Spawn {
                    stage,
                    argv: argv_summary(&stage_plan.argv),
                    source,
                });
            }
        }
    }

    Ok(PipelineHandle {
        label: label.to_string(),
        children: Mutex::new(children),
        pids,
        stdin: first_stdin,
        stdout: last_stdout,
    })
}

/// Logs a spawned stage and, at debug level only, what its fd 0/1 point at.
///
/// `/proc/<pid>/fd/{0,1}` is read as a symlink directly (no shell): the target
/// is a `pipe:[inode]` for a pipe and a path for an inherited file, so this
/// makes a broken chain visible without spawning anything extra.
fn log_spawned_stage(stage: usize, pid: u32, label: &str) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    let fd0 = std::fs::read_link(format!("/proc/{pid}/fd/0"))
        .map(|target| target.display().to_string())
        .unwrap_or_else(|error| format!("<unavailable: {error}>"));
    let fd1 = std::fs::read_link(format!("/proc/{pid}/fd/1"))
        .map(|target| target.display().to_string())
        .unwrap_or_else(|error| format!("<unavailable: {error}>"));
    tracing::debug!(session = label, stage, pid, %fd0, %fd1, "spawned stage");
}

/// Reads a stage's stderr line by line.
///
/// Every line is prefixed with the session label and the stage index. Lines
/// that look like an error are logged at `warn`; the rest at `debug`, so a
/// chatty parser does not drown the log at the default level.
fn spawn_stderr_logger(stderr: tokio::process::ChildStderr, label: String, stage: usize) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let lower = line.to_ascii_lowercase();
                    if lower.contains("error") || lower.contains("fail") || lower.contains("fatal")
                    {
                        tracing::warn!(session = %label, stage, "stderr: {line}");
                    } else {
                        tracing::debug!(session = %label, stage, "stderr: {line}");
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::debug!(session = %label, stage, %error, "stderr read failed");
                    break;
                }
            }
        }
    });
}

/// Sends `signal` to the process group led by `pid`, ignoring `ESRCH`.
///
/// Falls back to signalling the pid directly when `killpg` reports `ESRCH`.
/// That covers the spawn race where the child has not run `setsid` yet (so
/// no group with `pgid == pid` exists and the group signal would be lost
/// entirely, leaving the process to survive the whole grace period), and it
/// is also the only signal that reaches a leader whose group has already
/// drained. The fallback is *not* used when `killpg` succeeds, so a leader
/// that is a group member never gets the signal twice.
fn signal_group(pid: u32, signal: libc::c_int, allow_pid_fallback: bool) {
    if pid == 0 || pid > i32::MAX as u32 {
        return;
    }
    // SAFETY: `killpg` only takes a pgid and a signal number.
    let result = unsafe { libc::killpg(pid as libc::pid_t, signal) };
    // The `kill(pid)` fallback covers a child spawned a moment ago that has not
    // reached `setsid` yet: its group does not exist, so `killpg` is ESRCH and
    // the leader would otherwise survive the whole grace period. It is only
    // safe while the child is still unreaped, because after a reap the pid can
    // be recycled and `kill(pid)` would hit an unrelated process; the
    // post-reap `SIGKILL` paths therefore pass `false`.
    if allow_pid_fallback
        && result != 0
        && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    {
        // SAFETY: `kill` only takes a pid and a signal number.
        unsafe {
            libc::kill(pid as libc::pid_t, signal);
        }
    }
}

/// Whether the process group led by `pid` still has a member.
fn group_exists(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    // SAFETY: signal 0 only checks for existence and permission.
    let result = unsafe { libc::killpg(pid as libc::pid_t, 0) };
    if result == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn any_running(children: &Mutex<Vec<Child>>) -> bool {
    let mut children = lock(children);
    children
        .iter_mut()
        .any(|child| matches!(child.try_wait(), Ok(None)))
}

/// Reaps exited children without blocking.
fn reap(children: &Mutex<Vec<Child>>) {
    let mut children = lock(children);
    for child in children.iter_mut() {
        let _ = child.try_wait();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn ctx() -> PipelineCtx {
        PipelineCtx::new(CommandVars::new("27", "GR"), "test")
    }

    fn packet(seed: u8) -> Vec<u8> {
        let mut packet = vec![seed; 188];
        packet[0] = 0x47;
        packet
    }

    #[test]
    fn shell_split_handles_quotes_and_escapes() {
        assert_eq!(shell_split("cat /tmp/a"), ["cat", "/tmp/a"]);
        assert_eq!(
            shell_split("sh -c 'cat a; sleep 1'"),
            ["sh", "-c", "cat a; sleep 1"]
        );
        assert_eq!(shell_split(r#"echo "a b" c"#), ["echo", "a b", "c"]);
        assert_eq!(shell_split(r"echo a\ b"), ["echo", "a b"]);
        assert_eq!(shell_split(r"echo 'a\b'"), ["echo", r"a\b"]);
        assert_eq!(shell_split(r#""a\"b""#), [r#"a"b"#]);
        assert_eq!(shell_split("  a   b  "), ["a", "b"]);
        assert_eq!(shell_split(""), Vec::<String>::new());
        assert_eq!(shell_split("   "), Vec::<String>::new());
        // No variable expansion, no globbing.
        assert_eq!(shell_split("echo $HOME *"), ["echo", "$HOME", "*"]);
    }

    #[test]
    fn shell_split_keeps_unbalanced_quotes_in_the_word() {
        assert_eq!(shell_split(r#"cat "a b"#), ["cat", "a b"]);
        assert_eq!(shell_split(r"cat 'a b"), ["cat", "a b"]);
    }

    #[test]
    fn placeholders_are_substituted_and_unknown_ones_stay_literal() {
        let vars = CommandVars::new("BS13_1", "BS");
        assert_eq!(
            substitute("recpt1 {channel} {type}", &vars),
            "recpt1 BS13_1 BS"
        );
        assert_eq!(
            substitute("run -c {channel}/{type}", &vars),
            "run -c BS13_1/BS"
        );
        assert_eq!(substitute("x {chanel} y", &vars), "x {chanel} y");
        assert_eq!(substitute("x {unclosed", &vars), "x {unclosed");
        assert_eq!(substitute("no placeholders", &vars), "no placeholders");
        // Only exact `{channel}` / `{type}` keys are recognised; a nested brace
        // is an unknown placeholder and stays literal.
        assert_eq!(substitute("{{channel}}", &vars), "{{channel}}");
    }

    #[test]
    fn extra_args_placeholder_is_substituted_and_defaults_to_empty() {
        let vars = CommandVars::new("13", "CS").with_extra_args("--lnb 15");
        assert_eq!(
            substitute("tuner {channel} {extra_args} -", &vars),
            "tuner 13 --lnb 15 -"
        );

        // Unset: the placeholder is replaced by nothing and the surrounding
        // whitespace collapses, so the token disappears from the argv.
        let vars = CommandVars::new("13", "CS");
        assert_eq!(
            substitute("tuner {channel} {extra_args} -", &vars),
            "tuner 13  -"
        );
        assert_eq!(
            shell_split(&substitute("tuner {channel} {extra_args} -", &vars)),
            ["tuner", "13", "-"]
        );

        // Quoted, an empty substitution is one empty token, as in a shell.
        assert_eq!(
            shell_split(&substitute("tuner {channel} \"{extra_args}\" -", &vars)),
            ["tuner", "13", "", "-"]
        );

        // Unknown placeholders still stay literal, including near extra_args.
        assert_eq!(substitute("a {extra_argz} b", &vars), "a {extra_argz} b");
    }

    #[test]
    fn for_type_resolves_the_binding_and_defaults_to_passthrough() {
        use std::collections::HashMap;

        use crate::config::ServerConfig;
        use crate::config::{PipelineBinding, PipelinesConfig, TunerConfig};

        let config = Config {
            server: ServerConfig::default(),
            tuners: vec![TunerConfig {
                name: "t0".to_string(),
                types: vec!["GR".to_string()],
                command: "cat {channel}".to_string(),
                disabled: false,
            }],
            channels: Vec::new(),
            pipelines: PipelinesConfig {
                pipelines: HashMap::from([(
                    "soft".to_string(),
                    vec!["cat".to_string(), "cat".to_string()],
                )]),
                types: HashMap::from([
                    (
                        "GR".to_string(),
                        PipelineBinding {
                            pipeline: "soft".to_string(),
                        },
                    ),
                    (
                        "BS".to_string(),
                        PipelineBinding {
                            pipeline: "missing".to_string(),
                        },
                    ),
                ]),
            },
        };

        assert_eq!(
            PipelineSpec::for_type(&config, "GR").commands,
            ["cat", "cat"]
        );
        // Unbound type and a dangling binding both deliver the raw TS.
        assert!(PipelineSpec::for_type(&config, "CS").is_empty());
        assert!(PipelineSpec::for_type(&config, "BS").is_empty());
    }

    #[test]
    fn passthrough_stages_are_dropped_from_the_spec() {
        assert!(PipelineSpec::new(vec!["passthrough".to_string()]).is_empty());
        assert!(PipelineSpec::new(vec!["Passthrough".to_string()]).is_empty());
        assert!(PipelineSpec::new(vec!["  passthrough  ".to_string()]).is_empty());
        assert!(
            PipelineSpec::new(vec!["passthrough".to_string(), "passthrough".to_string()])
                .is_empty()
        );

        assert_eq!(
            PipelineSpec::new(vec!["passthrough".to_string(), "cat".to_string()]).commands,
            ["cat"]
        );
        assert_eq!(
            PipelineSpec::new(vec!["cat".to_string(), "passthrough".to_string()]).commands,
            ["cat"]
        );

        // Only the exact token is special; longer names stay real commands.
        assert_eq!(
            PipelineSpec::new(vec!["passthrough-x".to_string()]).commands,
            ["passthrough-x"]
        );
    }

    #[tokio::test]
    async fn spawn_pipeline_filters_passthrough_before_spawning() {
        let mut pipeline = spawn_pipeline(&["passthrough".to_string()], &ctx())
            .await
            .expect("spawn");
        assert!(pipeline.pids().is_empty());
        assert!(pipeline.take_stdin().is_none());
        assert!(pipeline.take_stdout().is_none());
        assert!(!pipeline.is_alive());
        pipeline.stop().await;

        // Mixed chains keep only the real stages, in order.
        for commands in [["passthrough", "cat"], ["cat", "passthrough"]] {
            let pipeline = spawn_pipeline(&commands.map(str::to_string), &ctx())
                .await
                .expect("spawn mixed chain");
            assert_eq!(pipeline.pids().len(), 1);
            assert!(pipeline.is_alive());
            pipeline.stop().await;
        }

        let pipeline = spawn_pipeline(
            &["passthrough".to_string(), "passthrough".to_string()],
            &ctx(),
        )
        .await
        .expect("spawn all-passthrough chain");
        assert!(pipeline.pids().is_empty());
        assert!(!pipeline.is_alive());
        pipeline.stop().await;
    }

    #[test]
    fn empty_command_is_rejected() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let vars = CommandVars::new("27", "GR");
        runtime.block_on(async {
            assert!(matches!(
                spawn_tuner_command("   ", &vars).await,
                Err(PipelineError::NoArgv { .. })
            ));
            assert!(matches!(
                spawn_pipeline(&["  ".to_string()], &ctx()).await,
                Err(PipelineError::NoArgv { .. })
            ));
        });
    }

    #[test]
    fn an_invalid_pipeline_variable_is_rejected_before_spawning() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let bad = PipelineCtx::new(CommandVars::new("27", "GR!"), "test");
        runtime.block_on(async {
            assert!(matches!(
                spawn_pipeline(&["cat".to_string()], &bad).await,
                Err(PipelineError::InvalidValue { field: "type", .. })
            ));
        });
    }

    #[test]
    fn invalid_channel_value_is_rejected_before_spawning() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let vars = CommandVars::new("27; rm -rf /", "GR");
        runtime.block_on(async {
            assert!(matches!(
                spawn_tuner_command("cat {channel}", &vars).await,
                Err(PipelineError::InvalidValue {
                    field: "channel",
                    ..
                })
            ));
        });
    }

    #[test]
    fn a_spawn_error_reports_the_expanded_argv_instead_of_the_template() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let vars = CommandVars::new("27", "GR");
        runtime.block_on(async {
            let error = spawn_tuner_command("/nonexistent/unituner-tuner {channel}", &vars)
                .await
                .expect_err("the missing program must fail to spawn");
            let message = error.to_string();
            assert!(
                message.contains("/nonexistent/unituner-tuner 27"),
                "the expanded argv must be visible: {message}"
            );
            assert!(
                !message.contains("{channel}"),
                "the unsubstituted template must not leak: {message}"
            );
            assert!(!message.contains('\n'), "the message must stay one line");
        });
    }

    #[test]
    fn argv_summary_is_one_short_line() {
        let argv = vec!["sh".to_string(), "-c".to_string(), "x\n".repeat(200)];
        let summary = argv_summary(&argv);
        assert!(!summary.contains('\n'), "{summary}");
        assert_eq!(summary.chars().count(), 140, "{summary}");
        assert!(summary.ends_with("..."), "{summary}");
        assert_eq!(argv_summary(&["cat".to_string()]), "cat");
    }

    #[tokio::test]
    async fn cat_chain_round_trips_bytes() {
        let mut pipeline = spawn_pipeline(
            &["cat".to_string(), "cat".to_string(), "cat".to_string()],
            &ctx(),
        )
        .await
        .expect("spawn cat chain");
        assert_eq!(pipeline.pids().len(), 3);
        assert!(pipeline.is_alive());

        let mut stdin = pipeline.take_stdin().expect("first stage stdin");
        let mut stdout = pipeline.take_stdout().expect("last stage stdout");

        let payload = packet(0x11);
        let expected = payload.clone();
        let writer = tokio::spawn(async move {
            stdin.write_all(&payload).await.expect("write");
            stdin.shutdown().await.expect("close stdin");
        });
        let mut received = Vec::new();
        let mut buf = vec![0u8; 4096];
        loop {
            let read = tokio::time::timeout(Duration::from_secs(5), stdout.read(&mut buf))
                .await
                .expect("read timeout")
                .expect("read");
            if read == 0 {
                break;
            }
            received.extend_from_slice(&buf[..read]);
            if received.len() >= expected.len() {
                break;
            }
        }
        writer.await.expect("writer task");
        assert_eq!(received, expected);

        pipeline.stop().await;
        assert!(!pipeline.is_alive());
    }

    #[tokio::test]
    async fn children_get_the_configured_environment_and_cwd() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let cwd = dir.path().join("cwd");
        std::fs::create_dir(&cwd).expect("create cwd");
        let dump = dir.path().join("env.txt");
        let command = format!(
            "sh -c 'printf \"PATH=%s\\nHOME=%s\\nTMPDIR=%s\\nPWD=%s\\n\" \
             \"$PATH\" \"$HOME\" \"$TMPDIR\" \"$PWD\" > {}; cat'",
            dump.display()
        );

        // `child_env` is applied after PATH/HOME/TMPDIR, so its HOME wins.
        let mut vars = CommandVars::new("27", "GR");
        vars.child_env = ChildEnv {
            path: "/configured/bin:/usr/bin:/bin".to_string(),
            home: "/configured/home".to_string(),
            tmpdir: "/configured/tmp".to_string(),
            cwd: Some(cwd.display().to_string()),
            extra: HashMap::from([("HOME".to_string(), "/overridden/home".to_string())]),
        };
        let mut pipeline = spawn_pipeline(&[command], &PipelineCtx::new(vars, "env-test"))
            .await
            .expect("spawn env probe");
        let mut stdin = pipeline.take_stdin().expect("stdin");
        let mut stdout = pipeline.take_stdout().expect("stdout");
        stdin.write_all(&packet(0x33)).await.expect("write");
        stdin.shutdown().await.expect("close stdin");
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut buf = [0u8; 188];
            stdout.read_exact(&mut buf).await.expect("read");
        })
        .await
        .expect("stage output");

        let reported = std::fs::read_to_string(&dump).expect("read env dump");
        // The parent's environment is cleared, so only the configured values
        // can appear here (the test process may or may not have HOME set).
        assert!(
            reported.contains("PATH=/configured/bin:/usr/bin:/bin\n"),
            "{reported}"
        );
        assert!(reported.contains("HOME=/overridden/home\n"), "{reported}");
        assert!(reported.contains("TMPDIR=/configured/tmp\n"), "{reported}");
        assert!(
            reported.contains(&format!("PWD={}\n", cwd.display())),
            "{reported}"
        );
        pipeline.stop().await;
    }

    #[tokio::test]
    async fn every_stage_gets_pipes_on_stdin_and_stdout() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        // Capture the link targets into shell variables *before* redirecting:
        // `readlink ... > file` would otherwise show the redirection's own file
        // as fd 1 when the shell execs the last command.
        let stage = |index: usize| {
            format!(
                "sh -c 'fd0=$(readlink /proc/$$/fd/0); fd1=$(readlink /proc/$$/fd/1); \
                 printf \"%s\" \"$fd0\" > {}/fd0_{index}; \
                 printf \"%s\" \"$fd1\" > {}/fd1_{index}; cat'",
                dir.path().display(),
                dir.path().display()
            )
        };
        let commands = [stage(0), stage(1), stage(2)];
        let mut pipeline = spawn_pipeline(&commands, &ctx()).await.expect("spawn");
        let mut stdin = pipeline.take_stdin().expect("stdin");
        let mut stdout = pipeline.take_stdout().expect("stdout");
        stdin.write_all(&packet(0x44)).await.expect("write");
        stdin.shutdown().await.expect("close stdin");

        let mut received = Vec::new();
        let mut buf = [0u8; 188];
        while received.len() < 188 {
            let read = tokio::time::timeout(Duration::from_secs(5), stdout.read(&mut buf))
                .await
                .expect("read timeout")
                .expect("read");
            if read == 0 {
                break;
            }
            received.extend_from_slice(&buf[..read]);
        }
        assert_eq!(received, packet(0x44));

        let fd = |stage: usize, which: u8| {
            let path = dir.path().join(format!("fd{which}_{stage}"));
            std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
                .trim()
                .to_string()
        };
        for stage in 0..3 {
            assert!(
                fd(stage, 0).starts_with("pipe:["),
                "stage {stage} stdin is not a pipe: {}",
                fd(stage, 0)
            );
            assert!(
                fd(stage, 1).starts_with("pipe:["),
                "stage {stage} stdout is not a pipe: {}",
                fd(stage, 1)
            );
        }
        // The chain really hands one pipe from a stage's stdout to the next
        // stage's stdin.
        assert_eq!(fd(0, 1), fd(1, 0), "stage 0 stdout -> stage 1 stdin");
        assert_eq!(fd(1, 1), fd(2, 0), "stage 1 stdout -> stage 2 stdin");

        pipeline.stop().await;
    }

    #[tokio::test]
    async fn kill_all_stops_a_sleeping_chain_quickly() {
        let pipeline = spawn_pipeline(
            &[r#"sh -c "sleep 60""#.to_string(), "cat".to_string()],
            &ctx(),
        )
        .await
        .expect("spawn sleep chain");
        let pids = pipeline.pids().to_vec();
        assert_eq!(pids.len(), 2);
        assert!(pipeline.is_alive());

        let started = Instant::now();
        pipeline.kill_all(KILL_GRACE).await;
        let elapsed = started.elapsed();

        println!("kill_all(sleep chain) took {elapsed:?}");
        assert!(
            elapsed < Duration::from_secs(3),
            "kill_all took {elapsed:?}, expected well under the 2 s grace"
        );
        assert!(!pipeline.is_alive());
        for pid in pids {
            assert!(
                !process_exists(pid),
                "pid {pid} survived kill_all after {elapsed:?}"
            );
        }
    }

    #[tokio::test]
    async fn kill_all_escalates_to_sigkill_after_the_grace() {
        // The shell ignores SIGTERM, so only SIGKILL can stop it. Wait for its
        // `ready` line before signalling: a signal that arrives before the
        // trap is installed would kill it with the default action.
        let mut pipeline = spawn_pipeline(
            &[r#"sh -c "trap '' TERM; echo ready; while :; do sleep 1; done""#.to_string()],
            &ctx(),
        )
        .await
        .expect("spawn TERM-ignoring chain");
        let pid = pipeline.pid().expect("pid");
        let mut stdout = pipeline.take_stdout().expect("stdout");
        let mut ready = [0u8; 6];
        tokio::time::timeout(Duration::from_secs(5), stdout.read_exact(&mut ready))
            .await
            .expect("ready timeout")
            .expect("read ready");
        assert_eq!(&ready, b"ready\n");
        assert!(pipeline.is_alive());

        let grace = Duration::from_millis(300);
        let started = Instant::now();
        pipeline.kill_all(grace).await;
        let elapsed = started.elapsed();

        println!("kill_all(TERM-ignoring chain, grace 300 ms) took {elapsed:?}");
        assert!(
            elapsed >= grace,
            "SIGKILL was sent before the grace period: {elapsed:?}"
        );
        assert!(
            elapsed < grace + Duration::from_secs(2),
            "kill_all did not escalate promptly: {elapsed:?}"
        );
        assert!(!process_exists(pid), "pid {pid} survived SIGKILL");
        assert!(!pipeline.is_alive());
    }

    #[tokio::test]
    async fn kill_all_returns_early_when_children_exit_on_their_own() {
        let pipeline = spawn_pipeline(&["true".to_string()], &ctx())
            .await
            .expect("spawn true");
        let started = Instant::now();
        pipeline.kill_all(KILL_GRACE).await;
        // No child is left, so no grace period is spent waiting.
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "kill_all waited unnecessarily: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn an_empty_pipeline_is_a_passthrough() {
        let mut pipeline = spawn_pipeline(&[], &ctx()).await.expect("spawn");
        assert!(pipeline.pids().is_empty());
        assert!(pipeline.take_stdin().is_none());
        assert!(pipeline.take_stdout().is_none());
        assert!(!pipeline.is_alive());
        pipeline.stop().await;
    }

    #[tokio::test]
    async fn a_tuner_command_pipes_stdout() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("payload.bin");
        std::fs::write(&path, packet(0x22)).expect("write fixture");

        let command = format!("cat {}", path.display());
        let mut tuner = spawn_tuner_command(&command, &CommandVars::new("27", "GR"))
            .await
            .expect("spawn tuner");
        assert!(tuner.pid().is_some());
        let mut stdout = tuner.take_stdout().expect("stdout");
        let mut received = Vec::new();
        let mut buf = vec![0u8; 1024];
        loop {
            let read = tokio::time::timeout(Duration::from_secs(5), stdout.read(&mut buf))
                .await
                .expect("read timeout")
                .expect("read");
            if read == 0 {
                break;
            }
            received.extend_from_slice(&buf[..read]);
        }
        assert_eq!(received, packet(0x22));
        tuner.stop().await;
    }

    /// `kill(pid, 0)`: true while the process (or a zombie) exists.
    fn process_exists(pid: u32) -> bool {
        // SAFETY: signal 0 only checks for existence and permission.
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }
}
