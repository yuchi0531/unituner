//! File plumbing for the webui editors (SPEC §10).
//!
//! A `PUT` runs the same validation as startup: the candidate YAML is staged in
//! a scratch directory together with copies of the other three config files and
//! then loaded with [`Config::load`], so the startup rules are reused instead of
//! duplicated. Only a valid candidate is written.
//!
//! The write itself is `copy to .bak` → write `<file>.tmp` → `fsync` →
//! `rename`, which is atomic for a reader on the same filesystem: a request
//! either sees the old file or the new one, never a half-written document.
//!
//! # Blocking work and locking
//!
//! Staging, `fsync` and `rename` are synchronous filesystem calls; on a slow
//! medium (USB stick, NFS) they can block for a long time. The async handlers
//! therefore run them through [`tokio::task::spawn_blocking`], and the per-file
//! mutex is a [`tokio::sync::Mutex`]:
//!
//! * the guard is acquired in async code with [`lock_async`], so waiting for a
//!   busy file yields to the runtime instead of parking a worker;
//! * the guard is then *moved into* the blocking closure, so it is held for the
//!   whole read-validate-write sequence and released when that closure returns.
//!
//! Holding the guard inside the blocking closure cannot deadlock: the runtime's
//! blocking pool is only used for work that does not need the async reactor,
//! and no other lock is taken while waiting for this one. Tasks that want the
//! same file wait on the mutex (asynchronously); tasks on other files take other
//! mutexes and proceed in parallel on the blocking pool.
//!
//! The scan layer ([`crate::scan`]) still calls the synchronous [`lock`], which
//! spins on the same mutex with `try_lock`. That keeps scan and webui saves
//! mutually exclusive, but the scan owner should switch to
//! `lock_async(..).await` (and run its write inside `spawn_blocking`) so a slow
//! `fsync` does not park a worker there.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

use crate::config::{Config, ConfigError};

/// The tuner definitions (SPEC §3.2). Editable.
pub const TUNERS_FILE: &str = "tuners.yml";
/// The channel list (SPEC §3.3). Editable.
pub const CHANNELS_FILE: &str = "channels.yml";
/// The pipeline definitions and type bindings (SPEC §3.4). Editable.
pub const PIPELINES_FILE: &str = "pipelines.yml";
/// The server settings (SPEC §3.1). Read-only in the UI.
pub const SERVER_FILE: &str = "server.yml";

/// Every file [`Config::load`] needs.
pub const ALL_FILES: [&str; 4] = [SERVER_FILE, TUNERS_FILE, CHANNELS_FILE, PIPELINES_FILE];

/// Why a save was refused.
#[derive(Debug)]
pub enum SaveError {
    /// The candidate configuration is invalid; nothing was written (400).
    Invalid(Vec<String>),
    /// The files could not be staged, read or written (500).
    Internal(String),
}

impl SaveError {
    /// Wraps an internal (server-side) failure message.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

/// One mutex per editable file, so two `PUT`s of the same file cannot
/// interleave their read-validate-write sequence.
#[derive(Debug, Default)]
pub struct FileLocks {
    tuners: Mutex<()>,
    channels: Mutex<()>,
    pipelines: Mutex<()>,
}

impl FileLocks {
    fn mutex(&self, file: &str) -> &Mutex<()> {
        match file {
            TUNERS_FILE => &self.tuners,
            CHANNELS_FILE => &self.channels,
            PIPELINES_FILE => &self.pipelines,
            other => unreachable!("{other:?} is not an editable config file"),
        }
    }
}

/// The process-wide locks. One instance, so every router built in this process
/// serialises against the same mutexes.
static LOCKS: OnceLock<FileLocks> = OnceLock::new();

fn file_mutex(file: &str) -> &'static Mutex<()> {
    LOCKS.get_or_init(FileLocks::default).mutex(file)
}

/// Locks `file` for the duration of its save sequence, asynchronously.
///
/// The returned guard is `'static` (the mutexes live in a `OnceLock`), so it can
/// be moved into a `spawn_blocking` closure and held across the blocking body.
/// See the module docs for why that is safe.
pub async fn lock_async(file: &str) -> MutexGuard<'static, ()> {
    file_mutex(file).lock().await
}

/// Locks `file` for a synchronous caller (the channel scan).
///
/// Tokio's `blocking_lock` panics inside a runtime, and the scan calls this from
/// an async task, so this spins with `try_lock` instead. It shares the same
/// mutex as [`lock_async`], which keeps scan writes and webui saves mutually
/// exclusive. The critical section in the scan is a short read-merge-write; a
/// webui save holding the lock during a slow `fsync` makes the scan's worker
/// thread wait, which is why the scan should move to [`lock_async`].
pub fn lock(file: &str) -> MutexGuard<'static, ()> {
    let mutex = file_mutex(file);
    loop {
        if let Ok(guard) = mutex.try_lock() {
            return guard;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Validates `yaml` as the new content of `file` and, when the whole
/// configuration is valid, writes it to `dir`.
///
/// This is the blocking body of a save: call it inside
/// [`tokio::task::spawn_blocking`] while holding [`lock_async`]'s guard (see the
/// module docs). The scan layer calls it directly, from its own blocking
/// section.
pub fn save(dir: &Path, file: &'static str, yaml: &str) -> Result<(), SaveError> {
    validate_document(dir, file, yaml)?;
    write_atomic(&dir.join(file), yaml).map_err(SaveError::Internal)
}

/// [`save`] on the blocking pool, serialised per file by [`lock_async`].
///
/// The guard is acquired in async code and moved into the blocking closure, so
/// the file is held for the whole validate-and-write sequence with no guard
/// held across an `await` by a runtime worker. The webui handler inlines the
/// same pattern (it also has to check `If-Match` inside the lock); this wrapper
/// keeps the "same file serialises, other files do not" property testable.
#[cfg(test)]
pub async fn save_blocking_async(
    dir: PathBuf,
    file: &'static str,
    yaml: String,
) -> Result<(), SaveError> {
    let guard = lock_async(file).await;
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        save(&dir, file, &yaml)
    })
    .await
    .map_err(|error| SaveError::internal(format!("save task failed: {error}")))?
}

/// Re-parses the candidate through [`Config::load`] in a scratch directory.
///
/// The scratch directory holds copies of the three files that are not being
/// edited, so cross-file rules (for example a `pipelines.types` binding to an
/// existing pipeline) are checked exactly like at startup.
pub fn validate_document(dir: &Path, file: &'static str, yaml: &str) -> Result<(), SaveError> {
    let scratch = scratch_dir()?;
    let result = stage_and_load(&scratch, dir, file, yaml);
    // Best effort: a leftover scratch directory must not fail a successful save.
    let _ = fs::remove_dir_all(&scratch);
    result
}

fn stage_and_load(
    scratch: &Path,
    dir: &Path,
    file: &'static str,
    yaml: &str,
) -> Result<(), SaveError> {
    for other in ALL_FILES.iter().copied().filter(|name| *name != file) {
        let from = dir.join(other);
        let to = scratch.join(other);
        fs::copy(&from, &to).map_err(|error| {
            SaveError::internal(format!("failed to stage {}: {error}", from.display()))
        })?;
    }
    fs::write(scratch.join(file), yaml)
        .map_err(|error| SaveError::internal(format!("failed to stage {file}: {error}")))?;

    match Config::load(scratch) {
        Ok(_) => Ok(()),
        Err(ConfigError::Invalid(issues)) => Err(SaveError::Invalid(
            issues.iter().map(|i| i.to_string()).collect(),
        )),
        // Parse and read failures are configuration problems too: the user has
        // to fix the document, so they are reported as 400 issues.
        Err(error) => Err(SaveError::Invalid(vec![error.to_string()])),
    }
}

/// Writes `contents` to `path` atomically, keeping one `.bak` generation.
///
/// The replacement is created with mode `0600` (on Unix) and, when the file
/// already exists, the original mode is applied again after the rename: a
/// `chmod 600 tuners.yml` survives a webui save instead of being loosened to the
/// umask default. Both the tuner command lines and key paths are sensitive.
pub fn write_atomic(path: &Path, contents: &str) -> Result<(), String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("{} has no file name", path.display()))?;
    let tmp = path.with_file_name(format!("{name}.tmp"));
    let bak = path.with_file_name(format!("{name}.bak"));

    // Read the mode before writing: `rename` replaces the inode, so the mode of
    // the file being replaced would otherwise be lost.
    #[cfg(unix)]
    let original_mode = path
        .metadata()
        .ok()
        .map(|metadata| std::os::unix::fs::PermissionsExt::mode(&metadata.permissions()));

    // SPEC §10 order: back up the previous file, then write the replacement.
    if path.exists() {
        fs::copy(path, &bak)
            .map_err(|error| format!("failed to back up {}: {error}", path.display()))?;
    }

    let written = (|| -> std::io::Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // The mode is set explicitly below; `mode` only covers the case
            // where the handle is created here.
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        // A leftover `.tmp` from a crashed save may already exist with another
        // mode, and `create(true)` does not change it, so the mode is applied
        // to the open handle. It is also immune to the umask.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = original_mode.unwrap_or(0o600);
            file.set_permissions(fs::Permissions::from_mode(mode))?;
        }
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)?;
        // Persist the rename itself (best effort; not every platform allows
        // opening a directory for fsync).
        if let Some(dir) = path.parent() {
            if let Ok(handle) = File::open(dir) {
                let _ = handle.sync_all();
            }
        }
        Ok(())
    })();

    if written.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    written.map_err(|error| format!("failed to write {}: {error}", path.display()))
}

/// A scratch directory unique to this process and validation attempt.
///
/// `<tmp>/unituner-validate-<pid>-<seq>`, created with `create_dir` (which fails
/// if the name already exists, so a pre-seeded path cannot be reused) and mode
/// `0700`.
fn scratch_dir() -> Result<PathBuf, SaveError> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "unituner-validate-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&path).map_err(|error| {
        SaveError::internal(format!("failed to create {}: {error}", path.display()))
    })?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const SERVER: &str = "addr: \":0\"\n";
    const TUNERS: &str = "- name: t0\n  types: [GR]\n  command: \"recpt1 {channel} -\"\n";
    const PIPELINES: &str =
        "pipelines:\n  pass:\n    - \"cat\"\ntypes:\n  GR: { pipeline: pass }\n";

    fn fixture() -> TempDir {
        let dir = TempDir::new().expect("tempdir");
        fs::write(dir.path().join(SERVER_FILE), SERVER).expect("server");
        fs::write(dir.path().join(TUNERS_FILE), TUNERS).expect("tuners");
        fs::write(
            dir.path().join(CHANNELS_FILE),
            "- name: NHK\n  type: GR\n  channel: \"27\"\n",
        )
        .expect("channels");
        fs::write(dir.path().join(PIPELINES_FILE), PIPELINES).expect("pipelines");
        dir
    }

    #[test]
    fn valid_candidate_is_written_and_backed_up() {
        let dir = fixture();
        let path = dir.path().join(CHANNELS_FILE);
        let before = fs::read_to_string(&path).expect("read");
        save(
            dir.path(),
            CHANNELS_FILE,
            "- name: NHK2\n  type: GR\n  channel: \"28\"\n",
        )
        .expect("valid save");
        assert!(fs::read_to_string(&path).expect("read").contains("28"));
        assert_eq!(
            fs::read_to_string(dir.path().join("channels.yml.bak")).expect("bak"),
            before
        );
        assert!(!dir.path().join("channels.yml.tmp").exists());
        // The scratch directory is removed again.
        Config::load(dir.path()).expect("still loadable");
    }

    #[test]
    fn invalid_candidate_is_not_written() {
        let dir = fixture();
        let path = dir.path().join(CHANNELS_FILE);
        let before = fs::read_to_string(&path).expect("read");
        let error = save(
            dir.path(),
            CHANNELS_FILE,
            "- name: NHK\n  type: \"GR!\"\n  channel: \"27\"\n",
        )
        .expect_err("invalid type name");
        match error {
            SaveError::Invalid(issues) => {
                assert!(issues
                    .iter()
                    .any(|issue| issue.contains("invalid type name")));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(fs::read_to_string(&path).expect("read"), before);
        assert!(!dir.path().join("channels.yml.bak").exists());
    }

    #[test]
    fn cross_file_rules_are_checked_against_the_other_files() {
        let dir = fixture();
        let error = save(
            dir.path(),
            PIPELINES_FILE,
            "pipelines:\n  pass:\n    - \"cat\"\ntypes:\n  GR: { pipeline: nope }\n",
        )
        .expect_err("unknown pipeline");
        match error {
            SaveError::Invalid(issues) => {
                assert!(issues
                    .iter()
                    .any(|issue| issue.contains("references unknown pipeline")));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn unparseable_candidate_is_reported_as_a_client_error() {
        let dir = fixture();
        match save(dir.path(), TUNERS_FILE, "unknown_field: true\n") {
            Err(SaveError::Invalid(issues)) => assert!(!issues.is_empty()),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path).expect("metadata").permissions().mode() & 0o777
    }

    #[cfg(unix)]
    #[test]
    fn saving_preserves_a_restrictive_mode() {
        use std::os::unix::fs::PermissionsExt;

        let dir = fixture();
        let path = dir.path().join(TUNERS_FILE);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("chmod");
        save(
            dir.path(),
            TUNERS_FILE,
            "- name: t0\n  types: [GR]\n  command: \"cat {channel}\"\n",
        )
        .expect("save");
        assert_eq!(mode_of(&path), 0o600, "a chmod 600 must survive the save");
    }

    #[cfg(unix)]
    #[test]
    fn saving_preserves_the_original_mode_exactly() {
        use std::os::unix::fs::PermissionsExt;

        let dir = fixture();
        let path = dir.path().join(CHANNELS_FILE);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).expect("chmod");
        save(
            dir.path(),
            CHANNELS_FILE,
            "- name: NHK\n  type: GR\n  channel: \"27\"\n  disabled: true\n",
        )
        .expect("save");
        assert_eq!(mode_of(&path), 0o640, "the operator's mode is preserved");
        // The backup keeps the mode `fs::copy` gave it; it is not the file the
        // server serves.
        assert_eq!(mode_of(&dir.path().join("channels.yml.bak")), 0o640);
    }

    #[cfg(unix)]
    #[test]
    fn a_new_file_is_created_with_mode_600() {
        let dir = fixture();
        let path = dir.path().join(CHANNELS_FILE);
        fs::remove_file(&path).expect("remove");
        save(dir.path(), CHANNELS_FILE, "[]\n").expect("save");
        assert_eq!(mode_of(&path), 0o600, "a new file must be private");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_saves_of_the_same_file_serialise() {
        let dir = fixture();
        let path = dir.path().join(CHANNELS_FILE);

        // Hold the file lock, then start a save: it must wait for the guard.
        let guard = lock_async(CHANNELS_FILE).await;
        let waiter = tokio::spawn(save_blocking_async(
            dir.path().to_path_buf(),
            CHANNELS_FILE,
            "- name: B\n  type: GR\n  channel: \"2\"\n".to_string(),
        ));
        // A save of a *different* file is not blocked by the guard.
        save_blocking_async(
            dir.path().to_path_buf(),
            TUNERS_FILE,
            "- name: t0\n  types: [GR]\n  command: \"cat {channel}\"\n".to_string(),
        )
        .await
        .expect("the other file is independent");

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "the same-file save must wait");

        drop(guard);
        waiter
            .await
            .expect("join")
            .expect("the queued save succeeds");
        assert!(
            fs::read_to_string(&path).expect("read").contains("name: B"),
            "the queued save won the race"
        );
    }

    #[test]
    fn the_scratch_directory_is_private_and_exclusive() {
        let scratch = scratch_dir().expect("scratch dir");
        let metadata = fs::metadata(&scratch).expect("metadata");
        assert!(metadata.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                metadata.permissions().mode() & 0o777,
                0o700,
                "the staged config must not be world-readable"
            );
        }
        // `create_dir` (not `create_dir_all`) fails when the path exists, so a
        // pre-seeded directory cannot be reused.
        assert!(fs::DirBuilder::new().create(&scratch).is_err());
        let _ = fs::remove_dir_all(&scratch);
    }
}
