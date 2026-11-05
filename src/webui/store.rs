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

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

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
    /// Takes the lock belonging to `file` (one of the `*_FILE` constants).
    pub fn lock(&self, file: &str) -> MutexGuard<'_, ()> {
        let mutex = match file {
            TUNERS_FILE => &self.tuners,
            CHANNELS_FILE => &self.channels,
            PIPELINES_FILE => &self.pipelines,
            other => unreachable!("{other:?} is not an editable config file"),
        };
        // The critical section only touches the filesystem, so a panic inside
        // it leaves no in-memory state that a poisoned lock would protect.
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The process-wide locks. One instance, so every router built in this process
/// serialises against the same mutexes.
static LOCKS: std::sync::OnceLock<FileLocks> = std::sync::OnceLock::new();

/// Locks `file` for the duration of its save sequence.
pub fn lock(file: &str) -> MutexGuard<'static, ()> {
    LOCKS.get_or_init(FileLocks::default).lock(file)
}

/// Validates `yaml` as the new content of `file` and, when the whole
/// configuration is valid, writes it to `dir`.
pub fn save(dir: &Path, file: &'static str, yaml: &str) -> Result<(), SaveError> {
    validate_document(dir, file, yaml)?;
    write_atomic(&dir.join(file), yaml).map_err(SaveError::Internal)
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
pub fn write_atomic(path: &Path, contents: &str) -> Result<(), String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("{} has no file name", path.display()))?;
    let tmp = path.with_file_name(format!("{name}.tmp"));
    let bak = path.with_file_name(format!("{name}.bak"));

    // SPEC §10 order: back up the previous file, then write the replacement.
    if path.exists() {
        fs::copy(path, &bak)
            .map_err(|error| format!("failed to back up {}: {error}", path.display()))?;
    }

    let written = (|| -> std::io::Result<()> {
        let mut file = File::create(&tmp)?;
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
