use super::{
    Mode, Request,
    evidence::{self, Provenance},
    verification,
};
use sqlx::{
    Connection, SqliteConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode},
};
use std::{fs, os::unix::fs::PermissionsExt, path::Path, sync::OnceLock};

pub(super) struct Fixture {
    pub root: tempfile::TempDir,
    pub request: Request,
}

impl Fixture {
    pub async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("state.db");
        let mut connection = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&database)
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await
        .unwrap();
        verification::embedded_through(18)
            .run(&mut connection)
            .await
            .unwrap();
        connection.close().await.unwrap();
        let request = Self::request(root.path(), Mode::Apply);
        fs::create_dir(&request.manifest_dir).unwrap();
        fs::set_permissions(&request.manifest_dir, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(
            &request.config,
            format!(
                "[bridge]\ndatabase = {:?}\nsession_root = {:?}\n",
                database,
                root.path().join("sessions")
            ),
        )
        .unwrap();
        let mut record = provenance();
        record.launcher_hold.config = request.config.clone();
        fs::write(
            &request.release_provenance,
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        fs::set_permissions(
            &request.release_provenance,
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        Self { root, request }
    }

    pub fn request(root: &Path, mode: Mode) -> Request {
        Request {
            mode,
            config: root.join("config.toml"),
            database: root.join("state.db"),
            operation_id: "fixture-operation".into(),
            manifest_dir: root.join("manifest"),
            release_provenance: root.join("release.json"),
            executable: std::env::current_exe().unwrap(),
        }
    }

    pub fn mode(&self, mode: Mode) -> Request {
        Request {
            mode,
            ..self.request.clone()
        }
    }
}

pub(super) fn provenance() -> Provenance {
    static RECORD: OnceLock<Provenance> = OnceLock::new();
    RECORD
        .get_or_init(|| {
            let executable = std::env::current_exe().unwrap();
            let mut file = fs::File::open(&executable).unwrap();
            let mut elf = [0u8; 64];
            std::io::Read::read_exact(&mut file, &mut elf).unwrap();
            Provenance {
                source_commit: "361409cfef3c60a4e1cf804dace7289cb7d05db7".into(),
                package: "guigu-agent-bridge".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                publication: "fixture-reviewed-release".into(),
                review: "fixture-review".into(),
                package_sha256: "a".repeat(64),
                executable_sha256: evidence::hash_file(&executable).unwrap(),
                launcher_hold: evidence::LauncherHold {
                    operation_id: "fixture-operation".into(),
                    controller: "fixture-controller".into(),
                    prior_state: "inactive/dead/MainPID=0".into(),
                    prior_show_sha256: "b".repeat(64),
                    executable: executable.clone(),
                    config: "fixture".into(),
                    manual_launchers_excluded: true,
                },
                executable,
                elf_sha256: evidence::digest(&elf),
                migration_set_sha256: evidence::migration_digest(),
            }
        })
        .clone()
}
