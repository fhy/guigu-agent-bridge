use std::{fs, io::Write, os::unix::fs::OpenOptionsExt, path::Path};

use guigu_agent_bridge::storage;
use matrix_sdk::Client;
use serde_json::{Value, json};

const USER: &str = "@bridge-restart-test-20261105:matrix.0x8.xyz";
const SERVER: &str = "https://matrix.0x8.xyz";

fn private_file(path: &Path, bytes: &[u8]) -> Result<(), &'static str> {
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| "private-file-create")?;
    output
        .write_all(bytes)
        .and_then(|_| output.sync_all())
        .map_err(|_| "private-file-persist")
}

fn configured(template: &str, root: &Path) -> Result<String, &'static str> {
    let mut config: toml::Value = toml::from_str(template).map_err(|_| "template-parse")?;
    if config
        .get("agents")
        .and_then(|agents| agents.get("test"))
        .is_none()
    {
        return Err("test-agent-template-required");
    }
    for (section, field, relative) in [
        ("bridge", "database", "state/state.db"),
        ("bridge", "session_root", "state/sessions"),
    ] {
        config[section][field] = toml::Value::String(root.join(relative).display().to_string());
    }
    config["bridge"]["health_bind"] = "127.0.0.1:19403".into();
    config["transports"]["matrix"]["crypto_store_path"] =
        root.join("state/crypto").display().to_string().into();
    config["transports"]["matrix"]["allowed_users"] = toml::Value::Array(vec![
        "@fhy:matrix.0x8.xyz".into(),
        "@bridge-staging:matrix.0x8.xyz".into(),
    ]);
    config["agents"]["test"]["workspace"] =
        root.join("state/workspace").display().to_string().into();
    toml::to_string(&config).map_err(|_| "config-serialize")
}

async fn initialize_native(root: &Path) -> Result<(), &'static str> {
    let pool = storage::connect(root.join("state/state.db"))
        .await
        .map_err(|_| "database-create")?;
    storage::migrate(&pool)
        .await
        .map_err(|_| "database-migrate")?;
    let gate = storage::MatrixCompatibilityStore::new(pool.clone())
        .gate(true, Some(&root.join("state/crypto")))
        .await
        .map_err(|_| "native-bootstrap")?;
    pool.close().await;
    if !matches!(gate, storage::CompatibilityGate::Native(_)) {
        return Err("native-bootstrap-refused-no-login");
    }
    Ok(())
}

async fn run() -> Result<(), &'static str> {
    let root = std::env::var_os("R107_FRESH_ROOT").ok_or("root-missing")?;
    let root = Path::new(&root);
    if !root.is_absolute() || root.parent() != Some(Path::new("/home/fhy/r107-repair")) {
        return Err("root-boundary");
    }
    let password_path = std::env::var_os("R107_PASSWORD_FILE").ok_or("password-path-missing")?;
    let template_path = std::env::var_os("R107_CONFIG_TEMPLATE").ok_or("template-path-missing")?;
    let agent_path = std::env::var_os("R107_AGENT_ENV").ok_or("agent-path-missing")?;
    let password: Value =
        serde_json::from_slice(&fs::read(password_path).map_err(|_| "password-read")?)
            .map_err(|_| "password-json")?;
    if password["user_id"] != USER {
        return Err("password-identity");
    }
    let secret = password["password"].as_str().ok_or("password-field")?;
    let template = fs::read_to_string(template_path).map_err(|_| "template-read")?;
    let config = configured(&template, root)?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root)
        .map_err(|_| "existing-root-refused")?;
    for directory in ["config", "state", "state/sessions", "state/workspace"] {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join(directory))
            .map_err(|_| "directory-create")?;
    }
    private_file(&root.join("config/config.toml"), config.as_bytes())?;
    private_file(
        &root.join("config/agent.env"),
        &fs::read(agent_path).map_err(|_| "agent-read")?,
    )?;
    initialize_native(root).await?;
    private_file(
        &root.join("login-attempted"),
        b"single SDK login; never retry this root\n",
    )?;
    let client = Client::builder()
        .homeserver_url(SERVER)
        .request_config(matrix_sdk::config::RequestConfig::new().retry_limit(0))
        .sqlite_store(root.join("state/crypto"), None)
        .build()
        .await
        .map_err(|_| "client-build")?;
    client
        .matrix_auth()
        .login_username(USER, secret)
        .initial_device_display_name("R107 fresh Bridge acceptance")
        .send()
        .await
        .map_err(|_| "login-failed-or-ambiguous-no-retry")?;
    let identity = client.whoami().await.map_err(|_| "whoami")?;
    let session = client.matrix_auth().session().ok_or("session-missing")?;
    if identity.user_id.as_str() != USER
        || identity.device_id.as_ref() != Some(&session.meta.device_id)
        || identity.is_guest
    {
        return Err("online-identity-mismatch");
    }
    let session_value = json!({"user_id": session.meta.user_id,
        "device_id": session.meta.device_id, "access_token": session.tokens.access_token,
        "home_server": SERVER});
    private_file(
        &root.join("config/session.json"),
        session_value.to_string().as_bytes(),
    )?;
    let environment = format!(
        "MATRIX_USER_ID={}\nMATRIX_DEVICE_ID={}\nMATRIX_ACCESS_TOKEN={}\n",
        session.meta.user_id, session.meta.device_id, session.tokens.access_token
    );
    private_file(&root.join("config/matrix.env"), environment.as_bytes())?;
    client
        .encryption()
        .wait_for_e2ee_initialization_tasks()
        .await;
    private_file(
        &root.join("state/crypto/identity-v1.json"),
        json!({"version":1,"homeserver_origin":"https://matrix.0x8.xyz:443",
            "user_id":identity.user_id,"device_id":identity.device_id})
        .to_string()
        .as_bytes(),
    )?;
    for entry in fs::read_dir(root.join("state/crypto")).map_err(|_| "store-list")? {
        let entry = entry.map_err(|_| "store-entry")?;
        if entry.file_type().map_err(|_| "store-type")?.is_file() {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o600))
                .map_err(|_| "store-permissions")?;
        }
    }
    println!(
        "{}",
        json!({"paired": true, "root":root, "user":identity.user_id,
        "device":identity.device_id, "crypto_initialized": true,
        "next":"Bridge validates the newly issued device and persistent store"})
    );
    Ok(())
}

use std::os::unix::fs::DirBuilderExt;

#[tokio::main]
async fn main() {
    if let Err(category) = run().await {
        eprintln!("fresh-pair result=refused category={category}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_replaces_every_runtime_path() {
        let template =
            include_str!("../config.example.toml").replace("agents.example", "agents.test");
        let root = Path::new("/home/fhy/r107-repair/new-test-root");
        let actual: toml::Value = toml::from_str(&configured(&template, root).unwrap()).unwrap();
        assert_eq!(
            actual["bridge"]["database"].as_str(),
            Some("/home/fhy/r107-repair/new-test-root/state/state.db")
        );
        assert_eq!(
            actual["bridge"]["session_root"].as_str(),
            Some("/home/fhy/r107-repair/new-test-root/state/sessions")
        );
        assert_eq!(
            actual["transports"]["matrix"]["crypto_store_path"].as_str(),
            Some("/home/fhy/r107-repair/new-test-root/state/crypto")
        );
        assert_eq!(
            actual["agents"]["test"]["workspace"].as_str(),
            Some("/home/fhy/r107-repair/new-test-root/state/workspace")
        );
    }

    #[test]
    fn missing_test_agent_is_rejected() {
        assert_eq!(
            configured(
                include_str!("../config.example.toml"),
                Path::new("/new-root")
            ),
            Err("test-agent-template-required")
        );
    }

    #[test]
    fn existing_private_output_is_never_overwritten() {
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("session.json");
        private_file(&output, b"retained").unwrap();
        assert_eq!(
            private_file(&output, b"replacement"),
            Err("private-file-create")
        );
        assert_eq!(fs::read(output).unwrap(), b"retained");
    }

    #[tokio::test]
    async fn native_bootstrap_precedes_crypto_creation() {
        let root = tempfile::tempdir_in(std::env::var_os("HOME").unwrap()).unwrap();
        fs::create_dir(root.path().join("state")).unwrap();
        initialize_native(root.path()).await.unwrap();
        assert!(!root.path().join("state/crypto").exists());
    }

    #[tokio::test]
    async fn nonempty_store_is_refused_without_login() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir_in(std::env::var_os("HOME").unwrap()).unwrap();
        fs::create_dir_all(root.path().join("state/crypto")).unwrap();
        fs::set_permissions(
            root.path().join("state/crypto"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        private_file(&root.path().join("state/crypto/retained"), b"retained").unwrap();
        assert_eq!(
            initialize_native(root.path()).await,
            Err("native-bootstrap-refused-no-login")
        );
        assert_eq!(
            fs::read(root.path().join("state/crypto/retained")).unwrap(),
            b"retained"
        );
    }
}
