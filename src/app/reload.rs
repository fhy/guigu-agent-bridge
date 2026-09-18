use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use tokio::sync::Mutex;

use crate::{
    bus::EndpointRegistry,
    config::{self, Config, ConfigError},
    matrix::{AdminPermissionPolicy, PermissionPolicy, RoutePolicy},
};

pub trait ConfigSource: Send + Sync {
    fn load(&self) -> Result<Config, ConfigError>;
}

pub struct FileConfigSource {
    path: PathBuf,
}

impl FileConfigSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl ConfigSource for FileConfigSource {
    fn load(&self) -> Result<Config, ConfigError> {
        config::load(&self.path)
    }
}

#[derive(Clone)]
pub struct HotConfig {
    pub route: RoutePolicy,
    pub permissions: PermissionPolicy,
    pub admin: AdminPermissionPolicy,
    pub monitor_room: Option<String>,
}

impl HotConfig {
    fn build(config: &Config, registry: &EndpointRegistry) -> Result<Self, ReloadError> {
        let matrix = &config.transports.matrix;
        let mut route = RoutePolicy::new();
        for (alias, agent) in &matrix.routes.aliases {
            validate_target(registry, agent)?;
            route = route.alias(alias, agent);
        }
        for (room, agent) in &matrix.routes.rooms {
            validate_target(registry, agent)?;
            route = route.bind_room(room, agent);
        }
        for (room, agents) in &matrix.routes.direct {
            for agent in agents {
                validate_target(registry, agent)?;
            }
            route = route.direct_room(room, agents.iter().map(String::as_str));
        }
        let mut permissions = PermissionPolicy::new();
        for user in &matrix.allowed_users {
            permissions = permissions.allow_user(user);
        }
        let mut admin = AdminPermissionPolicy::new();
        for user in &matrix.admin_users {
            admin = admin.allow_user(user);
        }
        admin = admin.restrict_rooms(matrix.admin_rooms.iter().map(String::as_str));
        Ok(Self {
            route,
            permissions,
            admin,
            monitor_room: (!matrix.monitor_room.is_empty()).then(|| matrix.monitor_room.clone()),
        })
    }
}

fn validate_target(registry: &EndpointRegistry, agent: &str) -> Result<(), ReloadError> {
    let Some(endpoint) = registry.get_by_agent_id(agent) else {
        return Err(ReloadError::Policy);
    };
    if !endpoint.is_enabled() || !endpoint.is_addressable() {
        return Err(ReloadError::Policy);
    }
    Ok(())
}

#[derive(Clone)]
pub struct ActiveSnapshot {
    pub generation: u64,
    pub hot: Arc<HotConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadOutcome {
    Applied { generation: u64 },
    RestartRequired,
}

#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    #[error("configuration reload failed")]
    Config(#[from] ConfigError),
    #[error("configuration policy is invalid")]
    Policy,
    #[error("configuration reload is stopping")]
    Stopping,
}

pub struct ReloadController {
    source: Arc<dyn ConfigSource>,
    active_config: RwLock<Config>,
    active: RwLock<Arc<ActiveSnapshot>>,
    serial: Mutex<()>,
    stopping: std::sync::atomic::AtomicBool,
}

impl ReloadController {
    pub fn new(config: Config, source: Arc<dyn ConfigSource>) -> Result<Self, ReloadError> {
        let registry = EndpointRegistry::from_config(&config);
        let hot = HotConfig::build(&config, &registry)?;
        Ok(Self {
            source,
            active_config: RwLock::new(config),
            active: RwLock::new(Arc::new(ActiveSnapshot {
                generation: 1,
                hot: Arc::new(hot),
            })),
            serial: Mutex::new(()),
            stopping: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn snapshot(&self) -> Arc<ActiveSnapshot> {
        self.active
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub async fn stop(&self) {
        let _serial = self.serial.lock().await;
        self.stopping
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub async fn reload(&self) -> Result<ReloadOutcome, ReloadError> {
        let _serial = self.serial.lock().await;
        if self.stopping.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ReloadError::Stopping);
        }
        let candidate = self.source.load()?;
        let current = self
            .active_config
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if restart_projection(&candidate) != restart_projection(&current) {
            return Ok(ReloadOutcome::RestartRequired);
        }
        let registry = EndpointRegistry::from_config(&current);
        let hot = HotConfig::build(&candidate, &registry)?;
        let generation = self.snapshot().generation.saturating_add(1);
        if self.stopping.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ReloadError::Stopping);
        }
        *self.active.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(ActiveSnapshot {
            generation,
            hot: Arc::new(hot),
        });
        *self
            .active_config
            .write()
            .unwrap_or_else(|p| p.into_inner()) = candidate;
        Ok(ReloadOutcome::Applied { generation })
    }
}

#[derive(PartialEq, Eq)]
struct RestartProjection {
    bridge: crate::config::BridgeConfig,
    enabled: bool,
    homeserver: String,
    user_id: String,
    access_token: crate::config::SecretString,
    sync_capacity: usize,
    runtime: crate::config::RuntimeConfig,
    agents: BTreeMap<String, crate::config::AgentEndpointConfig>,
}

fn restart_projection(config: &Config) -> RestartProjection {
    let matrix = &config.transports.matrix;
    RestartProjection {
        bridge: config.bridge.clone(),
        enabled: matrix.enabled,
        homeserver: matrix.homeserver.clone(),
        user_id: matrix.user_id.clone(),
        access_token: matrix.access_token.clone(),
        sync_capacity: matrix.sync_capacity,
        runtime: config.runtime.clone(),
        agents: config.agents.clone(),
    }
}
