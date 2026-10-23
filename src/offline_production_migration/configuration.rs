use super::{Failure, Request};
use serde::Deserialize;
use std::{fs, net::SocketAddr, path::PathBuf};

#[derive(Deserialize)]
struct Configuration {
    bridge: Bridge,
    #[serde(default)]
    transports: Transports,
}

#[derive(Deserialize)]
struct Bridge {
    database: PathBuf,
    #[serde(default)]
    health_bind: String,
}

#[derive(Default, Deserialize)]
struct Transports {
    #[serde(default)]
    a2a: A2a,
}

#[derive(Default, Deserialize)]
struct A2a {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    listen: String,
}

pub(super) fn listeners(req: &Request) -> Result<Vec<SocketAddr>, Failure> {
    let text = fs::read_to_string(&req.config).map_err(|_| Failure::Preflight)?;
    let config: Configuration = toml::from_str(&text).map_err(|_| Failure::Preflight)?;
    let database = if let Ok(rest) = config.bridge.database.strip_prefix("~") {
        PathBuf::from(std::env::var_os("HOME").ok_or(Failure::Preflight)?).join(rest)
    } else {
        config.bridge.database
    };
    if database != req.database {
        return Err(Failure::Preflight);
    }
    let mut addresses = Vec::new();
    if !config.bridge.health_bind.is_empty() {
        addresses.push(
            config
                .bridge
                .health_bind
                .parse()
                .map_err(|_| Failure::Preflight)?,
        );
    }
    if config.transports.a2a.enabled {
        let listen = config.transports.a2a.listen;
        if let Some(path) = listen.strip_prefix("unix:") {
            if std::path::Path::new(path).exists() {
                return Err(Failure::Busy);
            }
        } else {
            addresses.push(
                listen
                    .strip_prefix("tcp:")
                    .unwrap_or(&listen)
                    .parse()
                    .map_err(|_| Failure::Preflight)?,
            );
        }
    }
    Ok(addresses)
}
