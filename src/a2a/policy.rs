use std::net::{IpAddr, SocketAddr};

use url::Url;

use super::A2aError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenPolicy {
    pub allow_private_plaintext: bool,
}

pub fn validate_listener(address: SocketAddr, policy: ListenPolicy) -> Result<(), A2aError> {
    let ip = address.ip();
    if ip.is_unspecified() || ip.is_multicast() {
        return Err(A2aError::Config("wildcard or multicast listener rejected"));
    }
    if ip.is_loopback() {
        return Ok(());
    }
    if policy.allow_private_plaintext && is_private_or_link_local(ip) {
        tracing::warn!(
            "A2A private-LAN listener uses plaintext; bearer credentials and content are visible on the LAN"
        );
        return Ok(());
    }
    Err(A2aError::Config(
        "listener must be loopback or explicitly allowed private IP",
    ))
}

pub fn validate_peer_url(url: &Url, resolved: &[SocketAddr]) -> Result<(), A2aError> {
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(A2aError::Config(
            "peer URL cannot contain credentials, query or fragment",
        ));
    }
    if resolved.is_empty() {
        return Err(A2aError::Config("peer DNS returned no addresses"));
    }
    match url.scheme() {
        "http" if resolved.iter().all(|addr| addr.ip().is_loopback()) => Ok(()),
        "https" if resolved.iter().all(|addr| allowed_outbound(addr.ip())) => Ok(()),
        "http" => Err(A2aError::Config(
            "plaintext peer must resolve only to loopback",
        )),
        _ => Err(A2aError::Config("peer URL must use http or https")),
    }
}

fn allowed_outbound(ip: IpAddr) -> bool {
    ip.is_loopback() || is_private_or_link_local(ip)
}

fn is_private_or_link_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_private() || ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_unicast_link_local() || (ip.segments()[0] & 0xfe00) == 0xfc00,
    }
}
