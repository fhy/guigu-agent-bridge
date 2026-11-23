#[path = "verify_release_manifest/evidence.rs"]
mod evidence;
#[path = "verify_release_manifest/manifest.rs"]
mod manifest;
#[path = "verify_release_manifest/probe.rs"]
mod probe;

use std::{env, process::ExitCode};

fn main() -> ExitCode {
    let args = env::args_os().skip(1).collect::<Vec<_>>();
    let result = if args.first().and_then(|v| v.to_str()) == Some("append-evidence") {
        evidence::run(&args[1..])
    } else {
        manifest::run(&args)
    };
    result.unwrap_or_else(|failure| failure.emit())
}
