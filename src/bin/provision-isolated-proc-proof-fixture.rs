use std::path::PathBuf;

use guigu_agent_bridge::privileged_proc_proof::{
    FixturePaths, provision_temp_fixture, stage_policy, teardown_temp_fixture,
};

fn usage() -> ! {
    eprintln!(
        "usage: provision-isolated-proc-proof-fixture provision --root ROOT --parent P --helper H --config C --database-root D [--generation N] | teardown --root ROOT"
    );
    std::process::exit(2)
}

fn value(args: &mut impl Iterator<Item = String>, flag: &str) -> PathBuf {
    match args.next() {
        Some(value) if !value.starts_with('-') => PathBuf::from(value),
        _ => {
            eprintln!("missing {flag}");
            usage()
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("stage-policy") => {
            let mut parent = None;
            let mut helper = None;
            let mut config = None;
            let mut database_root = None;
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--parent" => parent = Some(value(&mut args, "--parent")),
                    "--helper" => helper = Some(value(&mut args, "--helper")),
                    "--config" => config = Some(value(&mut args, "--config")),
                    "--database-root" => database_root = Some(value(&mut args, "--database-root")),
                    _ => usage(),
                }
            }
            let (Some(parent), Some(helper), Some(config), Some(database_root)) =
                (parent, helper, config, database_root)
            else {
                usage()
            };
            let paths = FixturePaths {
                root: PathBuf::new(),
                parent,
                helper,
                config,
                database_root,
            };
            match stage_policy(&paths, 3) {
                Ok(_) => eprintln!("stage-policy status=0 cause=none"),
                Err(cause) => {
                    eprintln!("stage-policy status=3 cause={}", cause.code());
                    std::process::exit(3);
                }
            }
        }
        Some("production-provision") => {
            let parent = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--parent-sha" => value,
                _ => None,
            };
            let helper = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--helper-sha" => value,
                _ => None,
            };
            let policy = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--policy-sha" => value,
                _ => None,
            };
            if args.next().is_some() {
                std::process::exit(3);
            }
            let (Some(parent), Some(helper), Some(policy)) = (parent, helper, policy) else {
                std::process::exit(3)
            };
            if guigu_agent_bridge::privileged_proc_proof::provision_production(
                &parent, &helper, &policy, 3,
            )
            .is_err()
            {
                std::process::exit(3);
            }
            println!("production provisioning complete");
        }
        Some("production-teardown") => {
            let parent = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--parent-sha" => value,
                _ => None,
            };
            let helper = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--helper-sha" => value,
                _ => None,
            };
            let policy = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--policy-sha" => value,
                _ => None,
            };
            let generation = match (args.next(), args.next()) {
                (Some(flag), value) if flag == "--generation" => value.and_then(|v| v.parse().ok()),
                _ => None,
            };
            if args.next().is_some()
                || parent.is_none()
                || helper.is_none()
                || policy.is_none()
                || generation.is_none()
                || guigu_agent_bridge::privileged_proc_proof::teardown_production(
                    &parent.unwrap(),
                    &helper.unwrap(),
                    &policy.unwrap(),
                    generation.unwrap(),
                )
                .is_err()
            {
                std::process::exit(3);
            }
            println!("production teardown complete");
        }
        Some("provision") => {
            let mut root = None;
            let mut parent = None;
            let mut helper = None;
            let mut config = None;
            let mut database_root = None;
            let mut generation = 1u64;
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--root" => root = Some(value(&mut args, "--root")),
                    "--parent" => parent = Some(value(&mut args, "--parent")),
                    "--helper" => helper = Some(value(&mut args, "--helper")),
                    "--config" => config = Some(value(&mut args, "--config")),
                    "--database-root" => database_root = Some(value(&mut args, "--database-root")),
                    "--generation" => {
                        generation = args.next().and_then(|v| v.parse().ok()).unwrap_or(0)
                    }
                    _ => usage(),
                }
            }
            let (Some(root), Some(parent), Some(helper), Some(config), Some(database_root)) =
                (root, parent, helper, config, database_root)
            else {
                usage()
            };
            let paths = FixturePaths {
                root,
                parent,
                helper,
                config,
                database_root,
            };
            if provision_temp_fixture(&paths, generation).is_err() {
                std::process::exit(3);
            }
            println!(
                "provisioned manifest-last fixture root={}",
                paths.root.display()
            );
        }
        Some("teardown") => {
            let root = match args.next().as_deref() {
                Some("--root") => args.next().map(PathBuf::from),
                _ => None,
            };
            let Some(root) = root else { usage() };
            if teardown_temp_fixture(&root).is_err() {
                std::process::exit(3);
            }
            println!("teardown manifest-first complete");
        }
        _ => usage(),
    }
}
