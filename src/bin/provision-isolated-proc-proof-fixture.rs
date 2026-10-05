use std::path::PathBuf;

use guigu_agent_bridge::privileged_proc_proof::{
    FixturePaths, provision_temp_fixture, teardown_temp_fixture,
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
