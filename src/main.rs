#[tokio::main]
async fn main() -> std::process::ExitCode {
    match guigu_agent_bridge::run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(guigu_agent_bridge::Error::Cli(failure)) => {
            if failure.command == "reconcile-lease-owner"
                || failure.command == "historical-terminal-cleanup"
                || failure.command == "reconcile-orphaned-preacceptance"
                || failure.command == "reconcile-isolated-legacy-owner"
                || failure.command == "prepare-isolated-legacy-owner-manifest"
                || failure.command == "prepare-isolated-legacy-fixture-migration-manifest"
                || failure.command == "migrate-isolated-legacy-fixture"
                || failure.command == "backup-isolated-t050-state"
                || failure.command == "verify-isolated-t050-backup"
                || failure.command == "restore-isolated-t050-state"
                || failure.command == "cleanup-isolated-t050-backup"
            {
                if failure.command.ends_with("t050-state")
                    || failure.command == "verify-isolated-t050-backup"
                    || failure.command == "cleanup-isolated-t050-backup"
                {
                    println!(
                        "{} result=rejected status={} cause={}",
                        failure.command, failure.status, failure.category
                    );
                } else {
                    println!(
                        "{} result={} status={}",
                        failure.command, failure.category, failure.status
                    );
                }
                return std::process::ExitCode::from(failure.status);
            }
            println!("{} result={}", failure.command, failure.category);
            std::process::ExitCode::from(failure.status)
        }
        Err(guigu_agent_bridge::Error::OwnerBinding(failure)) => {
            println!(
                "remediate-isolated-owner-bindings result=rejected status=3 cause={} substage={}",
                failure.cause, failure.substage
            );
            std::process::ExitCode::from(3)
        }
        Err(guigu_agent_bridge::Error::Diagnostic(causes)) => {
            let public = causes.public_causes();
            println!(
                "diagnose-preacceptance-conflict result=conflicting causes={} count={}",
                public.join(","),
                public.len()
            );
            std::process::ExitCode::from(6)
        }
        Err(error) => {
            eprintln!("Error: {error:?}");
            std::process::ExitCode::from(1)
        }
    }
}
