#[tokio::main]
async fn main() -> std::process::ExitCode {
    match guigu_agent_bridge::run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(guigu_agent_bridge::Error::Cli(failure)) => {
            println!("{} result={}", failure.command, failure.category);
            std::process::ExitCode::from(failure.status)
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
