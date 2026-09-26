#[tokio::main]
async fn main() -> std::process::ExitCode {
    match guigu_agent_bridge::run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(guigu_agent_bridge::Error::Cli(failure)) => {
            println!("{} result={}", failure.command, failure.category);
            std::process::ExitCode::from(failure.status)
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::from(1)
        }
    }
}
