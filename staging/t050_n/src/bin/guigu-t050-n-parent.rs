use guigu_t050_n::{Result, contract::Leaf, secret_channel};

fn setup() -> Result<guigu_t050_n::contract::NamespaceSpec> {
    let spec = secret_channel::worker_spec(&std::env::args().collect::<Vec<_>>())?;
    let (channel, provider, owner) = secret_channel::worker_channels()?;
    secret_channel::install_worker_secret(channel, provider, owner, &spec)?;
    unsafe {
        std::env::set_var("HOME", spec.root());
        std::env::set_var("XDG_STATE_HOME", spec.path(Leaf::State));
        std::env::set_var("XDG_CACHE_HOME", spec.path(Leaf::State));
    }
    Ok(spec)
}
fn main() {
    let result =
        setup().and_then(|spec| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| guigu_t050_n::Refusal::Unsupported)?;
            runtime.block_on(async {
                guigu_agent_bridge::run_with_shutdown(spec.path(Leaf::Config), async {
                use tokio::signal::unix::{SignalKind, signal};
                let mut interrupt = signal(SignalKind::interrupt())?;
                let mut terminate = signal(SignalKind::terminate())?;
                tokio::select! { _ = interrupt.recv() => Ok(()), _ = terminate.recv() => Ok(()) }
            }).await.map_err(|_| guigu_t050_n::Refusal::Nonzero)
            })
        });
    if let Err(error) = result {
        eprintln!("t050-n-parent category={error}");
        std::process::exit(3);
    }
}
