#[tokio::main]
async fn main() -> Result<(), guigu_agent_bridge::Error> {
    guigu_agent_bridge::run().await
}
