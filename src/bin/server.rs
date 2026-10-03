use anyhow::Result;
use clap::Parser;
use pulse::server::{ServerState, router};
use std::net::SocketAddr;

#[derive(Parser)]
#[command(version, about = "Companion streaming measurement server for Pulse")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8787")]
    bind: SocketAddr,
    #[arg(long, default_value = "Local Pulse")]
    name: String,
    #[arg(long, env = "PULSE_SERVER_TOKEN", hide_env_values = true)]
    token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    eprintln!(
        "Pulse server: http://{} · {}",
        listener.local_addr()?,
        args.name
    );
    axum::serve(listener, router(ServerState::new(args.name, args.token)))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
