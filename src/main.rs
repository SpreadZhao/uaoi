mod agent;
mod bindings;
mod broker;
mod config;
mod im;
mod model;
mod service;
mod store;
mod wrapper;

use anyhow::{Context, Result};
use clap::{CommandFactory, Parser, Subcommand};
use config::Config;
use serde_json::json;
use std::{ffi::OsString, path::PathBuf, sync::Arc};

#[derive(Parser)]
#[command(
    version,
    about = "Run a configured agent in its terminal and Telegram together."
)]
struct Cli {
    /// Path to the uaoi configuration.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Launch this configured agent instance.
    #[arg(long)]
    agent: Option<String>,
    /// Native agent arguments, passed unchanged after --.
    #[arg(last = true, requires = "agent")]
    args: Vec<OsString>,
    #[command(subcommand)]
    command: Option<Action>,
}
#[derive(Subcommand)]
enum Action {
    /// Show running profiles and their terminal counts.
    Status,
    /// Validate the uaoi configuration.
    Check,
    /// Edit all topic/session bindings in VISUAL or EDITOR.
    Bindings,
    /// Inspect operations whose outcome is unknown.
    Recovery,
    #[command(name = "__broker", hide = true)]
    Broker,
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "uaoi=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    anyhow::ensure!(
        cli.agent.is_none() || cli.command.is_none(),
        "--agent cannot be combined with a management command"
    );
    if cli.agent.is_none() && cli.command.is_none() {
        Cli::command().print_help()?;
        anyhow::bail!("Select an agent with --agent NAME");
    }
    let path = std::fs::canonicalize(cli.config.unwrap_or_else(Config::default_path))
        .context("Locate uaoi configuration")?;
    let cfg = Arc::new(Config::load(&path)?);
    match cli.command {
        None => {
            let code = wrapper::run(&cfg, &path, cli.agent.as_deref(), cli.args).await?;
            std::process::exit(code);
        }
        Some(Action::Check) => {
            println!("Configuration valid: {} agent instances", cfg.agents.len())
        }
        Some(Action::Broker) => broker::run(cfg, path).await?,
        Some(Action::Status) => match wrapper::connect(&cfg).await {
            Ok(mut io) => println!(
                "{}",
                serde_json::to_string_pretty(
                    &wrapper::request(
                        &mut io,
                        json!({"method":"status", "fingerprint":cfg.fingerprint()})
                    )
                    .await?
                )?
            ),
            Err(_) => println!("uaoi stopped"),
        },
        Some(Action::Bindings) => bindings::edit(&cfg).await?,
        Some(Action::Recovery) => {
            let mut io = wrapper::connect(&cfg).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &wrapper::request(
                        &mut io,
                        json!({"method":"recovery", "fingerprint":cfg.fingerprint()})
                    )
                    .await?
                )?
            );
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod cli_tests {
    use super::*;
    #[test]
    fn agent_launch_and_native_arguments_are_unambiguous() {
        let cli = Cli::try_parse_from(["uaoi", "--agent", "work"]).unwrap();
        assert_eq!(cli.agent.as_deref(), Some("work"));
        assert!(cli.command.is_none() && cli.args.is_empty());
        let cli = Cli::try_parse_from([
            "uaoi",
            "--config",
            "/uaoi.toml",
            "--agent",
            "personal",
            "--",
            "--config",
            "model=example",
            "-C",
            "/tmp",
            "resume",
            "--help",
        ])
        .unwrap();
        assert_eq!(
            cli.args,
            [
                "--config",
                "model=example",
                "-C",
                "/tmp",
                "resume",
                "--help"
            ]
            .map(OsString::from)
        );
        assert!(Cli::try_parse_from(["uaoi", "--", "resume"]).is_err());
        assert!(Cli::try_parse_from(["uaoi", "--agent", "work", "codex"]).is_err());
        assert!(Cli::try_parse_from(["uaoi", "--agent", "work", "-C", "/tmp"]).is_err());
    }
}
