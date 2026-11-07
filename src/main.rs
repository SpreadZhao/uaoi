mod agent;
mod bindings;
mod config;
mod im;
mod model;
mod service;
mod store;

mod wrapper;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::Config;
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};
use tokio_util::sync::CancellationToken;
use wrapper::private_dir;

#[derive(Parser)]
#[command(version, about = "Use Codex from its terminal and Telegram together.")]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,
    /// Agent instance required when starting the Codex CLI.
    #[arg(long)]
    agent: Option<String>,
    #[command(subcommand)]
    command: Action,
}
#[derive(Subcommand)]
enum Action {
    /// Run the official Codex CLI with Telegram access.
    #[command(disable_help_flag = true)]
    Codex {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<std::ffi::OsString>,
    },
    /// Start the shared background service.
    Start,
    /// Run the shared service in the foreground.
    Serve,
    /// Show shared service status.
    Status,
    /// Stop the service; --force also interrupts running tasks.
    Stop {
        #[arg(long)]
        force: bool,
    },
    /// Validate the uaoi configuration.
    Check,
    /// Edit all topic/session bindings in VISUAL or EDITOR.
    Bindings,
    /// Inspect operations whose outcome is unknown.
    Recovery,
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
    let path = std::fs::canonicalize(cli.config.unwrap_or_else(Config::default_path))
        .context("Locate uaoi configuration")?;
    let cfg = Arc::new(Config::load(&path)?);
    match cli.command {
        Action::Codex { args } => {
            let code = wrapper::run(&cfg, &path, cli.agent.as_deref(), args).await?;
            std::process::exit(code);
        }
        Action::Check => println!("Configuration valid: {} agent instances", cfg.agents.len()),
        Action::Serve => serve(cfg, path).await?,
        Action::Start => {
            let mut io = wrapper::connect(&cfg, &path).await?;
            wrapper::request(
                &mut io,
                json!({"method":"status","fingerprint":cfg.fingerprint()}),
            )
            .await?;
            println!("uaoi service ready");
        }
        Action::Status | Action::Stop { .. } => {
            let Ok(socket) = UnixStream::connect(cfg.runtime().join("control.sock")).await else {
                println!("uaoi stopped");
                return Ok(());
            };
            let mut io = BufReader::new(socket);
            let result = wrapper::request(
                &mut io,
                match cli.command {
                    Action::Stop { force } => json!({"method":"stop","force":force}),
                    _ => json!({"method":"status"}),
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Action::Bindings => bindings::edit(&cfg, &path).await?,
        Action::Recovery => {
            let mut io = BufReader::new(
                UnixStream::connect(cfg.runtime().join("control.sock"))
                    .await
                    .context("No active uaoi service")?,
            );
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &wrapper::request(&mut io, json!({"method":"recovery"})).await?
                )?
            );
        }
    }
    Ok(())
}
async fn serve(cfg: Arc<Config>, config_path: PathBuf) -> Result<()> {
    private_dir(&cfg.state())?;
    private_dir(&cfg.runtime())?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(cfg.state().join("service.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock)
        .context("Another uaoi instance is using this state directory")?;
    let runtime_lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(cfg.runtime().join("control.lock"))?;
    fs2::FileExt::try_lock_exclusive(&runtime_lock)
        .context("Another uaoi instance is using this runtime directory")?;
    let socket_path = cfg.runtime().join("control.sock");
    anyhow::ensure!(
        socket_path.as_os_str().len() < 100,
        "runtime_dir is too long for Unix sockets"
    );
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }
    let store = store::Store::open(&cfg.state().join("state.sqlite"))?;
    let cancel = CancellationToken::new();
    let (events, _) = tokio::sync::broadcast::channel(4096);
    let agent: Arc<dyn agent::AgentAdapter> = Arc::new(agent::codex::Codex::new(
        cfg.clone(),
        store.clone(),
        events.clone(),
        cancel.clone(),
    ));
    let mut ims: Vec<Arc<dyn im::ImAdapter>> = vec![];
    let mut cursors = Vec::new();
    let mut bots = std::collections::HashSet::new();
    for (name, spec) in &cfg.agents {
        let projects = cfg
            .projects
            .iter()
            .filter(|(_, p)| p.agents.contains(name))
            .map(|(id, p)| (id.clone(), p.telegram.clone()))
            .collect();
        let mut t = im::telegram::Telegram::new(name, &spec.telegram, projects)?;
        t.validate().await?;
        anyhow::ensure!(
            bots.insert(im::ImAdapter::name(&t).to_owned()),
            "Each agent requires a distinct Telegram Bot"
        );
        cursors.push(im::ImAdapter::discard_backlog(&t).await?);
        ims.push(Arc::new(t));
    }
    agent.start().await?;
    let svc = Arc::new(service::Service::new(
        cfg.clone(),
        store,
        agent.clone(),
        ims,
    ));
    let listener = UnixListener::bind(&socket_path)?;
    std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
    svc.initialize().await?;
    let mut workers = tokio::task::JoinSet::new();
    workers.spawn(svc.clone().events(events.subscribe(), cancel.clone()));
    workers.spawn(svc.clone().deliver(cancel.clone()));
    workers.spawn(svc.clone().sync_titles(cancel.clone()));
    workers.spawn(svc.clone().activity(cancel.clone()));
    for (im, cursor) in svc.ims.iter().zip(cursors) {
        workers.spawn(svc.clone().poll(im.clone(), cursor, cancel.clone()));
    }
    tracing::info!(socket=%socket_path.display(),"uaoi service ready");
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut hup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    loop {
        tokio::select! {
            _=cancel.cancelled()=>break,
            _=tokio::signal::ctrl_c()=>break,
            _=term.recv()=>break,
            _=hup.recv()=>break,
            accepted=listener.accept()=>match accepted{
                Ok((socket,_))=>{let svc=svc.clone(); let stop=cancel.clone(); let path=config_path.clone(); workers.spawn(async move{if let Err(error)=handle_control(socket,svc,stop,path).await{tracing::warn!(%error,"Control connection failed");}});},
                Err(error)=>{tracing::error!(%error,"Control listener stopped");break},
            },
            result=workers.join_next(),if !workers.is_empty()=>{if let Some(Err(error))=result{tracing::error!(%error,"Worker failed");break}},
        }
    }
    cancel.cancel();
    agent.shutdown().await;
    if tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while workers.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }
    drop(listener);
    let _ = std::fs::remove_file(socket_path);
    tracing::info!("uaoi stopped");
    Ok(())
}
async fn handle_control(
    socket: UnixStream,
    svc: Arc<service::Service>,
    cancel: CancellationToken,
    path: PathBuf,
) -> Result<()> {
    let mut io = BufReader::new(socket);
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(10), io.read_line(&mut line)).await??;
    let v: Value = serde_json::from_str(&line)?;
    if let Some(fingerprint) = v.get("fingerprint")
        && fingerprint != &serde_json::json!(svc.cfg.fingerprint())
    {
        io.get_mut().write_all(b"{\"error\":\"Service uses another configuration; stop and restart uaoi first\"}\n").await?;
        return Ok(());
    }
    if v["method"] != "open" {
        let result: Result<Value> = match v["method"].as_str() {
            Some("status") => Ok(
                json!({"state":"running","agents":svc.cfg.agents.keys().collect::<Vec<_>>(),"busy":svc.agent.busy().await}),
            ),
            Some("stop") => {
                if svc.agent.busy().await && v["force"] != true {
                    Err(anyhow::anyhow!(
                        "Tasks are running; use uaoi stop --force to interrupt them"
                    ))
                } else {
                    Ok(json!({"state":"stopping"}))
                }
            }
            _ => svc.control(v.clone()).await,
        };
        let should_stop = v["method"] == "stop" && result.is_ok();
        let response = match result {
            Ok(v) => json!({"result":v}),
            Err(e) => json!({"error":e.to_string()}),
        };
        io.get_mut()
            .write_all(format!("{response}\n").as_bytes())
            .await?;
        if should_stop {
            cancel.cancel();
        }
        return Ok(());
    }
    if v["config"].as_str() != path.to_str() || v["fingerprint"] != svc.cfg.fingerprint() {
        io.get_mut().write_all(b"{\"error\":\"Service uses another configuration; stop and restart uaoi first\"}\n").await?;
        return Ok(());
    }
    let cwd = std::fs::canonicalize(v["cwd"].as_str().context("Missing cwd")?)?;
    let agent_name = v["agent"].as_str().context("Missing agent")?;
    anyhow::ensure!(svc.cfg.agents.contains_key(agent_name), "Unknown agent");
    let client = uuid::Uuid::new_v4().to_string();
    let result: Result<()> = async {
        let env = serde_json::from_value(v["env"].clone())?;
        let args=serde_json::from_value::<Vec<std::ffi::OsString>>(v["args"].clone())?;
        let plan = svc.agent.open(&client, agent_name, &cwd, &env, &args).await?;
        io.get_mut()
            .write_all(format!("{}\n", json!({"result":plan})).as_bytes())
            .await?;
        line.clear();
        // A terminal disconnect releases only its gateway, never its threads.
        tokio::select! { r=io.read_line(&mut line)=>{r?;}, _=svc.agent.disconnected(&client)=>{anyhow::bail!("Codex runtime disconnected");} }
        Ok(())
    }
    .await;
    svc.agent.close(&client).await;
    let response = match &result {
        Ok(()) => json!({"result":{}}),
        Err(e) => json!({"error":e.to_string()}),
    };
    let _ = io
        .get_mut()
        .write_all(format!("{response}\n").as_bytes())
        .await;
    result
}

#[cfg(test)]
mod tests;
