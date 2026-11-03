mod agent;
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
    /// Validate the uaoi configuration.
    Check,
    /// List known sessions and their live connection state.
    Sessions,
    /// Inspect operations whose outcome is unknown.
    Recovery,
    #[command(hide = true)]
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
    let path = std::fs::canonicalize(cli.config.unwrap_or_else(Config::default_path))
        .context("Locate uaoi configuration")?;
    let cfg = Arc::new(Config::load(&path)?);
    match cli.command {
        Action::Codex { args } => {
            let code = wrapper::run(&cfg, &path, args).await?;
            std::process::exit(code);
        }
        Action::Check => println!("Configuration valid: {} projects", cfg.projects.len()),
        Action::Broker => serve(cfg, path).await?,
        Action::Sessions | Action::Recovery => {
            let method = if matches!(cli.command, Action::Sessions) {
                "sessions"
            } else {
                "recovery"
            };
            let mut io = BufReader::new(
                UnixStream::connect(cfg.runtime().join("control.sock"))
                    .await
                    .context("No active uaoi service")?,
            );
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &wrapper::request(&mut io, json!({"method":method})).await?
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
    if let Some(config) = cfg.im.telegram.clone() {
        let t = im::telegram::Telegram::new(config)?;
        t.validate().await?;
        cursors.push(im::ImAdapter::discard_backlog(&t).await?);
        ims.push(Arc::new(t));
    }
    let svc = Arc::new(service::Service::new(
        cfg.clone(),
        store,
        agent.clone(),
        ims,
    ));
    let listener = UnixListener::bind(&socket_path)?;
    std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
    let clients = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut last_active = tokio::time::Instant::now();
    let mut seen_client = false;
    let mut idle = tokio::time::interval(Duration::from_millis(250));
    let mut workers = tokio::task::JoinSet::new();
    workers.spawn(svc.clone().events(events.subscribe(), cancel.clone()));
    workers.spawn(svc.clone().deliver(cancel.clone()));
    workers.spawn(svc.clone().sync_titles(cancel.clone()));
    for (im, cursor) in svc.ims.iter().zip(cursors) {
        workers.spawn(svc.clone().poll(im.clone(), cursor, cancel.clone()));
    }
    tracing::info!(socket=%socket_path.display(),"uaoi service ready");
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut hup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            _=term.recv()=>break,
            _=hup.recv()=>break,
            _=idle.tick()=>{
                if clients.load(std::sync::atomic::Ordering::SeqCst)>0 { seen_client=true; last_active=tokio::time::Instant::now(); }
                else if last_active.elapsed() > Duration::from_secs(if seen_client {2} else {30}) { break; }
            },
            accepted=listener.accept()=>match accepted{
                Ok((socket,_))=>{let svc=svc.clone(); let clients=clients.clone(); let path=config_path.clone(); workers.spawn(async move{if let Err(error)=handle_control(socket,svc,clients,path).await{tracing::warn!(%error,"Control connection failed");}});},
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
    clients: Arc<std::sync::atomic::AtomicUsize>,
    path: PathBuf,
) -> Result<()> {
    use std::sync::atomic::Ordering;
    let mut io = BufReader::new(socket);
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(10), io.read_line(&mut line)).await??;
    let v: Value = serde_json::from_str(&line)?;
    if v["method"] != "open" {
        let response = match svc.control(v).await {
            Ok(v) => json!({"result":v}),
            Err(e) => json!({"error":e.to_string()}),
        };
        io.get_mut()
            .write_all(format!("{response}\n").as_bytes())
            .await?;
        return Ok(());
    }
    if v["config"].as_str() != path.to_str() || v["fingerprint"] != svc.cfg.fingerprint() {
        io.get_mut().write_all(b"{\"error\":\"Service uses another configuration; close existing uaoi terminals first\"}\n").await?;
        return Ok(());
    }
    let cwd = std::fs::canonicalize(v["cwd"].as_str().context("Missing cwd")?)?;
    let project = v["project"].as_str().context("Missing project")?;
    anyhow::ensure!(
        svc.cfg.project(&cwd)?.0 == project,
        "Project does not match cwd"
    );
    let client = uuid::Uuid::new_v4().to_string();
    clients.fetch_add(1, Ordering::SeqCst);
    let result: Result<()> = async {
        let env = serde_json::from_value(v["env"].clone())?;
        let args=serde_json::from_value::<Vec<std::ffi::OsString>>(v["args"].clone())?;
        let plan = svc.agent.open(&client, project, &cwd, &env, &args).await?;
        io.get_mut()
            .write_all(format!("{}\n", json!({"result":plan})).as_bytes())
            .await?;
        line.clear();
        // EOF also releases a lease, including SIGKILL of the wrapper.
        tokio::select! { r=io.read_line(&mut line)=>{r?;}, _=svc.agent.disconnected(&client)=>{anyhow::bail!("Codex runtime disconnected");} }
        Ok(())
    }
    .await;
    svc.agent.close(&client).await;
    clients.fetch_sub(1, Ordering::SeqCst);
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
