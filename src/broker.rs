use crate::{
    agent, config::Config, im, model::LaunchPlan, service::Service, store::Store,
    wrapper::private_dir,
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    os::{fd::FromRawFd, unix::fs::PermissionsExt},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::Mutex,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

struct Profile {
    instances: HashSet<String>,
    generation: String,
    cancel: CancellationToken,
    poll: JoinHandle<()>,
}
#[derive(Default)]
struct State {
    profiles: BTreeMap<String, Profile>,
    identities: BTreeMap<String, String>,
    closing: bool,
    menus_reset: bool,
}
struct Broker {
    svc: Arc<Service>,
    path: PathBuf,
    state: Mutex<State>,
    cancel: CancellationToken,
}
impl Broker {
    async fn open(&self, instance: &str, request: &Value) -> Result<Option<LaunchPlan>> {
        let name = request["agent"].as_str().context("Missing agent")?;
        anyhow::ensure!(self.svc.cfg.agents.contains_key(name), "Unknown agent");
        let cwd = std::fs::canonicalize(request["cwd"].as_str().context("Missing cwd")?)?;
        let env: BTreeMap<String, String> = serde_json::from_value(request["env"].clone())?;
        let args: Vec<std::ffi::OsString> = serde_json::from_value(request["args"].clone())?;
        let mut state = self.state.lock().await;
        if state.closing || self.cancel.is_cancelled() {
            return Ok(None);
        }
        if !state.menus_reset {
            // Reconcile stale menus left by an interrupted previous broker.
            // This does not activate any Bot or native runtime.
            futures_util::future::join_all(self.svc.cfg.agents.iter().map(|(name, spec)| {
                let mut environment = env.clone();
                environment.extend(spec.env.clone());
                async move {
                    if let Ok(im) = self.svc.im(name)
                        && let Err(error) = im.clear_commands(&environment).await
                    {
                        tracing::warn!(agent=name, %error, "Could not reset offline command menus");
                    }
                }
            }))
            .await;
            state.menus_reset = true;
        }
        let fresh = !state.profiles.contains_key(name);
        if fresh {
            let im = self.svc.im(name)?;
            let activate = async {
                let mut environment = env.clone();
                environment.extend(self.svc.cfg.agents[name].env.clone());
                im.activate(&environment).await?;
                let identity = im.name();
                anyhow::ensure!(
                    !state
                        .identities
                        .iter()
                        .any(|(other, id)| other != name && id == &identity),
                    "Each agent requires a distinct Telegram Bot"
                );
                let cursor = im.discard_backlog().await?;
                self.svc.agent.activate(name, &env).await?;
                self.svc.initialize_agent(name).await?;
                Ok::<_, anyhow::Error>((identity, cursor))
            };
            let result = tokio::select! {
                _ = self.cancel.cancelled() => Err(anyhow::anyhow!("uaoi is stopping")),
                result = tokio::time::timeout(Duration::from_secs(90), activate) => result.unwrap_or_else(|_| Err(anyhow::anyhow!("Agent startup timed out"))),
            };
            match result {
                Ok((identity, cursor)) => {
                    let cancel = self.cancel.child_token();
                    let poll =
                        tokio::spawn(self.svc.clone().poll(im.clone(), cursor, cancel.clone()));
                    state.identities.insert(name.to_owned(), identity);
                    state.profiles.insert(
                        name.to_owned(),
                        Profile {
                            instances: HashSet::new(),
                            generation: uuid::Uuid::new_v4().to_string(),
                            cancel,
                            poll,
                        },
                    );
                }
                Err(error) => {
                    im.deactivate().await;
                    self.svc.agent.deactivate(name).await;
                    return Err(error);
                }
            }
        }
        let result = self.svc.agent.open(instance, name, &cwd, &env, &args).await;
        match result {
            Ok(plan) => {
                state
                    .profiles
                    .get_mut(name)
                    .unwrap()
                    .instances
                    .insert(instance.into());
                Ok(Some(plan))
            }
            Err(error) => {
                self.svc.agent.close(instance).await;
                if fresh {
                    self.stop_profile(&mut state, name).await;
                }
                Err(error)
            }
        }
    }
    async fn stop_profile(&self, state: &mut State, name: &str) {
        if let Some(profile) = state.profiles.remove(name) {
            profile.cancel.cancel();
            self.svc.agent.deactivate(name).await;
            if let Ok(im) = self.svc.im(name) {
                im.deactivate().await;
            }
            profile.poll.abort();
            let _ = profile.poll.await;
            if let Err(error) = self.svc.expire_profile(name).await {
                tracing::error!(%error, agent=name, "Expire profile interactions");
            }
        }
    }
    async fn release(&self, name: &str, instance: &str) -> bool {
        let mut state = self.state.lock().await;
        self.svc.agent.close(instance).await;
        if let Some(profile) = state.profiles.get_mut(name) {
            profile.instances.remove(instance);
            if profile.instances.is_empty() {
                self.stop_profile(&mut state, name).await;
            }
        }
        if state.profiles.is_empty() {
            state.closing = true;
        }
        state.closing
    }
    async fn status(&self) -> Value {
        let state = self.state.lock().await;
        let agents: BTreeMap<_, _> = self.svc.cfg.agents.keys().map(|name| {
            let profile = state.profiles.get(name);
            (name, json!({"state":if profile.is_some() {"running"} else {"stopped"}, "clients":profile.map_or(0, |p| p.instances.len()), "generation":profile.map(|p| &p.generation)}))
        }).collect();
        let closing = state.closing;
        drop(state);
        json!({"state":if closing {"stopping"} else {"running"}, "pid":std::process::id(), "agents":agents, "busy":self.svc.agent.busy().await})
    }
    async fn control(self: Arc<Self>, socket: UnixStream) -> Result<()> {
        let mut io = BufReader::new(socket);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(10), io.read_line(&mut line)).await??;
        let request: Value = serde_json::from_str(&line)?;
        let fingerprint = json!(self.svc.cfg.fingerprint());
        let valid = request.get("fingerprint") == Some(&fingerprint)
            && (request["method"] != "open" || request["config"].as_str() == self.path.to_str());
        if !valid {
            io.get_mut().write_all(b"{\"error\":\"Configuration changed; close all uaoi terminals before restarting\"}\n").await?;
            return Ok(());
        }
        if request["method"] != "open" {
            let result = if request["method"] == "status" {
                Ok(self.status().await)
            } else {
                self.svc.control(request).await
            };
            respond(&mut io, result).await?;
            return Ok(());
        }
        let instance = uuid::Uuid::new_v4().to_string();
        let name = request["agent"].as_str().unwrap_or("").to_owned();
        let opened = self.open(&instance, &request).await;
        let plan = match opened {
            Ok(Some(plan)) => plan,
            Ok(None) => {
                respond(&mut io, Ok(json!({"retry":true}))).await?;
                return Ok(());
            }
            Err(error) => {
                respond(&mut io, Err(error)).await?;
                return Ok(());
            }
        };
        let result = async {
            respond(&mut io, Ok(serde_json::to_value(plan)?)).await?;
            line.clear();
            tokio::select! {
                _ = self.cancel.cancelled() => anyhow::bail!("uaoi is stopping"),
                _ = self.svc.agent.disconnected(&instance) => anyhow::bail!("Agent runtime disconnected"),
                read = io.read_line(&mut line) => {
                    if read? != 0 { anyhow::ensure!(serde_json::from_str::<Value>(&line)?["method"] == "close", "Expected close"); }
                }
            }
            Ok(json!({}))
        }.await;
        let last = self.release(&name, &instance).await;
        let _ = respond(&mut io, result).await;
        if last {
            self.cancel.cancel();
        }
        Ok(())
    }
}
async fn respond(io: &mut BufReader<UnixStream>, result: Result<Value>) -> Result<()> {
    let response = match result {
        Ok(value) => json!({"result":value}),
        Err(error) => json!({"error":error.to_string()}),
    };
    io.get_mut()
        .write_all(format!("{response}\n").as_bytes())
        .await?;
    Ok(())
}

pub async fn run(cfg: Arc<Config>, path: PathBuf) -> Result<()> {
    // This inherited socket ties startup to its requesting wrapper, without tying
    // the running broker to whichever terminal happened to launch it first.
    let bootstrap = unsafe { std::os::unix::net::UnixStream::from_raw_fd(0) };
    bootstrap
        .peer_addr()
        .context("Internal broker requires a startup socket")?;
    bootstrap.set_nonblocking(true)?;
    let bootstrap = UnixStream::from_std(bootstrap)?;
    private_dir(&cfg.state())?;
    private_dir(&cfg.runtime())?;
    let state_lock = lock(&cfg.state().join("broker.lock"))?;
    let runtime_lock = lock(&cfg.runtime().join("control.lock"))?;
    let socket_path = cfg.runtime().join("control.sock");
    anyhow::ensure!(
        socket_path.as_os_str().len() < 100,
        "runtime_dir is too long for Unix sockets"
    );
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }
    let store = Store::open(&cfg.state().join("state.sqlite"))?;
    let cancel = CancellationToken::new();
    let (events, _) = tokio::sync::broadcast::channel(4096);
    let agent: Arc<dyn agent::AgentAdapter> = Arc::new(agent::codex::Codex::new(
        cfg.clone(),
        store.clone(),
        events.clone(),
        cancel.clone(),
    ));
    let ims = cfg
        .agents
        .iter()
        .map(|(name, spec)| {
            let projects = cfg
                .projects
                .iter()
                .filter(|(_, p)| p.agents.contains(name))
                .map(|(id, p)| (id.clone(), p.telegram.clone()))
                .collect();
            Ok(
                Arc::new(im::telegram::Telegram::new(name, &spec.telegram, projects)?)
                    as Arc<dyn im::ImAdapter>,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let svc = Arc::new(Service::new(cfg, store, agent.clone(), ims));
    svc.initialize().await?;
    let broker = Arc::new(Broker {
        svc: svc.clone(),
        path,
        state: Mutex::new(State::default()),
        cancel: cancel.clone(),
    });
    let listener = UnixListener::bind(&socket_path)?;
    std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
    serve(broker, listener, bootstrap, events).await?;
    let _ = std::fs::remove_file(socket_path);
    drop((runtime_lock, state_lock));
    Ok(())
}
async fn serve(
    broker: Arc<Broker>,
    listener: UnixListener,
    mut bootstrap: UnixStream,
    events: tokio::sync::broadcast::Sender<crate::model::AgentEvent>,
) -> Result<()> {
    let svc = broker.svc.clone();
    let agent = svc.agent.clone();
    let cancel = broker.cancel.clone();
    let mut workers = tokio::task::JoinSet::new();
    workers.spawn(svc.clone().events(events.subscribe(), cancel.clone()));
    workers.spawn(svc.clone().deliver(cancel.clone()));
    workers.spawn(svc.clone().sync_titles(cancel.clone()));
    workers.spawn(svc.clone().activity(cancel.clone()));
    let boot_broker = broker.clone();
    workers.spawn(async move {
        let mut byte = [0];
        let _ = bootstrap.read(&mut byte).await;
        let mut state = boot_broker.state.lock().await;
        if state.profiles.is_empty() {
            state.closing = true;
            boot_broker.cancel.cancel();
        }
    });
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut hup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::signal::ctrl_c() => break,
            _ = term.recv() => break,
            _ = hup.recv() => break,
            accepted = listener.accept() => {
                let (socket, _) = match accepted { Ok(value) => value, Err(error) => { tracing::error!(%error, "Control listener failed"); break; } };
                let broker = broker.clone();
                workers.spawn(async move { if let Err(error) = broker.control(socket).await { tracing::warn!(%error,"Control connection failed"); } });
            },
            result = workers.join_next(), if !workers.is_empty() => {
                if let Some(Err(error)) = result { tracing::error!(%error,"Worker failed"); break; }
            }
        }
    }
    cancel.cancel();
    agent.shutdown().await;
    let mut state = broker.state.lock().await;
    state.closing = true;
    let names: Vec<_> = state.profiles.keys().cloned().collect();
    for name in names {
        broker.stop_profile(&mut state, &name).await;
    }
    drop(state);
    if tokio::time::timeout(Duration::from_secs(5), async {
        while workers.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }

    drop(listener);
    let _ = std::fs::remove_file(svc.cfg.runtime().join("control.sock"));
    Ok(())
}
#[cfg(test)]
#[path = "broker_tests.rs"]
mod tests;

fn lock(path: &std::path::Path) -> Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    fs2::FileExt::try_lock_exclusive(&file)
        .context("Another uaoi instance is using this state or runtime directory")?;
    Ok(file)
}
