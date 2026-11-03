use super::{
    AgentAdapter,
    rpc::{self, Rpc},
};
use crate::{config::Config, model::*, store::Store};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::HashMap, os::unix::fs::PermissionsExt, path::PathBuf, sync::Arc};
use tokio::{
    net::UnixListener,
    process::{Child, Command},
    sync::{Mutex, broadcast, mpsc},
};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

pub struct Codex {
    cfg: Arc<Config>,
    store: Store,
    events: broadcast::Sender<AgentEvent>,
    runtimes: Mutex<HashMap<String, Arc<Runtime>>>,
    cancel: CancellationToken,
    owners: Arc<Mutex<HashMap<String, String>>>,
}
struct Runtime {
    current: Mutex<Option<Session>>,
    since: Mutex<u64>,
    owners: Arc<Mutex<HashMap<String, String>>>,
    client: String,
    gate: Mutex<()>,
    cancel: CancellationToken,
    rpc: Rpc,
    notices: broadcast::Sender<Value>,
    native: Mutex<Option<mpsc::Sender<Value>>>,
    gateway: PathBuf,
    child: Mutex<AgentProcess>,
    agent: String,
    permissions: super::codex_cli::Permissions,
    requests: Mutex<HashMap<String, (Value, String, Value)>>,
    turns: Mutex<HashMap<String, String>>,
}
// Startup errors and unwinding must release the entire private runtime too.
struct AgentProcess {
    child: Child,
    dir: PathBuf,
}
impl std::ops::Deref for AgentProcess {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.child
    }
}
impl std::ops::DerefMut for AgentProcess {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}
impl Drop for AgentProcess {
    fn drop(&mut self) {
        if let Some(pid) = self.child.id() {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
impl Codex {
    pub fn new(
        cfg: Arc<Config>,
        store: Store,
        events: broadcast::Sender<AgentEvent>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            cfg,
            store,
            events,
            runtimes: Mutex::new(HashMap::new()),
            owners: Arc::new(Mutex::new(HashMap::new())),
            cancel,
        }
    }
    async fn launch(
        &self,
        client: &str,
        project: &str,
        cwd: &std::path::Path,
        env: &std::collections::BTreeMap<String, String>,
        args: &[std::ffi::OsString],
    ) -> Result<Arc<Runtime>> {
        let (_, permissions) = super::codex_cli::remote_arguments(args)?;
        let mut map = self.runtimes.lock().await;
        anyhow::ensure!(!map.contains_key(client), "Client already connected");
        let p = self.cfg.projects.get(project).context("Unknown project")?;
        let spec = self.cfg.agents.get(&p.agent).context("Unknown agent")?;
        let mut env = env.clone();
        env.extend(spec.env.clone());
        let version = Command::new(&spec.command[0])
            .args(&spec.command[1..])
            .arg("--version")
            .env_clear()
            .envs(&env)
            .output()
            .await?;
        let version = String::from_utf8_lossy(&version.stdout);
        let components: Vec<_> = version
            .trim()
            .trim_start_matches("codex-cli ")
            .split('.')
            .collect();
        anyhow::ensure!(
            components.len() >= 2
                && (components[0].parse::<u64>().unwrap_or(0) > 0
                    || components[1].parse::<u64>().unwrap_or(0) >= 166),
            "Codex >=0.166 required; got {version}"
        );
        let dir = self
            .cfg
            .runtime()
            .join(&uuid::Uuid::new_v4().simple().to_string()[..12]);
        let engine = dir.join("engine.sock");
        let gateway = dir.join("cli.sock");
        anyhow::ensure!(
            gateway.as_os_str().len() < 100,
            "Runtime path too long for Unix sockets; set a short runtime_dir"
        );
        std::fs::create_dir_all(&dir)?;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        let mut cmd = Command::new(&spec.command[0]);
        cmd.args(&spec.command[1..])
            .args([
                "app-server",
                "--listen",
                &format!("unix://{}", engine.display()),
            ])
            .env_clear()
            .envs(&env)
            .current_dir(cwd)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .process_group(0)
            .kill_on_drop(true);
        #[cfg(target_os = "linux")]
        unsafe {
            cmd.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = match cmd.spawn() {
            Ok(child) => child,
            Err(error) => {
                let _ = std::fs::remove_dir_all(&dir);
                return Err(error.into());
            }
        };
        let mut child = AgentProcess { child, dir };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let rpc = loop {
            match Rpc::open(&engine).await {
                Ok(r) => break r,
                Err(e) => {
                    if child.try_wait()?.is_some() || std::time::Instant::now() > deadline {
                        return Err(e.context("Start Codex app-server"));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        };
        // Subscribe before initialization: empty/new threads can emit lifecycle events immediately.
        let (notices, received) = broadcast::channel(4096);
        rpc.initialize().await?;
        rpc.check().await?;
        let listener = UnixListener::bind(&gateway)?;
        std::fs::set_permissions(&gateway, std::fs::Permissions::from_mode(0o600))?;
        let runtime = Arc::new(Runtime {
            current: Mutex::new(None),
            since: Mutex::new(0),
            owners: self.owners.clone(),
            client: client.into(),
            gate: Mutex::new(()),
            cancel: self.cancel.child_token(),
            rpc,
            notices,
            native: Mutex::new(None),
            gateway,
            child: Mutex::new(child),
            agent: p.agent.clone(),
            permissions,
            requests: Mutex::new(HashMap::new()),
            turns: Mutex::new(HashMap::new()),
        });
        let (r, store, events, cancel) = (
            runtime.clone(),
            self.store.clone(),
            self.events.clone(),
            runtime.cancel.clone(),
        );
        let name = project.to_owned();
        tokio::spawn(async move {
            observe(r, store, events, received, cancel, name).await;
        });
        let (r, store, cancel, events) = (
            runtime.clone(),
            self.store.clone(),
            runtime.cancel.clone(),
            self.events.clone(),
        );
        let cfg = self.cfg.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _=cancel.cancelled()=>break,
                    accepted=listener.accept()=>match accepted {
                        Ok((io,_))=>{
                            let(r,s,c,e,engine,cfg)=(r.clone(),store.clone(),cancel.clone(),events.clone(),engine.clone(),cfg.clone());
                            tokio::spawn(async move{if let Err(error)=serve_gateway(io,&engine,r,s,e,c,cfg).await{tracing::warn!(%error,"CLI connection ended");}});
                        },Err(error)=>{tracing::error!(%error,"CLI listener failed");break}
                    }
                }
            }
        });
        let mut health = runtime.rpc.notices.subscribe();
        let cancel = runtime.cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _=cancel.cancelled()=>break,
                    message=health.recv()=>match message {
                        Ok(v) if v["method"]=="uaoi/disconnected"=>{cancel.cancel();break;},
                        Err(broadcast::error::RecvError::Closed)=>{cancel.cancel();break;},
                        _=>{}
                    }
                }
            }
        });
        map.insert(client.to_owned(), runtime.clone());
        Ok(runtime)
    }
    async fn for_session(&self, s: &Session) -> Result<Arc<Runtime>> {
        for r in self.runtimes.lock().await.values() {
            if r.current
                .lock()
                .await
                .as_ref()
                .is_some_and(|c| c.id == s.id)
                && !r.cancel.is_cancelled()
            {
                return Ok(r.clone());
            }
        }
        anyhow::bail!("Session offline. Open it with uaoi codex resume in its project directory.")
    }
    async fn emit_history(&self, r: &Runtime, s: &Session) -> Result<()> {
        let thread = r
            .rpc
            .request(
                "thread/read",
                json!({"threadId":s.native,"includeTurns":false}),
            )
            .await?;
        let mut turns = vec![];
        if thread["thread"]["historyMode"] == "paginated" {
            let mut cursor = Value::Null;
            loop {
                let page=r.rpc.request("thread/turns/list",json!({"threadId":s.native,"sortDirection":"asc","itemsView":"full","cursor":cursor})).await?;
                turns.extend(page["data"].as_array().cloned().unwrap_or_default());
                cursor = page["nextCursor"].clone();
                if cursor.is_null() {
                    break;
                }
            }
        } else {
            let full = r
                .rpc
                .request(
                    "thread/read",
                    json!({"threadId":s.native,"includeTurns":true}),
                )
                .await?;
            turns = full["thread"]["turns"]
                .as_array()
                .cloned()
                .unwrap_or_default();
        }
        for turn in turns {
            for item in turn["items"].as_array().into_iter().flatten() {
                if let Some(e) = item_event(s, item, true) {
                    if matches!(e.kind, EventKind::User)
                        && let Some(origin) = &e.origin
                    {
                        self.store.confirm_input(origin).await?;
                    }
                    self.store.event(&e).await?;
                }
            }
        }
        Ok(())
    }
}
#[async_trait]
impl AgentAdapter for Codex {
    async fn open(
        &self,
        client: &str,
        project: &str,
        cwd: &std::path::Path,
        env: &std::collections::BTreeMap<String, String>,
        args: &[std::ffi::OsString],
    ) -> Result<LaunchPlan> {
        let r = self.launch(client, project, cwd, env, args).await?;
        let a = &self.cfg.agents[&r.agent];
        let mut command = a.command.clone();
        command.extend(["--remote".into(), format!("unix://{}", r.gateway.display())]);
        command.extend(
            super::codex_cli::remote_arguments(args)?
                .0
                .into_iter()
                .map(|s| {
                    s.into_string()
                        .map_err(|_| anyhow::anyhow!("Codex arguments must be UTF-8"))
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let mut env = env.clone();
        env.extend(a.env.clone());
        Ok(LaunchPlan {
            command,
            env,
            cwd: cwd.into(),
        })
    }
    async fn active(&self, s: &Session) -> bool {
        self.for_session(s).await.is_ok()
    }
    async fn accepts(&self, s: &Session, sent_at: u64) -> bool {
        let Ok(r) = self.for_session(s).await else {
            return false;
        };
        sent_at > *r.since.lock().await
    }
    async fn disconnected(&self, client: &str) {
        let cancel = self
            .runtimes
            .lock()
            .await
            .get(client)
            .map(|r| r.cancel.clone());
        if let Some(cancel) = cancel {
            cancel.cancelled().await;
        }
    }
    async fn close(&self, client: &str) {
        let r = self.runtimes.lock().await.remove(client);
        if let Some(r) = r {
            let _gate = r.gate.lock().await;
            deactivate(&r, &self.store, &self.events).await;
            r.cancel.cancel();
            stop(&r).await;
            r.owners.lock().await.retain(|_, owner| owner != client);
        }
    }
    async fn submit(&self, s: &Session, text: &str, input_id: &str, steer: bool) -> Result<()> {
        let r = self.for_session(s).await?;
        let _gate = r.gate.lock().await;
        anyhow::ensure!(
            r.current
                .lock()
                .await
                .as_ref()
                .is_some_and(|c| c.id == s.id),
            "Session offline"
        );
        let mut p = json!({"threadId":s.native,"input":[{"type":"text","text":text,"text_elements":[]}],"clientUserMessageId":input_id});
        if steer {
            p["expectedTurnId"] = json!(
                r.turns
                    .lock()
                    .await
                    .get(&s.native)
                    .context("No active turn to steer")?
            );
            r.rpc.request("turn/steer", p).await?;
        } else {
            r.rpc.request("thread/queue/add", p).await?;
            // Native queue/start has an atomic idle precondition. A competing
            // terminal may already have started the turn or consumed the queue.
            // The queue/add receipt remains valid in either case.
            if let Err(error) = r
                .rpc
                .request("thread/queue/start", json!({"threadId":s.native}))
                .await
            {
                tracing::debug!(%error, "Queue start deferred to the native scheduler");
            }
        }
        Ok(())
    }
    async fn interrupt(&self, s: &Session) -> Result<()> {
        let r = self.for_session(s).await?;
        let _gate = r.gate.lock().await;
        anyhow::ensure!(
            r.current
                .lock()
                .await
                .as_ref()
                .is_some_and(|c| c.id == s.id),
            "Session offline"
        );
        let turn = r
            .turns
            .lock()
            .await
            .get(&s.native)
            .cloned()
            .context("No active turn")?;
        r.rpc
            .request("turn/interrupt", json!({"threadId":s.native,"turnId":turn}))
            .await?;
        Ok(())
    }
    async fn respond(&self, s: &Session, request: &str, answer: &str) -> Result<()> {
        let r = self.for_session(s).await?;
        let _gate = r.gate.lock().await;
        anyhow::ensure!(
            r.current
                .lock()
                .await
                .as_ref()
                .is_some_and(|c| c.id == s.id),
            "Session offline"
        );
        let requests = r.requests.lock().await;
        let (id, method, p) = requests
            .get(request)
            .context("Request expired or was resolved in another client")?;
        anyhow::ensure!(
            p["threadId"] == s.native,
            "Request belongs to another thread"
        );
        let result = if method == "item/tool/requestUserInput" {
            let mut answers = serde_json::Map::new();
            let questions = p["questions"].as_array().context("Missing questions")?;
            let supplied: Value = serde_json::from_str(answer).unwrap_or(Value::Null);
            for q in questions {
                let key = q["id"].as_str().context("Missing question ID")?;
                let text = if questions.len() == 1 {
                    answer
                } else {
                    supplied[key].as_str().context(
                        "For multiple questions use a JSON object mapping question IDs to answers",
                    )?
                };
                answers.insert(key.into(), json!({"answers":[text]}));
            }
            json!({"answers":answers})
        } else {
            anyhow::ensure!(
                ["accept", "decline", "cancel"].contains(&answer),
                "Expected accept, decline, or cancel"
            );
            json!({"decision":answer})
        };
        r.native
            .lock()
            .await
            .as_ref()
            .context("CLI reconnecting; try again")?
            .send(json!({"id":id,"result":result}))
            .await?;
        Ok(())
    }
    async fn history(&self, s: &Session) -> Result<()> {
        let runtime = self.for_session(s).await?;
        let _gate = runtime.gate.lock().await;
        anyhow::ensure!(
            runtime
                .current
                .lock()
                .await
                .as_ref()
                .is_some_and(|c| c.id == s.id),
            "Session offline"
        );
        self.emit_history(&runtime, s).await
    }
    async fn status(&self, s: &Session) -> Result<String> {
        let r = self.for_session(s).await?;
        let _gate = r.gate.lock().await;
        anyhow::ensure!(
            r.current
                .lock()
                .await
                .as_ref()
                .is_some_and(|c| c.id == s.id),
            "Session offline"
        );
        let thread = r
            .rpc
            .request(
                "thread/read",
                json!({"threadId":s.native,"includeTurns":false}),
            )
            .await?;
        let queue = r
            .rpc
            .request("thread/queue/list", json!({"threadId":s.native}))
            .await?;
        Ok(format!(
            "{}\nQueued: {}",
            thread["thread"]["status"]["type"]
                .as_str()
                .unwrap_or("online"),
            queue["data"].as_array().map_or(0, Vec::len)
        ))
    }
    async fn shutdown(&self) {
        let clients: Vec<_> = self.runtimes.lock().await.keys().cloned().collect();
        for client in clients {
            self.close(&client).await;
        }
    }
}
async fn stop(r: &Runtime) {
    let mut child = r.child.lock().await;
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGTERM);
        }
        if tokio::time::timeout(std::time::Duration::from_secs(3), child.wait())
            .await
            .is_err()
        {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
            let _ = child.wait().await;
        }
    }
    if let Some(dir) = r.gateway.parent() {
        let _ = std::fs::remove_dir_all(dir);
    }
}
async fn deactivate(r: &Runtime, store: &Store, events: &broadcast::Sender<AgentEvent>) {
    if let Some(s) = r.current.lock().await.take() {
        r.requests.lock().await.clear();
        r.owners
            .lock()
            .await
            .remove(&format!("{}:{}", r.agent, s.native));
        let _ = store.expire_requests(&s.id).await;
        let _ = events.send(AgentEvent {
            session: s.id,
            key: uuid::Uuid::new_v4().to_string(),
            kind: EventKind::Offline,
            text: format!(
                "Offline. Resume native session {} in the CLI to reconnect.",
                s.native
            ),
            complete: true,
            request: None,
            origin: None,
        });
    }
}
fn item_event(s: &Session, item: &Value, complete: bool) -> Option<AgentEvent> {
    let kind = item["type"].as_str()?;
    let (kind, text) = match kind {
        "userMessage" => (
            EventKind::User,
            item["content"]
                .as_array()?
                .iter()
                .filter_map(|x| x["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        "agentMessage" => (EventKind::Text, item["text"].as_str()?.to_owned()),
        "reasoning" => (
            EventKind::Reasoning,
            item["summary"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().or_else(|| x["text"].as_str()))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default(),
        ),
        "commandExecution" => (
            EventKind::Tool,
            format!(
                "{}\n{}\n{}",
                item["command"].as_str().unwrap_or("Command"),
                item["status"].as_str().unwrap_or("running"),
                item["aggregatedOutput"].as_str().unwrap_or("")
            ),
        ),
        "fileChange" | "mcpToolCall" | "collabAgentToolCall" | "plan" => {
            (EventKind::Tool, serde_json::to_string_pretty(item).ok()?)
        }
        _ => return None,
    };
    if text.is_empty() {
        return None;
    }
    Some(AgentEvent {
        session: s.id.clone(),
        key: format!("item:{}", item["id"].as_str()?),
        kind,
        text,
        complete,
        request: None,
        origin: item["clientId"]
            .as_str()
            .or_else(|| item["clientUserMessageId"].as_str())
            .map(str::to_owned),
    })
}
async fn observe(
    r: Arc<Runtime>,
    store: Store,
    events: broadcast::Sender<AgentEvent>,
    mut notices: broadcast::Receiver<Value>,
    cancel: CancellationToken,
    _project: String,
) {
    let mut streams: HashMap<String, String> = HashMap::new();
    loop {
        let v = tokio::select! {_=cancel.cancelled()=>break,v=notices.recv()=>match v{Ok(v)=>v,Err(broadcast::error::RecvError::Lagged(n))=>{tracing::error!(n,"Agent event stream lagged; use /history to reconcile");continue},Err(_)=>break}};
        let result: Result<()> = async {
            let method = v["method"].as_str().unwrap_or("");
            let p = &v["params"];
            let native = p["threadId"]
                .as_str()
                .or_else(|| p["thread"]["id"].as_str())
                .unwrap_or("");
            if native.is_empty() {
                return Ok(());
            }
            let _gate = r.gate.lock().await;
            let Some(s) = r
                .current
                .lock()
                .await
                .clone()
                .filter(|s| s.native == native)
            else {
                return Ok(());
            };
            if method == "turn/started"
                && let Some(id) = p["turn"]["id"].as_str()
            {
                r.turns.lock().await.insert(native.into(), id.into());
            }
            if method == "turn/completed" {
                r.turns.lock().await.remove(native);
            }
            if method == "serverRequest/resolved" {
                let mut requests = r.requests.lock().await;
                let key = requests
                    .iter()
                    .find(|(_, (id, _, _))| *id == p["requestId"])
                    .map(|(k, _)| k.clone());
                if let Some(k) = key {
                    requests.remove(&k);
                    store.resolve(&k).await?;
                    let event = AgentEvent {
                        session: s.id,
                        key: format!("request:{k}"),
                        kind: EventKind::Status,
                        text: "Resolved in Codex".into(),
                        complete: true,
                        request: None,
                        origin: None,
                    };
                    store.event(&event).await?;
                    let _ = events.send(event);
                }
                return Ok(());
            }
            let mut event = None;
            if v.get("id").is_some() {
                if ![
                    "item/commandExecution/requestApproval",
                    "item/fileChange/requestApproval",
                    "item/tool/requestUserInput",
                ]
                .contains(&method)
                {
                    tracing::warn!(method, "Unknown request left for native terminal");
                    return Ok(());
                }
                let key = uuid::Uuid::new_v4().simple().to_string();
                let choices = if method == "item/tool/requestUserInput" {
                    vec![]
                } else {
                    vec!["accept".into(), "decline".into(), "cancel".into()]
                };
                let text = if method == "item/tool/requestUserInput" {
                    p["questions"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|q| {
                            format!(
                                "{}: {}",
                                q["id"].as_str().unwrap_or("Question"),
                                q["question"].as_str().unwrap_or("")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                } else {
                    format!(
                        "Approval required\n{}\n{}",
                        p["command"].as_str().unwrap_or("File changes"),
                        p["cwd"].as_str().unwrap_or("")
                    )
                };
                r.requests
                    .lock()
                    .await
                    .insert(key.clone(), (v["id"].clone(), method.into(), p.clone()));
                event = Some(AgentEvent {
                    session: s.id.clone(),
                    key: format!("request:{key}"),
                    kind: EventKind::Question,
                    text: text.clone(),
                    complete: true,
                    request: Some(PendingRequest {
                        id: key,
                        label: text,
                        choices,
                    }),
                    origin: None,
                });
            } else if method == "item/started" || method == "item/completed" {
                event = item_event(&s, &p["item"], method == "item/completed");
            } else if [
                "item/agentMessage/delta",
                "item/reasoning/summaryTextDelta",
                "item/plan/delta",
            ]
            .contains(&method)
            {
                let key = format!("item:{}", p["itemId"].as_str().unwrap_or("unknown"));
                let text = streams.entry(format!("{}:{key}", s.id)).or_default();
                text.push_str(p["delta"].as_str().unwrap_or(""));
                event = Some(AgentEvent {
                    session: s.id.clone(),
                    key,
                    kind: if method.contains("reasoning") {
                        EventKind::Reasoning
                    } else {
                        EventKind::Text
                    },
                    text: text.clone(),
                    complete: false,
                    request: None,
                    origin: None,
                });
            } else if method == "turn/completed" {
                event = Some(AgentEvent {
                    session: s.id.clone(),
                    key: format!("turn:{}", p["turn"]["id"]),
                    kind: EventKind::Finished,
                    text: format!(
                        "Turn {}{}",
                        p["turn"]["status"].as_str().unwrap_or("finished"),
                        p["turn"]
                            .get("error")
                            .filter(|v| !v.is_null())
                            .map(|v| format!("\n{v}"))
                            .unwrap_or_default()
                    ),
                    complete: true,
                    request: None,
                    origin: None,
                });
                streams.retain(|k, _| !k.starts_with(&format!("{}:", s.id)));
            }
            if let Some(e) = event {
                if matches!(e.kind, EventKind::User)
                    && let Some(origin) = &e.origin
                {
                    store.confirm_input(origin).await?;
                }
                store.event(&e).await?;
                let _ = events.send(e);
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::error!(%error,"Native event processing failed");
        }
    }
}
async fn serve_gateway(
    io: tokio::net::UnixStream,
    engine: &std::path::Path,
    r: Arc<Runtime>,
    store: Store,
    events: broadcast::Sender<AgentEvent>,
    cancel: CancellationToken,
    cfg: Arc<Config>,
) -> Result<()> {
    let down = tokio_tungstenite::accept_async_with_config(io, Some(rpc::limits())).await?;
    let up = rpc::connect(engine).await?;
    let (mut ds, mut dr) = down.split();
    let (mut us, mut ur) = up.split();
    let (tx, mut replies) = mpsc::channel::<Value>(64);
    *r.native.lock().await = Some(tx);
    let mut pending: HashMap<String, Option<String>> = HashMap::new();
    loop {
        tokio::select! {
            _=cancel.cancelled()=>break,
            Some(reply)=replies.recv()=>{us.send(Message::Text(reply.to_string().into())).await?;},
            m=dr.next()=>match m {
                Some(Ok(Message::Text(t)))=>{
                    let mut v:Value=serde_json::from_str(&t)?;
                    let method=v["method"].as_str().unwrap_or("").to_owned();
                    if method == "thread/resume" && let Some(native) = v["params"]["threadId"].as_str() {
                        let key = format!("{}:{native}",r.agent);
                        let mut owners = r.owners.lock().await;
                        if owners.get(&key).is_some_and(|owner| owner != &r.client) {
                            ds.send(Message::Text(json!({"id":v["id"],"error":{"code":-32600,"message":"Session is already open in another uaoi terminal"}}).to_string().into())).await?;
                            continue;
                        }
                        owners.insert(key, r.client.clone());
                    }
                    if ["thread/start","thread/resume","thread/fork"].contains(&method.as_str()) {
                        pending.insert(v["id"].to_string(),v["params"]["threadId"].as_str().map(str::to_owned));
                    }
                    if ["thread/start","thread/resume","thread/fork"].contains(&method.as_str()) && (v["params"]["threadSource"].is_null() || v["params"]["threadSource"]=="user") {r.permissions.apply(&mut v["params"]);}
                    us.send(Message::Text(v.to_string().into())).await?;
                },Some(Ok(Message::Ping(b)))=>{ds.send(Message::Pong(b)).await?;},Some(Ok(Message::Close(_)))|None=>break,Some(Err(e))=>return Err(e.into()),_=>{},
            },
            m=ur.next()=>match m{
                Some(Ok(Message::Text(t)))=>{
                    let v:Value=serde_json::from_str(&t)?;
                    // Metadata must follow wire order, not the asynchronously observed
                    // item stream: an older notification must not overwrite a resume snapshot.
                    if v["method"]=="thread/name/updated" && let Some(native)=v["params"]["threadId"].as_str()
                        && let Some(session)=store.native_session(&r.agent,native).await? {
                        store.set_title(&session,v["params"]["threadName"].as_str()).await?;
                    }
                    let lifecycle=if v.get("method").is_none() {pending.remove(&v["id"].to_string())} else {None};
                    if v.get("error").is_some() && let Some(Some(native))=&lifecycle
                        && r.current.lock().await.as_ref().is_none_or(|s|s.native!=*native) {
                        r.owners.lock().await.remove(&format!("{}:{native}",r.agent));
                    }
                    if lifecycle.is_some() && v.get("error").is_none()
                        && let Some(native)=v["result"]["thread"]["id"].as_str()
                        && v["result"]["thread"]["parentThreadId"].is_null()
                        && (v["result"]["thread"]["threadSource"].is_null() || v["result"]["thread"]["threadSource"]=="user") {
                        let _gate = r.gate.lock().await;
                        if let Some(session)=r.current.lock().await.as_ref().filter(|s|s.native==native) {
                            store.set_title(session,v["result"]["thread"]["name"].as_str()).await?;
                        }
                        if r.current.lock().await.as_ref().is_none_or(|s| s.native != native) {
                            let selection:Result<String>=(|| {
                                let cwd=v["result"]["thread"]["cwd"].as_str().context("Thread has no cwd")?;
                                let (project,p)=cfg.project(&std::fs::canonicalize(cwd)?)?;
                                anyhow::ensure!(p.agent==r.agent,"Session uses a different configured agent; open another uaoi codex terminal in that project");
                                Ok(project.to_owned())
                            })();
                            let project=match selection {
                                Ok(project)=>project,
                                Err(error)=>{ds.send(Message::Text(json!({"id":v["id"],"error":{"code":-32600,"message":error.to_string()}}).to_string().into())).await?;continue;}
                            };
                            let s=store.ensure_session(&project,&r.agent,native).await?;
                            store.set_title(&s,v["result"]["thread"]["name"].as_str()).await?;
                            deactivate(&r,&store,&events).await;
                            r.owners.lock().await.insert(format!("{}:{native}",r.agent),r.client.clone());
                            *r.since.lock().await=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
                            *r.current.lock().await=Some(s.clone());
                            let _=events.send(AgentEvent{session:s.id,key:uuid::Uuid::new_v4().to_string(),kind:EventKind::Registered,text:format!("Online · {}\nSession {}",s.project,s.native),complete:true,request:None,origin:None});
                        }
                    }
                    if v.get("method").is_some() { let _=r.notices.send(v); }
                    ds.send(Message::Text(t)).await?;
                },Some(Ok(Message::Ping(b)))=>{us.send(Message::Pong(b)).await?;},Some(Ok(Message::Close(_)))|None=>break,Some(Err(e))=>return Err(e.into()),_=>{},
            }
        }
    }
    Ok(())
}
