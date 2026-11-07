use super::{
    AgentAdapter, OutcomeUnknown, codex_cli,
    rpc::{self, Rpc},
};
use crate::{config::Config, model::*, store::Store};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    net::UnixListener,
    process::{Child, Command},
    sync::{Mutex, broadcast},
};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

pub struct Codex {
    cfg: Arc<Config>,
    store: Store,
    events: broadcast::Sender<AgentEvent>,
    runtimes: Mutex<HashMap<String, Arc<Runtime>>>,
    clients: Mutex<HashMap<String, (CancellationToken, PathBuf)>>,
    cancel: CancellationToken,
}
struct Runtime {
    config: Arc<Config>,
    agent: String,
    engine: PathBuf,
    rpc: Rpc,
    cancel: CancellationToken,
    child: Mutex<AgentProcess>,
    attach_gate: Mutex<()>,
    input_gate: Mutex<()>,
    sessions: Mutex<HashMap<String, Session>>,
    subscriptions: Mutex<HashSet<String>>,
    turns: Mutex<HashMap<String, String>>,
    starting: Mutex<HashSet<String>>,
    modes: Mutex<HashMap<String, Value>>,
    plans: Mutex<HashMap<String, (String, String)>>,
    requests: Mutex<HashMap<String, NativeRequest>>,
    answered: Mutex<HashSet<String>>,
    pending_bindings: Mutex<HashMap<String, PathBuf>>,
}
#[derive(Clone)]
struct NativeRequest {
    wire_id: Option<Value>,
    thread: String,
    turn: String,
    view: PendingRequest,
}
struct AgentProcess {
    child: Child,
    dir: PathBuf,
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
            clients: Mutex::new(HashMap::new()),
            cancel,
        }
    }
    async fn runtime(&self, agent: &str) -> Result<Arc<Runtime>> {
        let mut runtimes = self.runtimes.lock().await;
        if let Some(r) = runtimes.get(agent)
            && !r.cancel.is_cancelled()
        {
            return Ok(r.clone());
        }
        if let Some(old) = runtimes.remove(agent) {
            stop(&old).await;
        }
        let spec = self.cfg.agents.get(agent).context("Unknown agent")?;
        let version = Command::new(&spec.command[0])
            .args(&spec.command[1..])
            .arg("--version")
            .envs(&spec.env)
            .output()
            .await?;
        let version = String::from_utf8_lossy(&version.stdout);
        let parts: Vec<_> = version
            .trim()
            .trim_start_matches("codex-cli ")
            .split('.')
            .collect();
        anyhow::ensure!(
            parts.len() >= 2
                && (parts[0].parse::<u64>().unwrap_or(0) > 0
                    || parts[1].parse::<u64>().unwrap_or(0) >= 166),
            "Codex >=0.166 required; got {version}"
        );
        let dir = self
            .cfg
            .runtime()
            .join(uuid::Uuid::new_v4().simple().to_string());
        crate::wrapper::private_dir(&dir)?;
        let engine = dir.join("engine.sock");
        anyhow::ensure!(
            engine.as_os_str().len() < 104,
            "Runtime path too long for Unix sockets"
        );
        let mut cmd = Command::new(&spec.command[0]);
        cmd.args(&spec.command[1..])
            .args([
                "app-server",
                "--listen",
                &format!("unix://{}", engine.display()),
            ])
            .envs(&spec.env)
            .current_dir(&dir)
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
        let mut child = AgentProcess {
            child: cmd.spawn()?,
            dir,
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        let rpc = loop {
            match Rpc::open(&engine).await {
                Ok(rpc) => break rpc,
                Err(e) => {
                    if child.child.try_wait()?.is_some() || tokio::time::Instant::now() > deadline {
                        return Err(e.context("Start Codex app-server"));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        };
        let notices = rpc.notices.subscribe();
        rpc.initialize().await?;
        rpc.check().await?;
        let r = Arc::new(Runtime {
            config: self.cfg.clone(),
            agent: agent.into(),
            engine,
            rpc,
            cancel: self.cancel.child_token(),
            child: Mutex::new(child),
            attach_gate: Mutex::new(()),
            input_gate: Mutex::new(()),
            sessions: Mutex::new(HashMap::new()),
            subscriptions: Mutex::new(HashSet::new()),
            turns: Mutex::new(HashMap::new()),
            starting: Mutex::new(HashSet::new()),
            modes: Mutex::new(HashMap::new()),
            plans: Mutex::new(HashMap::new()),
            requests: Mutex::new(HashMap::new()),
            answered: Mutex::new(HashSet::new()),
            pending_bindings: Mutex::new(HashMap::new()),
        });
        tokio::spawn(observe(
            r.clone(),
            self.store.clone(),
            self.events.clone(),
            notices,
        ));
        tokio::spawn(supervise(r.clone()));
        runtimes.insert(agent.into(), r.clone());
        Ok(r)
    }
    async fn attached(&self, s: &Session) -> Result<Arc<Runtime>> {
        let r = self.runtime(&s.agent).await?;
        attach(&r, &self.store, &s.native).await?;
        Ok(r)
    }
    async fn update_mode(&self, s: &Session, mode: Option<SessionMode>) -> Result<SessionMode> {
        let r = self.attached(s).await?;
        let _gate = r.input_gate.lock().await;
        let mode = if let Some(mode) = mode {
            mode
        } else {
            match current_mode(&r, s).await?["mode"].as_str() {
                Some("plan") => SessionMode::Default,
                Some("default") => SessionMode::Plan,
                _ => anyhow::bail!("Current session mode is unavailable"),
            }
        };
        let selected = select_mode(&r, s, mode).await?;
        r.rpc
            .request(
                "thread/settings/update",
                json!({"threadId":s.native,"collaborationMode":selected}),
            )
            .await?;
        r.modes.lock().await.insert(s.native.clone(), selected);
        invalidate(
            &r,
            &self.store,
            &self.events,
            &s.native,
            Some(RequestKind::Plan),
        )
        .await?;
        Ok(mode)
    }
    async fn submit_input(
        &self,
        s: &Session,
        text: &str,
        input_id: &str,
        mode: Option<SessionMode>,
    ) -> Result<()> {
        let r = self.attached(s).await?;
        let _gate = r.input_gate.lock().await;
        let turn = current_turn(&r, &s.native).await?;
        anyhow::ensure!(
            mode.is_none() || turn.is_none(),
            "A turn is running. Wait for it to finish before starting a task in another mode; /plan or /default without a task changes only the next turn"
        );
        let mut p = json!({"threadId":s.native,"input":[{"type":"text","text":text,"text_elements":[]}],"clientUserMessageId":input_id});
        if let Some(turn) = turn {
            p["expectedTurnId"] = json!(turn);
            r.rpc.request("turn/steer", p).await?;
        } else {
            let selected = if let Some(mode) = mode {
                Some(select_mode(&r, s, mode).await?)
            } else {
                None
            };
            if let Some(mode) = &selected {
                p["collaborationMode"] = mode.clone();
            }
            start_turn(&r, p).await?;
            if let Some(mode) = selected {
                r.modes.lock().await.insert(s.native.clone(), mode);
            }
        }
        invalidate(
            &r,
            &self.store,
            &self.events,
            &s.native,
            Some(RequestKind::Plan),
        )
        .await?;
        Ok(())
    }
    async fn history_into_store(&self, r: &Runtime, s: &Session) -> Result<()> {
        let mut cursor = Value::Null;
        loop {
            let page = r.rpc.request("thread/turns/list",json!({"threadId":s.native,"sortDirection":"asc","itemsView":"full","limit":20,"cursor":cursor})).await?;
            for turn in page["data"].as_array().into_iter().flatten() {
                for item in turn["items"].as_array().into_iter().flatten() {
                    if let Some(e) = item_event(s, item, true) {
                        self.store.event(&e).await?;
                    }
                }
            }
            cursor = page["nextCursor"].clone();
            if cursor.is_null() {
                break;
            }
        }
        Ok(())
    }
}
#[async_trait]
impl AgentAdapter for Codex {
    async fn start(&self) -> Result<()> {
        for name in self.cfg.agents.keys() {
            self.runtime(name).await?;
        }
        for record in self.store.bindings().await?.1 {
            if let Err(error) = self
                .restore(&record.session.agent, &record.session.native)
                .await
            {
                tracing::warn!(%error, session=record.session.native, "Bound session could not be attached");
            }
        }
        Ok(())
    }
    async fn describe(&self, s: &Session) -> Result<SessionSummary> {
        let r = self.runtime(&s.agent).await?;
        let value = r
            .rpc
            .request(
                "thread/read",
                json!({"threadId":s.native,"includeTurns":false}),
            )
            .await?;
        summary(&value["thread"]).context("Session metadata unavailable")
    }
    async fn restore(&self, agent: &str, native: &str) -> Result<Session> {
        let r = self.runtime(agent).await?;
        attach(&r, &self.store, native).await
    }
    async fn create(&self, agent: &str, cwd: &Path) -> Result<Session> {
        self.cfg.project_for_cwd(agent, cwd)?;
        anyhow::ensure!(
            cwd.is_absolute() && cwd.is_dir(),
            "Use an existing absolute working directory"
        );
        let r = self.runtime(agent).await?;
        let result = r.rpc.request("thread/start", json!({"cwd":cwd})).await?;
        let session = register(&r, &self.store, &result["thread"]).await?;
        remember_mode(&r, &session.native, &result).await;
        r.subscriptions.lock().await.insert(session.native.clone());
        announce(&self.store, &self.events, &session).await?;
        Ok(session)
    }
    async fn busy(&self) -> bool {
        let runtimes: Vec<_> = self.runtimes.lock().await.values().cloned().collect();
        for r in runtimes {
            if r.cancel.is_cancelled() {
                continue;
            }
            if !r.starting.lock().await.is_empty() {
                return true;
            }
            // Stopping must also protect work in deliberately unbound sessions.
            // This read-only check never imports or binds native history.
            let mut cursor = Value::Null;
            loop {
                let Ok(page) = r
                    .rpc
                    .request("thread/loaded/list", json!({"cursor":cursor,"limit":100}))
                    .await
                else {
                    return true;
                };
                let Some(sessions) = page["data"].as_array() else {
                    return true;
                };
                for native in sessions {
                    match r
                        .rpc
                        .request(
                            "thread/read",
                            json!({"threadId":native,"includeTurns":false}),
                        )
                        .await
                    {
                        Ok(t) if t["thread"]["status"]["type"] != "active" => {}
                        _ => return true,
                    }
                }
                cursor = page["nextCursor"].clone();
                if cursor.is_null() {
                    break;
                }
            }
        }
        false
    }
    async fn open(
        &self,
        client: &str,
        agent: &str,
        cwd: &Path,
        env: &std::collections::BTreeMap<String, String>,
        args: &[std::ffi::OsString],
    ) -> Result<LaunchPlan> {
        let r = self.runtime(agent).await?;
        let (args, permissions) = codex_cli::remote_arguments(args)?;
        let path = self
            .cfg
            .runtime()
            .join(format!("{}.sock", uuid::Uuid::new_v4().simple()));
        anyhow::ensure!(
            path.as_os_str().len() < 104,
            "Runtime path too long for Unix sockets"
        );
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let cancel = r.cancel.child_token();
        self.clients
            .lock()
            .await
            .insert(client.into(), (cancel.clone(), path.clone()));
        let (store, events) = (self.store.clone(), self.events.clone());
        let gateway_cwd = cwd.to_owned();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _=cancel.cancelled()=>break,
                    accepted=listener.accept()=>match accepted {
                        Ok((io,_))=>{
                            let (r,store,events,cancel,permissions,cwd)=(r.clone(),store.clone(),events.clone(),cancel.clone(),permissions.clone(),gateway_cwd.clone());
                            tokio::spawn(async move {if let Err(error)=gateway(io,r,store,events,cancel,permissions,cwd).await {tracing::debug!(%error,"CLI connection ended");}});
                        },Err(_)=>break,
                    }
                }
            }
        });
        let spec = &self.cfg.agents[agent];
        let mut command = spec.command.clone();
        command.extend(["--remote".into(), format!("unix://{}", path.display())]);
        command.extend(
            args.into_iter()
                .map(|s| {
                    s.into_string()
                        .map_err(|_| anyhow::anyhow!("Arguments must be UTF-8"))
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let mut env = env.clone();
        env.extend(spec.env.clone());
        Ok(LaunchPlan {
            command,
            env,
            cwd: cwd.into(),
        })
    }
    async fn close(&self, client: &str) {
        if let Some((cancel, path)) = self.clients.lock().await.remove(client) {
            cancel.cancel();
            let _ = std::fs::remove_file(path);
        }
    }
    async fn disconnected(&self, client: &str) {
        let cancel = self
            .clients
            .lock()
            .await
            .get(client)
            .map(|(c, _)| c.clone());
        if let Some(cancel) = cancel {
            cancel.cancelled().await;
        }
    }
    async fn active(&self, s: &Session) -> bool {
        let r = self.runtimes.lock().await.get(&s.agent).cloned();
        if let Some(r) = r {
            !r.cancel.is_cancelled()
                && r.rpc.connected()
                && r.sessions.lock().await.contains_key(&s.native)
        } else {
            false
        }
    }
    async fn working(&self, s: &Session) -> bool {
        if !self.active(s).await {
            return false;
        }
        let Some(r) = self.runtimes.lock().await.get(&s.agent).cloned() else {
            return false;
        };
        if r.cancel.is_cancelled() || !r.sessions.lock().await.contains_key(&s.native) {
            return false;
        }
        let Ok(result) = r
            .rpc
            .request(
                "thread/read",
                json!({"threadId":s.native,"includeTurns":false}),
            )
            .await
        else {
            return false;
        };
        let status = &result["thread"]["status"];
        status["type"] == "active"
            && status["activeFlags"]
                .as_array()
                .is_none_or(|flags| flags.is_empty())
    }
    async fn submit(&self, s: &Session, text: &str, input_id: &str) -> Result<()> {
        self.submit_input(s, text, input_id, None).await
    }
    async fn submit_in_mode(
        &self,
        s: &Session,
        text: &str,
        input_id: &str,
        mode: SessionMode,
    ) -> Result<()> {
        self.submit_input(s, text, input_id, Some(mode)).await
    }
    async fn set_mode(&self, s: &Session, mode: SessionMode) -> Result<()> {
        self.update_mode(s, Some(mode)).await.map(|_| ())
    }
    async fn toggle_plan_mode(&self, s: &Session) -> Result<SessionMode> {
        self.update_mode(s, None).await
    }
    async fn interrupt(&self, s: &Session) -> Result<()> {
        let r = self.attached(s).await?;
        let turn = current_turn(&r, &s.native)
            .await?
            .context("No active turn")?;
        r.rpc
            .request("turn/interrupt", json!({"threadId":s.native,"turnId":turn}))
            .await?;
        Ok(())
    }
    async fn respond(&self, s: &Session, id: &str, answer: &str) -> Result<()> {
        let r = self.attached(s).await?;
        let _gate = r.input_gate.lock().await;
        let pending = r
            .requests
            .lock()
            .await
            .get(id)
            .cloned()
            .context("Request expired or already answered")?;
        anyhow::ensure!(
            pending.thread == s.native,
            "Request belongs to another session"
        );
        if pending.view.kind == RequestKind::Plan {
            anyhow::ensure!(
                ["execute", "stay"].contains(&answer),
                "Invalid plan decision"
            );
            anyhow::ensure!(
                current_turn(&r, &s.native).await?.is_none(),
                "A new turn already started"
            );
            let last=r.rpc.request("thread/turns/list",json!({"threadId":s.native,"limit":1,"sortDirection":"desc","itemsView":"notLoaded"})).await?;
            anyhow::ensure!(
                last["data"][0]["id"] == pending.turn,
                "This plan has been superseded"
            );
            let mode = r
                .modes
                .lock()
                .await
                .get(&s.native)
                .cloned()
                .context("Missing collaboration mode")?;
            anyhow::ensure!(mode["mode"] == "plan", "Session is no longer in Plan mode");
            if answer == "execute" {
                let mut mode = mode;
                mode["mode"] = json!("default");
                mode["settings"]["developer_instructions"] = Value::Null;
                // One turn/start atomically selects the mode and begins implementation.
                start_turn(&r,json!({"threadId":s.native,"collaborationMode":mode,"clientUserMessageId":format!("answer:{id}"),"input":[{"type":"text","text":"Implement the plan.","text_elements":[]}]})).await?;
            }
            finish(
                &r,
                &self.store,
                &self.events,
                id,
                if answer == "execute" {
                    "Plan execution started"
                } else {
                    "Staying in Plan mode"
                },
            )
            .await?;
            report_resolution(
                &self.store,
                &self.events,
                s,
                id,
                if answer == "execute" {
                    "Plan execution started"
                } else {
                    "Staying in Plan mode"
                },
            )
            .await?;
            return Ok(());
        }
        let result = if pending.view.kind == RequestKind::Approval {
            anyhow::ensure!(
                pending
                    .view
                    .choices
                    .iter()
                    .any(|(_, value)| value == answer),
                "Invalid approval decision"
            );
            json!({"decision":answer})
        } else {
            answers_payload(&pending.view, answer)?
        };
        if let Some(wire) = &pending.wire_id {
            anyhow::ensure!(
                r.answered.lock().await.insert(wire.to_string()),
                "Request already answered in another client"
            );
            r.rpc
                .send(json!({"id":wire,"result":result}))
                .await
                .map_err(|e| OutcomeUnknown(e.to_string()))?;
        } else {
            let text = answer_text(&pending.view, &result);
            let mut p = json!({"threadId":s.native,"input":[{"type":"text","text":text,"text_elements":[]}],"clientUserMessageId":format!("answer:{id}")});
            if let Some(turn) = current_turn(&r, &s.native).await? {
                p["expectedTurnId"] = json!(turn);
                r.rpc.request("turn/steer", p).await?;
            } else {
                start_turn(&r, p).await?;
            }
        }
        finish(
            &r,
            &self.store,
            &self.events,
            id,
            &format!(
                "Answer submitted via Telegram\n{}",
                answer_text(&pending.view, &result)
            ),
        )
        .await?;
        report_resolution(
            &self.store,
            &self.events,
            s,
            id,
            &format!(
                "Answer submitted via Telegram\n{}",
                answer_text(&pending.view, &result)
            ),
        )
        .await?;
        Ok(())
    }
    async fn history(&self, s: &Session) -> Result<()> {
        let r = self.attached(s).await?;
        self.history_into_store(&r, s).await
    }
    async fn status(&self, s: &Session) -> Result<SessionStatus> {
        let r = self.runtime(&s.agent).await?;
        let t = r
            .rpc
            .request(
                "thread/read",
                json!({"threadId":s.native,"includeTurns":false}),
            )
            .await?;
        let status = &t["thread"]["status"];
        let mut state = status["type"].as_str().unwrap_or("unknown").to_owned();
        if let Some(flags) = status["activeFlags"]
            .as_array()
            .filter(|flags| !flags.is_empty())
        {
            state.push_str(&format!(
                " ({})",
                flags
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let mode = r
            .modes
            .lock()
            .await
            .get(&s.native)
            .and_then(|m| match m["mode"].as_str() {
                Some("plan") => Some(SessionMode::Plan),
                Some("default") => Some(SessionMode::Default),
                _ => None,
            });
        Ok(SessionStatus {
            cwd: t["thread"]["cwd"]
                .as_str()
                .context("Missing session directory")?
                .into(),
            state,
            mode,
        })
    }
    async fn shutdown(&self) {
        for (_, (cancel, path)) in self.clients.lock().await.drain() {
            cancel.cancel();
            let _ = std::fs::remove_file(path);
        }
        for r in self.runtimes.lock().await.values() {
            r.cancel.cancel();
            stop(r).await;
        }
    }
}
async fn remember_mode(r: &Runtime, native: &str, result: &Value) {
    let mode = if result["collaborationMode"].is_object() {
        result["collaborationMode"].clone()
    } else if let Some(model) = result["model"].as_str() {
        json!({"mode":"default","settings":{"model":model,"reasoning_effort":result["reasoningEffort"],"developer_instructions":null}})
    } else {
        return;
    };
    r.modes.lock().await.insert(native.into(), mode);
}
async fn current_mode(r: &Runtime, s: &Session) -> Result<Value> {
    if !r.modes.lock().await.contains_key(&s.native) {
        let resumed = r
            .rpc
            .request(
                "thread/resume",
                json!({"threadId":s.native,"excludeTurns":true}),
            )
            .await?;
        remember_mode(r, &s.native, &resumed).await;
    }
    r.modes
        .lock()
        .await
        .get(&s.native)
        .cloned()
        .context("Session settings unavailable; reopen the session")
}
async fn select_mode(r: &Runtime, s: &Session, selected: SessionMode) -> Result<Value> {
    let mut mode = current_mode(r, s).await?;
    mode["mode"] = json!(match selected {
        SessionMode::Default => "default",
        SessionMode::Plan => "plan",
    });
    mode["settings"]["developer_instructions"] = Value::Null;
    Ok(mode)
}
fn human_thread(t: &Value) -> bool {
    t["parentThreadId"].is_null() && (t["threadSource"].is_null() || t["threadSource"] == "user")
}
async fn has_user_turn(r: &Runtime, thread: &Value) -> Result<bool> {
    if thread["preview"]
        .as_str()
        .is_some_and(|text| !text.trim().is_empty())
        || thread["status"]["type"] == "active"
        || thread["turns"]
            .as_array()
            .is_some_and(|turns| !turns.is_empty())
    {
        return Ok(true);
    }
    if thread["path"].is_null() {
        return Ok(false);
    }
    // A persisted thread can contain only initial context and a name. Check
    // for an actual turn before exposing it as a conversation.
    let turns = match r.rpc.request("thread/turns/list", json!({"threadId":thread["id"],"limit":1,"sortDirection":"desc","itemsView":"notLoaded"})).await {
        Ok(turns) => turns,
        Err(error) if error.to_string().contains("no rollout") || error.to_string().contains("not materialized yet") => return Ok(false),
        Err(error) => return Err(error),
    };
    Ok(turns["data"]
        .as_array()
        .is_some_and(|turns| !turns.is_empty()))
}
fn summary(t: &Value) -> Option<SessionSummary> {
    if !human_thread(t) {
        return None;
    }
    let cwd = PathBuf::from(t["cwd"].as_str()?);
    Some(SessionSummary {
        native: t["id"].as_str()?.into(),
        title: t["name"]
            .as_str()
            .filter(|s| !s.is_empty())
            .or(t["preview"].as_str())
            .unwrap_or("Untitled session")
            .chars()
            .take(120)
            .collect(),
        cwd,
        status: t["status"]["type"].as_str().unwrap_or("notLoaded").into(),
    })
}
async fn register(r: &Runtime, store: &Store, thread: &Value) -> Result<Session> {
    let info = summary(thread).context("Session is not a user conversation")?;
    let session = store.ensure_session(&r.agent, &info.native).await?;
    store.set_title(&session, thread["name"].as_str()).await?;
    r.sessions.lock().await.insert(info.native, session.clone());
    Ok(session)
}
async fn announce(
    store: &Store,
    events: &broadcast::Sender<AgentEvent>,
    session: &Session,
) -> Result<()> {
    emit(
        store,
        events,
        AgentEvent {
            session: session.id.clone(),
            key: format!("created:{}", session.native),
            kind: EventKind::Registered,
            text: String::new(),
            complete: true,
            request: None,
            origin: None,
        },
    )
    .await
}
async fn attach(r: &Arc<Runtime>, store: &Store, native: &str) -> Result<Session> {
    let _gate = r.attach_gate.lock().await;
    let existing = r.sessions.lock().await.get(native).cloned();
    if let Some(s) = &existing
        && r.subscriptions.lock().await.contains(native)
    {
        return Ok(s.clone());
    }
    let read = match r
        .rpc
        .request(
            "thread/read",
            json!({"threadId":native,"includeTurns":false}),
        )
        .await
    {
        Ok(v) => v,
        // Empty live threads have no rollout yet. Their creating client is the
        // event source until the first turn makes them resumable.
        Err(e) if e.to_string().contains("no rollout") && existing.is_some() => {
            return Ok(existing.unwrap());
        }
        Err(e) => return Err(e),
    };
    let s = register(r, store, &read["thread"]).await?;
    let resumed = match r
        .rpc
        .request(
            "thread/resume",
            json!({"threadId":native,"excludeTurns":true}),
        )
        .await
    {
        Ok(v) => v,
        Err(e) => {
            r.starting.lock().await.remove(native);
            if e.to_string().contains("no rollout") {
                return Ok(s);
            }
            return Err(e);
        }
    };
    // Keep forwarding the creating CLI's events until the control connection
    // has actually subscribed; otherwise a short first turn can be lost.
    r.subscriptions.lock().await.insert(native.into());
    remember_mode(r, native, &resumed).await;
    let _ = current_turn(r, native).await;
    Ok(s)
}
async fn start_turn(r: &Runtime, p: Value) -> Result<Value> {
    let thread = p["threadId"]
        .as_str()
        .context("Missing thread ID")?
        .to_owned();
    r.starting.lock().await.insert(thread.clone());
    let result = r.rpc.request("turn/start", p).await;
    if result.as_ref().is_err_and(|e| !e.is::<OutcomeUnknown>()) {
        r.starting.lock().await.remove(&thread);
    }
    result
}
async fn current_turn(r: &Runtime, native: &str) -> Result<Option<String>> {
    anyhow::ensure!(
        !r.starting.lock().await.contains(native),
        "The turn is starting; retry shortly"
    );
    let snapshot = match r
        .rpc
        .request(
            "thread/read",
            json!({"threadId":native,"includeTurns":false}),
        )
        .await
    {
        Ok(v) => v,
        Err(e) if e.to_string().contains("no rollout") => {
            return Ok(r.turns.lock().await.get(native).cloned());
        }
        Err(e) => return Err(e),
    };
    if snapshot["thread"]["status"]["type"] != "active" {
        r.turns.lock().await.remove(native);
        return Ok(None);
    }
    if let Some(id) = r.turns.lock().await.get(native).cloned() {
        return Ok(Some(id));
    }
    let page = r
        .rpc
        .request(
            "thread/turns/list",
            json!({"threadId":native,"sortDirection":"desc","itemsView":"notLoaded","limit":1}),
        )
        .await?;
    let id = page["data"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|t| t["status"] == "inProgress")
        .and_then(|t| t["id"].as_str())
        .map(str::to_owned);
    anyhow::ensure!(
        id.is_some(),
        "Active turn is being synchronized; retry the message shortly"
    );
    if let Some(id) = &id {
        r.turns.lock().await.insert(native.into(), id.clone());
    }
    Ok(id)
}
async fn supervise(r: Arc<Runtime>) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! { _=r.cancel.cancelled()=>break, _=tick.tick()=>{} }
        if r.child
            .lock()
            .await
            .child
            .try_wait()
            .ok()
            .flatten()
            .is_some()
        {
            r.cancel.cancel();
            break;
        }
    }
}
async fn stop(r: &Runtime) {
    r.rpc.close();
    let mut process = r.child.lock().await;
    if let Some(pid) = process.child.id() {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGTERM);
        }
        if tokio::time::timeout(std::time::Duration::from_secs(3), process.child.wait())
            .await
            .is_err()
        {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
            let _ = process.child.wait().await;
        }
    }
}
async fn emit(store: &Store, events: &broadcast::Sender<AgentEvent>, e: AgentEvent) -> Result<()> {
    store.event(&e).await?;
    let _ = events.send(e);
    Ok(())
}
async fn report_resolution(
    store: &Store,
    events: &broadcast::Sender<AgentEvent>,
    s: &Session,
    id: &str,
    text: &str,
) -> Result<()> {
    store.resolve(id).await?;
    emit(
        store,
        events,
        AgentEvent {
            session: s.id.clone(),
            key: format!("request:{id}"),
            kind: EventKind::Status,
            text: text.into(),
            complete: true,
            request: None,
            origin: None,
        },
    )
    .await
}
async fn finish(
    r: &Runtime,
    store: &Store,
    events: &broadcast::Sender<AgentEvent>,
    id: &str,
    text: &str,
) -> Result<()> {
    let request = r.requests.lock().await.remove(id);
    if let Some(request) = request
        && let Some(s) = r.sessions.lock().await.get(&request.thread).cloned()
    {
        store.resolve(id).await?;
        emit(
            store,
            events,
            AgentEvent {
                session: s.id.clone(),
                key: format!("request:{id}"),
                kind: EventKind::Status,
                text: text.into(),
                complete: true,
                request: None,
                origin: None,
            },
        )
        .await?;
    }
    Ok(())
}
async fn invalidate(
    r: &Runtime,
    store: &Store,
    events: &broadcast::Sender<AgentEvent>,
    thread: &str,
    kind: Option<RequestKind>,
) -> Result<()> {
    let ids: Vec<_> = r
        .requests
        .lock()
        .await
        .iter()
        .filter(|(_, p)| p.thread == thread && kind.as_ref().is_none_or(|k| *k == p.view.kind))
        .map(|(id, _)| id.clone())
        .collect();
    for id in ids {
        finish(r, store, events, &id, "Request expired").await?;
    }
    Ok(())
}
pub(crate) fn answers_payload(request: &PendingRequest, answer: &str) -> Result<Value> {
    let supplied: Value = serde_json::from_str(answer).unwrap_or(Value::Null);
    let mut answers = serde_json::Map::new();
    for q in &request.questions {
        let text = if let Some(text) = supplied[&q.id].as_str() {
            text
        } else if request.questions.len() == 1 {
            answer
        } else {
            anyhow::bail!("Provide one answer for each question");
        };
        anyhow::ensure!(!text.trim().is_empty(), "Answer must not be empty");
        answers.insert(q.id.clone(), json!({"answers":[text]}));
    }
    Ok(json!({"answers":answers}))
}
fn answer_text(request: &PendingRequest, result: &Value) -> String {
    if request.kind == RequestKind::Approval {
        return result["decision"].as_str().unwrap_or("Resolved").into();
    }
    request
        .questions
        .iter()
        .map(|q| {
            let text = if q.secret {
                "[redacted]".into()
            } else {
                result["answers"][&q.id]["answers"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join("; ")
                    })
                    .unwrap_or_else(|| "No answer".into())
            };
            format!("{}: {text}", q.title)
        })
        .collect::<Vec<_>>()
        .join("\n")
}
fn native_question(method: &str, p: &Value, id: String) -> PendingRequest {
    let question = method == "item/tool/requestUserInput";
    PendingRequest {
        id,
        label: if question {
            "Questions".into()
        } else {
            format!(
                "Approval required\n{}",
                p["command"].as_str().unwrap_or("File changes")
            )
        },
        choices: if question {
            vec![]
        } else {
            vec![
                ("Approve".into(), "accept".into()),
                ("Decline".into(), "decline".into()),
                ("Cancel".into(), "cancel".into()),
            ]
        },
        kind: if question {
            RequestKind::Questions
        } else {
            RequestKind::Approval
        },
        blocking: p["isBlocking"].as_bool().unwrap_or(true),
        questions: if question {
            p["questions"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|q| Question {
                    id: q["id"].as_str().unwrap_or("").into(),
                    title: q["question"].as_str().unwrap_or("").into(),
                    secret: q["isSecret"].as_bool().unwrap_or(false),
                    options: q["options"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|o| {
                            (
                                o["label"].as_str().unwrap_or("").into(),
                                o["description"].as_str().unwrap_or("").into(),
                            )
                        })
                        .collect(),
                })
                .collect()
        } else {
            vec![]
        },
    }
}
async fn add_request(
    r: &Runtime,
    store: &Store,
    events: &broadcast::Sender<AgentEvent>,
    s: &Session,
    pending: NativeRequest,
) -> Result<()> {
    let view = pending.view.clone();
    r.requests.lock().await.insert(view.id.clone(), pending);
    emit(
        store,
        events,
        AgentEvent {
            session: s.id.clone(),
            key: format!("request:{}", view.id),
            kind: EventKind::Question,
            text: view.label.clone(),
            complete: true,
            request: Some(view),
            origin: None,
        },
    )
    .await
}
fn item_event(s: &Session, item: &Value, complete: bool) -> Option<AgentEvent> {
    let (kind, text) = match item["type"].as_str()? {
        "userMessage" => (
            EventKind::User,
            item["content"]
                .as_array()?
                .iter()
                .filter_map(|x| x["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        "agentMessage" => (EventKind::Text, item["text"].as_str()?.into()),
        "plan" => (EventKind::Plan, item["text"].as_str().unwrap_or("").into()),
        "reasoning" => (
            EventKind::Reasoning,
            item["summary"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|s| s.as_str().or_else(|| s["text"].as_str()))
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        "commandExecution" => (
            EventKind::Tool,
            format!(
                "{} · {}\n{}",
                item["status"].as_str().unwrap_or("running"),
                item["command"]
                    .as_str()
                    .unwrap_or("Command")
                    .replace('\n', " "),
                item["aggregatedOutput"].as_str().unwrap_or("")
            ),
        ),
        "fileChange" | "mcpToolCall" | "collabAgentToolCall" | "webSearch" => (
            EventKind::Tool,
            format!(
                "{} · {}\n{}",
                item["type"].as_str()?,
                item["status"].as_str().unwrap_or("running"),
                serde_json::to_string_pretty(item).ok()?
            ),
        ),
        _ => return None,
    };
    if text.is_empty() {
        return None;
    }
    let origin = item["clientId"]
        .as_str()
        .or_else(|| item["clientUserMessageId"].as_str());
    // Answers and plan execution already update their own interaction card.
    if matches!(kind, EventKind::User) && origin.is_some_and(|id| id.starts_with("answer:")) {
        return None;
    }
    Some(AgentEvent {
        session: s.id.clone(),
        key: format!("item:{}", item["id"].as_str()?),
        kind,
        text,
        complete,
        request: None,
        origin: origin.map(str::to_owned),
    })
}
async fn observe(
    r: Arc<Runtime>,
    store: Store,
    events: broadcast::Sender<AgentEvent>,
    mut notices: broadcast::Receiver<Value>,
) {
    let mut streams: HashMap<String, String> = HashMap::new();
    let mut async_seen = HashSet::new();
    loop {
        let v = tokio::select! {_=r.cancel.cancelled()=>break,v=notices.recv()=>match v {Ok(v)=>v,Err(broadcast::error::RecvError::Lagged(n))=>{tracing::error!(n,"Agent stream lagged; reconcile using history");continue;},Err(_)=>break}};
        if v["method"] == "uaoi/disconnected" {
            r.subscriptions.lock().await.clear();
            r.starting.lock().await.clear();
            r.turns.lock().await.clear();
            let sessions: Vec<_> = r.sessions.lock().await.values().cloned().collect();
            for s in sessions {
                let _ = invalidate(&r, &store, &events, &s.native, None).await;
            }
            continue;
        }
        let result: Result<()> = async {
            let method = v["method"].as_str().unwrap_or("");
            let p = &v["params"];
            let native = p["threadId"]
                .as_str()
                .or_else(|| p["thread"]["id"].as_str())
                .unwrap_or("");
            if matches!(method, "thread/closed" | "thread/archived") {
                r.pending_bindings.lock().await.remove(native);
            }
            if matches!(
                method,
                "turn/started" | "turn/completed" | "thread/closed" | "thread/archived"
            ) {
                r.starting.lock().await.remove(native);
            }
            if method == "thread/name/updated" {
                if let Some(s) = store.native_session(&r.agent, native).await? {
                    store.set_title(&s, p["threadName"].as_str()).await?;
                }
                return Ok(());
            }
            if method == "thread/settings/updated" {
                r.modes.lock().await.insert(
                    native.into(),
                    p["threadSettings"]["collaborationMode"].clone(),
                );
                return Ok(());
            }
            let begins_work = method == "turn/started"
                || (["item/started", "item/completed"].contains(&method)
                    && p["item"]["type"] == "userMessage");
            let known = r.sessions.lock().await.contains_key(native);
            if begins_work && !known {
                let pending = r.pending_bindings.lock().await.contains_key(native);
                let bound = if let Some(s) = store.native_session(&r.agent, native).await? {
                    !store.targets(&s.id).await?.is_empty()
                } else {
                    false
                };
                if !pending && !bound {
                    return Ok(());
                }
                let s = attach(&r, &store, native).await?;
                if r.pending_bindings.lock().await.remove(native).is_some() {
                    announce(&store, &events, &s).await?;
                }
            }
            let Some(s) = r.sessions.lock().await.get(native).cloned() else {
                return Ok(());
            };
            match method {
                "thread/name/updated" => {
                    store.set_title(&s, p["threadName"].as_str()).await?;
                }
                "thread/closed" | "thread/archived" => {
                    invalidate(&r, &store, &events, native, None).await?;
                    r.sessions.lock().await.remove(native);
                    r.subscriptions.lock().await.remove(native);
                    r.starting.lock().await.remove(native);
                    r.turns.lock().await.remove(native);
                    emit(
                        &store,
                        &events,
                        AgentEvent {
                            session: s.id.clone(),
                            key: format!("offline:{}", uuid::Uuid::new_v4()),
                            kind: EventKind::Offline,
                            text: "Session offline; send a new message to resume".into(),
                            complete: true,
                            request: None,
                            origin: None,
                        },
                    )
                    .await?;
                }
                "turn/started" => {
                    r.starting.lock().await.remove(native);
                    if let Some(id) = p["turn"]["id"].as_str() {
                        r.turns.lock().await.insert(native.into(), id.into());
                    }
                    r.plans.lock().await.remove(native);
                    invalidate(&r, &store, &events, native, Some(RequestKind::Plan)).await?;
                }
                "serverRequest/resolved" => {
                    let id = r
                        .requests
                        .lock()
                        .await
                        .iter()
                        .find(|(_, q)| q.wire_id.as_ref() == Some(&p["requestId"]))
                        .map(|(id, _)| id.clone());
                    if let Some(id) = id {
                        finish(
                            &r,
                            &store,
                            &events,
                            &id,
                            "Request resolved or cleared in Codex",
                        )
                        .await?;
                    }
                }
                _ => {}
            }
            if v.get("id").is_some() {
                if [
                    "item/tool/requestUserInput",
                    "item/commandExecution/requestApproval",
                    "item/fileChange/requestApproval",
                ]
                .contains(&method)
                {
                    let id = uuid::Uuid::new_v4().simple().to_string();
                    let view = native_question(method, p, id);
                    let existing = r
                        .requests
                        .lock()
                        .await
                        .values()
                        .any(|q| q.wire_id.as_ref() == Some(&v["id"]));
                    if !existing && !r.answered.lock().await.contains(&v["id"].to_string()) {
                        add_request(
                            &r,
                            &store,
                            &events,
                            &s,
                            NativeRequest {
                                wire_id: Some(v["id"].clone()),
                                thread: native.into(),
                                turn: p["turnId"].as_str().unwrap_or("").into(),
                                view,
                            },
                        )
                        .await?;
                    }
                }
                return Ok(());
            }
            let mut event = None;
            if method == "item/started" || method == "item/completed" {
                let item = &p["item"];
                let complete = method == "item/completed";
                if item["type"] == "userMessage" {
                    r.plans.lock().await.remove(native);
                    invalidate(&r, &store, &events, native, Some(RequestKind::Plan)).await?;
                }
                if complete && item["type"] == "plan" {
                    r.plans.lock().await.insert(
                        native.into(),
                        (
                            p["turnId"].as_str().unwrap_or("").into(),
                            item["text"].as_str().unwrap_or("").into(),
                        ),
                    );
                }
                if item["type"] == "agentMessage" && item["questions"].is_array() {
                    // The question card replaces this item's text projection.
                    if !complete {
                        return Ok(());
                    }
                    if !async_seen.insert(format!("{}:{}", s.id, item["id"])) {
                        return Ok(());
                    }
                    let questions = item["questions"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .enumerate()
                        .map(|(i, q)| Question {
                            id: i.to_string(),
                            title: q["title"].as_str().unwrap_or("").into(),
                            secret: false,
                            options: q["options"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .filter_map(Value::as_str)
                                .map(|s| (s.into(), String::new()))
                                .collect(),
                        })
                        .collect();
                    let view = PendingRequest {
                        id: uuid::Uuid::new_v4().simple().to_string(),
                        label: "Questions · task continues".into(),
                        choices: vec![],
                        kind: RequestKind::AsyncQuestions,
                        questions,
                        blocking: false,
                    };
                    add_request(
                        &r,
                        &store,
                        &events,
                        &s,
                        NativeRequest {
                            wire_id: None,
                            thread: native.into(),
                            turn: p["turnId"].as_str().unwrap_or("").into(),
                            view,
                        },
                    )
                    .await?;
                } else {
                    event = item_event(&s, item, complete);
                }
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
                    } else if method.contains("plan") {
                        EventKind::Plan
                    } else {
                        EventKind::Text
                    },
                    text: text.clone(),
                    complete: false,
                    request: None,
                    origin: None,
                });
            } else if method == "turn/completed" {
                r.starting.lock().await.remove(native);
                r.turns.lock().await.remove(native);
                event = Some(AgentEvent {
                    session: s.id.clone(),
                    key: format!("turn:{}", p["turn"]["id"]),
                    kind: if p["turn"]["status"] == "failed" {
                        EventKind::Error
                    } else {
                        EventKind::Finished
                    },
                    text: format!(
                        "Turn {}{}",
                        p["turn"]["status"].as_str().unwrap_or("finished"),
                        p["turn"]
                            .get("error")
                            .filter(|e| !e.is_null())
                            .map(|e| format!("\n{e}"))
                            .unwrap_or_default()
                    ),
                    complete: true,
                    request: None,
                    origin: None,
                });
                let plan = r.plans.lock().await.remove(native);
                if let Some((turn, text)) = plan
                    && p["turn"]["status"] == "completed"
                    && p["turn"]["id"] == turn
                    && !text.trim().is_empty()
                    && r.modes
                        .lock()
                        .await
                        .get(native)
                        .is_some_and(|m| m["mode"] == "plan")
                {
                    let view = PendingRequest {
                        id: uuid::Uuid::new_v4().simple().to_string(),
                        label: "Implement this plan?".into(),
                        choices: vec![
                            ("Execute plan".into(), "execute".into()),
                            ("Stay in Plan mode".into(), "stay".into()),
                        ],
                        kind: RequestKind::Plan,
                        questions: vec![],
                        blocking: false,
                    };
                    add_request(
                        &r,
                        &store,
                        &events,
                        &s,
                        NativeRequest {
                            wire_id: None,
                            thread: native.into(),
                            turn,
                            view,
                        },
                    )
                    .await?;
                }
                streams.retain(|k, _| !k.starts_with(&format!("{}:", s.id)));
            }
            if let Some(e) = event {
                if matches!(e.kind, EventKind::User)
                    && let Some(origin) = &e.origin
                {
                    store.confirm_input(origin).await?;
                }
                emit(&store, &events, e).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%error,"Native event processing failed");
        }
    }
    for s in r.sessions.lock().await.values() {
        let _ = store.expire_requests(&s.id).await;
    }
    stop(&r).await;
}
async fn validate_creation(
    r: &Runtime,
    method: &str,
    params: &mut Value,
    fallback: &Path,
) -> Result<()> {
    let native = params["threadId"].as_str().unwrap_or("");
    let pending_turn =
        method == "turn/start" && r.pending_bindings.lock().await.contains_key(native);
    if !matches!(method, "thread/start" | "thread/fork") && !pending_turn {
        return Ok(());
    }
    if !params["threadSource"].is_null() && params["threadSource"] != "user" {
        return Ok(());
    }
    let cwd = if let Some(path) = params["cwd"].as_str() {
        let path = Path::new(path);
        if path.is_absolute() {
            path.to_owned()
        } else {
            fallback.join(path)
        }
    } else if pending_turn {
        r.pending_bindings
            .lock()
            .await
            .get(native)
            .cloned()
            .context("New session directory unavailable")?
    } else if !native.is_empty() {
        let thread = r
            .rpc
            .request(
                "thread/read",
                json!({"threadId":native,"includeTurns":false}),
            )
            .await?;
        PathBuf::from(
            thread["thread"]["cwd"]
                .as_str()
                .context("Session working directory unavailable")?,
        )
    } else {
        fallback.to_owned()
    };
    let (_, project) = r.config.project_for_cwd(&r.agent, &cwd)?;
    params["cwd"] = json!(project.cwd);
    Ok(())
}
async fn gateway(
    io: tokio::net::UnixStream,
    r: Arc<Runtime>,
    store: Store,
    events: broadcast::Sender<AgentEvent>,
    cancel: CancellationToken,
    permissions: codex_cli::Permissions,
    cwd: PathBuf,
) -> Result<()> {
    let down = tokio_tungstenite::accept_async_with_config(io, Some(rpc::limits())).await?;
    let up = rpc::connect(&r.engine).await?;
    let (mut ds, mut dr) = down.split();
    let (mut us, mut ur) = up.split();
    let mut lifecycle: HashMap<String, String> = HashMap::new();
    let mut owned_threads: HashSet<String> = HashSet::new();
    let mut questions: HashMap<String, (String, Value)> = HashMap::new();
    let mut starts: HashMap<String, String> = HashMap::new();
    loop {
        tokio::select! {
            _=cancel.cancelled()=>break,
            m=dr.next()=>match m {
                Some(Ok(Message::Text(t)))=>{
                    let mut v:Value=serde_json::from_str(&t)?;
                    let method=v["method"].as_str().unwrap_or("").to_owned();
                    if let Err(error) = validate_creation(&r, &method, &mut v["params"], &cwd).await {
                        ds.send(Message::Text(json!({"id":v["id"],"error":{"code":-32602,"message":error.to_string()}}).to_string().into())).await?;
                        continue;
                    }
                    if method=="turn/start" && let Some(native)=v["params"]["threadId"].as_str() {
                        r.starting.lock().await.insert(native.into());starts.insert(v["id"].to_string(),native.into());
                    }
                    if ["thread/start","thread/resume","thread/fork"].contains(&method.as_str()) {
                        lifecycle.insert(v["id"].to_string(),method.clone());
                        if v["params"]["threadSource"].is_null() || v["params"]["threadSource"]=="user" {
                            permissions.apply(&mut v["params"]);
                        }
                    }
                    if method.is_empty() && let Some((method,p))=questions.remove(&v["id"].to_string()) {
                        if !r.answered.lock().await.insert(v["id"].to_string()) {continue;}
                        let session=r.sessions.lock().await.get(p["threadId"].as_str().unwrap_or("")).cloned();
                        if let Some(s)=session {
                            let view=native_question(&method,&p,String::new());
                            let text=format!("Answer submitted via CLI\n{}",answer_text(&view,&v["result"]));
                            let id=r.requests.lock().await.iter().find(|(_,q)|q.wire_id.as_ref()==Some(&v["id"])).map(|(id,_)|id.clone());
                            if let Some(id)=id{finish(&r,&store,&events,&id,&text).await?;}else{emit(&store,&events,AgentEvent{session:s.id,key:format!("answer:{}",v["id"]),kind:EventKind::Status,text,complete:true,request:None,origin:None}).await?;}
                        }
                    }
                    us.send(Message::Text(v.to_string().into())).await?;
                },Some(Ok(Message::Ping(b)))=>{ds.send(Message::Pong(b)).await?;},Some(Ok(Message::Close(_)))|None=>break,Some(Err(e))=>return Err(e.into()),_=>{},
            },
            m=ur.next()=>match m {
                Some(Ok(Message::Text(t)))=>{
                    let v:Value=serde_json::from_str(&t)?;
                    if v.get("method").is_none() && let Some(native)=starts.remove(&v["id"].to_string()) && v.get("error").is_some() {r.starting.lock().await.remove(&native);}
                    if v.get("method").is_none() && let Some(method)=lifecycle.remove(&v["id"].to_string()) && v.get("error").is_none() && human_thread(&v["result"]["thread"]) && let Some(native)=v["result"]["thread"]["id"].as_str() {
                        let created=method!="thread/resume";
                        if created {r.pending_bindings.lock().await.insert(native.into(),PathBuf::from(v["result"]["thread"]["cwd"].as_str().context("Missing new session cwd")?));owned_threads.insert(native.into());}
                        let bound=if let Some(s)=store.native_session(&r.agent,native).await? { !store.targets(&s.id).await?.is_empty() } else {false};
                        if bound || (created && has_user_turn(&r,&v["result"]["thread"]).await.unwrap_or(false)) {
                            let s=attach(&r,&store,native).await?;
                            if r.pending_bindings.lock().await.remove(native).is_some() {announce(&store,&events,&s).await?;}
                        }
                        remember_mode(&r,native,&v["result"]).await;
                    }
                    if let Some(method)=v["method"].as_str() && v.get("id").is_some() && ["item/tool/requestUserInput","item/commandExecution/requestApproval","item/fileChange/requestApproval"].contains(&method) {
                        questions.insert(v["id"].to_string(),(method.into(),v["params"].clone()));
                    }
                    let native=v["params"]["threadId"].as_str().or_else(||v["params"]["thread"]["id"].as_str()).unwrap_or("");
                    if v.get("method").is_some() && !native.is_empty() && !r.subscriptions.lock().await.contains(native) {
                        let _=r.rpc.notices.send(v.clone());
                    }
                    if ds.send(Message::Text(t)).await.is_err(){break;}
                },Some(Ok(Message::Ping(b)))=>{us.send(Message::Pong(b)).await?;},Some(Ok(Message::Close(_)))|None=>break,Some(Err(e))=>return Err(e.into()),_=>{},
            }
        }
    }
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        let subscribed = r.subscriptions.lock().await;
        let sessions = r.sessions.lock().await;
        owned_threads.retain(|id| {
            !subscribed.contains(id)
                && (sessions.contains_key(id)
                    || r.starting
                        .try_lock()
                        .is_ok_and(|starting| starting.contains(id)))
        });
        drop(sessions);
        drop(subscribed);
        if owned_threads.is_empty() {
            break;
        }
        // A blank thread has no rollout and cannot be resumed onto the control
        // connection yet. Retain its original subscription after the TUI exits.
        tokio::select! {
            _=r.cancel.cancelled()=>break,
            _=tick.tick()=>{},
            message=ur.next()=>match message {
                Some(Ok(Message::Text(t)))=>{
                    if let Ok(v)=serde_json::from_str::<Value>(&t) {
                        let native=v["params"]["threadId"].as_str().or_else(||v["params"]["thread"]["id"].as_str()).unwrap_or("");
                        if owned_threads.contains(native) && !r.subscriptions.lock().await.contains(native) {let _=r.rpc.notices.send(v);}
                    }
                },
                Some(Ok(Message::Ping(b)))=>{if us.send(Message::Pong(b)).await.is_err(){break;}},
                Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                _=>{},
            }
        }
    }
    Ok(())
}
