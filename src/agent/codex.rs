mod gateway;
use gateway::gateway;

use super::{
    AgentAdapter, OutcomeUnknown, codex_cli,
    rpc::{self, Rpc},
};
use crate::{config::Config, model::*, store::Store};
use anyhow::{Context, Result};
use async_trait::async_trait;
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
use tokio_util::sync::CancellationToken;

pub struct Codex {
    cfg: Arc<Config>,
    store: Store,
    events: broadcast::Sender<AgentEvent>,
    runtimes: Mutex<HashMap<String, Arc<Runtime>>>,
    instances: Mutex<HashMap<String, (CancellationToken, PathBuf)>>,
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
    threads: Mutex<HashMap<String, ThreadState>>,
    requests: Mutex<HashMap<String, NativeRequest>>,
    answered: Mutex<HashSet<String>>,
}
// A thread may be an unregistered draft, a cached session, or subscribed live.
// These facts are independent: opening a draft/history must not publish it.
#[derive(Default)]
struct ThreadState {
    session: Option<Session>,
    subscribed: bool,
    starting: bool,
    turn: Option<String>,
    mode: Option<Value>,
    plan: Option<(String, String)>,
    draft_directory: Option<PathBuf>,
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
    group: Option<i32>,
    dir: PathBuf,
}
impl Drop for AgentProcess {
    fn drop(&mut self) {
        if let Some(group) = self.group.take() {
            unsafe {
                libc::kill(-group, libc::SIGKILL);
            }
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
impl Codex {
    pub(crate) async fn active(&self, s: &Session) -> bool {
        let r = self.runtimes.lock().await.get(&s.agent).cloned();
        if let Some(r) = r {
            !r.cancel.is_cancelled()
                && r.rpc.connected()
                && r.threads
                    .lock()
                    .await
                    .get(&s.native)
                    .is_some_and(|thread| thread.session.is_some())
        } else {
            false
        }
    }

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
            instances: Mutex::new(HashMap::new()),
            cancel,
        }
    }
    async fn runtime(&self, agent: &str) -> Result<Arc<Runtime>> {
        self.runtimes
            .lock()
            .await
            .get(agent)
            .filter(|r| !r.cancel.is_cancelled())
            .cloned()
            .with_context(|| format!("Agent {agent} is offline; open uaoi --agent {agent} first"))
    }
    async fn start_runtime(
        &self,
        agent: &str,
        env: &std::collections::BTreeMap<String, String>,
    ) -> Result<Arc<Runtime>> {
        if let Some(r) = self.runtimes.lock().await.get(agent)
            && !r.cancel.is_cancelled()
        {
            return Ok(r.clone());
        }
        let old = self.runtimes.lock().await.remove(agent);
        if let Some(old) = old {
            stop(&old).await;
        }
        let spec = self.cfg.agents.get(agent).context("Unknown agent")?;
        let version = Command::new(&spec.command[0])
            .args(&spec.command[1..])
            .arg("--version")
            .env_clear()
            .envs(env)
            .envs(&spec.env)
            .kill_on_drop(true)
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
            .env_clear()
            .envs(env)
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
        let spawned = cmd.spawn()?;
        let mut child = AgentProcess {
            group: spawned.id().map(|pid| pid as i32),
            child: spawned,
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
            threads: Mutex::new(HashMap::new()),
            requests: Mutex::new(HashMap::new()),
            answered: Mutex::new(HashSet::new()),
        });
        tokio::spawn(observe(
            r.clone(),
            self.store.clone(),
            self.events.clone(),
            notices,
        ));
        tokio::spawn(supervise(r.clone()));
        self.runtimes.lock().await.insert(agent.into(), r.clone());
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
        r.threads
            .lock()
            .await
            .entry(s.native.clone())
            .or_default()
            .mode = Some(selected);
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
                r.threads
                    .lock()
                    .await
                    .entry(s.native.clone())
                    .or_default()
                    .mode = Some(mode);
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
    async fn activate(
        &self,
        agent: &str,
        env: &std::collections::BTreeMap<String, String>,
    ) -> Result<()> {
        self.start_runtime(agent, env).await?;
        for record in self
            .store
            .bindings()
            .await?
            .1
            .into_iter()
            .filter(|r| r.session.agent == agent)
        {
            if let Err(error) = self
                .restore(&record.session.agent, &record.session.native)
                .await
            {
                tracing::warn!(%error, session=record.session.native, "Bound session could not be attached");
            }
        }
        Ok(())
    }
    async fn deactivate(&self, agent: &str) {
        let runtime = self.runtimes.lock().await.remove(agent);
        if let Some(runtime) = runtime {
            runtime.cancel.cancel();
            stop(&runtime).await;
        }
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
        r.threads
            .lock()
            .await
            .entry(session.native.clone())
            .or_default()
            .subscribed = true;
        announce(&self.store, &self.events, &session).await?;
        Ok(session)
    }
    async fn busy(&self) -> bool {
        let runtimes: Vec<_> = self.runtimes.lock().await.values().cloned().collect();
        for r in runtimes {
            if r.cancel.is_cancelled() {
                continue;
            }
            if r.threads
                .lock()
                .await
                .values()
                .any(|thread| thread.starting)
            {
                return true;
            }
            // Status includes work in deliberately unbound sessions.
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
        instance: &str,
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
        self.instances
            .lock()
            .await
            .insert(instance.into(), (cancel.clone(), path.clone()));
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
    async fn close(&self, instance: &str) {
        if let Some((cancel, path)) = self.instances.lock().await.remove(instance) {
            cancel.cancel();
            let _ = std::fs::remove_file(path);
        }
    }
    async fn disconnected(&self, instance: &str) {
        let cancel = self
            .instances
            .lock()
            .await
            .get(instance)
            .map(|(c, _)| c.clone());
        if let Some(cancel) = cancel {
            cancel.cancelled().await;
        }
    }
    async fn working(&self, s: &Session) -> bool {
        if !self.active(s).await {
            return false;
        }
        let Some(r) = self.runtimes.lock().await.get(&s.agent).cloned() else {
            return false;
        };
        if r.cancel.is_cancelled()
            || !r
                .threads
                .lock()
                .await
                .get(&s.native)
                .is_some_and(|thread| thread.session.is_some())
        {
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
                .threads
                .lock()
                .await
                .get(&s.native)
                .and_then(|thread| thread.mode.clone())
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
            let text = async_answer_text(&pending.view, &result);
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
            .threads
            .lock()
            .await
            .get(&s.native)
            .and_then(|thread| thread.mode.as_ref())
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
        for (_, (cancel, path)) in self.instances.lock().await.drain() {
            cancel.cancel();
            let _ = std::fs::remove_file(path);
        }
        let agents: Vec<_> = self.runtimes.lock().await.keys().cloned().collect();
        for agent in agents {
            self.deactivate(&agent).await;
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
    r.threads
        .lock()
        .await
        .entry(native.into())
        .or_default()
        .mode = Some(mode);
}
async fn current_mode(r: &Runtime, s: &Session) -> Result<Value> {
    if r.threads
        .lock()
        .await
        .get(&s.native)
        .is_none_or(|thread| thread.mode.is_none())
    {
        let resumed = r
            .rpc
            .request(
                "thread/resume",
                json!({"threadId":s.native,"excludeTurns":true}),
            )
            .await?;
        remember_mode(r, &s.native, &resumed).await;
    }
    r.threads
        .lock()
        .await
        .get(&s.native)
        .and_then(|thread| thread.mode.clone())
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
    r.threads
        .lock()
        .await
        .entry(info.native)
        .or_default()
        .session = Some(session.clone());
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
    let existing = r
        .threads
        .lock()
        .await
        .get(native)
        .and_then(|thread| thread.session.clone());
    if let Some(s) = &existing
        && r.threads
            .lock()
            .await
            .get(native)
            .is_some_and(|thread| thread.subscribed)
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
            if let Some(thread) = r.threads.lock().await.get_mut(native) {
                thread.starting = false;
            }
            if e.to_string().contains("no rollout") {
                return Ok(s);
            }
            return Err(e);
        }
    };
    // Keep forwarding the creating CLI's events until the control connection
    // has actually subscribed; otherwise a short first turn can be lost.
    r.threads
        .lock()
        .await
        .entry(native.into())
        .or_default()
        .subscribed = true;
    remember_mode(r, native, &resumed).await;
    let _ = current_turn(r, native).await;
    Ok(s)
}
async fn start_turn(r: &Runtime, p: Value) -> Result<Value> {
    let thread = p["threadId"]
        .as_str()
        .context("Missing thread ID")?
        .to_owned();
    r.threads
        .lock()
        .await
        .entry(thread.clone())
        .or_default()
        .starting = true;
    let result = r.rpc.request("turn/start", p).await;
    if result.as_ref().is_err_and(|e| !e.is::<OutcomeUnknown>())
        && let Some(state) = r.threads.lock().await.get_mut(&thread)
    {
        state.starting = false;
    }
    result
}
async fn current_turn(r: &Runtime, native: &str) -> Result<Option<String>> {
    anyhow::ensure!(
        !r.threads
            .lock()
            .await
            .get(native)
            .is_some_and(|thread| thread.starting),
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
            return Ok(r
                .threads
                .lock()
                .await
                .get(native)
                .and_then(|thread| thread.turn.clone()));
        }
        Err(e) => return Err(e),
    };
    if snapshot["thread"]["status"]["type"] != "active" {
        if let Some(thread) = r.threads.lock().await.get_mut(native) {
            thread.turn = None;
        }
        return Ok(None);
    }
    if let Some(id) = r
        .threads
        .lock()
        .await
        .get(native)
        .and_then(|thread| thread.turn.clone())
    {
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
        r.threads
            .lock()
            .await
            .entry(native.into())
            .or_default()
            .turn = Some(id.clone());
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
    if let Some(group) = process.group.take() {
        unsafe {
            libc::kill(-group, libc::SIGTERM);
        }
        let _ = tokio::time::timeout(std::time::Duration::from_secs(3), process.child.wait()).await;
        // Also reap tool processes that outlived the app-server's leader.
        unsafe {
            libc::kill(-group, libc::SIGKILL);
        }
        let _ = process.child.wait().await;
    }
    let _ = std::fs::remove_dir_all(&process.dir);
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
        && let Some(s) = r
            .threads
            .lock()
            .await
            .get(&request.thread)
            .and_then(|thread| thread.session.clone())
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
fn async_answer_text(request: &PendingRequest, result: &Value) -> String {
    let replies: Vec<_> = request
        .questions
        .iter()
        .map(|q| {
            json!({
                "questionItemId": q.id,
                "question": q.title,
                "answer": result["answers"][&q.id]["answers"][0],
            })
        })
        .collect();
    // The native TUI uses this envelope to render answers and dismiss the
    // matching async editors, including when it replays the conversation.
    format!(
        "<send_user_message_question_reply>\n{}\n</send_user_message_question_reply>",
        json!(replies)
    )
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
// Publication is triggered by live messages, independently of how the thread
// was created or resumed. Persisted binding receipts preserve explicit unbinds.
async fn observe_message_session(
    r: &Arc<Runtime>,
    store: &Store,
    events: &broadcast::Sender<AgentEvent>,
    native: &str,
) -> Result<Option<Session>> {
    let cached = r
        .threads
        .lock()
        .await
        .get(native)
        .and_then(|thread| thread.session.clone());
    let existing = match &cached {
        Some(session) => Some(session.clone()),
        None => store.native_session(&r.agent, native).await?,
    };
    if let Some(session) = &existing
        && store.was_bound(&session.id).await?
    {
        if cached.is_some() {
            return Ok(existing);
        }
        if store.targets(&session.id).await?.is_empty() {
            return Ok(None);
        }
        return Ok(Some(attach(r, store, native).await?));
    }
    let thread = r
        .rpc
        .request(
            "thread/read",
            json!({"threadId":native,"includeTurns":false}),
        )
        .await?;
    let Some(info) = summary(&thread["thread"]) else {
        return Ok(None);
    };
    if r.config.project_for_cwd(&r.agent, &info.cwd).is_err() {
        return Ok(None);
    }
    let session = attach(r, store, native).await?;
    if let Some(thread) = r.threads.lock().await.get_mut(native) {
        thread.draft_directory = None;
    }
    announce(store, events, &session).await?;
    Ok(Some(session))
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
            let sessions: Vec<_> = {
                let mut threads = r.threads.lock().await;
                for thread in threads.values_mut() {
                    thread.subscribed = false;
                    thread.starting = false;
                    thread.turn = None;
                }
                threads
                    .values()
                    .filter_map(|thread| thread.session.clone())
                    .collect()
            };
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
            if matches!(method, "thread/closed" | "thread/archived")
                && let Some(thread) = r.threads.lock().await.get_mut(native)
            {
                thread.draft_directory = None;
            }
            if matches!(
                method,
                "turn/started" | "turn/completed" | "thread/closed" | "thread/archived"
            ) && let Some(thread) = r.threads.lock().await.get_mut(native)
            {
                thread.starting = false;
            }
            if method == "thread/name/updated" {
                if let Some(s) = store.native_session(&r.agent, native).await? {
                    store.set_title(&s, p["threadName"].as_str()).await?;
                }
                return Ok(());
            }
            if method == "thread/settings/updated" {
                r.threads
                    .lock()
                    .await
                    .entry(native.into())
                    .or_default()
                    .mode = Some(p["threadSettings"]["collaborationMode"].clone());
                return Ok(());
            }
            let known = r
                .threads
                .lock()
                .await
                .get(native)
                .is_some_and(|thread| thread.session.is_some());
            let message = (["item/started", "item/completed"].contains(&method)
                && ["userMessage", "agentMessage"]
                    .contains(&p["item"]["type"].as_str().unwrap_or("")))
                || (!known
                    && method == "item/agentMessage/delta"
                    && p["delta"].as_str().is_some_and(|text| !text.is_empty()));
            if message
                && observe_message_session(&r, &store, &events, native)
                    .await?
                    .is_none()
            {
                return Ok(());
            }
            let Some(s) = r
                .threads
                .lock()
                .await
                .get(native)
                .and_then(|thread| thread.session.clone())
            else {
                return Ok(());
            };
            match method {
                "thread/name/updated" => {
                    store.set_title(&s, p["threadName"].as_str()).await?;
                }
                "thread/closed" | "thread/archived" => {
                    invalidate(&r, &store, &events, native, None).await?;
                    if let Some(thread) = r.threads.lock().await.get_mut(native) {
                        thread.session = None;
                        thread.subscribed = false;
                        thread.starting = false;
                        thread.turn = None;
                    }
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
                    {
                        let mut threads = r.threads.lock().await;
                        let thread = threads.entry(native.into()).or_default();
                        thread.starting = false;
                        if let Some(id) = p["turn"]["id"].as_str() {
                            thread.turn = Some(id.into());
                        }
                        thread.plan = None;
                    }
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
                    if let Some(thread) = r.threads.lock().await.get_mut(native) {
                        thread.plan = None;
                    }
                    invalidate(&r, &store, &events, native, Some(RequestKind::Plan)).await?;
                }
                if complete && item["type"] == "plan" {
                    r.threads
                        .lock()
                        .await
                        .entry(native.into())
                        .or_default()
                        .plan = Some((
                        p["turnId"].as_str().unwrap_or("").into(),
                        item["text"].as_str().unwrap_or("").into(),
                    ));
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
                            id: json!(["request_user_input_async", item["id"], i]).to_string(),
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
                let plan = {
                    let mut threads = r.threads.lock().await;
                    threads.get_mut(native).and_then(|thread| {
                        thread.starting = false;
                        thread.turn = None;
                        thread.plan.take()
                    })
                };
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
                if let Some((turn, text)) = plan
                    && p["turn"]["status"] == "completed"
                    && p["turn"]["id"] == turn
                    && !text.trim().is_empty()
                    && r.threads
                        .lock()
                        .await
                        .get(native)
                        .and_then(|thread| thread.mode.as_ref())
                        .is_some_and(|mode| mode["mode"] == "plan")
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
    let sessions: Vec<_> = r
        .threads
        .lock()
        .await
        .values()
        .filter_map(|thread| thread.session.clone())
        .collect();
    for session in sessions {
        let _ = store.expire_requests(&session.id).await;
    }
    stop(&r).await;
}
