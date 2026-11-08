mod interaction;
mod output;
use interaction::QuestionDraft;
#[cfg(test)]
pub(crate) use output::split_output;
use output::{Message, OutputState};

use crate::{
    agent::AgentAdapter,
    config::Config,
    im::{ImAdapter, SendFailure},
    model::*,
    store::Store,
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;

const MANAGEMENT_COMMANDS: &[(&str, &str, &str)] = &[
    (
        "agents",
        "/agents [running|text]",
        "List this project's bound sessions",
    ),
    ("new", "/new", "Create a session in this project"),
    (
        "status",
        "/status",
        "Show project or current-session status",
    ),
];
const TASK_COMMANDS: &[(&str, &str, &str)] = &[
    (
        "plan",
        "/plan [task]",
        "Toggle Plan mode, or start a planning task",
    ),
    (
        "default",
        "/default [task]",
        "Select Default mode, optionally starting a task",
    ),
    ("stop", "/stop", "Interrupt this session's task"),
    ("history", "/history [offset]", "Read earlier messages"),
];

pub struct Service {
    pub cfg: Arc<Config>,
    pub store: Store,
    pub agent: Arc<dyn AgentAdapter>,
    pub ims: Vec<Arc<dyn ImAdapter>>,
    routes: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    started: u64,
    drafts: Mutex<HashMap<String, QuestionDraft>>,
    outputs: Mutex<HashMap<(String, Conversation), OutputState>>,
    binding_gate: Mutex<()>,
}
impl Service {
    pub fn new(
        cfg: Arc<Config>,
        store: Store,
        agent: Arc<dyn AgentAdapter>,
        ims: Vec<Arc<dyn ImAdapter>>,
    ) -> Self {
        Self {
            cfg,
            store,
            agent,
            ims,
            routes: Mutex::new(HashMap::new()),
            drafts: Mutex::new(HashMap::new()),
            outputs: Mutex::new(HashMap::new()),
            binding_gate: Mutex::new(()),
            started: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
    fn active_agents(&self) -> Vec<String> {
        self.ims
            .iter()
            .filter(|im| im.enabled())
            .map(|im| im.agent().to_owned())
            .collect()
    }
    pub(crate) fn im(&self, name: &str) -> Result<&Arc<dyn ImAdapter>> {
        self.ims
            .iter()
            .find(|i| i.agent() == name)
            .context("IM adapter unavailable")
    }
    pub(crate) async fn expire_profile(&self, name: &str) -> Result<()> {
        let ids = self.store.expire_profile(name).await?;
        let mut drafts = self.drafts.lock().await;
        for id in ids {
            drafts.remove(&id);
        }
        drop(drafts);
        self.outputs
            .lock()
            .await
            .retain(|_, state| state.agent != name);
        Ok(())
    }
    async fn route_lock(&self, t: &Conversation) -> Arc<Mutex<()>> {
        self.routes
            .lock()
            .await
            .entry(format!("{}:{}:{}", t.platform, t.space, t.thread))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
    pub async fn handle(&self, msg: Inbound) -> Result<()> {
        let im = self.im(&msg.agent)?;
        if !im.enabled() || msg.sent_at < self.started.max(im.started()) {
            return Ok(());
        }
        let Some(project) = im.project(&msg.target) else {
            return Ok(());
        };
        let lock = self.route_lock(&msg.target).await;
        let _guard = lock.lock().await;
        let command = msg
            .text
            .split_whitespace()
            .next()
            .unwrap_or("")
            .split('@')
            .next()
            .unwrap_or("");
        let bound = self.store.bound(&msg.target).await?;
        let owner = match bound
            .as_ref()
            .filter(|_| !matches!(command, "/agents" | "/help"))
        {
            Some(session) => session.agent.as_str(),
            None => self.cfg.projects[project]
                .agents
                .iter()
                .find(|name| self.im(name).is_ok_and(|im| im.enabled()))
                .context("Project has no active agents")?
                .as_str(),
        };
        if !msg.addressed && owner != msg.agent {
            return Ok(());
        }
        let mut receipt = serde_json::to_value(&msg.target)?;
        // Input text is deliberately excluded: replies may contain sensitive data.
        receipt["action"] = json!(msg.action);
        let origin = serde_json::to_string(&receipt)?;
        if !self.store.claim_message(&msg.id, &origin).await? {
            let _ = self.im(&msg.agent)?.acknowledge(&msg, None).await;
            return Ok(());
        }
        let result = self.handle_once(&msg).await;
        let feedback = result.as_ref().err().map(ToString::to_string);
        match result {
            Ok(()) => self.store.operation(&msg.id, "accepted", &origin).await?,
            Err(e) => {
                receipt["error"] = json!(e.to_string());
                self.store
                    .operation(
                        &msg.id,
                        if e.is::<crate::agent::OutcomeUnknown>() {
                            "unknown"
                        } else {
                            "rejected"
                        },
                        &receipt.to_string(),
                    )
                    .await?;
                if msg.action.is_none() {
                    self.output_as(
                        &msg.agent,
                        &msg.target,
                        &format!("error:{}", msg.id),
                        Message {
                            text: &e.to_string(),
                            ..Default::default()
                        },
                    )
                    .await?;
                }
            }
        }
        if let Err(error) = self
            .im(&msg.agent)?
            .acknowledge(&msg, feedback.as_deref())
            .await
        {
            tracing::debug!(%error, "Interaction acknowledgement unavailable");
        }
        Ok(())
    }
    fn project_name(&self, target: &Conversation) -> Result<&str> {
        self.ims
            .iter()
            .find_map(|im| im.project(target))
            .context("Topic group is not a configured project")
    }
    fn group(&self, agent: &str, project: &str) -> Result<Conversation> {
        self.im(agent)?
            .destination(project)
            .context("Agent is not available in this project")
    }
    pub(crate) async fn initialize_agent(&self, name: &str) -> Result<()> {
        let mut menu = vec![(
            "help",
            "Help · Show all commands and their categories".to_owned(),
        )];
        for (category, commands) in [("Project", MANAGEMENT_COMMANDS), ("Session", TASK_COMMANDS)] {
            menu.extend(
                commands
                    .iter()
                    .map(|(name, _, description)| (*name, format!("{category} · {description}"))),
            );
        }
        let commands: Vec<_> = menu
            .iter()
            .map(|(name, description)| (*name, description.as_str()))
            .collect();
        let im = self.im(name)?;
        {
            for target in im.spaces() {
                let project = im
                    .project(&target)
                    .context("IM destination has no project")?;
                anyhow::ensure!(
                    self.cfg.projects[project]
                        .agents
                        .iter()
                        .any(|a| a == im.agent()),
                    "IM destination has an unavailable agent"
                );
            }
            if let Err(error) = im.set_commands(&commands).await {
                tracing::warn!(%error, "Command menu update failed");
            }
        }
        Ok(())
    }
    pub async fn initialize(&self) -> Result<()> {
        for binding in self.store.bindings().await?.1 {
            let project = self.project_name(&binding.target)?;
            anyhow::ensure!(
                self.group(&binding.session.agent, project)?.space == binding.target.space,
                "Binding project is unavailable; restore the project configuration before editing bindings"
            );
        }
        Ok(())
    }
    async fn help(&self, agent: &str, target: &Conversation, key: &str) -> Result<()> {
        let project = self.project_name(target)?;
        let mut text = format!(
            "Commands · {}\nDirectory: {}",
            project,
            self.cfg.projects[project].cwd.display()
        );
        for (category, commands) in [
            ("Project", MANAGEMENT_COMMANDS),
            ("Current session", TASK_COMMANDS),
        ] {
            text.push_str(&format!("\n\n{category}"));
            for (_, usage, description) in commands {
                text.push_str(&format!("\n{usage} — {description}"));
            }
        }
        text.push_str("\n\n/help — Show this project's commands in any topic\n\n/new creates a session in this project's configured directory. With multiple online agents, use /new@BotUsername to choose one. In a session topic, /new uses that topic's agent.");
        text.push_str("\nSession commands and ordinary messages use this topic's bound session. After /new or /agents, follow the topic link to work in that session. Manage bindings locally with uaoi bindings.");
        self.output_as(
            agent,
            target,
            key,
            Message {
                text: &text,
                ..Default::default()
            },
        )
        .await
    }
    async fn list_agents(
        &self,
        msg: &Inbound,
        agent: &str,
        search: &str,
        active: bool,
    ) -> Result<()> {
        let bindings = self.store.bindings_in_space(&msg.target).await?;
        let mut text = format!("Sessions · {}\n", self.project_name(&msg.target)?);
        let mut links = vec![];
        let mut count = 0;
        for binding in bindings {
            let info = if self.im(&binding.session.agent)?.enabled() {
                self.agent.describe(&binding.session).await
            } else {
                Err(anyhow::anyhow!("Agent is offline"))
            };
            let (title, state, cwd) = match info {
                Ok(info) => (info.title, info.status, info.cwd.display().to_string()),
                Err(_) => (
                    self.store.title(&binding.session).await?,
                    "unavailable".into(),
                    String::new(),
                ),
            };
            if (!search.is_empty()
                && !title.to_lowercase().contains(&search.to_lowercase())
                && !binding.session.native.contains(search))
                || (active && state != "active")
            {
                continue;
            }
            text.push('\n');
            let start = text.len();
            text.push_str(&title);
            links.push(TextLink {
                start,
                end: text.len(),
                url: self.im(&binding.session.agent)?.link(&binding.target),
            });
            let state = match state.as_str() {
                "notLoaded" => "Sleeping",
                "idle" => "Idle",
                "active" => "Working",
                "unavailable" => "Unavailable",
                other => other,
            };
            text.push_str(&format!("\n{} · {state}", binding.session.agent));
            if !cwd.is_empty() {
                text.push_str(&format!(" · {cwd}"));
            }
            text.push('\n');
            count += 1;
        }
        if count == 0 {
            text.push_str("\nNo matching bound sessions.");
        }
        self.output_as(
            agent,
            &msg.target,
            &msg.id,
            Message {
                text: &text,
                links,
                ..Default::default()
            },
        )
        .await
    }
    async fn open_agent(&self, msg: &Inbound, session: &Session) -> Result<()> {
        let target = self
            .store
            .targets(&session.id)
            .await?
            .into_iter()
            .next()
            .context("Session topic unavailable")?;
        let text = self.store.title(session).await?;
        self.output_as(
            &msg.agent,
            &msg.target,
            &msg.id,
            Message {
                text: &text,
                links: vec![TextLink {
                    start: 0,
                    end: text.len(),
                    url: self.im(&session.agent)?.link(&target),
                }],
                ..Default::default()
            },
        )
        .await
    }
    async fn show_status(&self, msg: &Inbound, agent: &str) -> Result<()> {
        let project = self.project_name(&msg.target)?;
        if let Some(session) = self.store.bound(&msg.target).await? {
            anyhow::ensure!(session.agent == agent, "Session belongs to another agent");
            let pending = self.store.unconsumed(&session.id).await?;
            let status = self.agent.status(&session).await?;
            self.output_as(agent, &msg.target, &msg.id, Message { text: &format!(
                "Project: {}\nAgent: {}\nSession: {}\nCWD: {}\nMode (next turn): {}\nState: {}\nUnconfirmed inputs: {}",
                project, session.agent, session.native, status.cwd.display(),
                status.mode.map(SessionMode::label).unwrap_or("Unknown"), status.state,
                pending.as_array().map_or(0, Vec::len)
            ), ..Default::default() }).await
        } else {
            let spec = &self.cfg.projects[project];
            let count = self.store.bindings_in_space(&msg.target).await?.len();
            let agents = spec
                .agents
                .iter()
                .map(|name| {
                    format!(
                        "{}: {}",
                        name,
                        if self.im(name).is_ok_and(|im| im.enabled()) {
                            "online"
                        } else {
                            "offline"
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            self.output_as(agent, &msg.target, &msg.id, Message { text: &format!(
                "Project: {}\nDirectory: {}\nBound sessions: {}\nAgents:\n{}\n\nUse /new to create a session, or /agents to open an existing session's topic.",
                project, spec.cwd.display(), count, agents
            ), ..Default::default() }).await
        }
    }
    async fn management_command(
        &self,
        msg: &Inbound,
        agent: &str,
        command: &str,
        arg: &str,
    ) -> Result<()> {
        match command {
            "/agents" => {
                self.list_agents(
                    msg,
                    agent,
                    if arg == "running" { "" } else { arg },
                    arg == "running",
                )
                .await
            }
            "/new" => {
                let project = self.project_name(&msg.target)?;
                let spec = &self.cfg.projects[project];
                let online = spec
                    .agents
                    .iter()
                    .filter(|name| self.im(name).is_ok_and(|im| im.enabled()))
                    .count();
                anyhow::ensure!(
                    msg.addressed || self.store.bound(&msg.target).await?.is_some() || online == 1,
                    "Multiple agents are online. Use /new@BotUsername to choose one"
                );
                let session = self.agent.create(agent, &spec.cwd).await?;
                self.ensure_topics_in(&session, project).await?;
                self.open_agent(msg, &session).await
            }
            "/status" => self.show_status(msg, agent).await,
            _ => anyhow::bail!("Unknown management command"),
        }
    }
    async fn handle_once(&self, msg: &Inbound) -> Result<()> {
        if msg.conversation_created {
            return Ok(());
        }
        let agent = msg.agent.as_str();
        let (command, arg) = msg
            .text
            .split_once(char::is_whitespace)
            .unwrap_or((&msg.text, ""));
        let command = command.split('@').next().unwrap_or(command);
        let arg = arg.trim();
        if let Some(id) = &msg.action {
            let action = self.store.read_action(id, &msg.target).await?;
            return self.question_action(msg, &action).await;
        }
        if let Some(reply) = &msg.reply_to
            && let Some(key) = self.store.reply_key(&msg.target, reply).await?
            && let Some(id) = key
                .split_once(":request:")
                .and_then(|(_, key)| key.rsplit_once(':'))
                .map(|(id, _)| id)
        {
            return self.answer_question_reply(msg, id).await;
        }
        if command == "/new" {
            anyhow::ensure!(
                arg.is_empty(),
                "Use /new without arguments; the current group selects the project"
            );
        } else if matches!(command, "/status" | "/stop" | "/help") {
            anyhow::ensure!(arg.is_empty(), "Use {command} without arguments");
        }
        if command.starts_with('/')
            && command != "/help"
            && !MANAGEMENT_COMMANDS
                .iter()
                .chain(TASK_COMMANDS)
                .any(|(name, _, _)| command.strip_prefix('/') == Some(*name))
        {
            anyhow::bail!("Unknown command. Use /help to see this project's commands");
        }
        if command == "/help" {
            return self.help(agent, &msg.target, &msg.id).await;
        }
        if MANAGEMENT_COMMANDS
            .iter()
            .any(|(name, _, _)| command.strip_prefix('/') == Some(*name))
        {
            return self.management_command(msg, agent, command, arg).await;
        }
        let session = self.store.bound(&msg.target).await?.context(
            "This topic has no session. Use /agents or /new, then follow the session's topic link",
        )?;
        anyhow::ensure!(session.agent == agent, "Session belongs to another agent");
        match command {
            "/plan" | "/default" => {
                let mode = if command == "/plan" {
                    SessionMode::Plan
                } else {
                    SessionMode::Default
                };
                if arg.is_empty() {
                    let mode = if command == "/plan" {
                        self.agent.toggle_plan_mode(&session).await?
                    } else {
                        self.agent.set_mode(&session, mode).await?;
                        mode
                    };
                    self.notice(
                        &msg.target,
                        &msg.id,
                        &format!("Mode: {} (next turn).", mode.label()),
                    )
                    .await
                } else {
                    self.submit_input(&session, arg, &msg.id, Some(mode)).await
                }
            }
            "/stop" => {
                self.agent.interrupt(&session).await?;
                self.notice(&msg.target, &msg.id, "Interrupt requested.")
                    .await
            }
            "/history" => {
                let offset = if arg.is_empty() {
                    0
                } else {
                    arg.parse::<usize>().context("Use /history [offset]")?
                };
                self.agent.history(&session).await?;
                for e in self.store.history(&session.id, 20, offset).await? {
                    // Historical questions are text only: never recreate old controls.
                    self.notice(
                        &msg.target,
                        &format!("history:{}:{}", msg.id, e.key),
                        &format!("{:?}: {}", e.kind, e.text),
                    )
                    .await?;
                }
                self.notice(
                    &msg.target,
                    &msg.id,
                    &format!("Older records: /history {}", offset + 20),
                )
                .await
            }
            _ => {
                anyhow::ensure!(!msg.text.is_empty(), "Only text input is supported");
                self.submit_input(&session, &msg.text, &msg.id, None).await
            }
        }
    }
    async fn submit_input(
        &self,
        session: &Session,
        text: &str,
        id: &str,
        mode: Option<SessionMode>,
    ) -> Result<()> {
        self.store
            .submission(id, &session.id, text, "unknown")
            .await?;
        let result = if let Some(mode) = mode {
            self.agent.submit_in_mode(session, text, id, mode).await
        } else {
            self.agent.submit(session, text, id).await
        };
        self.store
            .submission(
                id,
                &session.id,
                text,
                match &result {
                    Ok(()) => "submitted",
                    Err(e) if e.is::<crate::agent::OutcomeUnknown>() => "unknown",
                    Err(_) => "rejected",
                },
            )
            .await?;
        result
    }
    pub async fn ensure_topics(&self, s: &Session) -> Result<()> {
        if !self.store.targets(&s.id).await?.is_empty() {
            return Ok(());
        }
        let info = self.agent.describe(s).await?;
        let (project, _) = self.cfg.project_for_cwd(&s.agent, &info.cwd)?;
        self.ensure_topics_in(s, project).await
    }
    async fn ensure_topics_in(&self, s: &Session, project: &str) -> Result<()> {
        let _bindings = self.binding_gate.lock().await;
        let lock = self
            .route_lock(&Conversation {
                platform: "internal".into(),
                space: "topics".into(),
                thread: s.id.clone(),
            })
            .await;
        let _guard = lock.lock().await;

        let space = self.group(&s.agent, project)?;
        if self
            .store
            .targets(&s.id)
            .await?
            .iter()
            .any(|t| t.space == space.space && t.platform == space.platform)
        {
            return Ok(());
        }
        anyhow::ensure!(
            !self.store.was_bound(&s.id).await?,
            "Session was unbound; manage its bindings explicitly"
        );
        let key = format!("topic:{}:{}:{}", s.id, space.platform, space.space);
        anyhow::ensure!(
            self.store.claim(&key).await?,
            "Topic creation previously attempted; inspect recovery before retrying"
        );
        let title = self.store.title(s).await?;
        let target = self
            .im(&s.agent)?
            .create_conversation(&space, &title)
            .await?;
        self.store.bind(&target, &s.id).await?;
        self.store
            .title_applied(&TitleChange {
                agent: s.agent.clone(),
                target,
                title,
            })
            .await?;
        self.store.operation(&key, "accepted", "").await
    }
    pub(crate) async fn bindings_snapshot(&self) -> Result<Value> {
        let (revision, records) = self.store.bindings().await?;
        let mut bindings = vec![];
        let mut titles = vec![];
        for record in records {
            titles.push(self.store.title(&record.session).await?);
            bindings.push(BindingSpec {
                project: self.project_name(&record.target)?.into(),
                agent: record.session.agent,
                session: record.session.native,
                topic: Some(record.target.thread),
            });
        }
        Ok(json!({"document":BindingsDocument{version:3,revision,bindings},"titles":titles}))
    }
    pub(crate) async fn apply_bindings(&self, document: BindingsDocument) -> Result<()> {
        anyhow::ensure!(document.version == 3, "Binding document version 3 required");
        let _guard = self.binding_gate.lock().await;
        let (revision, old) = self.store.bindings().await?;
        anyhow::ensure!(
            revision == document.revision,
            "Bindings changed while editing; reopen the editor and retry"
        );
        let mut sessions = std::collections::HashSet::new();
        let mut targets = vec![];
        let mut prepared = vec![];
        for entry in document.bindings {
            anyhow::ensure!(
                self.cfg.agents.contains_key(&entry.agent),
                "Unknown agent: {}",
                entry.agent
            );
            anyhow::ensure!(
                !entry.session.trim().is_empty()
                    && sessions.insert((entry.agent.clone(), entry.session.clone())),
                "Duplicate or empty session binding"
            );
            let space = self.group(&entry.agent, &entry.project)?;
            let target = entry
                .topic
                .as_deref()
                .map(|id| self.im(&entry.agent)?.topic(&entry.project, id))
                .transpose()?
                .or_else(|| {
                    old.iter()
                        .find(|b| {
                            b.session.agent == entry.agent
                                && b.session.native == entry.session
                                && b.target.space == space.space
                        })
                        .map(|b| b.target.clone())
                });
            if let Some(t) = &target {
                anyhow::ensure!(
                    !targets.contains(t),
                    "A topic cannot be bound to multiple sessions"
                );
                targets.push(t.clone());
            }
            let unchanged = old
                .iter()
                .find(|record| {
                    record.session.agent == entry.agent
                        && record.session.native == entry.session
                        && target.as_ref() == Some(&record.target)
                })
                .cloned();
            anyhow::ensure!(
                unchanged.is_some() || self.im(&entry.agent)?.enabled(),
                "Agent {} is offline; open uaoi --agent {} before changing its bindings",
                entry.agent,
                entry.agent
            );
            prepared.push((entry, target, unchanged));
        }
        // Validate every native session before allocating any remote topics.
        let mut resolved = vec![];
        for (entry, target, unchanged) in prepared {
            let session = if let Some(record) = unchanged {
                record.session
            } else {
                self.agent.restore(&entry.agent, &entry.session).await?
            };
            resolved.push((session, entry.project, target));
        }
        let mut records = vec![];
        for (session, project, target) in resolved {
            let target = if let Some(target) = target {
                target
            } else {
                let space = self.group(&session.agent, &project)?;
                let key = format!("binding:{revision}:{project}:{}", session.id);
                if let Some((state, result)) = self.store.operation_record(&key).await? {
                    anyhow::ensure!(
                        state == "accepted",
                        "Topic creation outcome is unknown; inspect recovery before retrying"
                    );
                    serde_json::from_str(&result)?
                } else {
                    anyhow::ensure!(
                        self.store.claim(&key).await?,
                        "Topic creation is already in progress"
                    );
                    let target = self
                        .im(&session.agent)?
                        .create_conversation(&space, &self.store.title(&session).await?)
                        .await?;
                    self.store
                        .operation(&key, "accepted", &serde_json::to_string(&target)?)
                        .await?;
                    target
                }
            };
            anyhow::ensure!(
                !records.iter().any(|b: &BindingRecord| b.target == target),
                "A topic cannot be bound to multiple sessions"
            );
            records.push(BindingRecord { session, target });
        }
        self.store.replace_bindings(revision, records).await
    }
    pub async fn events(
        self: Arc<Self>,
        mut rx: broadcast::Receiver<AgentEvent>,
        cancel: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _=cancel.cancelled()=>break,
                e=rx.recv()=>match e{
                    Ok(e)=>{let result:Result<()>=async{
                        let s=self.store.session(&e.session).await?;
                        if !self.im(&s.agent)?.enabled() { return Ok(()); }
                    if matches!(e.kind, EventKind::Registered){
                        self.ensure_topics(&s).await?;
                    }
                        for t in self.store.targets(&e.session).await?{self.project(&e,&t).await?;}
                        Ok(())
                    }.await;if let Err(error)=result{tracing::error!(%error,"Event projection failed");}},
                    Err(broadcast::error::RecvError::Lagged(n))=>{tracing::error!(n,"Projection lagged; reconcile with /history");},
                    Err(_)=>break,
                }
            }
        }
    }
    pub(crate) async fn indicate_working(&self) -> Result<()> {
        for binding in self.store.bindings().await?.1 {
            if self.im(&binding.session.agent)?.enabled()
                && self.agent.working(&binding.session).await
            {
                let target = binding.target;
                if let Err(error) = self
                    .im(&binding.session.agent)?
                    .indicate_working(&target)
                    .await
                {
                    tracing::debug!(%error, "Activity indicator unavailable");
                }
            }
        }
        Ok(())
    }
    pub async fn activity(self: Arc<Self>, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(Duration::from_secs(4));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _=cancel.cancelled()=>break, _=tick.tick()=>{} }
            tokio::select! {
                _=cancel.cancelled()=>break,
                result=self.indicate_working()=>{
                    if let Err(error)=result {tracing::debug!(%error,"Read activity state");}
                }
            }
        }
    }
    pub async fn poll(
        self: Arc<Self>,
        im: Arc<dyn ImAdapter>,
        mut cursor: String,
        cancel: CancellationToken,
    ) {
        loop {
            let result = tokio::select! {_=cancel.cancelled()=>break,r=im.poll(&cursor)=>r};
            match result {
                Ok((messages, next)) => {
                    let mut failed = false;
                    for msg in messages {
                        if let Err(error) = self.handle(msg).await {
                            tracing::error!(%error,"Input handling failed");
                            failed = true;
                            break;
                        }
                    }
                    if failed {
                        tokio::select! {_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(3))=>{}}
                        continue;
                    }
                    cursor = next;
                }
                Err(error) => {
                    tracing::warn!(%error,"IM polling failed");
                    tokio::select! {_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(3))=>{}}
                }
            }
        }
    }
    pub async fn sync_titles(self: Arc<Self>, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {_=cancel.cancelled()=>break,_=tick.tick()=>{}}
            let pending = match self
                .store
                .pending_titles_for(Some(self.active_agents()))
                .await
            {
                Ok(p) => p,
                Err(error) => {
                    tracing::error!(%error,"Read pending titles");
                    continue;
                }
            };
            for change in pending {
                if cancel.is_cancelled() {
                    return;
                }
                // Coalesce rapid renames before entering the platform API.
                if !self.store.title_is_current(&change).await.unwrap_or(false) {
                    continue;
                }
                let Some(im) = self.im(&change.agent).ok().filter(|im| im.enabled()) else {
                    continue;
                };
                let outcome = tokio::select! {_=cancel.cancelled()=>return,r=im.rename_conversation(&change.target,&change.title)=>r};
                let result = match outcome {
                    Ok(()) => self.store.title_applied(&change).await,
                    Err(error) => {
                        let delay = match error {
                            SendFailure::Retry(delay) => delay,
                            SendFailure::Unknown(error) => {
                                tracing::warn!(%error,"Title sync uncertain; idempotent retry scheduled");
                                Duration::from_secs(3)
                            }
                            SendFailure::Permanent(error) => {
                                tracing::warn!(%error,"Title sync rejected; retry scheduled");
                                Duration::from_secs(60)
                            }
                        };
                        self.store.retry_title(&change, delay).await
                    }
                };
                if let Err(error) = result {
                    tracing::error!(%error,"Persist title sync");
                }
                tokio::select! {_=cancel.cancelled()=>return,_=tokio::time::sleep(Duration::from_millis(1100))=>{}}
            }
        }
    }
    pub async fn deliver(self: Arc<Self>, cancel: CancellationToken) {
        loop {
            tokio::select! {_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_millis(500))=>{}}
            let active = self.active_agents();
            let batch = match self.store.pending_for(Some(active)).await {
                Ok(v) => v,
                Err(error) => {
                    tracing::error!(%error,"Read outbox");
                    continue;
                }
            };
            for d in batch {
                if !self.store.delivery_current(&d).await.unwrap_or(false) {
                    continue;
                }
                if cancel.is_cancelled() {
                    return;
                }
                let Some(im) = self.im(&d.agent).ok().filter(|im| im.enabled()) else {
                    continue;
                };
                if self.store.delivery(&d, "sending", None).await.is_err() {
                    continue;
                }
                let outcome = im.deliver(&d).await;
                let update = match outcome {
                    Ok(id) => self.store.delivery(&d, "sent", Some(id)).await,
                    Err(SendFailure::Retry(delay)) => {
                        let update = self.store.delivery(&d, "pending", None).await;
                        tokio::select! {_=cancel.cancelled()=>return,_=tokio::time::sleep(delay)=>{}}
                        update
                    }
                    Err(SendFailure::Unknown(error)) => {
                        tracing::warn!(%error,id=d.id,"Delivery outcome unknown; not resending");
                        self.store.delivery(&d, "unknown", None).await
                    }
                    Err(SendFailure::Permanent(error)) => {
                        tracing::error!(%error,id=d.id,"Delivery rejected");
                        self.store.delivery(&d, "failed", None).await
                    }
                };
                if let Err(error) = update {
                    tracing::error!(%error,"Persist delivery receipt");
                }
                // Serialize sends to stay below per-chat edit limits.
                tokio::select! {_=cancel.cancelled()=>return,_=tokio::time::sleep(Duration::from_millis(1100))=>{}}
            }
        }
    }
    pub async fn control(&self, v: Value) -> Result<Value> {
        match v["method"].as_str().unwrap_or("") {
            "bindings" => self.bindings_snapshot().await,
            "bindings/apply" => {
                self.apply_bindings(serde_json::from_value(v["document"].clone())?)
                    .await?;
                self.bindings_snapshot().await
            }
            "recovery" => self.store.recovery().await,
            _ => anyhow::bail!("Unknown control method"),
        }
    }
}

fn shorten(text: &str, limit: usize) -> String {
    if text.encode_utf16().count() <= limit {
        return text.into();
    }
    let mut size = 0;
    let end = text
        .char_indices()
        .find_map(|(index, ch)| {
            size += ch.len_utf16();
            (size >= limit).then_some(index)
        })
        .unwrap_or(text.len());
    format!("{}…", &text[..end])
}
