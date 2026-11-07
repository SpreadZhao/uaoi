use crate::{
    agent::AgentAdapter,
    config::{Config, OutputMode},
    im::{ImAdapter, SendFailure},
    model::*,
    store::Store,
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;

const MANAGEMENT_COMMANDS: &[(&str, &str, &str)] = &[
    ("agents", "/agents [running|text]", "Query bound sessions"),
    (
        "new",
        "/new <absolute directory>",
        "Create a session and its topic",
    ),
];
const TASK_COMMANDS: &[(&str, &str, &str)] = &[
    (
        "status",
        "/status",
        "Show session, directory, mode, and state",
    ),
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
    ("stop", "/stop", "Interrupt the current task"),
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
    binding_gate: Mutex<()>,
}
#[derive(Clone, Default)]
struct QuestionDraft {
    current: usize,
    review: bool,
    submitting: bool,
    answers: BTreeMap<String, String>,
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
            binding_gate: Mutex::new(()),
            started: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
    fn im(&self, name: &str) -> Result<&Arc<dyn ImAdapter>> {
        self.ims
            .iter()
            .find(|i| i.agent() == name)
            .context("IM adapter unavailable")
    }
    pub async fn notice(&self, target: &Conversation, key: &str, text: &str) -> Result<()> {
        self.output(target, key, text, vec![]).await
    }
    async fn output(
        &self,
        target: &Conversation,
        key: &str,
        text: &str,
        buttons: Vec<(String, String)>,
    ) -> Result<()> {
        self.output_rows(
            target,
            key,
            text,
            buttons.into_iter().map(|button| vec![button]).collect(),
        )
        .await
    }
    async fn output_rows(
        &self,
        target: &Conversation,
        key: &str,
        text: &str,
        buttons: Vec<ButtonRow>,
    ) -> Result<()> {
        self.output_linked(target, key, text, buttons, vec![]).await
    }
    async fn output_linked(
        &self,
        target: &Conversation,
        key: &str,
        text: &str,
        buttons: Vec<ButtonRow>,
        links: Vec<TextLink>,
    ) -> Result<()> {
        let agent = if let Some(session) = self.store.bound(target).await? {
            session.agent
        } else {
            let project = self.project_name(target)?;
            self.cfg.projects[project].agents[0].clone()
        };
        self.output_linked_as(&agent, target, key, text, buttons, links)
            .await
    }
    async fn notice_as(
        &self,
        agent: &str,
        target: &Conversation,
        key: &str,
        text: &str,
    ) -> Result<()> {
        self.output_linked_as(agent, target, key, text, vec![], vec![])
            .await
    }
    async fn output_linked_as(
        &self,
        agent: &str,
        target: &Conversation,
        key: &str,
        text: &str,
        buttons: Vec<ButtonRow>,
        links: Vec<TextLink>,
    ) -> Result<()> {
        let chunks = split_output(text);
        let mut offset = 0;
        for (i, part) in chunks.into_iter().enumerate() {
            let end = offset + part.len();
            let part_links = links
                .iter()
                .filter(|link| link.start >= offset && link.end <= end)
                .map(|link| TextLink {
                    start: link.start - offset,
                    end: link.end - offset,
                    url: link.url.clone(),
                })
                .collect();
            self.store
                .enqueue(
                    agent,
                    target,
                    &format!("{key}:{i}"),
                    &part,
                    if i == 0 { buttons.clone() } else { vec![] },
                    part_links,
                )
                .await?;
            offset = end;
        }
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
        if !im.authorized(&msg.sender, &msg.target) || msg.sent_at < self.started {
            return Ok(());
        }
        let project = im
            .project(&msg.target)
            .context("Group is not configured for this Bot")?;
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
                .first()
                .context("Project has no agents")?
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
                    self.notice_as(
                        &msg.agent,
                        &msg.target,
                        &format!("error:{}", msg.id),
                        &e.to_string(),
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
    async fn button(
        &self,
        target: &Conversation,
        label: &str,
        body: Value,
    ) -> Result<(String, String)> {
        Ok((label.into(), self.store.action(target, body).await?))
    }
    async fn answer_question_reply(&self, msg: &Inbound, id: &str) -> Result<()> {
        let (_, req) = self.store.request(id).await.context(
            "This question has expired or was already answered; reply to a current question",
        )?;
        anyhow::ensure!(
            matches!(
                req.kind,
                RequestKind::Questions | RequestKind::AsyncQuestions
            ),
            "Use this card's buttons to choose a decision"
        );
        let draft = self
            .drafts
            .lock()
            .await
            .get(id)
            .cloned()
            .unwrap_or_default();
        anyhow::ensure!(
            !draft.review,
            "Select a question before replying with an answer"
        );
        let question = req
            .questions
            .get(draft.current)
            .context("Question unavailable")?;
        self.save_answer(
            msg,
            &json!({"request":id,"question":question.id}),
            &msg.text,
        )
        .await
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
    pub async fn initialize(&self) -> Result<()> {
        let mut menu = vec![(
            "help",
            "Help · Show all commands and their categories".to_owned(),
        )];
        for (category, commands) in [("Manage", MANAGEMENT_COMMANDS), ("Task", TASK_COMMANDS)] {
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
        for im in &self.ims {
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
        let mut text = format!("Commands · {}", self.project_name(target)?);
        for (category, commands) in [
            ("Management", MANAGEMENT_COMMANDS),
            ("Current task", TASK_COMMANDS),
        ] {
            text.push_str(&format!("\n\n{category}"));
            for (_, usage, description) in commands {
                text.push_str(&format!("\n{usage} — {description}"));
            }
        }
        text.push_str("\n\n/help — Show this help in any topic\n\nTask commands use this topic's bound session. Ordinary messages start or steer its work. Manage bindings locally with uaoi bindings.");
        text.push_str(
            "\nUse /new@BotUsername <absolute directory> to select an agent in an unbound topic.",
        );
        self.notice_as(agent, target, key, &text).await
    }
    async fn list_agents(
        &self,
        msg: &Inbound,
        agent: &str,
        search: &str,
        active: bool,
    ) -> Result<()> {
        let (_, bindings) = self.store.bindings().await?;
        let mut text = format!("Sessions · {}\n", self.project_name(&msg.target)?);
        let mut links = vec![];
        let mut count = 0;
        for binding in bindings.into_iter().filter(|b| {
            b.target.space == msg.target.space && b.target.platform == msg.target.platform
        }) {
            let info = self.agent.describe(&binding.session).await;
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
        self.output_linked_as(agent, &msg.target, &msg.id, &text, vec![], links)
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
        self.output_linked_as(
            &msg.agent,
            &msg.target,
            &msg.id,
            &text,
            vec![],
            vec![TextLink {
                start: 0,
                end: text.len(),
                url: self.im(&session.agent)?.link(&target),
            }],
        )
        .await
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
                let path = std::path::Path::new(arg);
                anyhow::ensure!(
                    !arg.is_empty() && path.is_absolute(),
                    "Use /new <absolute directory>"
                );
                let cwd = std::fs::canonicalize(path).context("Working directory must exist")?;
                anyhow::ensure!(cwd.is_dir(), "Working directory must be a directory");
                anyhow::ensure!(
                    msg.addressed || self.store.bound(&msg.target).await?.is_some(),
                    "Choose an agent explicitly: /new@BotUsername <absolute directory>"
                );
                let (project, _) = self.cfg.project_for_cwd(agent, &cwd)?;
                let session = self.agent.create(agent, &cwd).await?;
                self.ensure_topics_in(&session, project).await?;
                self.open_agent(msg, &session).await
            }
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
            "/status" => {
                let pending = self.store.unconsumed(&session.id).await?;
                let status = self.agent.status(&session).await?;
                self.notice(
                    &msg.target,
                    &msg.id,
                    &format!(
                        "Project: {}\nAgent: {}\nSession: {}\nCWD: {}\nMode (next turn): {}\nState: {}\nUnconfirmed inputs: {}",
                        self.project_name(&msg.target)?, session.agent,
                        session.native,
                        status.cwd.display(),
                        status.mode.map(SessionMode::label).unwrap_or("Unknown"),
                        status.state,
                        pending.as_array().map_or(0, Vec::len)
                    ),
                )
                .await
            }
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
                self.agent.history(&session).await?;
                let offset = if arg.is_empty() {
                    0
                } else {
                    arg.parse::<usize>().context("Use /history [offset]")?
                };
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
            c if c.starts_with('/') => {
                anyhow::bail!("Unknown command. Use /help to see all commands")
            }
            _ => {
                anyhow::ensure!(!msg.text.is_empty(), "Only text input is supported");
                self.submit_input(&session, &msg.text, &msg.id, None).await
            }
        }
    }
    async fn question_action(&self, msg: &Inbound, action: &Value) -> Result<()> {
        let id = action["request"]
            .as_str()
            .context("Invalid question button")?;
        let (sid, req) = self
            .store
            .request(id)
            .await
            .context("This question expired or was already answered")?;
        let session = self
            .store
            .bound(&msg.target)
            .await?
            .context("Session unavailable")?;
        anyhow::ensure!(sid == session.id, "Question belongs to another session");
        anyhow::ensure!(
            session.agent == msg.agent,
            "This topic is controlled by agent {}",
            session.agent
        );
        match action["op"].as_str().unwrap_or("") {
            "decision" => {
                self.agent
                    .respond(
                        &session,
                        id,
                        action["value"].as_str().context("Missing decision")?,
                    )
                    .await
            }
            "choice" => {
                self.save_answer(
                    msg,
                    action,
                    action["value"].as_str().context("Missing answer")?,
                )
                .await
            }
            "navigate" | "review" => {
                let mut drafts = self.drafts.lock().await;
                let draft = drafts.entry(id.into()).or_default();
                anyhow::ensure!(
                    !draft.submitting,
                    "Answer submission is pending; check /status or /history"
                );
                if action["op"] == "navigate" {
                    let current = action["question"].as_u64().context("Missing question")? as usize;
                    anyhow::ensure!(current < req.questions.len(), "Unknown question");
                    draft.current = current;
                    draft.review = false;
                } else {
                    draft.review = true;
                }
                drop(drafts);
                self.render_question(&session, &msg.target, &req).await
            }
            "submit" => {
                let draft = self
                    .drafts
                    .lock()
                    .await
                    .get(id)
                    .cloned()
                    .unwrap_or_default();
                anyhow::ensure!(
                    req.questions
                        .iter()
                        .all(|q| draft.answers.contains_key(&q.id)),
                    "Answer every question before submitting"
                );
                self.submit_answers(msg, &session, &req, &draft.answers)
                    .await
            }
            _ => anyhow::bail!("Unknown question action"),
        }
    }
    async fn submit_answers(
        &self,
        msg: &Inbound,
        session: &Session,
        req: &PendingRequest,
        answers: &BTreeMap<String, String>,
    ) -> Result<()> {
        {
            let mut drafts = self.drafts.lock().await;
            let draft = drafts.entry(req.id.clone()).or_default();
            anyhow::ensure!(
                !draft.submitting,
                "Answer submission is pending; check /status or /history"
            );
            draft.submitting = true;
        }
        self.render_question(session, &msg.target, req).await?;
        if let Err(error) = self
            .agent
            .respond(session, &req.id, &serde_json::to_string(answers)?)
            .await
        {
            if !error.is::<crate::agent::OutcomeUnknown>() {
                self.drafts
                    .lock()
                    .await
                    .entry(req.id.clone())
                    .or_default()
                    .submitting = false;
            }
            self.render_question(session, &msg.target, req).await?;
            return Err(error);
        }
        self.store.resolve(&req.id).await?;
        self.drafts.lock().await.remove(&req.id);
        self.notice(
            &msg.target,
            &format!("{}:request:{}", session.id, req.id),
            &answer_summary(req, answers, "Answer submitted via Telegram"),
        )
        .await
    }
    async fn save_answer(&self, msg: &Inbound, action: &Value, text: &str) -> Result<()> {
        let id = action["request"].as_str().context("Missing request")?;
        let (sid, req) = self.store.request(id).await?;
        let session = self
            .store
            .bound(&msg.target)
            .await?
            .context("Session unavailable")?;
        anyhow::ensure!(sid == session.id, "Question belongs to another session");
        anyhow::ensure!(
            session.agent == msg.agent,
            "This topic is controlled by agent {}",
            session.agent
        );
        let q = req
            .questions
            .iter()
            .find(|q| Some(q.id.as_str()) == action["question"].as_str())
            .context("Unknown question")?;
        anyhow::ensure!(!q.secret, "Answer sensitive questions in the CLI");
        anyhow::ensure!(!text.trim().is_empty(), "Answer must not be empty");
        {
            let mut drafts = self.drafts.lock().await;
            let draft = drafts.entry(id.into()).or_default();
            anyhow::ensure!(
                !draft.submitting,
                "Answer submission is pending; check /status or /history"
            );
            draft.answers.insert(q.id.clone(), text.into());
            draft.current = req
                .questions
                .iter()
                .position(|question| question.id == q.id)
                .unwrap_or(0);
            draft.review = false;
        }
        self.render_question(&session, &msg.target, &req).await
    }
    async fn render_question(
        &self,
        s: &Session,
        t: &Conversation,
        req: &PendingRequest,
    ) -> Result<()> {
        let draft = self
            .drafts
            .lock()
            .await
            .get(&req.id)
            .cloned()
            .unwrap_or_default();
        let status = if req.kind == RequestKind::AsyncQuestions {
            "Task continues while you answer"
        } else if req.blocking {
            "Waiting for your answer"
        } else {
            "Optional question; the agent may clear it automatically"
        };
        let count = req.questions.len();
        let answered = draft.answers.len();
        let key = format!("{}:request:{}", s.id, req.id);
        if draft.submitting {
            return self
                .notice(
                    t,
                    &key,
                    &answer_summary(
                        req,
                        &draft.answers,
                        "Answer submission pending; check /status or /history before retrying",
                    ),
                )
                .await;
        }
        anyhow::ensure!(count > 0, "No questions to answer");
        let mut rows = vec![];
        if count > 1 || draft.review {
            let mut navigation = vec![];
            for (index, q) in req.questions.iter().enumerate() {
                let label = format!(
                    "{}{} {}",
                    if !draft.review && draft.current == index {
                        "• "
                    } else {
                        ""
                    },
                    index + 1,
                    if draft.answers.contains_key(&q.id) {
                        "✓"
                    } else {
                        "○"
                    }
                );
                navigation.push(
                    self.button(
                        t,
                        &label,
                        json!({"op":"navigate","request":req.id,"question":index}),
                    )
                    .await?,
                );
            }
            rows.extend(navigation.chunks(6).map(|row| row.to_vec()));
        }
        let text = if draft.review {
            answer_summary(
                req,
                &draft.answers,
                &format!("Review answers · {answered}/{count} answered\n{status}"),
            )
        } else {
            let q = req
                .questions
                .get(draft.current)
                .context("Question unavailable")?;
            let mut text = format!(
                "Question {}/{} · {answered}/{count} answered\n{status}\n\n{}",
                draft.current + 1,
                count,
                shorten(&q.title, 1400)
            );
            if q.secret {
                text.push_str("\n\nSensitive question: answer in the CLI.");
            } else {
                for (index, (label, description)) in q.options.iter().enumerate() {
                    if !description.is_empty() {
                        text.push_str(&format!(
                            "\n{}. {} — {}",
                            index + 1,
                            shorten(label, 80),
                            shorten(description, 180)
                        ));
                    }
                    let selected = draft
                        .answers
                        .get(&q.id)
                        .is_some_and(|answer| answer == label);
                    rows.push(vec![
                        self.button(
                            t,
                            &format!("{}{}", if selected { "✓ " } else { "" }, shorten(label, 80)),
                            json!({"op":"choice","request":req.id,"question":q.id,"value":label}),
                        )
                        .await?,
                    ]);
                }
                text.push_str(&format!("\n\nCurrent answer: {}\nReply to this message to write or replace this answer.",
                    draft.answers.get(&q.id).map(|answer| shorten(answer, 600)).unwrap_or_else(|| "Not answered".into())));
            }
            text
        };
        if draft.review || count == 1 {
            if answered == count {
                rows.push(vec![
                    self.button(t, "Submit answers", json!({"op":"submit","request":req.id}))
                        .await?,
                ]);
            }
        } else {
            rows.push(vec![
                self.button(t, "Review answers", json!({"op":"review","request":req.id}))
                    .await?,
            ]);
        }
        self.output_rows(t, &key, &shorten(&text, 3500), rows).await
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
        Ok(json!({"document":BindingsDocument{version:2,revision,bindings},"titles":titles}))
    }
    pub(crate) async fn apply_bindings(&self, document: BindingsDocument) -> Result<()> {
        anyhow::ensure!(document.version == 2, "Binding document version 2 required");
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
            prepared.push((entry, target));
        }
        // Validate every native session before allocating any remote topics.
        let mut resolved = vec![];
        for (entry, target) in prepared {
            let session = self.agent.restore(&entry.agent, &entry.session).await?;
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
    pub(crate) async fn project(&self, e: &AgentEvent, t: &Conversation) -> Result<()> {
        let lock = self.route_lock(t).await;
        let _guard = lock.lock().await;
        if self
            .store
            .bound(t)
            .await?
            .is_none_or(|session| session.id != e.session)
        {
            return Ok(());
        }
        if e.request.is_none()
            && let Some(id) = e.key.strip_prefix("request:")
        {
            self.drafts.lock().await.remove(id);
            return self
                .notice(
                    t,
                    &format!("{}:{}", e.session, e.key),
                    &shorten(&e.text, 1200),
                )
                .await;
        }
        if matches!(
            e.kind,
            EventKind::Registered | EventKind::Finished | EventKind::Offline
        ) {
            return Ok(());
        }

        if matches!(e.kind, EventKind::User)
            && let Some(origin) = &e.origin
            && self.store.origin(origin).await?.as_ref() == Some(t)
        {
            return Ok(());
        }
        let s = self.store.session(&e.session).await?;
        if let Some(req) = &e.request {
            if matches!(
                req.kind,
                RequestKind::Questions | RequestKind::AsyncQuestions
            ) {
                return self.render_question(&s, t, req).await;
            }
            let mut buttons = vec![];
            for (label, value) in &req.choices {
                buttons.push(
                    self.button(
                        t,
                        label,
                        json!({"op":"decision","request":req.id,"value":value}),
                    )
                    .await?,
                );
            }
            return self
                .output(t, &format!("{}:{}", e.session, e.key), &e.text, buttons)
                .await;
        }
        let mode = self.cfg.agents[&s.agent].output;
        if mode == OutputMode::Compact && matches!(e.kind, EventKind::Reasoning) {
            return Ok(());
        }
        let (key, text) = if matches!(e.kind, EventKind::User) {
            (
                format!("{}:{}", e.session, e.key),
                format!("You · CLI\n\n{}", e.text),
            )
        } else if mode == OutputMode::Compact && matches!(e.kind, EventKind::Tool) {
            (
                format!("{}:activity", s.id),
                e.text
                    .lines()
                    .next()
                    .unwrap_or("Working")
                    .chars()
                    .take(240)
                    .collect(),
            )
        } else {
            (format!("{}:{}", e.session, e.key), e.text.clone())
        };
        self.output(t, &key, &text, vec![]).await
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
            if self.agent.working(&binding.session).await {
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
            let pending = match self.store.pending_titles().await {
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
                let Ok(im) = self.im(&change.agent) else {
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
            let batch = match self.store.pending().await {
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
                let Ok(im) = self.im(&d.agent) else {
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

fn answer_summary(
    req: &PendingRequest,
    answers: &BTreeMap<String, String>,
    heading: &str,
) -> String {
    let mut text = heading.to_owned();
    for (index, question) in req.questions.iter().enumerate() {
        let answer = if question.secret {
            "Answer in the CLI".into()
        } else {
            answers
                .get(&question.id)
                .map(|answer| shorten(answer, 600))
                .unwrap_or_else(|| "Not answered".into())
        };
        text.push_str(&format!(
            "\n\n{}. {}\n{}",
            index + 1,
            shorten(&question.title, 160),
            answer
        ));
    }
    shorten(&text, 3500)
}

pub(crate) fn split_output(mut text: &str) -> Vec<String> {
    let mut chunks = vec![];
    while !text.is_empty() {
        let mut size = 0;
        let mut end = text.len();
        let mut newline = None;
        for (i, c) in text.char_indices() {
            if size + c.len_utf16() > 3500 {
                end = i;
                break;
            }
            size += c.len_utf16();
            if c == '\n' {
                newline = Some(i + 1);
            }
        }
        if end < text.len()
            && let Some(line) = newline
        {
            end = line;
        }
        chunks.push(text[..end].to_owned());
        text = &text[end..];
    }
    chunks
}
