use crate::{
    agent::AgentAdapter,
    config::{AgentSpec, Config, OutputMode, ProjectSpec, ProjectTelegram, TelegramConfig},
    im::{ImAdapter, SendFailure},
    model::*,
    service::Service,
    store::Store,
};
use anyhow::Result;
use async_trait::async_trait;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Default)]
pub(crate) struct FakeAgent {
    online: Mutex<Vec<String>>,
    inputs: Mutex<Vec<String>>,
    submitted: Mutex<Vec<String>>,
    responses: Mutex<Vec<String>>,
    pub(crate) store: Option<Store>,
    pub(crate) lifecycle: Mutex<Vec<String>>,
    pub(crate) runtimes: Mutex<BTreeMap<String, tokio_util::sync::CancellationToken>>,
    instances: Mutex<BTreeMap<String, tokio_util::sync::CancellationToken>>,
    listed: Mutex<Vec<SessionSummary>>,
    described: Mutex<Vec<String>>,
    histories: Mutex<Vec<String>>,
    management: Mutex<Vec<serde_json::Value>>,
    modes: Mutex<BTreeMap<String, SessionMode>>,
    unavailable: Vec<String>,
}
#[async_trait]
impl AgentAdapter for FakeAgent {
    async fn activate(&self, name: &str, env: &BTreeMap<String, String>) -> Result<()> {
        self.lifecycle.lock().await.push(format!("start:{name}"));
        anyhow::ensure!(!env.contains_key("FAIL_ACTIVATE"), "Activation failed");
        self.runtimes
            .lock()
            .await
            .insert(name.into(), tokio_util::sync::CancellationToken::new());
        Ok(())
    }
    async fn deactivate(&self, name: &str) {
        self.lifecycle.lock().await.push(format!("stop:{name}"));
        if let Some(cancel) = self.runtimes.lock().await.remove(name) {
            cancel.cancel();
        }
    }
    async fn describe(&self, session: &Session) -> Result<SessionSummary> {
        self.described.lock().await.push(session.native.clone());
        Ok(self
            .listed
            .lock()
            .await
            .iter()
            .find(|s| s.native == session.native)
            .cloned()
            .unwrap_or(SessionSummary {
                native: session.native.clone(),
                title: session.native.clone(),
                cwd: "/tmp".into(),
                status: if self.online.lock().await.contains(&session.id) {
                    "active"
                } else {
                    "idle"
                }
                .into(),
            }))
    }
    async fn restore(&self, agent: &str, native: &str) -> Result<Session> {
        anyhow::ensure!(
            !self.unavailable.iter().any(|id| id == native),
            "Session unavailable"
        );
        self.management
            .lock()
            .await
            .push(serde_json::json!({"op":"restore","agent":agent,"native":native}));
        self.store
            .as_ref()
            .unwrap()
            .ensure_session(agent, native)
            .await
    }
    async fn create(&self, agent: &str, cwd: &std::path::Path) -> Result<Session> {
        self.management
            .lock()
            .await
            .push(serde_json::json!({"op":"create","agent":agent,"cwd":cwd}));
        self.store
            .as_ref()
            .unwrap()
            .ensure_session(agent, &uuid::Uuid::new_v4().to_string())
            .await
    }
    async fn busy(&self) -> bool {
        false
    }

    async fn open(
        &self,
        instance: &str,
        name: &str,
        cwd: &std::path::Path,
        env: &BTreeMap<String, String>,
        args: &[std::ffi::OsString],
    ) -> Result<LaunchPlan> {
        let runtime = self
            .runtimes
            .lock()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Offline"))?;
        self.instances
            .lock()
            .await
            .insert(instance.into(), runtime.child_token());
        let mut command = vec!["python3".into(), "-c".into(), "import os,sys,time; gate=sys.argv[1]; open(gate+'.pid','w').write(str(os.getpid()));\nwhile os.path.exists(gate): time.sleep(.02)".into()];
        command.extend(args.iter().map(|a| a.to_str().unwrap().to_owned()));
        Ok(LaunchPlan {
            command,
            env: env.clone(),
            cwd: cwd.into(),
        })
    }
    async fn close(&self, instance: &str) {
        if let Some(cancel) = self.instances.lock().await.remove(instance) {
            cancel.cancel();
        }
    }
    async fn disconnected(&self, instance: &str) {
        let cancel = self.instances.lock().await.get(instance).cloned();
        if let Some(cancel) = cancel {
            cancel.cancelled().await;
        }
    }
    async fn working(&self, s: &Session) -> bool {
        self.online.lock().await.contains(&s.id)
    }
    async fn submit(&self, session: &Session, t: &str, _: &str) -> Result<()> {
        if t == "uncertain" {
            return Err(crate::agent::OutcomeUnknown("connection lost".into()).into());
        }
        self.inputs.lock().await.push(t.into());
        self.submitted.lock().await.push(session.id.clone());
        Ok(())
    }
    async fn set_mode(&self, session: &Session, mode: SessionMode) -> Result<()> {
        self.modes.lock().await.insert(session.id.clone(), mode);
        Ok(())
    }
    async fn toggle_plan_mode(&self, session: &Session) -> Result<SessionMode> {
        let mut modes = self.modes.lock().await;
        let mode = match modes
            .get(&session.id)
            .copied()
            .unwrap_or(SessionMode::Default)
        {
            SessionMode::Default => SessionMode::Plan,
            SessionMode::Plan => SessionMode::Default,
        };
        modes.insert(session.id.clone(), mode);
        Ok(mode)
    }
    async fn submit_in_mode(
        &self,
        session: &Session,
        text: &str,
        input_id: &str,
        mode: SessionMode,
    ) -> Result<()> {
        anyhow::ensure!(
            !self.online.lock().await.contains(&session.id),
            "A turn is running"
        );
        self.set_mode(session, mode).await?;
        self.submit(session, text, input_id).await
    }
    async fn interrupt(&self, _: &Session) -> Result<()> {
        Ok(())
    }
    async fn respond(&self, _: &Session, _: &str, answer: &str) -> Result<()> {
        self.responses.lock().await.push(answer.into());
        if answer.contains("uncertain-answer") {
            return Err(crate::agent::OutcomeUnknown("connection lost".into()).into());
        }
        Ok(())
    }
    async fn history(&self, session: &Session) -> Result<()> {
        self.histories.lock().await.push(session.id.clone());
        Ok(())
    }
    async fn status(&self, session: &Session) -> Result<SessionStatus> {
        Ok(SessionStatus {
            cwd: "/tmp".into(),
            state: if self.online.lock().await.contains(&session.id) {
                "active"
            } else {
                "idle"
            }
            .into(),
            mode: Some(
                self.modes
                    .lock()
                    .await
                    .get(&session.id)
                    .copied()
                    .unwrap_or(SessionMode::Default),
            ),
        })
    }
    async fn shutdown(&self) {
        let names: Vec<_> = self.runtimes.lock().await.keys().cloned().collect();
        for name in names {
            self.deactivate(&name).await;
        }
    }
}
struct FakeIm;
#[async_trait]
impl ImAdapter for FakeIm {
    fn name(&self) -> String {
        "test".into()
    }
    fn spaces(&self) -> Vec<Conversation> {
        vec![Conversation {
            platform: "test".into(),
            space: "-1001".into(),
            thread: String::new(),
        }]
    }
    fn agent(&self) -> &str {
        "work"
    }
    fn project(&self, t: &Conversation) -> Option<&str> {
        (t.space == "-1001").then_some("demo")
    }
    fn destination(&self, agent: &str) -> Option<Conversation> {
        (agent == "demo").then(|| self.spaces().remove(0))
    }
    fn topic(&self, agent: &str, id: &str) -> Result<Conversation> {
        Ok(Conversation {
            thread: id.into(),
            ..self
                .destination(agent)
                .ok_or_else(|| anyhow::anyhow!("Unknown agent"))?
        })
    }
    fn link(&self, t: &Conversation) -> String {
        format!("test:{}", t.thread)
    }
    async fn discard_backlog(&self) -> Result<String> {
        Ok(String::new())
    }
    async fn poll(&self, _: &str) -> Result<(Vec<Inbound>, String)> {
        Ok((vec![], String::new()))
    }
    async fn create_conversation(&self, t: &Conversation, _: &str) -> Result<Conversation> {
        Ok(Conversation {
            thread: uuid::Uuid::new_v4().to_string(),
            ..t.clone()
        })
    }
    async fn rename_conversation(
        &self,
        _: &Conversation,
        _: &str,
    ) -> std::result::Result<(), SendFailure> {
        Ok(())
    }
    async fn deliver(&self, _: &Delivery) -> std::result::Result<String, SendFailure> {
        Ok("42".into())
    }
}
pub(crate) fn message(id: &str, t: &Conversation, text: &str) -> Inbound {
    Inbound {
        agent: "work".into(),
        addressed: false,
        id: id.into(),
        target: t.clone(),
        text: text.into(),
        conversation_created: false,
        action: None,
        acknowledgement: None,
        reply_to: None,
        sent_at: u64::MAX,
    }
}
fn card_action(card: &Delivery, label: &str) -> String {
    card.buttons
        .iter()
        .flat_map(|row| row.iter())
        .find(|(text, _)| text == label)
        .unwrap_or_else(|| panic!("Missing button {label}: {}", card.text))
        .1
        .clone()
}
async fn question_card(store: &Store, request: &str) -> Result<Delivery> {
    Ok(store
        .pending()
        .await?
        .into_iter()
        .find(|d| d.key.contains(&format!(":request:{request}:")))
        .expect("Question card"))
}
async fn click_card(svc: &Service, card: &Delivery, id: &str, label: &str) -> Result<()> {
    let mut input = message(id, &card.target, "");
    input.action = Some(card_action(card, label));
    svc.handle(input).await
}
pub(crate) fn fake_config() -> Config {
    Config {
        version: 4,
        projects: BTreeMap::from([(
            "demo".into(),
            ProjectSpec {
                cwd: "/tmp".into(),
                agents: vec!["work".into()],
                telegram: ProjectTelegram { group: -1001 },
            },
        )]),
        agents: BTreeMap::from([(
            "work".into(),
            AgentSpec {
                kind: "codex".into(),
                command: vec!["codex".into()],
                env: BTreeMap::new(),
                telegram: TelegramConfig {
                    token_env: Some("TEST_TOKEN".into()),
                    ..TelegramConfig::default()
                },
                output: OutputMode::Compact,
            },
        )]),
        ..Config::default()
    }
}
#[tokio::test]
async fn group_topics_and_deduplication() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let agent = Arc::new(FakeAgent {
        online: Mutex::new(vec![]),
        inputs: Mutex::new(vec![]),
        responses: Mutex::new(vec![]),
        ..Default::default()
    });
    let svc = Service::new(
        Arc::new(fake_config()),
        store.clone(),
        agent.clone(),
        vec![Arc::new(FakeIm)],
    );
    let a = store.ensure_session("work", "native-a").await?;
    let b = store.ensure_session("work", "native-b").await?;
    svc.ensure_topics(&a).await?;
    svc.ensure_topics(&b).await?;
    let ta = store.targets(&a.id).await?.remove(0);
    let tb = store.targets(&b.id).await?.remove(0);
    assert_ne!(ta.thread, tb.thread);
    svc.handle(message("a1", &ta, "first")).await?;
    svc.handle(message("a1", &ta, "first")).await?;
    svc.handle(message("b1", &tb, "second")).await?;
    let mut outside = ta.clone();
    outside.space = "-1002".into();
    svc.handle(message("outside", &outside, "ignored")).await?;
    outside.space = "2".into();
    svc.handle(message("private", &outside, "ignored")).await?;
    assert_eq!(*agent.inputs.lock().await, vec!["first", "second"]);
    svc.ensure_topics(&a).await?;
    assert_eq!(store.targets(&a.id).await?, vec![ta]);
    Ok(())
}

#[test]
fn rejects_old_database_and_config() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("old.sqlite");
    rusqlite::Connection::open(&path)?.execute("CREATE TABLE spaces(id TEXT)", [])?;
    assert!(Store::open(&path).is_err());
    for version in [3, 4, 5] {
        let path = dir.path().join(format!("v{version}.sqlite"));
        let db = rusqlite::Connection::open(&path)?;
        db.pragma_update(None, "user_version", version)?;
        assert!(Store::open(&path).is_err());
        assert_eq!(
            db.pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))?,
            version
        );
    }
    let config = dir.path().join("config.toml");
    let mut cfg = fake_config();
    for version in [1, 2, 3] {
        cfg.version = version;
        std::fs::write(&config, toml::to_string(&cfg)?)?;
        assert!(Config::load(&config).is_err());
    }
    cfg.version = 4;
    std::fs::write(&config, toml::to_string(&cfg)?)?;
    Config::load(&config)?;
    let mut old = toml::Value::try_from(cfg)?;
    old["agents"]["work"]
        .as_table_mut()
        .unwrap()
        .insert("workspaces".into(), toml::Value::Table(Default::default()));
    std::fs::write(&config, toml::to_string(&old)?)?;
    assert!(Config::load(&config).is_err());
    assert!(
        toml::from_str::<Config>("[projects.a]\nworkspace='/'\nagent='a'\napproval_policy='never'")
            .is_err()
    );
    Ok(())
}
#[test]
fn wrapper_arguments_and_agent_selection() -> Result<()> {
    use crate::agent::codex_cli::invocation;
    let dir = tempfile::tempdir()?;
    let parse = |args: &[&str]| {
        invocation(
            &args
                .iter()
                .map(std::ffi::OsString::from)
                .collect::<Vec<_>>(),
            dir.path(),
        )
    };
    assert!(parse(&["agents"])?.0);
    assert!(!parse(&["exec", "--help"])?.0);
    assert!(parse(&["--remote", "unix://other"]).is_err());
    assert!(parse(&["--no-daemon"]).is_err());
    let mut cfg = fake_config();
    assert!(cfg.select_agent(None).is_err());
    assert_eq!(cfg.select_agent(Some("work"))?, "work");
    cfg.agents
        .insert("personal".into(), cfg.agents["work"].clone());
    assert!(cfg.select_agent(None).is_err());
    assert_eq!(cfg.select_agent(Some("personal"))?, "personal");
    Ok(())
}

#[tokio::test]
async fn large_frames_are_bounded() -> Result<()> {
    use crate::agent::rpc;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let (a, b) = tokio::io::duplex(1024 * 1024);
    let server = tokio::spawn(async move {
        let mut s = tokio_tungstenite::accept_async_with_config(a, Some(rpc::limits())).await?;
        let frame = s.next().await.unwrap()?;
        assert_eq!(frame.len(), 25_706_480);
        Ok::<_, anyhow::Error>(())
    });
    let (mut client, _) =
        tokio_tungstenite::client_async_with_config("ws://localhost/", b, Some(rpc::limits()))
            .await?;
    client
        .send(Message::Text("x".repeat(25_706_480).into()))
        .await?;
    server.await??;
    let (a, mut b) = tokio::io::duplex(1024);
    let server = tokio::spawn(async move {
        let mut stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
            a,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            Some(rpc::limits()),
        )
        .await;
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            tokio_tungstenite::tungstenite::Error::Capacity(_)
        ));
    });
    use tokio::io::AsyncWriteExt;
    let mut header = vec![0x82, 0xff];
    header.extend_from_slice(&(65_u64 * 1024 * 1024).to_be_bytes());
    header.extend_from_slice(&[0, 0, 0, 0]);
    b.write_all(&header).await?;
    server.await?;
    Ok(())
}
#[path = "native_tests.rs"]
mod native_tests;
#[path = "project_tests.rs"]
mod project_tests;

#[test]
fn output_preserves_lines_and_unicode() {
    let text = (0..1200).map(|i| format!("{i}: 🦀\n")).collect::<String>();
    let chunks = crate::service::split_output(&text);
    assert!(chunks.len() > 1);
    assert_eq!(chunks.concat(), text);
    assert!(chunks.iter().all(|c| c.encode_utf16().count() <= 3500));
    assert!(
        chunks
            .iter()
            .take(chunks.len() - 1)
            .all(|c| c.ends_with('\n'))
    );
    let text = "🦀".repeat(4000);
    assert_eq!(crate::service::split_output(&text).concat(), text);
}

#[test]
fn remote_permissions_do_not_block_native_navigation() -> Result<()> {
    use crate::agent::codex_cli::remote_arguments;
    let parse =
        |a: &[&str]| remote_arguments(&a.iter().map(std::ffi::OsString::from).collect::<Vec<_>>());
    let (tui, p) = parse(&[
        "-a=never",
        "--sandbox=read-only",
        "--model",
        "-strange-model",
        "resume",
        "some-id",
    ])?;
    assert_eq!(
        tui,
        vec!["--model", "-strange-model", "resume", "some-id"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect::<Vec<_>>()
    );
    let mut params = serde_json::json!({"config":{"default_permissions":"unrelated","model":"test"},"permissions":"old"});
    p.apply(&mut params);
    assert_eq!(params["approvalPolicy"], "never");
    assert_eq!(params["sandbox"], "read-only");
    assert!(params["permissions"].is_null());
    assert!(params["config"].get("default_permissions").is_none());
    assert_eq!(params["config"]["model"], "test");
    let (tui, p) = parse(&[
        "-c",
        "approval_policy=\"on-request\"",
        "--config=sandbox_mode='workspace-write'",
        "resume",
    ])?;
    assert_eq!(tui, vec![std::ffi::OsString::from("resume")]);
    assert_eq!(p.approval.as_deref(), Some("on-request"));
    assert_eq!(p.sandbox.as_deref(), Some("workspace-write"));
    assert_eq!(
        parse(&["--dangerously-bypass-approvals-and-sandbox"])?
            .1
            .sandbox
            .as_deref(),
        Some("danger-full-access")
    );
    assert_eq!(
        parse(&["--approve-for-me"])?.1.reviewer.as_deref(),
        Some("auto_review")
    );
    assert!(parse(&["-a", "never", "--dangerously-bypass-approvals-and-sandbox"]).is_err());
    assert!(parse(&["--sandbox", "unknown"]).is_err());
    assert_eq!(parse(&["--", "--sandbox", "read-only"])?.0.len(), 3);
    Ok(())
}

#[derive(Default)]
struct TitleIm {
    created: Mutex<Vec<String>>,
    renamed: Mutex<Vec<String>>,
    fail_once: std::sync::atomic::AtomicBool,
    working: Mutex<Vec<Conversation>>,
}
#[async_trait]
impl ImAdapter for TitleIm {
    fn name(&self) -> String {
        FakeIm.name()
    }
    fn spaces(&self) -> Vec<Conversation> {
        FakeIm.spaces()
    }
    fn agent(&self) -> &str {
        FakeIm.agent()
    }
    fn project(&self, t: &Conversation) -> Option<&str> {
        FakeIm.project(t)
    }
    fn destination(&self, agent: &str) -> Option<Conversation> {
        FakeIm.destination(agent)
    }
    fn topic(&self, agent: &str, id: &str) -> Result<Conversation> {
        FakeIm.topic(agent, id)
    }
    fn link(&self, target: &Conversation) -> String {
        FakeIm.link(target)
    }
    async fn discard_backlog(&self) -> Result<String> {
        FakeIm.discard_backlog().await
    }
    async fn poll(&self, c: &str) -> Result<(Vec<Inbound>, String)> {
        FakeIm.poll(c).await
    }
    async fn create_conversation(&self, t: &Conversation, title: &str) -> Result<Conversation> {
        self.created.lock().await.push(title.into());
        FakeIm.create_conversation(t, title).await
    }
    async fn rename_conversation(
        &self,
        _: &Conversation,
        title: &str,
    ) -> std::result::Result<(), SendFailure> {
        self.renamed.lock().await.push(title.into());
        if self
            .fail_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(SendFailure::Retry(std::time::Duration::from_millis(1)));
        }
        Ok(())
    }
    async fn deliver(&self, d: &Delivery) -> std::result::Result<String, SendFailure> {
        FakeIm.deliver(d).await
    }
    async fn indicate_working(&self, target: &Conversation) -> Result<()> {
        self.working.lock().await.push(target.clone());
        Ok(())
    }
}
#[tokio::test]
async fn title_sync_creation_latest_retry_and_restart() -> Result<()> {
    use tokio_util::sync::CancellationToken;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("state.sqlite");
    let store = Store::open(&path)?;
    let s = store.ensure_session("work", "native-title").await?;
    store.set_title(&s, Some("Existing session title")).await?;
    let im = Arc::new(TitleIm::default());
    let agent = Arc::new(FakeAgent {
        online: Mutex::new(vec![]),
        inputs: Mutex::new(vec![]),
        responses: Mutex::new(vec![]),
        ..Default::default()
    });
    let svc = Arc::new(Service::new(
        Arc::new(fake_config()),
        store.clone(),
        agent.clone(),
        vec![im.clone()],
    ));
    svc.ensure_topics(&s).await?;
    assert_eq!(*im.created.lock().await, vec!["Existing session title"]);
    assert!(store.pending_titles().await?.is_empty());
    let target = store.targets(&s.id).await?.remove(0);
    im.fail_once
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store.set_title(&s, Some("Intermediate")).await?;
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(svc.clone().sync_titles(cancel.clone()));
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while im.renamed.lock().await.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;
    // A later title replaces the failed attempt and clears its retry backoff.
    store.set_title(&s, Some("Latest title 中文 🦀")).await?;
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        while !store.pending_titles().await.unwrap().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;
    assert_eq!(
        *im.renamed.lock().await,
        vec!["Intermediate", "Latest title 中文 🦀"]
    );
    cancel.cancel();
    worker.await?;
    // The most recent desired title survives a broker restart, even while offline.
    store.set_title(&s, Some("After restart")).await?;
    let reopened = Store::open(&path)?;
    assert_eq!(
        reopened.pending_titles().await?,
        vec![TitleChange {
            agent: "work".into(),
            target: target.clone(),
            title: "After restart".into()
        }]
    );
    let svc = Arc::new(Service::new(
        Arc::new(fake_config()),
        reopened.clone(),
        agent,
        vec![im.clone()],
    ));
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(svc.sync_titles(cancel.clone()));
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !reopened.pending_titles().await.unwrap().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;
    cancel.cancel();
    worker.await?;
    assert_eq!(im.renamed.lock().await.last().unwrap(), "After restart");
    assert_eq!(reopened.targets(&s.id).await?, vec![target]);
    assert!(reopened.history(&s.id, 20, 0).await?.is_empty());
    Ok(())
}
#[tokio::test]
async fn title_sync_handles_updates_during_api_calls() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let a = store.ensure_session("work", "native-a").await?;
    let b = store.ensure_session("work", "native-b").await?;
    let mut ta = FakeIm.spaces().remove(0);
    ta.thread = "1".into();
    let mut tb = ta.clone();
    tb.thread = "2".into();
    store.set_title(&a, Some("A")).await?;
    store.bind(&ta, &a.id).await?;
    store.bind(&tb, &b.id).await?;
    let original = TitleChange {
        agent: "work".into(),
        target: ta.clone(),
        title: "A".into(),
    };
    store.title_applied(&original).await?;
    store.set_title(&a, Some("B")).await?;
    let inflight = store
        .pending_titles()
        .await?
        .into_iter()
        .find(|t| t.target == ta)
        .unwrap();
    store.set_title(&a, Some("A")).await?;
    assert!(!store.title_is_current(&inflight).await?);
    // B reached Telegram after the source returned to A. The acknowledgement
    // must reflect B so A remains pending rather than incorrectly looking synced.
    store.title_applied(&inflight).await?;
    assert!(store.pending_titles().await?.contains(&original));
    store
        .retry_title(&inflight, std::time::Duration::from_secs(3600))
        .await?;
    assert!(store.title_is_current(&original).await?);
    store.set_title(&a, None).await?;
    assert_eq!(store.title(&a).await?, "[work] native-a");
    assert_eq!(store.title(&b).await?, "[work] native-b");
    Ok(())
}

#[tokio::test]
async fn single_question_preserves_edits_until_explicit_submission() -> Result<()> {
    for custom_reply in [false, true] {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("state.sqlite"))?;
        let agent = Arc::new(FakeAgent::default());
        let svc = Service::new(
            Arc::new(fake_config()),
            store.clone(),
            agent.clone(),
            vec![Arc::new(FakeIm)],
        );
        let session = store.ensure_session("work", "questions").await?;
        svc.ensure_topics(&session).await?;
        let target = store.targets(&session.id).await?.remove(0);
        let event = AgentEvent {
            session: session.id.clone(),
            key: "request:question-id".into(),
            kind: EventKind::Question,
            text: "Choose".into(),
            complete: true,
            origin: None,
            request: Some(PendingRequest {
                id: "question-id".into(),
                label: "Choose".into(),
                choices: vec![],
                kind: RequestKind::Questions,
                blocking: true,
                questions: vec![Question {
                    id: "color".into(),
                    title: "Which color?".into(),
                    options: vec![("Blue".into(), "Cool".into())],
                    secret: false,
                }],
            }),
        };
        store.event(&event).await?;
        svc.project(&event, &target).await?;
        let initial = question_card(&store, "question-id").await?;
        let choice = card_action(&initial, "Blue");
        assert!(choice.len() <= 64);
        assert!(initial.text.contains("Reply to this message"));
        store.delivery(&initial, "sent", Some("77".into())).await?;
        if custom_reply {
            let mut input = message("answer-1", &target, "Custom green");
            input.reply_to = Some("77".into());
            svc.handle(input).await?;
        } else {
            click_card(&svc, &initial, "answer-1", "Blue").await?;
        }
        assert!(agent.responses.lock().await.is_empty());
        assert!(store.request("question-id").await.is_ok());
        let edited = question_card(&store, "question-id").await?;
        assert_eq!(edited.id, initial.id);
        assert_eq!(edited.message.as_deref(), Some("77"));
        let answer = if custom_reply {
            click_card(&svc, &edited, "answer-2", "Blue").await?;
            "Blue"
        } else {
            let mut input = message("answer-2", &target, "自定义紫色");
            input.reply_to = Some("77".into());
            svc.handle(input).await?;
            "自定义紫色"
        };
        let edited = question_card(&store, "question-id").await?;
        assert!(edited.text.contains(answer));
        click_card(&svc, &edited, "submit", "Submit answers").await?;
        assert_eq!(
            *agent.responses.lock().await,
            vec![serde_json::json!({"color":answer}).to_string()]
        );
        assert!(store.request("question-id").await.is_err());
        assert!(agent.inputs.lock().await.is_empty());
        let closed = question_card(&store, "question-id").await?;
        assert_eq!(closed.id, initial.id);
        assert!(closed.buttons.is_empty());
        assert!(closed.text.contains("Answer submitted via Telegram"));
        assert!(closed.text.contains(answer));
        let before = store.pending().await?.len();
        click_card(&svc, &edited, "duplicate-submit", "Submit answers").await?;
        assert_eq!(agent.responses.lock().await.len(), 1);
        assert_eq!(
            store.pending().await?.len(),
            before,
            "expired callbacks must not add chat messages"
        );
        let mut wrong = target.clone();
        wrong.thread = "wrong".into();
        assert!(store.read_action(&choice, &wrong).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn compact_projection_and_restart_expire_controls() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("state.sqlite");
    let store = Store::open(&path)?;
    let agent = Arc::new(FakeAgent {
        online: Mutex::new(vec![]),
        inputs: Mutex::new(vec![]),
        responses: Mutex::new(vec![]),
        ..Default::default()
    });
    let svc = Service::new(
        Arc::new(fake_config()),
        store.clone(),
        agent,
        vec![Arc::new(FakeIm)],
    );
    let s = store.ensure_session("work", "projection").await?;
    svc.ensure_topics(&s).await?;
    let t = store.targets(&s.id).await?.remove(0);
    let mut e = AgentEvent {
        session: s.id,
        key: "tool".into(),
        kind: EventKind::Tool,
        text: "Running tests\nverbose output".into(),
        complete: true,
        request: None,
        origin: None,
    };
    svc.project(&e, &t).await?;
    e.kind = EventKind::Reasoning;
    e.text = "private reasoning".into();
    svc.project(&e, &t).await?;
    let pending = store.pending().await?;
    assert!(pending.iter().any(|d| d.text == "Running tests"));
    assert!(
        pending
            .iter()
            .all(|d| !d.text.contains("verbose output") && !d.text.contains("private reasoning"))
    );
    for kind in [
        EventKind::Registered,
        EventKind::Finished,
        EventKind::Offline,
    ] {
        e.kind = kind;
        e.key = format!("lifecycle:{:?}", e.kind);
        e.text = "Routine lifecycle status".into();
        svc.project(&e, &t).await?;
    }
    assert_eq!(store.pending().await?.len(), pending.len());
    e.kind = EventKind::Error;
    e.key = "failed".into();
    e.text = "Task failed: actionable error".into();
    svc.project(&e, &t).await?;
    assert!(store.pending().await?.iter().any(|d| d.text == e.text));
    let token = store.action(&t, serde_json::json!({"op":"recent"})).await?;
    let reopened = Store::open(&path)?;
    assert!(reopened.read_action(&token, &t).await.is_err());
    Ok(())
}

#[tokio::test]
async fn output_modes_filter_work_but_keep_interactions() -> Result<()> {
    for mode in [OutputMode::Full, OutputMode::Compact, OutputMode::Minimal] {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("state.sqlite"))?;
        let mut cfg = fake_config();
        cfg.agents.get_mut("work").unwrap().output = mode;
        let path = dir.path().join("config.toml");
        std::fs::write(&path, toml::to_string(&cfg)?)?;
        let cfg = Config::load(&path)?;
        assert_eq!(cfg.agents["work"].output, mode);
        let agent = Arc::new(FakeAgent::default());
        let svc = Service::new(
            Arc::new(cfg),
            store.clone(),
            agent.clone(),
            vec![Arc::new(FakeIm)],
        );
        let session = store.ensure_session("work", "output-modes").await?;
        svc.ensure_topics(&session).await?;
        let target = store.targets(&session.id).await?.remove(0);
        let event = |kind, key: &str, text: &str| AgentEvent {
            session: session.id.clone(),
            kind,
            key: key.into(),
            text: text.into(),
            complete: true,
            request: None,
            origin: None,
        };
        svc.project(&event(EventKind::User, "user", "My task"), &target)
            .await?;
        for (kind, key, text) in [
            (EventKind::Text, "progress", "Working on the task"),
            (
                EventKind::Reasoning,
                "reasoning",
                "Available reasoning summary",
            ),
            (EventKind::Tool, "tool-one", "Tool one\nDetailed output one"),
            (EventKind::Tool, "tool-two", "Tool two\nDetailed output two"),
            (EventKind::Plan, "work-plan", "Intermediate work plan"),
        ] {
            svc.project(&event(kind, key, text), &target).await?;
        }
        let mut question = event(EventKind::Question, "request:question", "Choose");
        question.request = Some(PendingRequest {
            id: "question".into(),
            label: "Choose".into(),
            choices: vec![],
            kind: RequestKind::Questions,
            blocking: true,
            questions: vec![Question {
                id: "color".into(),
                title: "Which color?".into(),
                options: vec![("Blue".into(), "Cool".into())],
                secret: false,
            }],
        });
        store.event(&question).await?;
        svc.project(&question, &target).await?;
        let card = question_card(&store, "question").await?;
        assert!(!card.buttons.is_empty());
        click_card(&svc, &card, "choose", "Blue").await?;
        let card = question_card(&store, "question").await?;
        click_card(&svc, &card, "submit", "Submit answers").await?;
        assert_eq!(*agent.responses.lock().await, [r#"{"color":"Blue"}"#]);
        let card = question_card(&store, "question").await?;
        assert!(card.buttons.is_empty() && card.text.contains("Answer submitted"));
        svc.project(&event(EventKind::Text, "answer", "Partial answer"), &target)
            .await?;
        svc.project(&event(EventKind::Text, "answer", "Final answer"), &target)
            .await?;
        let pending = store.pending().await?;
        assert!(pending.iter().any(|d| d.text == "You · CLI\n\nMy task"));
        assert_eq!(
            pending.iter().any(|d| d.text == "Final answer"),
            mode != OutputMode::Minimal
        );
        assert_eq!(
            pending.iter().any(|d| d.text == "Working on the task"),
            mode != OutputMode::Minimal
        );
        assert_eq!(
            pending.iter().any(|d| d.text.contains("Detailed output")),
            mode == OutputMode::Full
        );
        assert_eq!(
            pending
                .iter()
                .any(|d| d.text.contains("Available reasoning")),
            mode == OutputMode::Full
        );
        if mode == OutputMode::Compact {
            assert!(pending.iter().any(|d| d.text == "Tool two"));
            assert!(!pending.iter().any(|d| d.text == "Tool one"));
        }
        svc.project(
            &event(EventKind::Finished, "finished", "Turn completed"),
            &target,
        )
        .await?;
        let pending = store.pending().await?;
        assert_eq!(
            pending.iter().filter(|d| d.text == "Final answer").count(),
            1
        );
        assert!(!pending.iter().any(|d| d.text == "Partial answer"));
        if mode == OutputMode::Minimal {
            assert_eq!(
                pending.len(),
                3,
                "user input, interaction card, final answer only"
            );
        }
        // A failed task must not publish the buffered progress as its answer.
        svc.project(
            &event(EventKind::Text, "failed-progress", "Unfinished progress"),
            &target,
        )
        .await?;
        svc.project(&event(EventKind::Error, "error", "Task failed"), &target)
            .await?;
        svc.project(
            &event(EventKind::Finished, "after-error", "Turn ended"),
            &target,
        )
        .await?;
        let pending = store.pending().await?;
        assert!(pending.iter().any(|d| d.text == "Task failed"));
        if mode == OutputMode::Minimal {
            assert!(!pending.iter().any(|d| d.text == "Unfinished progress"));
        }
        // Final plan content and its decision controls both remain usable.
        svc.project(&event(EventKind::Plan, "proposal", "Final plan"), &target)
            .await?;
        let mut proposal = event(
            EventKind::Question,
            "request:proposal",
            "Implement this plan?",
        );
        proposal.request = Some(PendingRequest {
            id: "proposal".into(),
            label: "Implement this plan?".into(),
            kind: RequestKind::Plan,
            blocking: false,
            questions: vec![],
            choices: vec![("Execute plan".into(), "execute".into())],
        });
        store.event(&proposal).await?;
        svc.project(&proposal, &target).await?;
        svc.project(
            &event(EventKind::Finished, "plan-finished", "Turn completed"),
            &target,
        )
        .await?;
        let pending = store.pending().await?;
        assert!(pending.iter().any(|d| d.text == "Final plan"));
        let decision = pending
            .iter()
            .find(|d| d.text == "Implement this plan?")
            .unwrap();
        assert_eq!(decision.buttons[0][0].0, "Execute plan");
        svc.project(
            &event(EventKind::Text, "before-async", "Not a final answer"),
            &target,
        )
        .await?;
        let mut last_question = question.clone();
        last_question.key = "request:last-question".into();
        last_question.request.as_mut().unwrap().id = "last-question".into();
        last_question.request.as_mut().unwrap().kind = RequestKind::AsyncQuestions;
        store.event(&last_question).await?;
        svc.project(&last_question, &target).await?;
        svc.project(
            &event(EventKind::Finished, "async-finished", "Turn completed"),
            &target,
        )
        .await?;
        assert!(
            !question_card(&store, "last-question")
                .await?
                .buttons
                .is_empty()
        );
        if mode == OutputMode::Minimal {
            assert!(
                !store
                    .pending()
                    .await?
                    .iter()
                    .any(|d| d.text == "Not a final answer")
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn compact_tools_group_between_visible_messages() -> Result<()> {
    async fn cards(store: &Store, session: &Session) -> Result<Vec<Delivery>> {
        let prefix = format!("{}:activity:", session.id);
        Ok(store
            .pending()
            .await?
            .into_iter()
            .filter(|d| d.key.starts_with(&prefix))
            .collect())
    }
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let svc = Service::new(
        Arc::new(fake_config()),
        store.clone(),
        Arc::new(FakeAgent::default()),
        vec![Arc::new(FakeIm)],
    );
    let session = store.ensure_session("work", "grouped-tools").await?;
    svc.ensure_topics(&session).await?;
    let target = store.targets(&session.id).await?.remove(0);
    let event = |kind, key: &str, text: &str| AgentEvent {
        session: session.id.clone(),
        kind,
        key: key.into(),
        text: text.into(),
        complete: false,
        request: None,
        origin: None,
    };
    svc.project(
        &event(EventKind::Text, "message-1", "I will inspect the project."),
        &target,
    )
    .await?;
    svc.project(
        &event(EventKind::Tool, "tool-a", "Reading files\nFull tool detail"),
        &target,
    )
    .await?;
    let first = cards(&store, &session).await?.remove(0);
    store.delivery(&first, "sent", Some("11".into())).await?;
    svc.project(
        &event(EventKind::Tool, "tool-b", "Searching the project"),
        &target,
    )
    .await?;
    svc.project(
        &event(EventKind::Reasoning, "reasoning", "Hidden reasoning"),
        &target,
    )
    .await?;
    svc.project(
        &event(EventKind::Tool, "tool-c", "Checking the configuration"),
        &target,
    )
    .await?;
    let group = cards(&store, &session).await?;
    assert_eq!(group.len(), 1);
    assert_eq!(group[0].id, first.id);
    assert_eq!(group[0].message.as_deref(), Some("11"));
    assert_eq!(group[0].text, "Checking the configuration");

    svc.project(
        &event(
            EventKind::Text,
            "message-2",
            "The configuration needs a change.",
        ),
        &target,
    )
    .await?;
    svc.project(
        &event(EventKind::Tool, "tool-d", "Editing the configuration"),
        &target,
    )
    .await?;
    let second = cards(&store, &session)
        .await?
        .into_iter()
        .find(|d| d.id != first.id)
        .unwrap();
    store.delivery(&second, "sent", Some("12".into())).await?;
    // More text from the same streamed message does not split this group.
    svc.project(
        &event(
            EventKind::Text,
            "message-2",
            "The configuration needs a change. I found the cause.",
        ),
        &target,
    )
    .await?;
    svc.project(&event(EventKind::Tool, "tool-e", "Running checks"), &target)
        .await?;
    let group = cards(&store, &session).await?;
    assert_eq!(group.len(), 2);
    let updated = group.iter().find(|d| d.id == second.id).unwrap();
    assert_eq!(updated.message.as_deref(), Some("12"));
    assert_eq!(updated.text, "Running checks");
    // A late result updates its original group without moving new tools there.
    svc.project(
        &event(EventKind::Tool, "tool-a", "File read completed"),
        &target,
    )
    .await?;
    svc.project(
        &event(EventKind::Tool, "tool-f", "Checks completed"),
        &target,
    )
    .await?;
    let group = cards(&store, &session).await?;
    assert_eq!(group.len(), 2);
    assert_eq!(
        group.iter().find(|d| d.id == first.id).unwrap().text,
        "File read completed"
    );
    assert_eq!(
        group.iter().find(|d| d.id == second.id).unwrap().text,
        "Checks completed"
    );

    store
        .claim_message("telegram-input", &serde_json::to_string(&target)?)
        .await?;
    let mut input = event(EventKind::User, "user-message", "Continue");
    input.origin = Some("telegram-input".into());
    svc.project(&input, &target).await?;
    svc.project(
        &event(EventKind::Tool, "tool-g", "Starting the next task"),
        &target,
    )
    .await?;
    assert_eq!(cards(&store, &session).await?.len(), 3);
    assert!(
        store
            .pending()
            .await?
            .iter()
            .all(|d| d.text != "You · CLI\n\nContinue")
    );
    svc.project(
        &event(EventKind::Finished, "turn-done", "Completed"),
        &target,
    )
    .await?;
    svc.project(&event(EventKind::Tool, "tool-h", "A fresh turn"), &target)
        .await?;
    assert_eq!(cards(&store, &session).await?.len(), 4);
    Ok(())
}

#[tokio::test]
async fn working_indicators_do_not_create_messages() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let agent = Arc::new(FakeAgent {
        online: Mutex::new(vec![]),
        inputs: Mutex::new(vec![]),
        responses: Mutex::new(vec![]),
        ..Default::default()
    });
    let im = Arc::new(TitleIm::default());
    let svc = Service::new(
        Arc::new(fake_config()),
        store.clone(),
        agent.clone(),
        vec![im.clone()],
    );
    let session = store.ensure_session("work", "working").await?;
    svc.ensure_topics(&session).await?;
    let target = store.targets(&session.id).await?.remove(0);
    agent.online.lock().await.push(session.id);
    svc.indicate_working().await?;
    assert_eq!(*im.working.lock().await, vec![target]);
    agent.online.lock().await.clear();
    svc.indicate_working().await?;
    assert_eq!(im.working.lock().await.len(), 1);
    assert!(store.pending().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn outbox_ack_does_not_lose_a_concurrent_button_update() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let t = FakeIm.spaces().remove(0);
    store
        .enqueue(
            "work",
            &t,
            "card",
            "Same text",
            vec![vec![("Act".into(), "token".into())]],
            vec![],
        )
        .await?;
    let sent = store.pending().await?.remove(0);
    assert_eq!(
        serde_json::to_value(&sent.buttons)?,
        serde_json::json!([[["Act", "token"]]])
    );
    store
        .enqueue("work", &t, "card", "Same text", vec![], vec![])
        .await?;
    store.delivery(&sent, "sent", Some("42".into())).await?;
    let updated = store.pending().await?.remove(0);
    assert!(updated.buttons.is_empty());
    assert_eq!(updated.message.as_deref(), Some("42"));
    Ok(())
}

#[test]
fn configuration_validates_projects_and_explicit_agents() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.toml");
    let mut cfg = fake_config();
    cfg.agents
        .insert("personal".into(), cfg.agents["work"].clone());
    let validate = |cfg: &Config| -> Result<Config> {
        std::fs::write(&path, toml::to_string(cfg)?)?;
        Config::load(&path)
    };
    assert!(
        validate(&cfg).is_err(),
        "each agent requires its own Bot reference"
    );
    cfg.agents.get_mut("personal").unwrap().telegram.token_env = Some("PERSONAL_TOKEN".into());
    cfg.projects
        .get_mut("demo")
        .unwrap()
        .agents
        .push("personal".into());
    assert_eq!(validate(&cfg)?.projects["demo"].agents.len(), 2);
    let mut duplicate = cfg.projects["demo"].clone();
    duplicate.cwd = dir.path().to_owned();
    cfg.projects.insert("second".into(), duplicate);
    assert!(validate(&cfg).is_err(), "one group belongs to one project");
    cfg.projects.get_mut("second").unwrap().telegram.group = -1002;
    validate(&cfg)?;
    cfg.projects.get_mut("second").unwrap().cwd = "/tmp".into();
    assert!(
        validate(&cfg).is_err(),
        "canonical directories must be unique"
    );
    cfg.projects.get_mut("second").unwrap().cwd = dir.path().to_owned();
    cfg.projects.get_mut("second").unwrap().agents = vec!["missing".into()];
    assert!(validate(&cfg).is_err());
    cfg.projects.get_mut("second").unwrap().agents = vec![];
    assert!(validate(&cfg).is_err());
    Ok(())
}

#[tokio::test]
async fn question_navigation_preserves_shared_drafts_and_leaves_task_input_alone() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let agent = Arc::new(FakeAgent::default());
    let svc = Service::new(
        Arc::new(fake_config()),
        store.clone(),
        agent.clone(),
        vec![Arc::new(FakeIm)],
    );
    let session = store.ensure_session("work", "direct-replies").await?;
    svc.ensure_topics(&session).await?;
    let target = store.targets(&session.id).await?.remove(0);
    let mut event = AgentEvent {
        session: session.id.clone(),
        key: "request:direct".into(),
        kind: EventKind::Question,
        text: "Question".into(),
        complete: true,
        origin: None,
        request: Some(PendingRequest {
            id: "direct".into(),
            label: "Question".into(),
            kind: RequestKind::AsyncQuestions,
            choices: vec![],
            blocking: false,
            questions: vec![
                Question {
                    id: "directory".into(),
                    title: "Which directory?".into(),
                    options: vec![],
                    secret: false,
                },
                Question {
                    id: "note".into(),
                    title: "Which note?".into(),
                    options: vec![("Default".into(), String::new())],
                    secret: false,
                },
            ],
        }),
    };
    store.event(&event).await?;
    svc.project(&event, &target).await?;
    let initial = question_card(&store, "direct").await?;
    assert!(initial.text.contains("Which directory?"));
    assert!(!initial.text.contains("Which note?"));
    assert!(!initial.text.contains("Custom answer"));
    assert_eq!(initial.buttons[0].len(), 2);
    store.delivery(&initial, "sent", Some("99".into())).await?;
    svc.handle(message("ordinary", &target, "keep working"))
        .await?;
    let mut first_reply = message("other-user", &target, "/tmp/another-member");
    first_reply.reply_to = Some("99".into());
    svc.handle(first_reply).await?;
    assert!(
        question_card(&store, "direct")
            .await?
            .text
            .contains("/tmp/another-member")
    );
    let mut reply = message("directory-answer", &target, "/tmp/project");
    reply.reply_to = Some("99".into());
    svc.handle(reply).await?;
    let first = question_card(&store, "direct").await?;
    assert!(first.text.contains("/tmp/project"));
    assert!(
        first.text.contains("Which directory?"),
        "saving never silently advances the current question"
    );
    click_card(&svc, &first, "go-to-note", "2 ○").await?;
    let second = question_card(&store, "direct").await?;
    assert!(!second.text.contains("Which directory?"));
    click_card(&svc, &second, "choose-note", "Default").await?;
    let mut reply = message("custom-note", &target, "中文备注");
    reply.reply_to = Some("99".into());
    svc.handle(reply).await?;
    let answered = question_card(&store, "direct").await?;
    click_card(&svc, &answered, "review", "Review answers").await?;
    let review = question_card(&store, "direct").await?;
    assert!(review.text.contains("/tmp/project"));
    assert!(review.text.contains("中文备注"));
    click_card(&svc, &review, "edit-first", "1 ✓").await?;
    let mut reply = message("directory-revised", &target, "/tmp/revised");
    reply.reply_to = Some("99".into());
    svc.handle(reply).await?;
    let revised = question_card(&store, "direct").await?;
    click_card(&svc, &revised, "review-revised", "Review answers").await?;
    let reviewed = question_card(&store, "direct").await?;
    assert!(reviewed.text.contains("/tmp/revised"));
    assert!(
        reviewed.text.contains("中文备注"),
        "all group members share the same draft"
    );
    assert!(agent.responses.lock().await.is_empty());
    click_card(&svc, &reviewed, "submit-replies", "Submit answers").await?;
    assert_eq!(
        *agent.responses.lock().await,
        vec![r#"{"directory":"/tmp/revised","note":"中文备注"}"#]
    );
    assert_eq!(*agent.inputs.lock().await, vec!["keep working"]);
    let closed = question_card(&store, "direct").await?;
    assert_eq!(closed.id, initial.id);
    assert_eq!(closed.message.as_deref(), Some("99"));
    assert!(closed.buttons.is_empty());

    // Native resolution edits the original card and discards its outstanding draft.
    event.key = "request:cli-resolved".into();
    event.request.as_mut().unwrap().id = "cli-resolved".into();
    store.event(&event).await?;
    svc.project(&event, &target).await?;
    let native_card = question_card(&store, "cli-resolved").await?;
    store
        .delivery(&native_card, "sent", Some("100".into()))
        .await?;
    let mut reply = message("unfinished", &target, "local draft");
    reply.reply_to = Some("100".into());
    svc.handle(reply).await?;
    store.resolve("cli-resolved").await?;
    event.request = None;
    event.kind = EventKind::Status;
    event.text = "Answer submitted via CLI\nWhich directory?: /cli\nWhich note?: Terminal".into();
    svc.project(&event, &target).await?;
    let resolved = question_card(&store, "cli-resolved").await?;
    assert_eq!(resolved.id, native_card.id);
    assert!(resolved.buttons.is_empty());
    assert!(resolved.text.contains("Answer submitted via CLI"));
    assert!(!resolved.text.contains("local draft"));
    let mut old_reply = message("expired-question", &target, "must not steer");
    old_reply.reply_to = Some("100".into());
    svc.handle(old_reply).await?;
    assert_eq!(*agent.inputs.lock().await, vec!["keep working"]);
    Ok(())
}

#[tokio::test]
async fn terminal_user_messages_are_labeled_without_echoing_telegram_input() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let svc = Service::new(
        Arc::new(fake_config()),
        store.clone(),
        Arc::new(FakeAgent::default()),
        vec![Arc::new(FakeIm)],
    );
    let session = store.ensure_session("work", "sources").await?;
    svc.ensure_topics(&session).await?;
    let target = store.targets(&session.id).await?.remove(0);
    let mut event = AgentEvent {
        session: session.id,
        key: "terminal-input".into(),
        kind: EventKind::User,
        text: "Please inspect the project".into(),
        complete: true,
        request: None,
        origin: None,
    };
    svc.project(&event, &target).await?;
    let pending = store.pending().await?;
    assert_eq!(pending[0].text, "You · CLI\n\nPlease inspect the project");
    store
        .claim_message("telegram-input", &serde_json::to_string(&target)?)
        .await?;
    event.key = "telegram-input".into();
    event.origin = Some("telegram-input".into());
    svc.project(&event, &target).await?;
    assert_eq!(store.pending().await?.len(), pending.len());
    event.kind = EventKind::Text;
    event.key = "agent-answer".into();
    event.text = "Project inspected".into();
    event.origin = None;
    svc.project(&event, &target).await?;
    assert!(
        store
            .pending()
            .await?
            .iter()
            .any(|d| d.text == "Project inspected")
    );
    Ok(())
}

#[tokio::test]
async fn topic_status_identifies_native_session_and_mode_commands_preserve_active_work()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let agent = Arc::new(FakeAgent::default());
    let svc = Service::new(
        Arc::new(fake_config()),
        store.clone(),
        agent.clone(),
        vec![Arc::new(FakeIm)],
    );
    let session = store.ensure_session("work", "native-thread-123").await?;
    svc.ensure_topics(&session).await?;
    let target = store.targets(&session.id).await?.remove(0);
    svc.handle(message("status", &target, "/status")).await?;
    let status = store
        .pending()
        .await?
        .into_iter()
        .find(|d| d.key == "status:0")
        .unwrap();
    assert!(status.text.contains("Session: native-thread-123"));
    assert!(status.text.contains("CWD: /tmp"));
    assert!(status.text.contains("Project: demo"));
    assert!(status.text.contains("Mode (next turn): Default"));
    assert!(status.text.contains("Agent: work"));
    assert!(!status.text.contains(&session.id));
    let toggle = message("plan-mode", &target, "/plan");
    svc.handle(toggle.clone()).await?;
    svc.handle(toggle).await?;
    assert_eq!(agent.modes.lock().await[&session.id], SessionMode::Plan);
    assert!(agent.inputs.lock().await.is_empty());
    svc.handle(message("plan-off", &target, "/plan")).await?;
    assert_eq!(agent.modes.lock().await[&session.id], SessionMode::Default);
    svc.handle(message("plan-on", &target, "/plan")).await?;
    svc.handle(message("plan-task", &target, "/plan inspect this project"))
        .await?;
    assert_eq!(agent.modes.lock().await[&session.id], SessionMode::Plan);
    assert_eq!(*agent.inputs.lock().await, vec!["inspect this project"]);
    svc.handle(message(
        "default-task",
        &target,
        "/default implement the changes",
    ))
    .await?;
    assert_eq!(agent.modes.lock().await[&session.id], SessionMode::Default);
    assert_eq!(
        *agent.inputs.lock().await,
        vec!["inspect this project", "implement the changes"]
    );
    agent.online.lock().await.push(session.id.clone());
    svc.handle(message(
        "busy-plan-task",
        &target,
        "/plan do not steer this",
    ))
    .await?;
    assert_eq!(agent.modes.lock().await[&session.id], SessionMode::Default);
    assert_eq!(agent.inputs.lock().await.len(), 2);
    svc.handle(message("busy-plan-mode", &target, "/plan"))
        .await?;
    assert_eq!(agent.modes.lock().await[&session.id], SessionMode::Plan);
    assert_eq!(agent.inputs.lock().await.len(), 2);
    assert!(agent.online.lock().await.contains(&session.id));
    svc.handle(message("busy-plan-off", &target, "/plan"))
        .await?;
    assert_eq!(agent.modes.lock().await[&session.id], SessionMode::Default);
    agent.set_mode(&session, SessionMode::Plan).await?;
    svc.handle(message("after-cli-mode-change", &target, "/plan"))
        .await?;
    assert_eq!(agent.modes.lock().await[&session.id], SessionMode::Default);
    assert_eq!(agent.inputs.lock().await.len(), 2);
    assert!(agent.online.lock().await.contains(&session.id));
    svc.handle(message("default-mode", &target, "/default"))
        .await?;
    assert_eq!(agent.modes.lock().await[&session.id], SessionMode::Default);
    Ok(())
}

#[tokio::test]
async fn long_question_cards_stay_single_and_uncertain_answers_cannot_be_resubmitted() -> Result<()>
{
    for answer in ["中文🧩".repeat(2000), "uncertain-answer".into()] {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("state.sqlite"))?;
        let agent = Arc::new(FakeAgent::default());
        let svc = Service::new(
            Arc::new(fake_config()),
            store.clone(),
            agent.clone(),
            vec![Arc::new(FakeIm)],
        );
        let session = store.ensure_session("work", "long-answer").await?;
        svc.ensure_topics(&session).await?;
        let target = store.targets(&session.id).await?.remove(0);
        let event = AgentEvent {
            session: session.id.clone(),
            key: "request:long".into(),
            kind: EventKind::Question,
            text: "Question".into(),
            complete: true,
            origin: None,
            request: Some(PendingRequest {
                id: "long".into(),
                label: "Question".into(),
                choices: vec![],
                kind: RequestKind::Questions,
                blocking: true,
                questions: vec![Question {
                    id: "note".into(),
                    title: "Long title 🧩".repeat(500),
                    options: vec![],
                    secret: false,
                }],
            }),
        };
        store.event(&event).await?;
        svc.project(&event, &target).await?;
        let initial = question_card(&store, "long").await?;
        store.delivery(&initial, "sent", Some("101".into())).await?;
        let mut input = message("answer", &target, &answer);
        input.reply_to = Some("101".into());
        svc.handle(input).await?;
        let ready = question_card(&store, "long").await?;
        assert_eq!(ready.id, initial.id);
        assert!(ready.text.encode_utf16().count() <= 3500);
        assert_eq!(store.pending().await?.len(), 1);
        click_card(&svc, &ready, "submit", "Submit answers").await?;
        assert_eq!(
            *agent.responses.lock().await,
            vec![serde_json::json!({"note":answer}).to_string()]
        );
        let closed = question_card(&store, "long").await?;
        assert!(closed.buttons.is_empty());
        assert_eq!(closed.id, initial.id);
        if answer == "uncertain-answer" {
            assert!(store.request("long").await.is_ok());
            click_card(&svc, &ready, "retry", "Submit answers").await?;
            assert_eq!(agent.responses.lock().await.len(), 1);
            assert_eq!(store.pending().await?.len(), 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn new_uses_project_directory_and_agents_only_links_bound_sessions() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let agent = Arc::new(FakeAgent {
        store: Some(store.clone()),
        ..Default::default()
    });
    let mut cfg = fake_config();
    cfg.projects.get_mut("demo").unwrap().cwd = dir.path().to_owned();
    let svc = Service::new(
        Arc::new(cfg),
        store.clone(),
        agent.clone(),
        vec![Arc::new(FakeIm)],
    );
    let group = FakeIm.spaces().remove(0);
    for (id, input) in [
        ("relative", "/new relative/path"),
        ("missing", "/new /uaoi-test-nonexistent-dir"),
    ] {
        svc.handle(message(id, &group, input)).await?;
    }
    assert!(agent.management.lock().await.is_empty());
    svc.handle(message("new", &group, "/new")).await?;
    let created = agent.management.lock().await;
    assert_eq!(created.len(), 1);
    assert_eq!(created[0]["op"], "create");
    assert_eq!(created[0]["cwd"], dir.path().to_str().unwrap());
    drop(created);
    let bound = store.bindings().await?.1.remove(0);
    let cwd = dir.path().join("not-present-anymore");
    let hidden = store.ensure_session("work", "unbound-history").await?;
    *agent.listed.lock().await = vec![
        SessionSummary {
            native: bound.session.native.clone(),
            title: "中文 🧩 Session".into(),
            cwd: cwd.clone(),
            status: "notLoaded".into(),
        },
        SessionSummary {
            native: hidden.native,
            title: "Unbound history".into(),
            cwd,
            status: "idle".into(),
        },
    ];
    svc.handle(message("list", &group, "/agents")).await?;
    let card = store
        .pending()
        .await?
        .into_iter()
        .find(|d| d.key == "list:0")
        .unwrap();
    assert!(card.text.contains("中文 🧩 Session"));
    assert!(card.text.contains("Sleeping"));
    assert!(!card.text.contains("Unbound history"));
    assert!(
        !card.text.contains("/resume")
            && !card.text.contains("https://")
            && !card.text.contains("Session:")
    );
    assert_eq!(
        &card.text[card.links[0].start..card.links[0].end],
        "中文 🧩 Session"
    );
    assert_eq!(card.links[0].url, FakeIm.link(&bound.target));
    let calls = agent.management.lock().await.len();
    for (i, command) in [
        "/list",
        "/resume anything",
        "/steer hello",
        "/output verbose",
    ]
    .iter()
    .enumerate()
    {
        svc.handle(message(&format!("removed-{i}"), &bound.target, command))
            .await?;
    }
    assert_eq!(agent.management.lock().await.len(), calls);
    assert!(agent.inputs.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn binding_edits_are_versioned_and_unbinding_stops_projection() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let agent = Arc::new(FakeAgent {
        store: Some(store.clone()),
        ..Default::default()
    });
    let svc = Service::new(
        Arc::new(fake_config()),
        store.clone(),
        agent,
        vec![Arc::new(FakeIm)],
    );
    let first = store.ensure_session("work", "first").await?;
    svc.ensure_topics(&first).await?;
    let old = store.bindings().await?.1.remove(0);
    let mut document: BindingsDocument =
        serde_json::from_value(svc.bindings_snapshot().await?["document"].clone())?;
    let stale = document.clone();
    svc.notice(
        &old.target,
        "pending",
        "must not be delivered after unbinding",
    )
    .await?;
    let queued = store.pending().await?.remove(0);
    let action = store
        .action(&old.target, serde_json::json!({"op":"submit"}))
        .await?;
    document.bindings = vec![BindingSpec {
        project: "demo".into(),
        agent: "work".into(),
        session: "second".into(),
        topic: None,
    }];
    svc.apply_bindings(document).await?;
    assert!(store.targets(&first.id).await?.is_empty());
    assert!(svc.ensure_topics(&first).await.is_err());
    assert!(!store.delivery_current(&queued).await?);
    assert!(store.read_action(&action, &old.target).await.is_err());
    let next = store.bindings().await?.1.remove(0);
    assert_eq!(next.session.native, "second");
    assert_ne!(next.target, old.target);
    assert!(svc.apply_bindings(stale).await.is_err());
    let event = AgentEvent {
        session: first.id,
        key: "late-event".into(),
        kind: EventKind::Text,
        text: "ignored".into(),
        complete: true,
        request: None,
        origin: None,
    };
    svc.project(&event, &old.target).await?;
    assert!(store.pending().await?.is_empty());
    let mut document: BindingsDocument =
        serde_json::from_value(svc.bindings_snapshot().await?["document"].clone())?;
    document.bindings.push(document.bindings[0].clone());
    assert!(svc.apply_bindings(document).await.is_err());
    assert_eq!(store.bindings().await?.1.len(), 1);
    let mut duplicate: BindingsDocument =
        serde_json::from_value(svc.bindings_snapshot().await?["document"].clone())?;
    duplicate.bindings[0].topic = None;
    duplicate.bindings.push(BindingSpec {
        project: "demo".into(),
        agent: "work".into(),
        session: "third".into(),
        topic: Some(next.target.thread.clone()),
    });
    assert!(svc.apply_bindings(duplicate).await.is_err());
    assert_eq!(store.bindings().await?.1.len(), 1);
    Ok(())
}

#[tokio::test]
async fn binding_history_survives_explicit_unbinding_and_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("state.sqlite");
    let store = Store::open(&path)?;
    let automatic = store.ensure_session("work", "automatic").await?;
    let explicit = store.ensure_session("work", "explicit").await?;
    let never_bound = store.ensure_session("work", "never-bound").await?;
    assert!(!store.was_bound(&automatic.id).await?);
    let target = Conversation {
        platform: "test".into(),
        space: "-1001".into(),
        thread: "first".into(),
    };
    store.bind(&target, &automatic.id).await?;
    let revision = store.bindings().await?.0;
    store
        .replace_bindings(
            revision,
            vec![BindingRecord {
                session: explicit.clone(),
                target: Conversation {
                    thread: "second".into(),
                    ..target
                },
            }],
        )
        .await?;
    let revision = store.bindings().await?.0;
    store.replace_bindings(revision, vec![]).await?;
    drop(store);
    let reopened = Store::open(&path)?;
    assert!(reopened.bindings().await?.1.is_empty());
    assert!(reopened.was_bound(&automatic.id).await?);
    assert!(reopened.was_bound(&explicit.id).await?);
    assert!(!reopened.was_bound(&never_bound.id).await?);
    Ok(())
}

#[tokio::test]
async fn invalid_binding_documents_never_allocate_topics() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let im = Arc::new(TitleIm::default());
    let svc = Service::new(
        Arc::new(fake_config()),
        store.clone(),
        Arc::new(FakeAgent {
            store: Some(store.clone()),
            unavailable: vec!["missing-native-id".into()],
            ..Default::default()
        }),
        vec![im.clone()],
    );
    let document = BindingsDocument {
        version: 3,
        revision: 0,
        bindings: ["valid-native-id", "missing-native-id"]
            .into_iter()
            .map(|session| BindingSpec {
                project: "demo".into(),
                agent: "work".into(),
                session: session.into(),
                topic: None,
            })
            .collect(),
    };
    assert!(svc.apply_bindings(document).await.is_err());
    assert!(im.created.lock().await.is_empty());
    let (revision, bindings) = store.bindings().await?;
    assert_eq!(revision, 0);
    assert!(bindings.is_empty());
    Ok(())
}

#[test]
fn configuration_accepts_exactly_one_telegram_credential_source() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.toml");
    let mut cfg = fake_config();
    let validate = |cfg: &Config| -> Result<Config> {
        std::fs::write(&path, toml::to_string(cfg)?)?;
        Config::load(&path)
    };
    let inline = TelegramConfig {
        token: Some("123456:example-test-token".into()),
        token_env: None,
    };
    cfg.agents.get_mut("work").unwrap().telegram = inline.clone();
    let parsed = validate(&cfg)?;
    assert_eq!(parsed.agents["work"].telegram.token, inline.token);
    assert!(parsed.agents["work"].telegram.token_env.is_none());
    for invalid in [
        TelegramConfig::default(),
        TelegramConfig {
            token: Some(String::new()),
            token_env: None,
        },
        TelegramConfig {
            token: None,
            token_env: Some(" ".into()),
        },
        TelegramConfig {
            token: inline.token.clone(),
            token_env: Some("TOKEN_ENV".into()),
        },
    ] {
        cfg.agents.get_mut("work").unwrap().telegram = invalid;
        let error = validate(&cfg)
            .err()
            .expect("invalid credential source accepted")
            .to_string();
        assert!(error.contains("exactly one"));
        assert!(
            !error.contains("example-test-token"),
            "validation must not disclose credentials"
        );
    }
    cfg.agents.get_mut("work").unwrap().telegram = inline;
    cfg.agents
        .insert("other".into(), cfg.agents["work"].clone());
    let error = validate(&cfg)
        .err()
        .expect("duplicate token accepted")
        .to_string();
    assert!(error.contains("own Bot credential"));
    assert!(!error.contains("example-test-token"));
    Ok(())
}

#[tokio::test]
async fn project_commands_work_in_general_and_session_arguments_are_validated() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let agent = Arc::new(FakeAgent {
        store: Some(store.clone()),
        ..Default::default()
    });
    let cfg = Arc::new(fake_config());
    let svc = Service::new(
        cfg.clone(),
        store.clone(),
        agent.clone(),
        vec![Arc::new(FakeIm)],
    );
    let general = FakeIm.spaces().remove(0);
    svc.handle(message("project-status", &general, "/status"))
        .await?;
    let status = store
        .pending()
        .await?
        .into_iter()
        .find(|d| d.key == "project-status:0")
        .unwrap();
    assert!(status.text.contains("Project: demo") && status.text.contains("Directory: /tmp"));
    assert!(status.text.contains("Bound sessions: 0") && status.text.contains("work: online"));
    assert!(agent.described.lock().await.is_empty());
    svc.handle(message("project-help", &general, "/help"))
        .await?;
    let help = store
        .pending()
        .await?
        .into_iter()
        .find(|d| d.key == "project-help:0")
        .unwrap();
    assert!(help.text.contains("Directory: /tmp") && help.text.contains("/new —"));
    assert!(!help.text.contains("absolute directory"));
    svc.handle(message("unknown", &general, "/not-a-command"))
        .await?;
    assert_eq!(
        store.operation_record("unknown").await?.unwrap().0,
        "rejected"
    );
    let session = store.ensure_session("work", "command-validation").await?;
    svc.ensure_topics(&session).await?;
    let topic = store.targets(&session.id).await?.remove(0);
    for (i, command) in [
        "/new /tmp",
        "/status extra",
        "/help extra",
        "/stop all",
        "/history -1",
        "/history wrong",
    ]
    .iter()
    .enumerate()
    {
        let id = format!("invalid-{i}");
        svc.handle(message(&id, &topic, command)).await?;
        assert_eq!(store.operation_record(&id).await?.unwrap().0, "rejected");
    }
    assert!(agent.histories.lock().await.is_empty());
    assert!(agent.management.lock().await.is_empty());
    svc.handle(message("history", &topic, "/history 0")).await?;
    assert_eq!(*agent.histories.lock().await, vec![session.id]);
    Ok(())
}
