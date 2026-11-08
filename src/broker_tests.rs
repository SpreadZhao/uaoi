use super::*;
use crate::{
    im::SendFailure,
    model::*,
    tests::{FakeAgent, fake_config},
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};

struct TestIm {
    name: String,
    active: AtomicBool,
    environment: Mutex<BTreeMap<String, String>>,
    menus: Arc<Mutex<Vec<String>>>,
}
#[async_trait]
impl im::ImAdapter for TestIm {
    fn name(&self) -> String {
        format!("test:{}", self.name)
    }
    fn agent(&self) -> &str {
        &self.name
    }
    fn enabled(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
    async fn activate(&self, env: &BTreeMap<String, String>) -> Result<()> {
        *self.environment.lock().await = env.clone();
        self.active.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn deactivate(&self) {
        self.active.store(false, Ordering::SeqCst);
        self.menus.lock().await.push(format!("{}:hide", self.name));
    }
    async fn clear_commands(&self, _: &BTreeMap<String, String>) -> Result<()> {
        self.menus.lock().await.push(format!("{}:reset", self.name));
        Ok(())
    }
    async fn set_commands(&self, commands: &[(&str, &str)]) -> Result<()> {
        assert!(commands.iter().any(|(command, _)| *command == "agents"));
        self.menus.lock().await.push(format!("{}:show", self.name));
        Ok(())
    }
    fn spaces(&self) -> Vec<Conversation> {
        vec![Conversation {
            platform: "test".into(),
            space: "-1001".into(),
            thread: String::new(),
        }]
    }
    fn project(&self, target: &Conversation) -> Option<&str> {
        (target.space == "-1001").then_some("demo")
    }
    fn destination(&self, project: &str) -> Option<Conversation> {
        (project == "demo").then(|| self.spaces().remove(0))
    }
    fn topic(&self, project: &str, id: &str) -> Result<Conversation> {
        Ok(Conversation {
            thread: id.into(),
            ..self.destination(project).context("Unknown project")?
        })
    }
    fn link(&self, target: &Conversation) -> String {
        format!("test:{}", target.thread)
    }
    async fn discard_backlog(&self) -> Result<String> {
        Ok(String::new())
    }
    async fn poll(&self, _: &str) -> Result<(Vec<Inbound>, String)> {
        std::future::pending().await
    }
    async fn create_conversation(&self, target: &Conversation, _: &str) -> Result<Conversation> {
        Ok(Conversation {
            thread: "42".into(),
            ..target.clone()
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
struct Harness {
    dir: tempfile::TempDir,
    broker: Arc<Broker>,
    agent: Arc<FakeAgent>,
    events: tokio::sync::broadcast::Sender<AgentEvent>,
    menus: Arc<Mutex<Vec<String>>>,
}
impl Harness {
    async fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let mut cfg = fake_config();
        cfg.runtime_dir = Some(dir.path().join("run"));
        cfg.state_dir = Some(dir.path().join("state"));
        private_dir(&cfg.runtime())?;
        private_dir(&cfg.state())?;
        cfg.agents
            .insert("personal".into(), cfg.agents["work"].clone());
        cfg.agents.get_mut("personal").unwrap().telegram.token_env =
            Some("PERSONAL_TEST_TOKEN".into());
        cfg.projects
            .get_mut("demo")
            .unwrap()
            .agents
            .push("personal".into());
        let cfg = Arc::new(cfg);
        let store = Store::open(&cfg.state().join("state.sqlite"))?;
        let mut fake = FakeAgent::default();
        fake.store = Some(store.clone());
        let agent = Arc::new(fake);
        let menus = Arc::new(Mutex::new(vec![]));
        let ims = ["work", "personal"]
            .into_iter()
            .map(|name| {
                Arc::new(TestIm {
                    name: name.into(),
                    active: AtomicBool::new(false),
                    environment: Mutex::new(BTreeMap::new()),
                    menus: menus.clone(),
                }) as Arc<dyn im::ImAdapter>
            })
            .collect();
        let cancel = CancellationToken::new();
        let svc = Arc::new(Service::new(cfg, store, agent.clone(), ims));
        let broker = Arc::new(Broker {
            svc,
            path: dir.path().join("config.toml"),
            state: Mutex::new(State::default()),
            cancel,
        });
        let (events, _) = tokio::sync::broadcast::channel(100);
        Ok(Self {
            dir,
            broker,
            agent,
            events,
            menus,
        })
    }
    async fn serve(&self) -> Result<(JoinHandle<()>, UnixStream)> {
        let listener = UnixListener::bind(self.broker.svc.cfg.runtime().join("control.sock"))?;
        let (bootstrap, inherited) = UnixStream::pair()?;
        let broker = self.broker.clone();
        let events = self.events.clone();
        Ok((
            tokio::spawn(async move {
                super::serve(broker, listener, inherited, events)
                    .await
                    .unwrap();
            }),
            bootstrap,
        ))
    }
    fn request(&self, name: &str) -> Value {
        json!({"method":"open", "config":self.broker.path, "fingerprint":self.broker.svc.cfg.fingerprint(), "agent":name, "cwd":self.dir.path(), "args":[], "env":{}})
    }
    fn terminal(&self, name: &str, gate: &str) -> Result<(PathBuf, JoinHandle<Result<i32>>)> {
        let gate = self.dir.path().join(gate);
        std::fs::write(&gate, "wait")?;
        let arg = gate.as_os_str().to_owned();
        let cfg = self.broker.svc.cfg.clone();
        let path = self.broker.path.clone();
        let name = name.to_owned();
        Ok((
            gate,
            tokio::spawn(
                async move { crate::wrapper::run(&cfg, &path, Some(&name), vec![arg]).await },
            ),
        ))
    }
}
async fn ready(gate: &std::path::Path) -> Result<()> {
    let pid = gate.with_extension("pid");
    // Gates in these tests have no extension; the fixture appends .pid.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !pid.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}
async fn finish(gate: PathBuf, child: JoinHandle<Result<i32>>) -> Result<()> {
    std::fs::remove_file(gate)?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), child).await???,
        0
    );
    Ok(())
}

#[tokio::test]
async fn terminals_share_one_runtime_and_last_exit_stops_broker() -> Result<()> {
    let h = Harness::new().await?;
    let (server, bootstrap) = h.serve().await?;
    let (first, a) = h.terminal("work", "first")?;
    ready(&first).await?;
    drop(bootstrap);
    let generation = h.broker.status().await["agents"]["work"]["generation"].clone();
    let (second, b) = h.terminal("work", "second")?;
    ready(&second).await?;
    assert_eq!(h.broker.status().await["agents"]["work"]["clients"], 2);
    assert_eq!(*h.agent.lifecycle.lock().await, ["start:work"]);
    finish(first, a).await?;
    assert_eq!(h.broker.status().await["agents"]["work"]["clients"], 1);
    assert_eq!(
        h.broker.status().await["agents"]["work"]["generation"],
        generation
    );
    assert!(h.broker.svc.im("work")?.enabled());
    finish(second, b).await?;
    tokio::time::timeout(Duration::from_secs(6), server).await??;
    assert!(!h.broker.svc.im("work")?.enabled());
    assert!(h.agent.runtimes.lock().await.is_empty());
    assert!(!h.broker.svc.cfg.runtime().join("control.sock").exists());
    Ok(())
}

#[tokio::test]
async fn profiles_stop_independently_and_can_reactivate() -> Result<()> {
    let h = Harness::new().await?;
    let (server, bootstrap) = h.serve().await?;
    let (work, a) = h.terminal("work", "work")?;
    ready(&work).await?;
    drop(bootstrap);
    let original = h.broker.status().await["agents"]["work"]["generation"].clone();
    let (personal, b) = h.terminal("personal", "personal")?;
    ready(&personal).await?;
    finish(work, a).await?;
    assert!(!h.broker.svc.im("work")?.enabled());
    assert!(h.broker.svc.im("personal")?.enabled());
    assert_eq!(h.broker.status().await["agents"]["personal"]["clients"], 1);
    let (work, c) = h.terminal("work", "work-again")?;
    ready(&work).await?;
    assert_ne!(
        h.broker.status().await["agents"]["work"]["generation"],
        original
    );
    finish(personal, b).await?;
    assert!(h.broker.svc.im("work")?.enabled());
    finish(work, c).await?;
    tokio::time::timeout(Duration::from_secs(6), server).await??;
    Ok(())
}

#[tokio::test]
async fn menus_follow_first_and_last_terminal_for_each_profile() -> Result<()> {
    let h = Harness::new().await?;
    h.broker.open("a", &h.request("work")).await?;
    assert_eq!(
        *h.menus.lock().await,
        ["personal:reset", "work:reset", "work:show"]
    );
    h.broker.open("b", &h.request("work")).await?;
    assert_eq!(h.menus.lock().await.len(), 3);
    h.broker.open("c", &h.request("personal")).await?;
    assert_eq!(h.menus.lock().await.last().unwrap(), "personal:show");
    assert!(!h.broker.release("work", "a").await);
    assert_eq!(h.menus.lock().await.last().unwrap(), "personal:show");
    assert!(!h.broker.release("work", "b").await);
    assert_eq!(h.menus.lock().await.last().unwrap(), "work:hide");
    assert!(h.broker.svc.im("personal")?.enabled());
    assert!(h.broker.release("personal", "c").await);
    assert_eq!(h.menus.lock().await.last().unwrap(), "personal:hide");
    Ok(())
}

#[tokio::test]
async fn disconnect_releases_lease_and_failed_startup_rolls_back() -> Result<()> {
    let h = Harness::new().await?;
    let mut bad = h.request("work");
    bad["env"] = json!({"FAIL_ACTIVATE":"1"});
    assert!(h.broker.open("failed", &bad).await.is_err());
    assert!(!h.broker.svc.im("work")?.enabled());
    assert!(h.agent.runtimes.lock().await.is_empty());
    let (server, bootstrap) = h.serve().await?;
    let mut io = crate::wrapper::connect(&h.broker.svc.cfg).await?;
    crate::wrapper::request(&mut io, h.request("work")).await?;
    drop(bootstrap);
    drop(io);
    tokio::time::timeout(Duration::from_secs(6), server).await??;
    assert!(h.agent.runtimes.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn abandoned_bootstrap_stops_without_starting_any_profile() -> Result<()> {
    let h = Harness::new().await?;
    let (server, bootstrap) = h.serve().await?;
    drop(bootstrap);
    tokio::time::timeout(Duration::from_secs(6), server).await??;
    assert!(h.agent.lifecycle.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn concurrent_admission_is_unique_and_shutdown_rejects_new_leases() -> Result<()> {
    let h = Harness::new().await?;
    let request = h.request("work");
    let (a, b) = tokio::join!(h.broker.open("a", &request), h.broker.open("b", &request));
    assert!(a?.is_some() && b?.is_some());
    assert_eq!(*h.agent.lifecycle.lock().await, ["start:work"]);
    assert!(!h.broker.release("work", "a").await);
    let (last, next) = tokio::join!(h.broker.release("work", "b"), h.broker.open("c", &request));
    assert!(last);
    assert!(next?.is_none());
    Ok(())
}

#[tokio::test]
async fn runtime_failure_closes_its_terminal_without_stopping_other_profile() -> Result<()> {
    let h = Harness::new().await?;
    let (server, bootstrap) = h.serve().await?;
    let (work, a) = h.terminal("work", "work")?;
    ready(&work).await?;
    drop(bootstrap);
    let (personal, b) = h.terminal("personal", "personal")?;
    ready(&personal).await?;
    h.agent.runtimes.lock().await["work"].cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), a)
            .await??
            .is_err()
    );
    assert!(h.broker.svc.im("personal")?.enabled());
    finish(personal, b).await?;
    tokio::time::timeout(Duration::from_secs(6), server).await??;
    Ok(())
}

// The subprocess runs the same wrapper against the parent's isolated broker.
#[test]
fn wrapper_process_entry() {
    let Ok(path) = std::env::var("UAOI_WRAPPER_FIXTURE") else {
        return;
    };
    let path = PathBuf::from(path);
    let cfg = Config::load(&path).unwrap();
    let gate = std::env::var_os("UAOI_WRAPPER_GATE").unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(crate::wrapper::run(&cfg, &path, Some("work"), vec![gate]));
    std::process::exit(result.unwrap_or(1));
}

#[tokio::test]
async fn killed_wrapper_releases_profile_and_terminates_native_child() -> Result<()> {
    let h = Harness::new().await?;
    std::fs::write(&h.broker.path, toml::to_string(h.broker.svc.cfg.as_ref())?)?;
    let (server, bootstrap) = h.serve().await?;
    let gate = h.dir.path().join("crash");
    std::fs::write(&gate, "wait")?;
    let mut wrapper = tokio::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "broker::tests::wrapper_process_entry",
            "--nocapture",
        ])
        .env("UAOI_WRAPPER_FIXTURE", &h.broker.path)
        .env("UAOI_WRAPPER_GATE", &gate)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    ready(&gate).await?;
    drop(bootstrap);
    let pid: i32 = std::fs::read_to_string(gate.with_extension("pid"))?.parse()?;
    wrapper.kill().await?;
    tokio::time::timeout(Duration::from_secs(6), server).await??;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let status = std::fs::read_to_string(format!("/proc/{pid}/stat"));
            if status.is_err() || status.is_ok_and(|s| s.split_whitespace().nth(2) == Some("Z")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(h.agent.runtimes.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn inactive_bindings_and_profile_interactions_stay_isolated() -> Result<()> {
    let h = Harness::new().await?;
    h.broker.open("p", &h.request("personal")).await?;
    let store = &h.broker.svc.store;
    let mut records = vec![];
    for name in ["work", "personal"] {
        let session = store.ensure_session(name, name).await?;
        let target = h.broker.svc.im(name)?.topic("demo", name)?;
        store.bind(&target, &session.id).await?;
        store
            .event(&AgentEvent {
                session: session.id.clone(),
                key: "question".into(),
                kind: EventKind::Question,
                text: "Choose".into(),
                complete: true,
                origin: None,
                request: Some(PendingRequest {
                    id: name.into(),
                    label: "Choose".into(),
                    choices: vec![],
                    kind: RequestKind::Questions,
                    questions: vec![],
                    blocking: true,
                }),
            })
            .await?;
        store.action(&target, json!({"request":name})).await?;
        records.push(BindingSpec {
            project: "demo".into(),
            agent: name.into(),
            session: name.into(),
            topic: Some(name.into()),
        });
    }
    let revision = store.bindings().await?.0;
    h.broker
        .svc
        .apply_bindings(BindingsDocument {
            version: 3,
            revision,
            bindings: records.clone(),
        })
        .await?;
    records[0].topic = Some("other".into());
    let revision = store.bindings().await?.0;
    assert!(
        h.broker
            .svc
            .apply_bindings(BindingsDocument {
                version: 3,
                revision,
                bindings: records
            })
            .await
            .is_err()
    );
    h.broker.svc.expire_profile("work").await?;
    assert!(store.request("work").await.is_err());
    assert!(store.request("personal").await.is_ok());
    assert_eq!(*h.agent.lifecycle.lock().await, ["start:personal"]);
    h.broker.release("personal", "p").await;
    Ok(())
}

#[tokio::test]
async fn offline_queues_cannot_starve_an_active_profile() -> Result<()> {
    let h = Harness::new().await?;
    let store = &h.broker.svc.store;
    for index in 0..31 {
        let name = if index < 30 { "work" } else { "personal" };
        let session = store
            .ensure_session(name, &format!("session-{index}"))
            .await?;
        let target = h.broker.svc.im(name)?.topic("demo", &index.to_string())?;
        store.bind(&target, &session.id).await?;
        store
            .enqueue(name, &target, "message", "queued", vec![], vec![])
            .await?;
    }
    let active = Some(vec!["personal".into()]);
    let titles = store.pending_titles_for(active.clone()).await?;
    let messages = store.pending_for(active).await?;
    assert_eq!(titles.len(), 1);
    assert_eq!(messages.len(), 1);
    assert_eq!(titles[0].agent, "personal");
    assert_eq!(messages[0].agent, "personal");
    assert!(store.pending_titles_for(Some(vec![])).await?.is_empty());
    assert!(store.pending_for(Some(vec![])).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn general_new_selects_the_only_online_project_agent() -> Result<()> {
    let h = Harness::new().await?;
    h.broker
        .open("personal-cli", &h.request("personal"))
        .await?;
    let general = h.broker.svc.im("personal")?.destination("demo").unwrap();
    let mut input = crate::tests::message("sole-agent-new", &general, "/new");
    input.agent = "personal".into();
    h.broker.svc.handle(input).await?;
    let bindings = h.broker.svc.store.bindings_in_space(&general).await?;
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].session.agent, "personal");
    let offline = h
        .broker
        .svc
        .store
        .ensure_session("work", "offline-session")
        .await?;
    let target = h.broker.svc.im("work")?.topic("demo", "offline-topic")?;
    h.broker.svc.store.bind(&target, &offline.id).await?;
    let mut list = crate::tests::message("offline-list", &general, "/agents");
    list.agent = "personal".into();
    h.broker.svc.handle(list).await?;
    let output = h
        .broker
        .svc
        .store
        .pending()
        .await?
        .into_iter()
        .find(|d| d.key == "offline-list:0")
        .unwrap();
    assert!(output.text.contains("work · Unavailable"));

    h.broker.release("personal", "personal-cli").await;
    Ok(())
}
