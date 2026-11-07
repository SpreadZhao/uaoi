use super::*;

struct ProjectIm {
    cfg: Arc<Config>,
    agent: String,
    created: Mutex<Vec<Conversation>>,
}
#[async_trait]
impl ImAdapter for ProjectIm {
    fn name(&self) -> &str {
        &self.agent
    }
    fn agent(&self) -> &str {
        &self.agent
    }
    fn spaces(&self) -> Vec<Conversation> {
        self.cfg
            .projects
            .values()
            .filter(|p| p.agents.contains(&self.agent))
            .map(|p| Conversation {
                platform: "test".into(),
                space: p.telegram.group.to_string(),
                thread: String::new(),
            })
            .collect()
    }
    fn project(&self, t: &Conversation) -> Option<&str> {
        self.cfg
            .projects
            .iter()
            .find(|(_, p)| {
                p.telegram.group.to_string() == t.space && p.agents.contains(&self.agent)
            })
            .map(|(name, _)| name.as_str())
    }
    fn destination(&self, project: &str) -> Option<Conversation> {
        self.cfg
            .projects
            .get(project)
            .filter(|p| p.agents.contains(&self.agent))
            .map(|p| Conversation {
                platform: "test".into(),
                space: p.telegram.group.to_string(),
                thread: String::new(),
            })
    }
    fn topic(&self, project: &str, id: &str) -> Result<Conversation> {
        Ok(Conversation {
            thread: id.into(),
            ..self
                .destination(project)
                .ok_or_else(|| anyhow::anyhow!("Unavailable project"))?
        })
    }
    fn authorized(&self, sender: &str, t: &Conversation) -> bool {
        sender == "1" && self.project(t).is_some()
    }
    fn link(&self, t: &Conversation) -> String {
        format!("test:{}/{}", t.space, t.thread)
    }
    async fn discard_backlog(&self) -> Result<String> {
        Ok(String::new())
    }
    async fn poll(&self, _: &str) -> Result<(Vec<Inbound>, String)> {
        Ok((vec![], String::new()))
    }
    async fn create_conversation(&self, space: &Conversation, _: &str) -> Result<Conversation> {
        let target = Conversation {
            thread: uuid::Uuid::new_v4().to_string(),
            ..space.clone()
        };
        self.created.lock().await.push(target.clone());
        Ok(target)
    }
    async fn rename_conversation(
        &self,
        _: &Conversation,
        _: &str,
    ) -> std::result::Result<(), SendFailure> {
        Ok(())
    }
    async fn deliver(&self, d: &Delivery) -> std::result::Result<String, SendFailure> {
        assert_eq!(d.agent, self.agent);
        Ok("42".into())
    }
}

#[tokio::test]
async fn projects_share_agents_and_bots_dispatch_once_per_topic() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let second = dir.path().join("second");
    std::fs::create_dir(&second)?;
    let mut cfg = fake_config();
    cfg.projects.get_mut("demo").unwrap().cwd = dir.path().into();
    cfg.projects
        .get_mut("demo")
        .unwrap()
        .agents
        .push("personal".into());
    cfg.projects.insert(
        "second".into(),
        ProjectSpec {
            cwd: second.clone(),
            agents: vec!["work".into(), "personal".into()],
            telegram: ProjectTelegram {
                group: -1002,
                allowed_users: vec![1],
            },
        },
    );
    let mut personal = cfg.agents["work"].clone();
    personal.telegram.token_env = "PERSONAL_TOKEN".into();
    cfg.agents.insert("personal".into(), personal);
    let cfg = Arc::new(cfg);
    let work = Arc::new(ProjectIm {
        cfg: cfg.clone(),
        agent: "work".into(),
        created: Mutex::new(vec![]),
    });
    let personal = Arc::new(ProjectIm {
        cfg: cfg.clone(),
        agent: "personal".into(),
        created: Mutex::new(vec![]),
    });
    let path = dir.path().join("state.sqlite");
    let store = Store::open(&path)?;
    let agent = Arc::new(FakeAgent {
        store: Some(store.clone()),
        ..Default::default()
    });
    let svc = Arc::new(Service::new(
        cfg.clone(),
        store.clone(),
        agent.clone(),
        vec![work.clone(), personal.clone()],
    ));
    svc.initialize().await?;
    let a = store.ensure_session("work", "a").await?;
    let b = store.ensure_session("work", "b").await?;
    let c = store.ensure_session("personal", "c").await?;
    *agent.listed.lock().await = vec![
        SessionSummary {
            native: a.native.clone(),
            title: "Project A".into(),
            cwd: dir.path().into(),
            status: "idle".into(),
        },
        SessionSummary {
            native: b.native.clone(),
            title: "Project B".into(),
            cwd: second.clone(),
            status: "idle".into(),
        },
        SessionSummary {
            native: c.native.clone(),
            title: "Personal C".into(),
            cwd: dir.path().into(),
            status: "idle".into(),
        },
    ];
    for s in [&a, &b, &c] {
        svc.ensure_topics(s).await?;
    }
    let ta = store.targets(&a.id).await?.remove(0);
    let tb = store.targets(&b.id).await?.remove(0);
    let tc = store.targets(&c.id).await?.remove(0);
    assert_ne!(ta.space, tb.space);
    assert_eq!(ta.space, tc.space);
    assert_eq!(work.created.lock().await.len(), 2);
    assert_eq!(personal.created.lock().await.len(), 1);

    let receive_twice = |id: &str, target: &Conversation, text: &str| {
        let first = message(id, target, text);
        let mut second = first.clone();
        second.agent = "personal".into();
        (first, second)
    };
    let (first, second_msg) = receive_twice("shared-message", &tc, "only once");
    let (x, y) = tokio::join!(svc.handle(first), svc.handle(second_msg));
    x?;
    y?;
    assert_eq!(*agent.submitted.lock().await, vec![c.id.clone()]);
    let (first, second_msg) = receive_twice("shared-plan", &tc, "/plan");
    svc.handle(first).await?;
    svc.handle(second_msg).await?;
    assert_eq!(agent.modes.lock().await[&c.id], SessionMode::Plan);
    let mut wrong_bot = message("wrong-owner", &tc, "/plan@WorkBot");
    wrong_bot.addressed = true;
    svc.handle(wrong_bot).await?;
    assert_eq!(agent.modes.lock().await[&c.id], SessionMode::Plan);
    let (first, second_msg) = receive_twice("shared-list", &tc, "/agents");
    svc.handle(second_msg).await?;
    svc.handle(first).await?;
    let list = store
        .pending()
        .await?
        .into_iter()
        .find(|d| d.key == "shared-list:0")
        .unwrap();
    assert_eq!(list.agent, "work");
    assert!(list.text.contains("Project A") && list.text.contains("Personal C"));
    assert!(!list.text.contains("Project B"));
    assert_eq!(list.links.len(), 2);
    let mut directed = message("directed-query", &tc, "/agents@PersonalBot");
    directed.agent = "personal".into();
    directed.addressed = true;
    svc.handle(directed).await?;
    let directed = store
        .pending()
        .await?
        .into_iter()
        .find(|d| d.key == "directed-query:0")
        .unwrap();
    assert_eq!(directed.agent, "personal");

    let source = work.destination("demo").unwrap();
    let (first, second_msg) = receive_twice(
        "ambiguous-new",
        &source,
        &format!("/new {}", second.display()),
    );
    svc.handle(first).await?;
    svc.handle(second_msg).await?;
    assert!(
        !agent
            .management
            .lock()
            .await
            .iter()
            .any(|v| v["op"] == "create")
    );
    let unknown = dir.path().join("unregistered");
    std::fs::create_dir(&unknown)?;
    let mut invalid = message(
        "unregistered-new",
        &source,
        &format!("/new@PersonalBot {}", unknown.display()),
    );
    invalid.agent = "personal".into();
    invalid.addressed = true;
    svc.handle(invalid).await?;
    assert!(
        !agent
            .management
            .lock()
            .await
            .iter()
            .any(|v| v["op"] == "create")
    );
    let mut create = message(
        "explicit-new",
        &source,
        &format!("/new@PersonalBot {}", second.display()),
    );
    create.agent = "personal".into();
    create.addressed = true;
    svc.handle(create).await?;
    let new = store
        .bindings()
        .await?
        .1
        .into_iter()
        .find(|r| ![&a.id, &b.id, &c.id].contains(&&r.session.id))
        .unwrap();
    assert_eq!(new.session.agent, "personal");
    assert_eq!(new.target.space, tb.space);
    let receipt = store
        .pending()
        .await?
        .into_iter()
        .find(|d| d.key == "explicit-new:0")
        .unwrap();
    assert_eq!(receipt.agent, "personal");
    assert_eq!(receipt.target.space, source.space);
    assert!(receipt.links[0].url.contains(&new.target.space));

    // Changing cwd is not a move. Only a binding edit reassigns the project.
    agent.listed.lock().await[0].cwd = "/unregistered".into();
    svc.ensure_topics(&a).await?;
    assert_eq!(store.targets(&a.id).await?, vec![ta]);
    let mut document: BindingsDocument =
        serde_json::from_value(svc.bindings_snapshot().await?["document"].clone())?;
    let entry = document
        .bindings
        .iter_mut()
        .find(|e| e.session == "a")
        .unwrap();
    entry.project = "second".into();
    entry.topic = None;
    svc.apply_bindings(document).await?;
    assert_eq!(store.targets(&a.id).await?[0].space, tb.space);
    let reopened = Store::open(&path)?;
    let restarted = Service::new(cfg, reopened.clone(), agent, vec![work, personal]);
    restarted.initialize().await?;
    assert_eq!(reopened.bindings().await?.1.len(), 4);
    assert_eq!(reopened.targets(&a.id).await?[0].space, tb.space);
    Ok(())
}

#[tokio::test]
async fn removing_binding_disables_old_question_buttons_and_replies() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let agent = Arc::new(FakeAgent::default());
    let svc = Service::new(
        Arc::new(fake_config()),
        store.clone(),
        agent.clone(),
        vec![Arc::new(FakeIm)],
    );
    let session = store.ensure_session("work", "old-question").await?;
    svc.ensure_topics(&session).await?;
    let target = store.targets(&session.id).await?.remove(0);
    let event = AgentEvent {
        session: session.id.clone(),
        key: "request:old".into(),
        kind: EventKind::Question,
        text: String::new(),
        complete: true,
        origin: None,
        request: Some(PendingRequest {
            id: "old".into(),
            label: "Choose".into(),
            kind: RequestKind::Questions,
            blocking: true,
            choices: vec![],
            questions: vec![Question {
                id: "color".into(),
                title: "Color".into(),
                options: vec![("Blue".into(), String::new())],
                secret: false,
            }],
        }),
    };
    store.event(&event).await?;
    svc.project(&event, &target).await?;
    let card = question_card(&store, "old").await?;
    store.delivery(&card, "sent", Some("100".into())).await?;
    let token = card_action(&card, "Blue");
    let (revision, _) = store.bindings().await?;
    svc.apply_bindings(BindingsDocument {
        version: 2,
        revision,
        bindings: vec![],
    })
    .await?;
    let mut click = message("old-button", &target, "");
    click.action = Some(token);
    click.addressed = true;
    svc.handle(click).await?;
    let mut reply = message("old-reply", &target, "custom");
    reply.reply_to = Some("100".into());
    svc.handle(reply).await?;
    assert!(agent.responses.lock().await.is_empty());
    assert!(agent.inputs.lock().await.is_empty());
    assert!(svc.ensure_topics(&session).await.is_err());
    assert!(store.targets(&session.id).await?.is_empty());
    Ok(())
}
