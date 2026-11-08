use super::*;
use crate::agent::{codex::Codex, rpc::Rpc};
use anyhow::Context;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires UAOI_TEST_CODEX; isolated home and loopback model only"]
async fn native_shared_runtime_and_answers() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let binary = std::env::var("UAOI_TEST_CODEX")?;
    let home = tempfile::tempdir()?;
    let cwd = home.path().join("work");
    let ch = home.path().join("codex");
    let second_cwd = home.path().join("second-project");
    std::fs::create_dir(&second_cwd)?;
    std::fs::create_dir_all(&cwd)?;
    std::fs::create_dir_all(&ch)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let cancel = tokio_util::sync::CancellationToken::new();
    let _guard = cancel.clone().drop_guard();
    let stop = cancel.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut io, _) =
                tokio::select! {_=stop.cancelled()=>break,r=listener.accept()=>r.unwrap()};
            tokio::spawn(async move {
                let mut buf = vec![];
                let mut block = [0u8; 8192];
                loop {
                    let n = io.read(&mut block).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&block[..n]);
                    if let Some(end) = buf.windows(4).position(|x| x == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&buf[..end]);
                        let length = header
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|n| n.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if buf.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                let end = buf.windows(4).position(|x| x == b"\r\n\r\n").unwrap() + 4;
                let payload: serde_json::Value = serde_json::from_slice(&buf[end..]).unwrap();
                let input = payload["input"].as_array().cloned().unwrap_or_default();
                let last = input.iter().rposition(|v| v["role"] == "user").unwrap_or(0);
                let ask = input
                    .get(last)
                    .is_some_and(|v| v.to_string().contains("ASK SHARED"))
                    && !input
                        .iter()
                        .skip(last + 1)
                        .any(|v| v["type"] == "function_call_output");
                let prompt = input.get(last).map(Value::to_string).unwrap_or_default();
                let ask_async = prompt.contains("ASK ASYNC")
                    && !input
                        .iter()
                        .skip(last + 1)
                        .any(|v| v["type"] == "function_call_output");
                if prompt.contains("HOLD FOR STEER")
                    || input
                        .iter()
                        .skip(last)
                        .any(|item| item.to_string().contains("CUSTOMANSWER42"))
                {
                    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                }
                let plan_text = if prompt.contains("PLAN PROPOSAL") {
                    "<proposed_plan>\n# A test plan\nRun the unit tests.\n</proposed_plan>"
                } else if prompt.contains("CONTINUE LEGACY") {
                    if input
                        .iter()
                        .any(|v| v.to_string().contains("LEGACY CONTEXT MARKER"))
                    {
                        "LEGACY_CONTEXT_OK"
                    } else {
                        "LEGACY_CONTEXT_MISSING"
                    }
                } else {
                    "MOCK ANSWER"
                };
                let id = uuid::Uuid::new_v4().to_string();
                let item = if ask_async {
                    json!({"type":"function_call","call_id":format!("async_{id}"),"name":"request_user_input_async","arguments":json!({"questions":[{"title":"Which color?","options":["Blue","Green"]},{"title":"Which color?","options":["Blue","Green"]}]}).to_string()})
                } else if ask {
                    json!({"type":"function_call","call_id":format!("ask_{id}"),"name":"request_user_input","arguments":json!({"questions":[{"id":"choice","header":"Choice","question":"Choose a shared client","options":[{"label":"Terminal","description":"Answer locally"},{"label":"IM","description":"Answer remotely"}]}]}).to_string()})
                } else {
                    json!({"type":"message","role":"assistant","phase":"final_answer","id":format!("msg_{id}"),"content":[{"type":"output_text","text":plan_text}]})
                };
                let mut events = vec![
                    json!({"type":"response.created","response":{"id":id}}),
                    json!({"type":"response.output_item.done","item":item}),
                    json!({"type":"response.completed","response":{"id":id,"usage":{"input_tokens":12,"output_tokens":3,"total_tokens":15}}}),
                ];
                if prompt.contains("MINIMAL OUTPUT") {
                    events.insert(1, json!({"type":"response.output_item.done","output_index":0,
                        "item":{"type":"message","role":"assistant","phase":"commentary","id":format!("progress_{id}"),
                        "content":[{"type":"output_text","text":"INTERMEDIATE WORK MESSAGE"}]}}));
                    events[2]["output_index"] = json!(1);
                }
                let body = events
                    .iter()
                    .map(|e| format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap()))
                    .collect::<String>();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = io.write_all(response.as_bytes()).await;
            });
        }
    });
    std::fs::write(
        ch.join("models.json"),
        json!({"models":[{
            "slug":"mock-model",
            "display_name":"mock-model",
            "base_instructions":"You are a test assistant.",
            "supported_reasoning_levels":[{"effort":"medium","description":"Test reasoning"}],
            "shell_type":"shell_command",
            "visibility":"list",
            "supported_in_api":true,
            "priority":0,
            "support_verbosity":false,
            "truncation_policy":{"mode":"tokens","limit":10000},
            "experimental_supported_tools":["request_user_input_async"]
        }]})
        .to_string(),
    )?;
    std::fs::write(
        ch.join("config.toml"),
        format!(
            r#"model = "mock-model"
model_catalog_json = "{}/models.json"
model_provider = "mock"
approval_policy = "never"
sandbox_mode = "read-only"
check_for_update_on_startup = false
thread_unload_delay_secs = 1
[model_providers.mock]
name = "Local mock"
base_url = "http://127.0.0.1:{port}/v1"
wire_api = "responses"
requires_openai_auth = false
supports_websockets = false
request_max_retries = 0
stream_max_retries = 0
[analytics]
enabled = false
[feedback]
enabled = false
[projects."{}"]
trust_level = "trusted"
"#,
            ch.display(),
            cwd.display()
        ),
    )?;

    let mut cfg = fake_config();
    cfg.state_dir = Some(home.path().join("state"));
    cfg.runtime_dir = Some(home.path().join("run"));
    std::fs::create_dir_all(cfg.runtime())?;
    let spec = cfg.agents.get_mut("work").unwrap();
    spec.command = vec![binary];
    spec.env = BTreeMap::from([
        ("CODEX_HOME".into(), ch.display().to_string()),
        ("OPENAI_API_KEY".into(), String::new()),
        ("CODEX_SQLITE_HOME".into(), ch.display().to_string()),
    ]);
    let personal_home = home.path().join("personal");
    std::fs::create_dir_all(&personal_home)?;
    std::fs::copy(ch.join("config.toml"), personal_home.join("config.toml"))?;
    let mut personal = cfg.agents["work"].clone();
    personal.telegram.token_env = Some("PERSONAL_TEST_TOKEN".into());
    personal
        .env
        .insert("CODEX_HOME".into(), personal_home.display().to_string());
    personal.env.insert(
        "CODEX_SQLITE_HOME".into(),
        personal_home.display().to_string(),
    );
    cfg.agents.insert("personal".into(), personal);
    let mut second_project = cfg.projects["demo"].clone();
    second_project.cwd = second_cwd.clone();
    second_project.telegram.group = -1002;
    cfg.projects.insert("second".into(), second_project);
    cfg.projects.get_mut("demo").unwrap().cwd = cwd.clone();
    cfg.projects
        .get_mut("demo")
        .unwrap()
        .agents
        .push("personal".into());
    // This history predates the managed runtime and has never had a binding.
    let legacy_id = seed_native_history(&cfg.agents["work"], &cwd, home.path()).await?;
    let store = Store::open(&home.path().join("state.sqlite"))?;
    let (events, _) = tokio::sync::broadcast::channel(4096);
    let mut received = events.subscribe();
    let backend = Arc::new(Codex::new(
        Arc::new(cfg.clone()),
        store.clone(),
        events.clone(),
        cancel.clone(),
    ));
    backend
        .activate("work", &std::env::vars().collect())
        .await?;
    backend
        .activate("personal", &std::env::vars().collect())
        .await?;
    let session = backend.create("work", &cwd).await?;
    let initial_status = backend.status(&session).await?;
    assert_eq!(initial_status.cwd, cwd);
    assert_eq!(initial_status.mode, Some(SessionMode::Default));
    assert_eq!(backend.toggle_plan_mode(&session).await?, SessionMode::Plan);
    assert_eq!(
        backend.toggle_plan_mode(&session).await?,
        SessionMode::Default
    );
    assert!(
        store
            .native_session("work", &session.native)
            .await?
            .is_some(),
        "explicit IM creation remains immediate"
    );
    backend.submit(&session, "FROM TELEGRAM", "first").await?;
    wait_finished(&mut received, &session).await?;
    let plan = backend
        .open("cli-a", "work", &cwd, &std::env::vars().collect(), &[])
        .await?;
    let native = Rpc::open(std::path::Path::new(
        plan.command
            .last()
            .unwrap()
            .strip_prefix("unix://")
            .unwrap(),
    ))
    .await?;
    native.initialize().await?;
    native
        .request(
            "thread/resume",
            json!({"threadId":session.native,"excludeTurns":true}),
        )
        .await
        .context("resume existing thread through CLI gateway")?;
    assert!(
        native
            .request("thread/start", json!({"cwd":home.path()}))
            .await
            .is_err(),
        "CLI creation outside configured projects must be rejected"
    );
    assert!(backend.create("work", home.path()).await.is_err());
    let second = native
        .request("thread/start", json!({"cwd":second_cwd}))
        .await
        .context("create blank thread through CLI gateway")?;
    assert_ne!(session.native, second["thread"]["id"]);
    assert_eq!(second["thread"]["cwd"], second_cwd.display().to_string());
    assert_eq!(
        std::fs::read_dir(cfg.runtime())?
            .filter_map(Result::ok)
            .filter(|p| p.path().join("engine.sock").exists())
            .count(),
        2,
        "multiple projects reuse the agent's server"
    );
    let second_id = second["thread"]["id"].as_str().unwrap();
    native
        .request(
            "thread/name/set",
            json!({"threadId":second_id,"name":"Unsaved CLI draft"}),
        )
        .await?;
    native.request("thread/settings/update",json!({"threadId":second_id,"collaborationMode":{"mode":"plan","settings":{"model":"mock-model","reasoning_effort":null,"developer_instructions":null}}})).await?;
    // Background supervision must not publish an empty CLI draft.
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    assert!(store.native_session("work", second_id).await?.is_none());
    native.request("turn/start", json!({"threadId":second_id,"input":[{"type":"text","text":"FIRST REAL CLI INPUT PLAN PROPOSAL","text_elements":[]}]})).await?;
    let promoted = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(s) = store.native_session("work", second_id).await? {
                break Ok::<_, anyhow::Error>(s);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    let initial_plan = wait_question(&mut received, RequestKind::Plan).await?;
    wait_finished(&mut received, &promoted).await?;
    backend.respond(&promoted, &initial_plan.id, "stay").await?;
    let history = store.history(&promoted.id, 20, 0).await?;
    assert!(
        history
            .iter()
            .any(|e| e.text == "FIRST REAL CLI INPUT PLAN PROPOSAL")
    );
    assert_eq!(
        history
            .iter()
            .filter(|e| matches!(e.kind, EventKind::Registered))
            .count(),
        1
    );
    let abandoned = native.request("thread/start", json!({"cwd":cwd})).await?;
    assert!(
        backend.active(&session).await,
        "switching the CLI must not release the old session"
    );
    backend.close("cli-a").await;
    assert!(
        backend.active(&session).await,
        "closing the CLI must not stop the server"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1400)).await;
    assert!(
        store
            .native_session("work", abandoned["thread"]["id"].as_str().unwrap())
            .await?
            .is_none()
    );
    backend
        .submit(&promoted, "AFTER CLI EXIT", "promoted")
        .await
        .context("submit promoted thread after CLI exit")?;
    wait_finished(&mut received, &promoted)
        .await
        .context("finish promoted thread after CLI exit")?;

    backend
        .submit(&session, "AFTER CLI EXIT", "after-exit")
        .await?;
    wait_finished(&mut received, &session).await?;
    let plan = backend
        .open("cli-b", "work", &cwd, &std::env::vars().collect(), &[])
        .await?;
    let cli = Rpc::open(std::path::Path::new(
        plan.command
            .last()
            .unwrap()
            .strip_prefix("unix://")
            .unwrap(),
    ))
    .await?;
    cli.initialize().await?;
    cli.request(
        "thread/resume",
        json!({"threadId":session.native,"excludeTurns":true}),
    )
    .await?;

    // Resume alone does not bind history. A new message binds the same native
    // session, and subsequent Telegram input retains its original context.
    let im = Arc::new(TitleIm::default());
    let mut projection_cfg = cfg.clone();
    projection_cfg.agents.get_mut("work").unwrap().output = OutputMode::Minimal;
    let svc = Arc::new(Service::new(
        Arc::new(projection_cfg),
        store.clone(),
        backend.clone(),
        vec![im.clone()],
    ));
    let projection_cancel = cancel.child_token();
    let projection = tokio::spawn(
        svc.clone()
            .events(events.subscribe(), projection_cancel.clone()),
    );
    cli.request(
        "thread/resume",
        json!({"threadId":legacy_id,"excludeTurns":true}),
    )
    .await?;
    cli.request(
        "thread/name/set",
        json!({"threadId":legacy_id,"name":"Existing conversation"}),
    )
    .await?;
    cli.request("thread/settings/update", json!({"threadId":legacy_id,"collaborationMode":{"mode":"default","settings":{"model":"mock-model","reasoning_effort":null,"developer_instructions":null}}})).await?;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(store.native_session("work", &legacy_id).await?.is_none());
    assert!(im.created.lock().await.is_empty());
    cli.request("turn/start", json!({"threadId":legacy_id,"input":[{"type":"text","text":"CONTINUE LEGACY FROM CLI","text_elements":[]}]})).await?;
    let (legacy, target) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(s) = store.native_session("work", &legacy_id).await?
                && let Some(target) = store.targets(&s.id).await?.into_iter().next()
            {
                break Ok::<_, anyhow::Error>((s, target));
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert_eq!(legacy.native, legacy_id);
    wait_finished(&mut received, &legacy).await?;
    svc.handle(message(
        "legacy-telegram",
        &target,
        "CONTINUE LEGACY FROM TELEGRAM",
    ))
    .await?;
    wait_finished(&mut received, &legacy).await?;
    svc.handle(message(
        "legacy-minimal",
        &target,
        "CONTINUE LEGACY MINIMAL OUTPUT",
    ))
    .await?;
    wait_finished(&mut received, &legacy).await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let pending = store.pending().await?;
            if pending.iter().filter(|d| d.target == target && d.text == "LEGACY_CONTEXT_OK").count() == 3 {
                assert!(!pending.iter().any(|d| d.target == target && d.text.contains("INTERMEDIATE WORK MESSAGE")));
                break Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await??;
    backend.history(&legacy).await?;
    let history = store.history(&legacy.id, 100, 0).await?;
    assert!(history.iter().any(|e| e.text == "LEGACY CONTEXT MARKER"));
    assert_eq!(
        history
            .iter()
            .filter(|e| e.text == "LEGACY_CONTEXT_OK")
            .count(),
        3
    );
    assert_eq!(im.created.lock().await.len(), 1);
    let mut document: BindingsDocument =
        serde_json::from_value(svc.bindings_snapshot().await?["document"].clone())?;
    document.bindings.clear();
    svc.apply_bindings(document).await?;
    cli.request("turn/start", json!({"threadId":legacy_id,"input":[{"type":"text","text":"CONTINUE LEGACY AFTER UNBIND","text_elements":[]}]})).await?;
    wait_finished(&mut received, &legacy).await?;
    assert!(store.targets(&legacy.id).await?.is_empty());
    assert!(store.was_bound(&legacy.id).await?);
    assert_eq!(im.created.lock().await.len(), 1);
    projection_cancel.cancel();
    projection.await?;

    let mut mode_notices = cli.notices.subscribe();
    backend.set_mode(&session, SessionMode::Plan).await?;
    assert_eq!(
        backend.status(&session).await?.mode,
        Some(SessionMode::Plan)
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let notice = mode_notices.recv().await?;
            if notice["method"] == "thread/settings/updated"
                && notice["params"]["threadId"] == session.native
            {
                assert_eq!(
                    notice["params"]["threadSettings"]["collaborationMode"]["mode"],
                    "plan"
                );
                break Ok::<_, anyhow::Error>(());
            }
        }
    })
    .await??;
    cli.request("thread/settings/update",json!({"threadId":session.native,"collaborationMode":{"mode":"default","settings":{"model":"mock-model","reasoning_effort":null,"developer_instructions":null}}})).await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if backend.status(&session).await?.mode == Some(SessionMode::Default) {
                break Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert_eq!(backend.toggle_plan_mode(&session).await?, SessionMode::Plan);
    backend.submit(&session, "ASK SHARED", "ask").await?;
    let request = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if let Some(r) = received.recv().await?.request {
                break Ok::<_, anyhow::Error>(r);
            }
        }
    })
    .await??;
    assert_eq!(request.questions[0].options.len(), 2);
    backend.respond(&session, &request.id, "IM").await?;
    wait_finished(&mut received, &session).await?;
    assert!(store.request(&request.id).await.is_err());
    backend.history(&session).await?;
    assert!(
        store
            .history(&session.id, 100, 0)
            .await?
            .iter()
            .any(|e| e.text.contains("AFTER CLI EXIT"))
    );
    let mut cli_notices = cli.notices.subscribe();
    backend.submit(&session, "ASK SHARED", "ask-cli").await?;
    let request = wait_question(&mut received, RequestKind::Questions).await?;
    let wire = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let v = cli_notices.recv().await?;
            if v["method"] == "item/tool/requestUserInput" {
                break Ok::<_, anyhow::Error>(v);
            }
        }
    })
    .await??;
    cli.send(json!({"id":wire["id"],"result":{"answers":{"choice":{"answers":["Terminal"]}}}}))
        .await?;
    wait_finished(&mut received, &session).await?;
    assert!(store.request(&request.id).await.is_err());
    assert!(
        store
            .history(&session.id, 100, 0)
            .await?
            .iter()
            .any(|e| e.text.contains("Answer submitted via CLI") && e.text.contains("Terminal"))
    );
    backend
        .submit(&session, "PLAN PROPOSAL", "plan-stay")
        .await?;
    let stay = wait_question(&mut received, RequestKind::Plan).await?;
    wait_finished(&mut received, &session).await?;
    assert_eq!(
        stay.choices
            .iter()
            .map(|(_, v)| v.as_str())
            .collect::<Vec<_>>(),
        vec!["execute", "stay"]
    );
    backend.respond(&session, &stay.id, "stay").await?;
    assert!(!backend.busy().await);
    assert!(
        backend
            .respond(&session, &stay.id, "execute")
            .await
            .is_err()
    );
    backend
        .submit(&session, "PLAN PROPOSAL", "plan-execute")
        .await?;
    let execute = wait_question(&mut received, RequestKind::Plan).await?;
    wait_finished(&mut received, &session).await?;
    backend.respond(&session, &execute.id, "execute").await?;
    wait_finished(&mut received, &session).await?;
    assert_eq!(
        backend.status(&session).await?.mode,
        Some(SessionMode::Default)
    );
    backend
        .submit_in_mode(&session, "PLAN PROPOSAL", "inline-plan", SessionMode::Plan)
        .await?;
    let inline_plan = wait_question(&mut received, RequestKind::Plan).await?;
    wait_finished(&mut received, &session).await?;
    assert_eq!(
        backend.status(&session).await?.mode,
        Some(SessionMode::Plan)
    );
    backend.respond(&session, &inline_plan.id, "stay").await?;
    backend
        .submit_in_mode(
            &session,
            "DEFAULT TASK",
            "inline-default",
            SessionMode::Default,
        )
        .await?;
    wait_finished(&mut received, &session).await?;
    backend.submit(&session, "HOLD FOR STEER", "hold").await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let state = cli
                .request(
                    "thread/read",
                    json!({"threadId":session.native,"includeTurns":false}),
                )
                .await?;
            if state["thread"]["status"]["type"] == "active" {
                break Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert!(backend.busy().await);
    assert!(backend.working(&session).await);
    assert!(
        backend
            .submit_in_mode(
                &session,
                "REJECTED INLINE PLAN",
                "busy-plan",
                SessionMode::Plan
            )
            .await
            .is_err()
    );
    assert_eq!(
        backend.status(&session).await?.mode,
        Some(SessionMode::Default)
    );
    assert_eq!(backend.toggle_plan_mode(&session).await?, SessionMode::Plan);
    assert!(
        backend.working(&session).await,
        "mode-only changes must not interrupt the active turn"
    );
    assert_eq!(
        backend.toggle_plan_mode(&session).await?,
        SessionMode::Default
    );
    backend.submit(&session, "STEER ADDITION", "steer").await?;
    wait_finished(&mut received, &session).await?;
    assert!(!backend.working(&session).await);
    backend.history(&session).await?;
    assert!(
        store
            .history(&session.id, 100, 0)
            .await?
            .iter()
            .any(|e| e.text.contains("STEER ADDITION"))
    );
    let personal = backend.create("personal", &cwd).await?;
    backend
        .submit(&personal, "PERSONAL ONLY", "personal")
        .await?;
    wait_finished(&mut received, &personal).await?;
    assert!(backend.restore("work", &personal.native).await.is_err());
    let terminal = backend
        .open("real-tui", "work", &cwd, &std::env::vars().collect(), &[])
        .await?;
    run_terminal(&terminal, &session.native).await?;
    run_terminal_async_answers(&backend, &terminal, &store, &mut received).await?;
    backend.close("real-tui").await;
    assert!(backend.active(&session).await);
    backend
        .submit(&session, "AFTER REAL TUI", "post-tui")
        .await?;
    wait_finished(&mut received, &session).await?;

    // Messages outside configured projects never create a binding.
    let mut engine = None;
    for entry in std::fs::read_dir(cfg.runtime())? {
        let path = entry?.path().join("engine.sock");
        if !path.exists() {
            continue;
        }
        let rpc = Rpc::open(&path).await?;
        rpc.initialize().await?;
        if rpc
            .request(
                "thread/read",
                json!({"threadId":session.native,"includeTurns":false}),
            )
            .await
            .is_ok()
        {
            engine = Some(rpc);
            break;
        }
    }
    let engine = engine.context("work agent engine not found")?;
    let other_cwd = home.path().join("another-directory");
    std::fs::create_dir(&other_cwd)?;
    let unbound = engine
        .request("thread/start", json!({"cwd":other_cwd}))
        .await?;
    let unbound_id = unbound["thread"]["id"].as_str().unwrap();
    assert!(
        cli.request("thread/fork", json!({"threadId":unbound_id}))
            .await
            .is_err(),
        "fork must validate the inherited cwd"
    );
    let mut engine_notices = engine.notices.subscribe();
    engine.request("turn/start", json!({"threadId":unbound_id,"input":[{"type":"text","text":"UNBOUND SEED","text_elements":[]}]})).await?;
    wait_native_finished(&mut engine_notices, unbound_id).await?;
    cli.request(
        "thread/resume",
        json!({"threadId":unbound_id,"excludeTurns":true}),
    )
    .await?;
    cli.request("turn/start", json!({"threadId":unbound_id,"input":[{"type":"text","text":"HOLD FOR STEER","text_elements":[]}]})).await?;
    assert!(
        backend.busy().await,
        "unbound native work is included in runtime status"
    );
    assert!(store.native_session("work", unbound_id).await?.is_none());
    wait_native_finished(&mut engine_notices, unbound_id).await?;
    assert!(store.native_session("work", unbound_id).await?.is_none());
    assert!(!backend.busy().await);
    backend.close("cli-b").await;
    backend
        .submit(&session, "HOLD FOR STEER", "shutdown-active-turn")
        .await?;
    assert!(backend.busy().await);
    backend.deactivate("work").await;
    assert!(backend.status(&session).await.is_err());
    assert!(
        backend.create("work", &cwd).await.is_err(),
        "queries must never restart an offline runtime"
    );
    let other = backend.create("personal", &cwd).await?;
    assert!(
        backend.status(&other).await.is_ok(),
        "other profiles keep working"
    );
    backend
        .activate("work", &std::env::vars().collect())
        .await?;
    backend.restore("work", &session.native).await?;
    assert!(backend.status(&session).await.is_ok());
    backend.shutdown().await;
    cancel.cancel();
    server.abort();
    Ok(())
}
async fn seed_native_history(
    spec: &crate::config::AgentSpec,
    cwd: &std::path::Path,
    home: &std::path::Path,
) -> Result<String> {
    let socket = home.join("seed.sock");
    let mut child = tokio::process::Command::new(&spec.command[0])
        .args(&spec.command[1..])
        .args([
            "app-server",
            "--listen",
            &format!("unix://{}", socket.display()),
        ])
        .envs(&spec.env)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let rpc = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Ok(rpc) = Rpc::open(&socket).await {
                break rpc;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await?;
    rpc.initialize().await?;
    let thread = rpc.request("thread/start", json!({"cwd":cwd})).await?;
    let native = thread["thread"]["id"]
        .as_str()
        .context("Missing seeded thread ID")?
        .to_owned();
    let mut notices = rpc.notices.subscribe();
    rpc.request("turn/start", json!({"threadId":native,"input":[{"type":"text","text":"LEGACY CONTEXT MARKER","text_elements":[]}]})).await?;
    wait_native_finished(&mut notices, &native).await?;
    rpc.close();
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    if tokio::time::timeout(std::time::Duration::from_secs(5), child.wait())
        .await
        .is_err()
    {
        child.kill().await?;
    }
    Ok(native)
}
async fn wait_native_finished(
    rx: &mut tokio::sync::broadcast::Receiver<Value>,
    native: &str,
) -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let event = rx.recv().await?;
            if event["method"] == "turn/completed" && event["params"]["threadId"] == native {
                break Ok::<_, anyhow::Error>(());
            }
        }
    })
    .await??;
    Ok(())
}
async fn wait_finished(
    rx: &mut tokio::sync::broadcast::Receiver<AgentEvent>,
    session: &Session,
) -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let event = rx.recv().await?;
            if event.session == session.id && matches!(event.kind, EventKind::Finished) {
                break Ok::<_, anyhow::Error>(());
            }
        }
    })
    .await??;
    Ok(())
}

async fn wait_question(
    rx: &mut tokio::sync::broadcast::Receiver<AgentEvent>,
    kind: RequestKind,
) -> Result<PendingRequest> {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if let Some(r) = rx.recv().await?.request
                && r.kind == kind
            {
                break Ok::<_, anyhow::Error>(r);
            }
        }
    })
    .await?
}

async fn run_terminal(plan: &LaunchPlan, session: &str) -> Result<()> {
    let script = r#"
import os,sys,json,pty,fcntl,termios,struct,select,time,signal
command=json.loads(sys.argv[1])+['resume',sys.argv[2]]
pid,fd=pty.fork()
if pid==0:
    os.environ['TERM']='xterm-256color'
    os.execv(command[0],command)
fcntl.ioctl(fd,termios.TIOCSWINSZ,struct.pack('HHHH',40,120,0,0))
output=bytearray()
def pump(seconds):
    until=time.monotonic()+seconds
    while time.monotonic()<until:
        if not select.select([fd],[],[],.05)[0]:continue
        try:data=os.read(fd,65536)
        except OSError:return
        if not data:return
        output.extend(data)
        for query,answer in [(b'\x1b[6n',b'\x1b[1;1R'),(b'\x1b[c',b'\x1b[?1;2c'),(b'\x1b]10;?',b'\x1b]10;rgb:ffff/ffff/ffff\x1b\\'),(b'\x1b]11;?',b'\x1b]11;rgb:0000/0000/0000\x1b\\')]:
            if query in data:os.write(fd,answer)
try:
    pump(2)
    os.write(fd,b'FROM REAL TUI')
    pump(.2)
    os.write(fd,b'\r')
    end=time.monotonic()+20
    while b'MOCK ANSWER' not in output and time.monotonic()<end:pump(.1)
    assert b'MOCK ANSWER' in output,output[-3000:].decode(errors='replace')
    os.write(fd,b'/agents');pump(.2);os.write(fd,b'\r');pump(.8)
    assert b'Manage' in output or b'Agent' in output or b'agent' in output
finally:
    os.kill(pid,signal.SIGTERM)
    os.waitpid(pid,0)
    os.close(fd)
"#;
    let status = tokio::process::Command::new(
        std::env::var("UAOI_TEST_PYTHON").unwrap_or_else(|_| "python3".into()),
    )
    .args([
        "-c",
        script,
        &serde_json::to_string(&plan.command)?,
        session,
    ])
    .envs(&plan.env)
    .current_dir(&plan.cwd)
    .status()
    .await?;
    anyhow::ensure!(status.success(), "Native TUI integration failed");
    Ok(())
}

async fn run_terminal_async_answers(
    backend: &Codex,
    plan: &LaunchPlan,
    store: &Store,
    received: &mut tokio::sync::broadcast::Receiver<AgentEvent>,
) -> Result<()> {
    let session = backend.create("work", &plan.cwd).await?;
    backend
        .submit(&session, "ASYNC TEST SEED", "async-seed")
        .await?;
    wait_finished(received, &session).await?;
    let temp = tempfile::tempdir()?;
    let log = temp.path().join("terminal.log");
    let mut command = plan.command.clone();
    command.extend(["resume".into(), session.native.clone()]);
    let observer = Rpc::open(std::path::Path::new(
        plan.command
            .last()
            .unwrap()
            .strip_prefix("unix://")
            .unwrap(),
    ))
    .await?;
    observer.initialize().await?;
    observer
        .request(
            "thread/resume",
            json!({"threadId":session.native,"excludeTurns":true}),
        )
        .await?;
    let mut child = tokio::process::Command::new(
        std::env::var("UAOI_TEST_PYTHON").unwrap_or_else(|_| "python3".into()),
    )
    .args(["-c", include_str!("terminal_fixture.py")])
    .envs(&plan.env)
    .env("TERM", "xterm-256color")
    .env("UAOI_TERMINAL_COMMAND", serde_json::to_string(&command)?)
    .env("UAOI_TERMINAL_LOG", &log)
    .current_dir(&plan.cwd)
    .stdin(std::process::Stdio::piped())
    .stdout(std::process::Stdio::null())
    .kill_on_drop(true)
    .spawn()?;
    let mut input = child.stdin.take().context("Missing terminal input")?;
    let result = async {
        wait_terminal_text(&log, 0, "mock-model").await?;
        for (prompt, active) in [("ASK ASYNC", false), ("ASK ASYNC HOLD FOR STEER", true)] {
            let before = std::fs::read(&log)?.len();
            backend.submit(&session, prompt, prompt).await?;
            let request = wait_question(received, RequestKind::AsyncQuestions).await?;
            if !active {
                wait_finished(received, &session).await?;
            }
            assert_eq!(request.questions.len(), 2);
            assert_eq!(request.questions[0].title, request.questions[1].title);
            assert_ne!(request.questions[0].id, request.questions[1].id);
            // Keep the editor open while another client submits both answers.
            input.write_all(b"\x1b[1;3A").await?;
            wait_terminal_text(&log, before, "color?").await?;
            assert_eq!(backend.working(&session).await, active);
            let before = std::fs::read(&log)?.len();
            let answers = BTreeMap::from([
                (request.questions[0].id.clone(), "Blue"),
                (request.questions[1].id.clone(), "紫色CUSTOMANSWER42"),
            ]);
            let answer = serde_json::to_string(&answers)?;
            let mut notices = observer.notices.subscribe();
            backend.respond(&session, &request.id, &answer).await?;
            if !active {
                wait_finished(received, &session).await?;
            } else {
                // A steer acknowledgment only confirms queueing. Wait until the
                // native runtime commits the reply, while work is still active.
                tokio::time::timeout(std::time::Duration::from_secs(20), async {
                    loop {
                        let notice = notices.recv().await?;
                        if notice["method"] == "item/completed"
                            && notice["params"]["item"]["type"] == "userMessage"
                            && notice["params"]["item"]["clientId"]
                                == format!("answer:{}", request.id)
                        {
                            break Ok::<_, anyhow::Error>(());
                        }
                    }
                })
                .await??;
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            assert!(store.request(&request.id).await.is_err());
            assert!(
                backend
                    .respond(&session, &request.id, &answer)
                    .await
                    .is_err()
            );
            wait_terminal_text(&log, before, "CUSTOMANSWER42").await?;
            if active {
                assert!(backend.working(&session).await);
            }

            // A stale editor would consume this input and submit Blue again.
            // With both questions resolved, navigation leaves ordinary input usable.
            input.write_all(b"\x1b[1;3A").await?;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let followup = format!("AFTER ASYNC ANSWERS {active}");
            input.write_all(followup.as_bytes()).await?;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            input.write_all(b"\r").await?;
            tokio::time::timeout(std::time::Duration::from_secs(20), async {
                loop {
                    let event = received.recv().await?;
                    if event.session == session.id
                        && matches!(event.kind, EventKind::User)
                        && event.text == followup
                    {
                        break Ok::<_, anyhow::Error>(());
                    }
                }
            })
            .await
            .context("Native editor consumed the follow-up as another answer")??;
            wait_finished(received, &session).await?;
            backend.history(&session).await?;
            assert!(
                store
                    .history(&session.id, 100, 0)
                    .await?
                    .iter()
                    .any(|e| matches!(e.kind, EventKind::User) && e.text == followup),
                "remote answers must dismiss their native editors"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    observer.close();
    drop(input);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await??;
    anyhow::ensure!(status.success(), "Async-question terminal failed");
    result
}

async fn wait_terminal_text(path: &std::path::Path, start: usize, text: &str) -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if std::fs::read(path).ok().is_some_and(|bytes| {
                bytes
                    .get(start..)
                    .is_some_and(|bytes| String::from_utf8_lossy(bytes).contains(text))
            }) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| format!("Terminal did not display {text:?}"))?;
    Ok(())
}
