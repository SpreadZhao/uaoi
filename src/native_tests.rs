use super::*;
use crate::agent::{codex::Codex, rpc::Rpc};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
async fn wait_title(store: &Store, session: &Session, title: &str) -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store.title(session).await? != title {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("Native title update was not observed")??;
    Ok(())
}
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires UAOI_TEST_CODEX; uses isolated home and mock model"]
async fn native_lifecycle_and_shared_clients() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("uaoi=debug")
        .try_init();
    let binary = std::env::var("UAOI_TEST_CODEX")?;
    let home = tempfile::tempdir()?;
    let cwd = home.path().join("work");
    let ch = home.path().join("codex");
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
                let id = uuid::Uuid::new_v4().to_string();
                let item = if ask {
                    json!({"type":"function_call","call_id":format!("ask_{id}"),"name":"request_user_input","arguments":json!({"questions":[{"id":"choice","header":"Choice","question":"Choose a shared client","options":[{"label":"Terminal","description":"Answer locally"},{"label":"IM","description":"Answer remotely"}]}]}).to_string()})
                } else {
                    json!({"type":"message","role":"assistant","id":format!("msg_{id}"),"content":[{"type":"output_text","text":"MOCK ANSWER"}]})
                };
                let events = [
                    json!({"type":"response.created","response":{"id":id}}),
                    json!({"type":"response.output_item.done","item":item}),
                    json!({"type":"response.completed","response":{"id":id,"usage":{"input_tokens":12,"output_tokens":3,"total_tokens":15}}}),
                ];
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
        ch.join("config.toml"),
        format!(
            r#"model = "mock-model"
model_provider = "mock"
approval_policy = "never"
sandbox_mode = "read-only"
check_for_update_on_startup = false
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
            cwd.display()
        ),
    )?;
    let mut cfg = Config {
        state_dir: Some(home.path().join("state")),
        runtime_dir: Some(home.path().join("run")),
        ..Config::default()
    };
    cfg.agents.insert(
        "test".into(),
        AgentSpec {
            kind: "codex".into(),
            command: vec![binary],
            env: BTreeMap::from([
                ("CODEX_HOME".into(), ch.display().to_string()),
                ("OPENAI_API_KEY".into(), String::new()),
                ("CODEX_SQLITE_HOME".into(), ch.display().to_string()),
            ]),
        },
    );
    cfg.projects.insert(
        "demo".into(),
        Project {
            workspace: cwd,
            agent: "test".into(),
        },
    );
    let store = Store::open(&home.path().join("state.sqlite"))?;
    let (events, _) = tokio::sync::broadcast::channel(4096);
    let mut received = events.subscribe();
    let backend = Arc::new(Codex::new(
        Arc::new(cfg),
        store.clone(),
        events,
        cancel.clone(),
    ));
    let plan = backend
        .open(
            "cli-a",
            "demo",
            &home.path().join("work"),
            &std::env::vars().collect(),
            &[
                "--ask-for-approval".into(),
                "never".into(),
                "--sandbox".into(),
                "workspace-write".into(),
            ],
        )
        .await?;
    let path = std::path::Path::new(
        plan.command
            .last()
            .unwrap()
            .strip_prefix("unix://")
            .unwrap(),
    );
    let native = Rpc::open(path).await?;
    native.initialize().await?;
    let root = native
        .request(
            "thread/start",
            json!({"cwd":plan.cwd,"approvalPolicy":"never","sandbox":"read-only"}),
        )
        .await?;
    assert_eq!(root["approvalPolicy"], "never");
    assert_eq!(root["sandbox"]["type"], "workspaceWrite");
    let id = root["thread"]["id"].as_str().unwrap();
    let s = store
        .sessions()
        .await?
        .into_iter()
        .find(|s| s.native == id)
        .unwrap();
    assert!(backend.active(&s).await);
    native
        .request(
            "thread/name/set",
            json!({"threadId":s.native,"name":"Initial title 中文"}),
        )
        .await?;
    wait_title(&store, &s, "Initial title 中文").await?;
    // Optional metadata projection into the dedicated Telegram test group.
    // This does not poll the bot or touch any existing user session.
    let title_worker = if let Ok(path) = std::env::var("UAOI_TEST_TELEGRAM_CONFIG") {
        let cfg = Arc::new(Config::load(std::path::Path::new(&path))?);
        let telegram = Arc::new(crate::im::telegram::Telegram::new(
            cfg.im
                .telegram
                .clone()
                .context("Missing Telegram test config")?,
        )?);
        telegram.validate().await?;
        let svc = Arc::new(Service::new(
            cfg,
            store.clone(),
            backend.clone(),
            vec![telegram],
        ));
        svc.ensure_topics(&s).await?;
        if let Ok(path) = std::env::var("UAOI_TEST_TITLE_RECEIPT") {
            std::fs::write(path, serde_json::to_vec(&store.targets(&s.id).await?)?)?;
        }
        Some(tokio::spawn(svc.sync_titles(cancel.clone())))
    } else {
        None
    };

    backend.submit(&s, "FROM IM", "im-1", false).await?;
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if matches!(received.recv().await?.kind, EventKind::Finished) {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    native.request("turn/start",json!({"threadId":s.native,"input":[{"type":"text","text":"FROM CLI","text_elements":[]}]})).await?;
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let e = received.recv().await?;
            if matches!(e.kind, EventKind::Finished) {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    backend.history(&s).await?;
    let history = store.history(&s.id, 100, 0).await?;
    assert!(history.iter().any(|e| e.text.contains("FROM IM")));
    assert!(history.iter().any(|e| e.text.contains("FROM CLI")));
    native.request("thread/settings/update",json!({"threadId":s.native,"collaborationMode":{"mode":"plan","settings":{"model":"mock-model","reasoning_effort":null,"developer_instructions":null}}})).await?;
    backend.submit(&s, "ASK SHARED", "ask", false).await?;
    let request = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Some(request) = received.recv().await?.request {
                break Ok::<_, anyhow::Error>(request);
            }
        }
    })
    .await??;
    backend.respond(&s, &request.id, "IM").await?;
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if matches!(received.recv().await?.kind, EventKind::Finished) {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert!(store.request(&request.id).await.is_err());
    let before = store.sessions().await?.len();
    let hidden = native
        .request(
            "thread/start",
            json!({"cwd":plan.cwd,"ephemeral":true,"threadSource":"feature:thread_title","sandbox":"read-only"}),
        )
        .await?;
    assert_eq!(
        hidden["sandbox"]["type"], "readOnly",
        "launch settings must not widen internal metadata threads"
    );
    assert!(backend.active(&s).await);
    assert_eq!(
        store.sessions().await?.len(),
        before,
        "hidden metadata threads must not become managed sessions"
    );
    native
        .request(
            "thread/unsubscribe",
            json!({"threadId":hidden["thread"]["id"]}),
        )
        .await?;
    // A failed navigation keeps the old connection active.
    assert!(
        native
            .request(
                "thread/resume",
                json!({"threadId":"00000000-0000-0000-0000-000000000000"})
            )
            .await
            .is_err()
    );
    assert!(backend.active(&s).await);
    let second = native
        .request("thread/start", json!({"cwd":plan.cwd}))
        .await?;
    let s2 = store
        .sessions()
        .await?
        .into_iter()
        .find(|s| s.native == second["thread"]["id"])
        .unwrap();
    assert!(!backend.active(&s).await);
    assert!(backend.active(&s2).await);
    native
        .request(
            "thread/name/set",
            json!({"threadId":s.native,"name":"Renamed while offscreen"}),
        )
        .await?;
    wait_title(&store, &s, "Renamed while offscreen").await?;
    assert!(!backend.active(&s).await);
    // Reconcile from a resume snapshot even if the name notification was missed.
    store.set_title(&s, Some("Stale cached title")).await?;

    assert!(
        backend
            .submit(&s, "OFFLINE", "offline", false)
            .await
            .is_err()
    );
    native
        .request("thread/resume", json!({"threadId":s.native}))
        .await?;
    assert!(backend.active(&s).await);
    assert!(!backend.active(&s2).await);
    assert_eq!(store.title(&s).await?, "Renamed while offscreen");
    let fork = native
        .request("thread/fork", json!({"threadId":s.native}))
        .await?;
    let forked = store
        .sessions()
        .await?
        .into_iter()
        .find(|session| session.native == fork["thread"]["id"])
        .context("Fork was not registered")?;
    assert!(backend.active(&forked).await);
    assert!(!backend.active(&s).await);
    native
        .request("thread/resume", json!({"threadId":s.native}))
        .await?;
    // A second terminal may use the same project but cannot join the same native writer.
    let plan_b = backend
        .open("cli-b", "demo", &plan.cwd, &std::env::vars().collect(), &[])
        .await?;
    let other = Rpc::open(std::path::Path::new(
        plan_b
            .command
            .last()
            .unwrap()
            .strip_prefix("unix://")
            .unwrap(),
    ))
    .await?;
    other.initialize().await?;
    assert!(
        other
            .request("thread/resume", json!({"threadId":s.native}))
            .await
            .unwrap_err()
            .to_string()
            .contains("another uaoi terminal")
    );
    let third = other
        .request("thread/start", json!({"cwd":plan.cwd}))
        .await?;
    let s3 = store
        .sessions()
        .await?
        .into_iter()
        .find(|s| s.native == third["thread"]["id"])
        .unwrap();
    backend.close("cli-b").await;
    assert!(backend.active(&s).await);
    assert!(!backend.active(&s3).await);
    // Exercise the official terminal under a PTY, including /new navigation.
    if let Ok(python) = std::env::var("UAOI_TEST_PYTHON") {
        let script = home.path().join("terminal.py");
        std::fs::write(&script, include_str!("terminal_fixture.py"))?;
        let mut command = plan.command.clone();
        command.extend(["resume".into(), s.native.clone()]);
        let mut terminal = tokio::process::Command::new(python)
            .arg(script)
            .env("UAOI_TERMINAL_COMMAND", serde_json::to_string(&command)?)
            .env("UAOI_TERMINAL_LOG", home.path().join("terminal.log"))
            .envs(&plan.env)
            .env("TERM", "xterm-256color")
            .current_dir(&plan.cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;
        terminal
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"/rename CLI renamed title")
            .await?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        terminal.stdin.as_mut().unwrap().write_all(b"\r").await?;
        wait_title(&store, &s, "CLI renamed title").await?;
        terminal.stdin.as_mut().unwrap().write_all(b"/new").await?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        terminal.stdin.as_mut().unwrap().write_all(b"\r").await?;
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while backend.active(&s).await {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .await
        .context("TUI /new did not switch sessions")?;
        terminal
            .stdin
            .as_mut()
            .unwrap()
            .write_all(format!("/resume {}", s.native).as_bytes())
            .await?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        terminal.stdin.as_mut().unwrap().write_all(b"\r").await?;
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while !backend.active(&s).await {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .await
        .context("TUI /resume did not switch back")?;
        drop(terminal.stdin.take());
        tokio::time::timeout(std::time::Duration::from_secs(10), terminal.wait()).await??;
    }
    backend.close("cli-a").await;
    assert!(!backend.active(&s).await);
    assert!(!path.exists());
    // Deliberately exit after version negotiation, before opening the socket.
    use std::os::unix::fs::PermissionsExt;
    let launcher = home.path().join("failing-launcher");
    std::fs::write(
        &launcher,
        "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'codex-cli 0.166.0'; exit 0; fi\nexit 7\n",
    )?;
    std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o700))?;
    let mut broken = Config {
        runtime_dir: Some(home.path().join("run")),
        ..Config::default()
    };
    broken.agents.insert(
        "test".into(),
        AgentSpec {
            kind: "codex".into(),
            command: vec![launcher.display().to_string()],
            env: BTreeMap::new(),
        },
    );
    broken.projects.insert(
        "demo".into(),
        Project {
            workspace: plan.cwd.clone(),
            agent: "test".into(),
        },
    );
    let (failed_events, _) = tokio::sync::broadcast::channel(16);
    let failed = Codex::new(
        Arc::new(broken),
        store.clone(),
        failed_events,
        cancel.clone(),
    );
    assert!(
        failed
            .open(
                "failed-cli",
                "demo",
                &plan.cwd,
                &std::env::vars().collect(),
                &[]
            )
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read_dir(home.path().join("run"))?.count(),
        0,
        "failed startup left runtime resources"
    );
    if title_worker.is_some() {
        let expected = store.title(&s).await?;
        tokio::time::timeout(std::time::Duration::from_secs(30),async {
            loop {
                let db=rusqlite::Connection::open(home.path().join("state.sqlite"))?;
                let pending:i64=db.query_row("SELECT count(*) FROM conversation_titles WHERE desired<>applied OR applied IS NULL",[],|r|r.get(0))?;
                if pending==0 {break;}
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Ok::<_,anyhow::Error>(())
        }).await.context("Telegram title did not converge")??;
        eprintln!("Live Telegram topic title converged: {expected}");
    }
    cancel.cancel();
    backend.shutdown().await;
    if let Some(worker) = title_worker {
        worker.await?;
    }
    server.await?;
    Ok(())
}
