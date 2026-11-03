use crate::{
    agent::AgentAdapter,
    config::{AgentSpec, Config, Project},
    im::{ImAdapter, SendFailure},
    model::*,
    service::Service,
    store::Store,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

struct FakeAgent {
    online: Mutex<Vec<String>>,
    inputs: Mutex<Vec<String>>,
}
#[async_trait]
impl AgentAdapter for FakeAgent {
    async fn open(
        &self,
        _: &str,
        _: &str,
        _: &std::path::Path,
        _: &BTreeMap<String, String>,
        _: &[std::ffi::OsString],
    ) -> Result<LaunchPlan> {
        unreachable!()
    }
    async fn close(&self, _: &str) {}
    async fn disconnected(&self, _: &str) {
        std::future::pending::<()>().await;
    }
    async fn active(&self, s: &Session) -> bool {
        self.online.lock().await.contains(&s.id)
    }
    async fn accepts(&self, s: &Session, date: u64) -> bool {
        self.active(s).await && date > 100
    }
    async fn submit(&self, _: &Session, t: &str, _: &str, steer: bool) -> Result<()> {
        if t == "uncertain" {
            return Err(crate::agent::OutcomeUnknown("connection lost".into()).into());
        }
        anyhow::ensure!(!steer, "No active turn to steer");
        self.inputs.lock().await.push(t.into());
        Ok(())
    }
    async fn interrupt(&self, _: &Session) -> Result<()> {
        Ok(())
    }
    async fn respond(&self, _: &Session, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    async fn history(&self, _: &Session) -> Result<()> {
        Ok(())
    }
    async fn status(&self, _: &Session) -> Result<String> {
        Ok("idle".into())
    }
    async fn shutdown(&self) {}
}
struct FakeIm;
#[async_trait]
impl ImAdapter for FakeIm {
    fn name(&self) -> &str {
        "test"
    }
    fn space(&self) -> Conversation {
        Conversation {
            platform: "test".into(),
            space: "group".into(),
            thread: String::new(),
        }
    }
    fn authorized(&self, id: &str) -> bool {
        id == "1"
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
fn message(id: &str, t: &Conversation, text: &str) -> Inbound {
    Inbound {
        id: id.into(),
        sender: "1".into(),
        target: t.clone(),
        text: text.into(),
        conversation_created: false,
        action: None,
        sent_at: 200,
    }
}
#[tokio::test]
async fn single_group_topics_offline_reuse_and_rejections() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::open(&dir.path().join("state.sqlite"))?;
    let agent = Arc::new(FakeAgent {
        online: Mutex::new(vec![]),
        inputs: Mutex::new(vec![]),
    });
    let svc = Service::new(
        Arc::new(Config::default()),
        store.clone(),
        agent.clone(),
        vec![Arc::new(FakeIm)],
    );
    let a = store.ensure_session("a", "work", "native-a").await?;
    let b = store.ensure_session("b", "work", "native-b").await?;
    svc.ensure_topics(&a).await?;
    svc.ensure_topics(&b).await?;
    let ta = store.targets(&a.id).await?.remove(0);
    let tb = store.targets(&b.id).await?.remove(0);
    assert_eq!(ta.space, tb.space);
    assert_ne!(ta.thread, tb.thread);
    agent
        .online
        .lock()
        .await
        .extend([a.id.clone(), b.id.clone()]);
    svc.handle(message("a1", &ta, "first")).await?;
    svc.handle(message("a1", &ta, "first")).await?;
    svc.handle(message("b1", &tb, "second")).await?;
    svc.handle(message("bad-steer", &ta, "/steer nope")).await?;
    assert!(store.recovery().await?.as_array().unwrap().is_empty());
    assert!(
        store
            .unconsumed(&a.id)
            .await?
            .to_string()
            .find("nope")
            .is_none()
    );
    agent.online.lock().await.retain(|id| id != &a.id);
    svc.handle(message("offline", &ta, "must not run")).await?;
    agent.online.lock().await.push(a.id.clone());
    let mut old = message("old", &ta, "stale");
    old.sent_at = 90;
    svc.handle(old).await?;
    svc.ensure_topics(&a).await?;
    assert_eq!(store.targets(&a.id).await?, vec![ta.clone()]);
    let mut unauthorized = message("bad-user", &ta, "evil");
    unauthorized.sender = "2".into();
    svc.handle(unauthorized).await?;
    svc.handle(message("uncertain", &ta, "uncertain")).await?;
    assert!(store.recovery().await?.to_string().contains("uncertain"));
    assert!(
        store
            .unconsumed(&a.id)
            .await?
            .to_string()
            .contains("unknown")
    );
    assert_eq!(*agent.inputs.lock().await, vec!["first", "second"]);
    Ok(())
}
#[test]
fn rejects_old_database_and_config() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("old.sqlite");
    rusqlite::Connection::open(&path)?.execute("CREATE TABLE spaces(id TEXT)", [])?;
    assert!(Store::open(&path).is_err());
    assert!(
        toml::from_str::<Config>("[projects.a]\nworkspace='/'\nagent='a'\napproval_policy='never'")
            .is_err()
    );
    Ok(())
}
#[test]
fn wrapper_arguments_and_project_resolution() -> Result<()> {
    use crate::agent::codex_cli::invocation;
    let dir = tempfile::tempdir()?;
    let nested = dir.path().join("nested");
    std::fs::create_dir(&nested)?;
    let mut cfg = Config::default();
    for (name, path) in [("root", dir.path()), ("child", nested.as_path())] {
        cfg.projects.insert(
            name.into(),
            Project {
                workspace: path.into(),
                agent: "a".into(),
            },
        );
    }
    assert_eq!(cfg.project(&nested)?.0, "child");
    let parse = |args: &[&str]| {
        invocation(
            &args
                .iter()
                .map(std::ffi::OsString::from)
                .collect::<Vec<_>>(),
            dir.path(),
        )
    };
    assert_eq!(
        parse(&["--model", "exec", "-C", "nested", "resume", "--last"])?,
        (true, nested.clone())
    );
    assert!(!parse(&["exec", "--help"])?.0);
    assert!(!parse(&["--model=x", "exec", "hello"])?.0);
    assert!(parse(&["--", "--remote"])?.0);
    assert!(parse(&["--remote", "unix://other"]).is_err());
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
}
#[async_trait]
impl ImAdapter for TitleIm {
    fn name(&self) -> &str {
        FakeIm.name()
    }
    fn space(&self) -> Conversation {
        FakeIm.space()
    }
    fn authorized(&self, id: &str) -> bool {
        FakeIm.authorized(id)
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
}
#[tokio::test]
async fn title_sync_creation_latest_retry_and_restart() -> Result<()> {
    use tokio_util::sync::CancellationToken;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("state.sqlite");
    let store = Store::open(&path)?;
    let s = store
        .ensure_session("demo", "codex", "native-title")
        .await?;
    store.set_title(&s, Some("Existing session title")).await?;
    let im = Arc::new(TitleIm::default());
    let agent = Arc::new(FakeAgent {
        online: Mutex::new(vec![]),
        inputs: Mutex::new(vec![]),
    });
    let svc = Arc::new(Service::new(
        Arc::new(Config::default()),
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
            target: target.clone(),
            title: "After restart".into()
        }]
    );
    let svc = Arc::new(Service::new(
        Arc::new(Config::default()),
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
    let a = store.ensure_session("a", "codex", "native-a").await?;
    let b = store.ensure_session("b", "codex", "native-b").await?;
    let mut ta = FakeIm.space();
    ta.thread = "1".into();
    let mut tb = ta.clone();
    tb.thread = "2".into();
    store.set_title(&a, Some("A")).await?;
    store.bind(&ta, &a.id).await?;
    store.bind(&tb, &b.id).await?;
    let original = TitleChange {
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
    assert_eq!(store.title(&a).await?, "[a] native-a");
    assert_eq!(store.title(&b).await?, "[b] native-b");
    Ok(())
}
