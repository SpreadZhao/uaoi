use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

async fn validate_creation(
    r: &Runtime,
    method: &str,
    params: &mut Value,
    fallback: &Path,
) -> Result<()> {
    let native = params["threadId"].as_str().unwrap_or("");
    let pending_turn = method == "turn/start"
        && r.threads
            .lock()
            .await
            .get(native)
            .is_some_and(|thread| thread.draft_directory.is_some());
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
        r.threads
            .lock()
            .await
            .get(native)
            .and_then(|thread| thread.draft_directory.clone())
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
pub(super) async fn gateway(
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
                        r.threads.lock().await.entry(native.into()).or_default().starting = true;starts.insert(v["id"].to_string(),native.into());
                    }
                    if ["thread/start","thread/resume","thread/fork"].contains(&method.as_str()) {
                        lifecycle.insert(v["id"].to_string(),method.clone());
                        if v["params"]["threadSource"].is_null() || v["params"]["threadSource"]=="user" {
                            permissions.apply(&mut v["params"]);
                        }
                    }
                    if method.is_empty() && let Some((method,p))=questions.remove(&v["id"].to_string()) {
                        if !r.answered.lock().await.insert(v["id"].to_string()) {continue;}
                        let session=r.threads.lock().await.get(p["threadId"].as_str().unwrap_or("")).and_then(|thread| thread.session.clone());
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
                    if v.get("method").is_none() && let Some(native)=starts.remove(&v["id"].to_string()) && v.get("error").is_some() && let Some(thread) = r.threads.lock().await.get_mut(&native) { thread.starting = false; }
                    if v.get("method").is_none() && let Some(method)=lifecycle.remove(&v["id"].to_string()) && v.get("error").is_none() && human_thread(&v["result"]["thread"]) && let Some(native)=v["result"]["thread"]["id"].as_str() {
                        // New drafts need creation-directory validation, but opening
                        // either a draft or old history does not publish a topic.
                        if method!="thread/resume" {r.threads.lock().await.entry(native.into()).or_default().draft_directory = Some(PathBuf::from(v["result"]["thread"]["cwd"].as_str().context("Missing new session cwd")?));}
                        owned_threads.insert(native.into());
                        let bound=if let Some(s)=store.native_session(&r.agent,native).await? { !store.targets(&s.id).await?.is_empty() } else {false};
                        if bound { attach(&r,&store,native).await?; }
                        remember_mode(&r,native,&v["result"]).await;
                    }
                    if let Some(method)=v["method"].as_str() && v.get("id").is_some() && ["item/tool/requestUserInput","item/commandExecution/requestApproval","item/fileChange/requestApproval"].contains(&method) {
                        questions.insert(v["id"].to_string(),(method.into(),v["params"].clone()));
                    }
                    let native=v["params"]["threadId"].as_str().or_else(||v["params"]["thread"]["id"].as_str()).unwrap_or("");
                    if v.get("method").is_some() && !native.is_empty() && !r.threads.lock().await.get(native).is_some_and(|thread| thread.subscribed) {
                        let _=r.rpc.notices.send(v.clone());
                    }
                    if ds.send(Message::Text(t)).await.is_err(){break;}
                },Some(Ok(Message::Ping(b)))=>{us.send(Message::Pong(b)).await?;},Some(Ok(Message::Close(_)))|None=>break,Some(Err(e))=>return Err(e.into()),_=>{},
            }
        }
    }
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        let subscribed: HashSet<_> = r
            .threads
            .lock()
            .await
            .iter()
            .filter(|(_, thread)| thread.subscribed)
            .map(|(id, _)| id.clone())
            .collect();
        let mut retained = HashSet::new();
        for native in owned_threads.drain() {
            if subscribed.contains(&native) {
                continue;
            }
            let pending = r
                .threads
                .lock()
                .await
                .get(&native)
                .is_some_and(|thread| thread.session.is_some() || thread.starting);
            let active = if pending {
                true
            } else {
                r.rpc
                    .request(
                        "thread/read",
                        json!({"threadId":native,"includeTurns":false}),
                    )
                    .await
                    .is_ok_and(|thread| thread["thread"]["status"]["type"] == "active")
            };
            if active {
                retained.insert(native);
            }
        }
        owned_threads = retained;
        if owned_threads.is_empty() {
            break;
        }
        // Keep live messages flowing until the control connection subscribes,
        // including a first turn started from previously untracked history.
        tokio::select! {
            _=r.cancel.cancelled()=>break,
            _=tick.tick()=>{},
            message=ur.next()=>match message {
                Some(Ok(Message::Text(t)))=>{
                    if let Ok(v)=serde_json::from_str::<Value>(&t) {
                        let native=v["params"]["threadId"].as_str().or_else(||v["params"]["thread"]["id"].as_str()).unwrap_or("");
                        if owned_threads.contains(native) && !r.threads.lock().await.get(native).is_some_and(|thread| thread.subscribed) {let _=r.rpc.notices.send(v);}
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
