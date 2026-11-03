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

pub struct Service {
    pub cfg: Arc<Config>,
    pub store: Store,
    pub agent: Arc<dyn AgentAdapter>,
    pub ims: Vec<Arc<dyn ImAdapter>>,
    routes: Mutex<HashMap<String, Arc<Mutex<()>>>>,
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
        }
    }
    fn im(&self, name: &str) -> Result<&Arc<dyn ImAdapter>> {
        self.ims
            .iter()
            .find(|i| i.name() == name)
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
        let chunks = split_output(text);

        for (i, part) in chunks.into_iter().enumerate() {
            self.store
                .enqueue(
                    target,
                    &format!("{key}:{i}"),
                    &part,
                    if i == 0 { buttons.clone() } else { vec![] },
                )
                .await?;
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
        if !self.im(&msg.target.platform)?.authorized(&msg.sender)
            || self.im(&msg.target.platform)?.space().space != msg.target.space
        {
            return Ok(());
        }
        let lock = self.route_lock(&msg.target).await;
        let _guard = lock.lock().await;
        let mut receipt = serde_json::to_value(&msg.target)?;
        receipt["input_text"] = json!(msg.text);
        receipt["action"] = json!(msg.action);
        let origin = serde_json::to_string(&receipt)?;
        if !self.store.claim_message(&msg.id, &origin).await? {
            return Ok(());
        }
        let result = self.handle_once(&msg).await;
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
                self.notice(&msg.target, &format!("error:{}", msg.id), &e.to_string())
                    .await?;
            }
        }
        Ok(())
    }
    async fn handle_once(&self, msg: &Inbound) -> Result<()> {
        let (command, arg) = msg
            .text
            .split_once(char::is_whitespace)
            .unwrap_or((&msg.text, ""));
        let command = command.split('@').next().unwrap_or(command);
        let arg = arg.trim();
        if msg.conversation_created {
            return Ok(());
        }
        let session = self
            .store
            .bound(&msg.target)
            .await?
            .context("Open a session with uaoi codex first")?;
        anyhow::ensure!(
            self.agent.active(&session).await,
            "Session offline. Open it with uaoi codex resume in its project directory."
        );
        anyhow::ensure!(
            self.agent.accepts(&session, msg.sent_at).await,
            "Message predates this CLI connection; send it again if still needed"
        );
        if let Some(action) = &msg.action {
            let (request, answer) = action.split_once(':').context("Invalid action")?;
            let (s, _) = self.store.request(request).await?;
            anyhow::ensure!(s == session.id, "Request belongs to another session");
            return self.agent.respond(&session, request, answer).await;
        }
        match command {
            "/status" => {
                let status = self.agent.status(&session).await?;
                let pending = self.store.unconsumed(&session.id).await?;
                self.notice(
                    &msg.target,
                    &msg.id,
                    &format!(
                        "{} · {}\nStatus: {}\nPending inputs: {}",
                        session.project,
                        session.native,
                        status,
                        pending.as_array().map_or(0, Vec::len)
                    ),
                )
                .await
            }
            "/stop" => {
                self.agent.interrupt(&session).await?;
                self.notice(
                    &msg.target,
                    &msg.id,
                    "Interrupt requested. Queued messages are retained.",
                )
                .await
            }
            "/steer" => {
                anyhow::ensure!(!arg.is_empty(), "Use /steer <message>");
                self.submit_input(&session, arg, &msg.id, true).await
            }
            "/answer" => {
                let(id,answer)=arg.split_once(char::is_whitespace).context("Use /answer <request-id> <answer>; for multiple questions provide a JSON object")?;
                let (s, _) = self.store.request(id).await?;
                anyhow::ensure!(s == session.id, "Request belongs to another session");
                self.agent.respond(&session, id, answer.trim()).await
            }
            "/history" => {
                self.agent.history(&session).await?;
                let offset = if arg.is_empty() {
                    0
                } else {
                    arg.parse::<usize>().context("Use /history [offset]")?
                };
                for e in self.store.history(&session.id, 20, offset).await? {
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
                anyhow::bail!("Commands: /status /stop /steer /history /answer")
            }
            _ => {
                anyhow::ensure!(
                    !msg.text.is_empty(),
                    "Only text messages are supported in v1"
                );
                self.submit_input(&session, &msg.text, &msg.id, false).await
            }
        }
    }
    async fn submit_input(
        &self,
        session: &Session,
        text: &str,
        id: &str,
        steer: bool,
    ) -> Result<()> {
        self.store
            .submission(id, &session.id, text, steer, "unknown")
            .await?;
        let result = self.agent.submit(session, text, id, steer).await;
        self.store
            .submission(
                id,
                &session.id,
                text,
                steer,
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
        for im in &self.ims {
            let space = im.space();
            if self
                .store
                .targets(&s.id)
                .await?
                .iter()
                .any(|t| t.platform == space.platform && t.space == space.space)
            {
                continue;
            }
            let key = format!("topic:{}:{}:{}", s.id, space.platform, space.space);
            anyhow::ensure!(
                self.store.claim(&key).await?,
                "Topic creation previously attempted; inspect recovery before retrying"
            );
            let title = self.store.title(s).await?;
            let t = im.create_conversation(&space, &title).await?;
            self.store.bind(&t, &s.id).await?;
            self.store
                .title_applied(&TitleChange { target: t, title })
                .await?;
            self.store.operation(&key, "accepted", "").await?;
        }
        Ok(())
    }
    async fn project(&self, e: &AgentEvent, t: &Conversation) -> Result<()> {
        // Inputs submitted through IM are already visible there. Native-origin inputs are mirrored.
        if matches!(e.kind, EventKind::User)
            && let Some(origin) = &e.origin
            && self.store.origin(origin).await?.as_ref() == Some(t)
        {
            return Ok(());
        }
        let buttons = e
            .request
            .as_ref()
            .map(|r| {
                r.choices
                    .iter()
                    .map(|c| (c.clone(), format!("{}:{c}", r.id)))
                    .collect()
            })
            .unwrap_or_default();
        let mut text = e.text.clone();
        if let Some(r) = &e.request
            && r.choices.is_empty()
        {
            text.push_str(&format!("\n/answer {} <answer>", r.id));
        }
        self.output(t, &format!("{}:{}", e.session, e.key), &text, buttons)
            .await
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
                        if !self.agent.active(&s).await { return Ok(()); }
                        self.ensure_topics(&s).await?;
                    }
                        for t in self.store.targets(&e.session).await?{self.project(&e,&t).await?;}Ok(())
                    }.await;if let Err(error)=result{tracing::error!(%error,"Event projection failed");}},
                    Err(broadcast::error::RecvError::Lagged(n))=>{tracing::error!(n,"Projection lagged; reconcile with /history");},
                    Err(_)=>break,
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
                let Ok(im) = self.im(&change.target.platform) else {
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
                if cancel.is_cancelled() {
                    return;
                }
                let Ok(im) = self.im(&d.target.platform) else {
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
                // One global sender keeps a shared Bot below per-chat edit limits.
                tokio::select! {_=cancel.cancelled()=>return,_=tokio::time::sleep(Duration::from_millis(1100))=>{}}
            }
        }
    }
    pub async fn control(&self, v: Value) -> Result<Value> {
        match v["method"].as_str().unwrap_or("") {
            "sessions" => {
                let mut sessions = vec![];
                for s in self.store.sessions().await? {
                    sessions.push(json!({"online":self.agent.active(&s).await,"session":s}));
                }
                Ok(json!(sessions))
            }
            "recovery" => self.store.recovery().await,
            _ => anyhow::bail!("Unknown control method"),
        }
    }
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
