use super::OutcomeUnknown;
use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::{
    net::UnixStream,
    sync::{broadcast, mpsc, oneshot},
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use tokio_util::sync::CancellationToken;

type Reply = oneshot::Sender<Result<Value>>;
#[derive(Clone)]
pub struct Rpc {
    send: mpsc::Sender<(u64, Value)>,
    pending: Arc<Mutex<HashMap<u64, Reply>>>,
    seq: Arc<AtomicU64>,
    generation: Arc<AtomicU64>,
    connected: Arc<AtomicBool>,
    cancel: CancellationToken,
    pub notices: broadcast::Sender<Value>,
}
pub fn limits() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_frame_size(Some(64 * 1024 * 1024))
        .max_message_size(Some(64 * 1024 * 1024))
}
pub async fn connect(path: &std::path::Path) -> Result<WebSocketStream<UnixStream>> {
    Ok(tokio_tungstenite::client_async_with_config(
        "ws://localhost/",
        UnixStream::connect(path).await?,
        Some(limits()),
    )
    .await?
    .0)
}
fn initialize_message(id: Value) -> Value {
    json!({"id":id,"method":"initialize","params":{"clientInfo":{"name":"uaoi","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}}})
}
impl Rpc {
    pub async fn open(path: &std::path::Path) -> Result<Self> {
        let socket = connect(path).await?;
        let path = path.to_owned();
        let (send, mut rx) = mpsc::channel::<(u64, Value)>(256);
        let (notices, _) = broadcast::channel(4096);
        let rpc = Self {
            send,
            pending: Arc::new(Mutex::new(HashMap::new())),
            seq: Arc::new(AtomicU64::new(1)),
            generation: Arc::new(AtomicU64::new(1)),
            connected: Arc::new(AtomicBool::new(true)),
            cancel: CancellationToken::new(),
            notices,
        };
        let this = rpc.clone();
        tokio::spawn(async move {
            let mut initial = Some(socket);
            loop {
                let socket = if let Some(socket) = initial.take() {
                    socket
                } else {
                    loop {
                        let connected = tokio::select! {
                            _=this.cancel.cancelled()=>return,
                            result=tokio::time::timeout(std::time::Duration::from_secs(10),reconnect(&path))=>result,
                        };
                        if let Ok(Ok(socket)) = connected {
                            this.connected.store(true, Ordering::SeqCst);
                            let _ = this.notices.send(json!({"method":"uaoi/reconnected"}));
                            break socket;
                        }
                        tokio::select! {_=this.cancel.cancelled()=>return,_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{}}
                    }
                };
                let (mut sink, mut stream) = socket.split();
                loop {
                    tokio::select! {
                        _=this.cancel.cancelled()=>{let _=sink.close().await;this.fail_pending();return;},
                        outgoing=rx.recv()=>match outgoing {
                            Some((generation,v))=>{
                                // Requests from a disconnected generation are never replayed.
                                if generation!=this.generation.load(Ordering::SeqCst) {
                                    if v.get("method").is_some() && let Some(id)=v["id"].as_u64() && let Some(tx)=this.pending.lock().unwrap().remove(&id) {let _=tx.send(Err(OutcomeUnknown("Connection changed before delivery".into()).into()));}
                                    continue;
                                }
                                if sink.send(Message::Text(v.to_string().into())).await.is_err(){break;}
                            },None=>return,
                        },
                        incoming=stream.next()=>match incoming {
                            Some(Ok(Message::Text(t)))=>if let Ok(v)=serde_json::from_str::<Value>(&t) {
                                if v.get("method").is_none(){
                                    if let Some(id)=v["id"].as_u64() && let Some(tx)=this.pending.lock().unwrap().remove(&id) {
                                        let result=if let Some(e)=v.get("error"){Err(anyhow::anyhow!("RPC {}: {}",e["code"],e["message"]))}else{Ok(v["result"].clone())};let _=tx.send(result);
                                    }
                                }else{let _=this.notices.send(v);}
                            },
                            Some(Ok(Message::Ping(b)))=>{if sink.send(Message::Pong(b)).await.is_err(){break;}},
                            Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                            _=>{},
                        }
                    }
                }
                this.connected.store(false, Ordering::SeqCst);
                this.generation.fetch_add(1, Ordering::SeqCst);
                this.fail_pending();
                let _ = this.notices.send(json!({"method":"uaoi/disconnected"}));
            }
        });
        Ok(rpc)
    }
    fn fail_pending(&self) {
        for (_, tx) in self.pending.lock().unwrap().drain() {
            let _ = tx.send(Err(OutcomeUnknown("Native connection lost".into()).into()));
        }
    }
    pub fn connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst) && !self.cancel.is_cancelled()
    }
    pub fn close(&self) {
        self.cancel.cancel();
    }
    pub async fn initialize(&self) -> Result<()> {
        self.request(
            "initialize",
            initialize_message(Value::Null)["params"].clone(),
        )
        .await?;
        self.send(json!({"method":"initialized","params":{}})).await
    }
    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let generation = self.generation.load(Ordering::SeqCst);
        anyhow::ensure!(
            self.connected(),
            "Native connection is reconnecting; retry shortly"
        );
        let id = self.seq.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        if let Err(e) = self
            .queue(generation, json!({"id":id,"method":method,"params":params}))
            .await
        {
            self.pending.lock().unwrap().remove(&id);
            return Err(e);
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(45), rx).await;
        self.pending.lock().unwrap().remove(&id);
        result
            .map_err(|_| OutcomeUnknown("Native request timed out".into()))?
            .map_err(|_| OutcomeUnknown("Native response channel closed".into()))?
    }
    pub async fn send(&self, v: Value) -> Result<()> {
        let generation = self.generation.load(Ordering::SeqCst);
        self.queue(generation, v).await
    }
    async fn queue(&self, generation: u64, v: Value) -> Result<()> {
        anyhow::ensure!(
            self.connected() && generation == self.generation.load(Ordering::SeqCst),
            "Native connection is reconnecting; retry shortly"
        );
        self.send
            .send((generation, v))
            .await
            .context("Native writer stopped")
    }
    pub async fn check(&self) -> Result<()> {
        for method in ["thread/loaded/list", "thread/turns/list", "turn/steer"] {
            if let Err(e)=self.request(method,json!({"threadId":"00000000-0000-0000-0000-000000000000","input":[],"clientUserMessageId":"uaoi-probe","expectedTurnId":"uaoi-probe"})).await {
                let s=e.to_string();if s.contains("-32601")||s.contains("Unknown method")||s.contains("unknown variant"){bail!("Codex does not support {method}: {s}");}
            }
        }
        Ok(())
    }
}
async fn reconnect(path: &std::path::Path) -> Result<WebSocketStream<UnixStream>> {
    let mut socket = connect(path).await?;
    socket
        .send(Message::Text(
            initialize_message(json!("uaoi-reconnect"))
                .to_string()
                .into(),
        ))
        .await?;
    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => {
                let v: Value = serde_json::from_str(&text)?;
                if v["id"] == "uaoi-reconnect" {
                    anyhow::ensure!(v.get("error").is_none(), "Native initialization failed");
                    socket
                        .send(Message::Text(
                            json!({"method":"initialized","params":{}})
                                .to_string()
                                .into(),
                        ))
                        .await?;
                    return Ok(socket);
                }
            }
            Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await?,
            Message::Close(_) => break,
            _ => {}
        }
    }
    bail!("Native connection closed during initialization")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn reconnect_does_not_repeat_an_uncertain_request() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("rpc.sock");
        let listener = tokio::net::UnixListener::bind(&path)?;
        let seen = Arc::new(AtomicU64::new(0));
        let count = seen.clone();
        let server = tokio::spawn(async move {
            for connection in 0..2 {
                let (io, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(io).await.unwrap();
                while let Some(Ok(Message::Text(text))) = ws.next().await {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == "once" {
                        assert_eq!(connection, 0);
                        count.fetch_add(1, Ordering::SeqCst);
                        ws.close(None).await.unwrap();
                        break;
                    }
                    if v.get("id").is_some() {
                        ws.send(Message::Text(
                            json!({"id":v["id"],"result":{}}).to_string().into(),
                        ))
                        .await
                        .unwrap();
                        if v["method"] == "after" {
                            return;
                        }
                    }
                }
            }
        });
        let rpc = Rpc::open(&path).await?;
        let mut notices = rpc.notices.subscribe();
        rpc.initialize().await?;
        let error = rpc.request("once", json!({})).await.unwrap_err();
        assert!(error.is::<OutcomeUnknown>());
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if notices.recv().await.unwrap()["method"] == "uaoi/reconnected" {
                    break;
                }
            }
        })
        .await?;
        rpc.request("after", json!({})).await?;
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        rpc.close();
        server.await?;
        Ok(())
    }
}
