use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    net::UnixStream,
    sync::{broadcast, mpsc, oneshot},
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

use super::OutcomeUnknown;
type Reply = oneshot::Sender<Result<Value>>;
#[derive(Clone)]
pub struct Rpc {
    send: mpsc::Sender<Value>,
    pending: Arc<Mutex<HashMap<u64, Reply>>>,
    seq: Arc<AtomicU64>,
    pub notices: broadcast::Sender<Value>,
}
pub fn limits() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_frame_size(Some(64 * 1024 * 1024))
        .max_message_size(Some(64 * 1024 * 1024))
}
pub async fn connect(path: &std::path::Path) -> Result<WebSocketStream<UnixStream>> {
    let io = UnixStream::connect(path).await?;
    Ok(
        tokio_tungstenite::client_async_with_config("ws://localhost/", io, Some(limits()))
            .await?
            .0,
    )
}
impl Rpc {
    pub async fn open(path: &std::path::Path) -> Result<Self> {
        let socket = connect(path).await?;
        let (mut sink, mut stream) = socket.split();
        let (send, mut rx) = mpsc::channel::<Value>(256);
        let (notices, _) = broadcast::channel(4096);
        let rpc = Self {
            send,
            pending: Arc::new(Mutex::new(HashMap::new())),
            seq: Arc::new(AtomicU64::new(1)),
            notices,
        };
        let this = rpc.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    outgoing=rx.recv()=>match outgoing {
                        Some(v)=>if sink.send(Message::Text(v.to_string().into())).await.is_err(){break},
                        None=>break,
                    },
                    incoming=stream.next()=>match incoming {
                        Some(Ok(Message::Text(t)))=> {
                            if let Ok(v)=serde_json::from_str::<Value>(&t) {
                                if v.get("method").is_none() {
                                    if let Some(id)=v["id"].as_u64()
                                        && let Some(tx)=this.pending.lock().unwrap().remove(&id) {
                                            let result=if let Some(e)=v.get("error"){Err(anyhow::anyhow!("RPC {}: {}",e["code"],e["message"]))}else{Ok(v["result"].clone())};
                                            let _=tx.send(result);
                                        }
                                }else{let _=this.notices.send(v);}
                            }
                        },
                        Some(Ok(Message::Ping(b)))=>{let _=sink.send(Message::Pong(b)).await;},
                        Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                        _=>{},
                    }
                }
            }
            for (_, tx) in this.pending.lock().unwrap().drain() {
                let _ = tx.send(Err(OutcomeUnknown("Native connection lost".into()).into()));
            }
            let _ = this.notices.send(json!({"method":"uaoi/disconnected"}));
        });
        Ok(rpc)
    }
    pub async fn initialize(&self) -> Result<()> {
        self.request("initialize",json!({"clientInfo":{"name":"uaoi","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})).await?;
        self.send(json!({"method":"initialized","params":{}})).await
    }
    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.seq.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        if let Err(e) = self
            .send(json!({"id":id,"method":method,"params":params}))
            .await
        {
            self.pending.lock().unwrap().remove(&id);
            return Err(OutcomeUnknown(e.to_string()).into());
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(45), rx).await;
        self.pending.lock().unwrap().remove(&id);
        result
            .map_err(|_| OutcomeUnknown("Native request timed out".into()))?
            .map_err(|_| OutcomeUnknown("Native response channel closed".into()))?
    }
    pub async fn send(&self, v: Value) -> Result<()> {
        self.send.send(v).await.context("Native writer stopped")
    }
    pub async fn check(&self) -> Result<()> {
        for method in [
            "thread/queue/list",
            "thread/queue/add",
            "thread/queue/start",
            "thread/turns/list",
            "turn/steer",
        ] {
            if let Err(e)=self.request(method,json!({"threadId":"00000000-0000-0000-0000-000000000000","input":[],"clientUserMessageId":"uaoi-probe","expectedTurnId":"uaoi-probe"})).await {
                let s=e.to_string();if s.contains("-32601")||s.contains("Unknown method")||s.contains("unknown variant") {bail!("Codex does not support {method}: {s}");}
            }
        }
        Ok(())
    }
}
