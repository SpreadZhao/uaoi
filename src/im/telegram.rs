use super::{ImAdapter, SendFailure};
use crate::{
    config::{ProjectTelegram, TelegramConfig},
    model::*,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub struct Telegram {
    configs: Vec<(String, ProjectTelegram)>,
    agent: String,
    credentials: TelegramConfig,
    connection: RwLock<Option<Arc<Connection>>>,
    client: reqwest::Client,
}
#[derive(Clone)]
struct Connection {
    name: String,
    username: String,
    url: String,
    started: u64,
    cancel: CancellationToken,
}
impl Telegram {
    pub fn new(
        agent: &str,
        config: &TelegramConfig,
        configs: Vec<(String, ProjectTelegram)>,
    ) -> Result<Self> {
        Ok(Self {
            agent: agent.into(),
            credentials: config.clone(),
            configs,
            connection: RwLock::new(None),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(40))
                .build()?,
        })
    }
    fn token<'a>(&'a self, env: &'a BTreeMap<String, String>) -> Result<&'a str> {
        let token = match (&self.credentials.token, &self.credentials.token_env) {
            (Some(token), None) => token,
            (None, Some(variable)) => env
                .get(variable)
                .with_context(|| format!("Missing {variable}"))?,
            _ => anyhow::bail!("Configure exactly one telegram.token or telegram.token_env"),
        };
        anyhow::ensure!(!token.trim().is_empty(), "Empty Telegram token");
        Ok(token)
    }
    fn connection(&self) -> Result<Arc<Connection>> {
        self.connection
            .read()
            .unwrap()
            .clone()
            .context("Telegram profile is offline")
    }
    async fn response(
        &self,
        connection: &Connection,
        method: &str,
        body: Value,
    ) -> Result<(reqwest::StatusCode, Value)> {
        // Transport errors contain credential-bearing URLs; never retain those URLs.
        let request = async {
            let response = self
                .client
                .post(format!("{}/{method}", connection.url))
                .json(&body)
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Telegram transport: {}", e.without_url()))?;
            let status = response.status();
            let value = response
                .json()
                .await
                .map_err(|e| anyhow::anyhow!("Telegram response: {}", e.without_url()))?;
            Ok((status, value))
        };
        tokio::select! {
            biased;
            _ = connection.cancel.cancelled() => anyhow::bail!("Telegram profile stopped"),
            response = request => response,
        }
    }
    async fn api_at(&self, connection: &Connection, method: &str, body: Value) -> Result<Value> {
        let (_, value) = self.response(connection, method, body).await?;
        anyhow::ensure!(
            value["ok"] == true,
            "Telegram {}",
            value["description"].as_str().unwrap_or("API failure")
        );
        Ok(value["result"].clone())
    }
    async fn api(&self, method: &str, body: Value) -> Result<Value> {
        let connection = self.connection()?;
        self.api_at(&connection, method, body).await
    }
    async fn commands_at(&self, connection: &Connection, commands: &[(&str, &str)]) -> Result<()> {
        let commands: Vec<_> = commands
            .iter()
            .map(|(command, description)| json!({"command":command,"description":description}))
            .collect();
        // Deleting a chat menu exposes inherited menus, so these dedicated
        // Bots must also clear their generic and administrator command scopes.
        let mut requests = vec![];
        for kind in [
            "default",
            "all_private_chats",
            "all_group_chats",
            "all_chat_administrators",
        ] {
            requests.push((None, "deleteMyCommands", json!({"scope":{"type":kind}})));
        }
        for (project, config) in &self.configs {
            requests.push((
                Some(project),
                "deleteMyCommands",
                json!({"scope":{"type":"chat_administrators","chat_id":config.group}}),
            ));
            let scope = json!({"type":"chat","chat_id":config.group});
            requests.push((
                Some(project),
                if commands.is_empty() {
                    "deleteMyCommands"
                } else {
                    "setMyCommands"
                },
                if commands.is_empty() {
                    json!({"scope":scope})
                } else {
                    json!({"scope":scope,"commands":commands})
                },
            ));
        }
        let results = futures_util::future::join_all(requests.into_iter().map(|(project, method, body)| async move {
            let result = self.api_at(connection, method, body.clone()).await;
            if let Err(error) = &result {
                tracing::warn!(agent=%self.agent, project=?project, scope=%body["scope"], %error, "Could not update Telegram command menu");
            }
            result
        })).await;
        let failures = results.iter().filter(|r| r.is_err()).count();
        anyhow::ensure!(
            failures == 0,
            "Command menu update failed for {failures} scopes"
        );
        Ok(())
    }
    async fn clear_commands_at(&self, connection: &Connection) -> Result<()> {
        // Long polling is cancelled before cleanup; use an independent token
        // and a fixed deadline so Telegram cannot hold up local shutdown.
        let cleanup = Connection {
            cancel: CancellationToken::new(),
            ..connection.clone()
        };
        tokio::time::timeout(Duration::from_secs(5), self.commands_at(&cleanup, &[]))
            .await
            .context("Command menu cleanup timed out")?
    }
}
#[async_trait]
impl ImAdapter for Telegram {
    fn name(&self) -> String {
        self.connection()
            .map(|c| c.name.clone())
            .unwrap_or_default()
    }
    fn enabled(&self) -> bool {
        self.connection.read().unwrap().is_some()
    }
    fn started(&self) -> u64 {
        self.connection().map(|c| c.started).unwrap_or(0)
    }
    async fn activate(&self, env: &BTreeMap<String, String>) -> Result<()> {
        let token = self.token(env)?;
        let mut connection = Connection {
            name: format!(
                "telegram:{}",
                token.split(':').next().context("Invalid bot token")?
            ),
            username: String::new(),
            url: format!("https://api.telegram.org/bot{token}"),
            started: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            cancel: CancellationToken::new(),
        };
        let me = self.api_at(&connection, "getMe", json!({})).await?;
        anyhow::ensure!(
            connection.name == format!("telegram:{}", me["id"]),
            "Bot token identity mismatch"
        );
        connection.username = me["username"]
            .as_str()
            .context("Bot username unavailable")?
            .into();
        let info = self
            .api_at(&connection, "getWebhookInfo", json!({}))
            .await?;
        anyhow::ensure!(
            info["url"].as_str().unwrap_or("").is_empty(),
            "Bot has a webhook; remove it explicitly before using long polling"
        );
        *self.connection.write().unwrap() = Some(Arc::new(connection));
        Ok(())
    }
    async fn deactivate(&self) {
        let connection = self.connection.write().unwrap().take();
        if let Some(connection) = connection {
            connection.cancel.cancel();
            if let Err(error) = self.clear_commands_at(&connection).await {
                tracing::warn!(agent=%self.agent, %error, "Could not clear offline Telegram menus");
            }
        }
    }
    async fn clear_commands(&self, env: &BTreeMap<String, String>) -> Result<()> {
        let Ok(token) = self.token(env) else {
            // Inactive profiles do not require credentials in this terminal.
            return Ok(());
        };
        let connection = Connection {
            name: String::new(),
            username: String::new(),
            url: format!("https://api.telegram.org/bot{token}"),
            started: 0,
            cancel: CancellationToken::new(),
        };
        self.clear_commands_at(&connection).await
    }
    fn spaces(&self) -> Vec<Conversation> {
        self.configs
            .iter()
            .map(|(_, config)| Conversation {
                platform: "telegram".into(),
                space: config.group.to_string(),
                thread: String::new(),
            })
            .collect()
    }
    fn agent(&self) -> &str {
        &self.agent
    }
    fn project(&self, target: &Conversation) -> Option<&str> {
        self.configs
            .iter()
            .find(|(_, c)| c.group.to_string() == target.space)
            .map(|(name, _)| name.as_str())
    }
    fn destination(&self, project: &str) -> Option<Conversation> {
        self.configs
            .iter()
            .find(|(name, _)| name == project)
            .map(|(_, c)| Conversation {
                platform: "telegram".into(),
                space: c.group.to_string(),
                thread: String::new(),
            })
    }
    fn topic(&self, project: &str, id: &str) -> Result<Conversation> {
        let id: i64 = id
            .parse()
            .context("Topic must be a positive Telegram topic ID")?;
        anyhow::ensure!(id > 0, "Topic must be a positive Telegram topic ID");
        Ok(Conversation {
            thread: id.to_string(),
            ..self.destination(project).context("Unknown project")?
        })
    }
    fn link(&self, target: &Conversation) -> String {
        format!(
            "https://t.me/c/{}/{}",
            target.space.strip_prefix("-100").unwrap_or(&target.space),
            target.thread
        )
    }
    async fn indicate_working(&self, target: &Conversation) -> Result<()> {
        let mut body = json!({"chat_id":target.space,"action":"typing"});
        if !target.thread.is_empty() {
            body["message_thread_id"] = json!(target.thread.parse::<i64>()?);
        }
        self.api("sendChatAction", body).await?;
        Ok(())
    }
    async fn acknowledge(&self, input: &Inbound, feedback: Option<&str>) -> Result<()> {
        if let Some(id) = &input.acknowledgement {
            let mut body = json!({"callback_query_id":id});
            if let Some(text) = feedback {
                body["text"] = json!(text.chars().take(200).collect::<String>());
            }
            self.api("answerCallbackQuery", body).await?;
        }
        Ok(())
    }
    async fn set_commands(&self, commands: &[(&str, &str)]) -> Result<()> {
        let connection = self.connection()?;
        self.commands_at(&connection, commands).await
    }
    async fn discard_backlog(&self) -> Result<String> {
        let updates = self
            .api(
                "getUpdates",
                json!({"offset":-1,"timeout":0,"allowed_updates":["message","callback_query"]}),
            )
            .await?;
        Ok(updates
            .as_array()
            .and_then(|a| a.last())
            .and_then(|u| u["update_id"].as_i64())
            .map(|id| (id + 1).to_string())
            .unwrap_or_default())
    }
    async fn poll(&self, cursor: &str) -> Result<(Vec<Inbound>, String)> {
        let connection = self.connection()?;
        let updates=self.api_at(&connection, "getUpdates",json!({"offset":cursor.parse::<i64>().unwrap_or(0),"timeout":25,"allowed_updates":["message","callback_query"]})).await?;
        let mut out = vec![];
        let mut next = cursor.to_owned();
        for u in updates.as_array().context("Invalid updates")? {
            let id = u["update_id"].as_i64().context("Missing update ID")?;
            next = (id + 1).to_string();
            let callback = u.get("callback_query");
            let m = callback.map(|c| &c["message"]).unwrap_or(&u["message"]);
            let Some(chat) = m["chat"]["id"].as_i64().filter(|id| *id < 0) else {
                continue;
            };
            // Anonymous administrators speak as this group and may have a
            // bot-shaped compatibility sender. Other Bots and channel posts
            // are not member input.
            let sender_chat = m["sender_chat"]["id"].as_i64();
            if callback.is_none()
                && sender_chat != Some(chat)
                && (sender_chat.is_some() || m["from"]["is_bot"] == true)
            {
                continue;
            }
            let target = Conversation {
                platform: "telegram".into(),
                space: chat.to_string(),
                thread: m["message_thread_id"]
                    .as_i64()
                    .map(|x| x.to_string())
                    .unwrap_or_default(),
            };
            if self.project(&target).is_none() {
                if let Some(c) = callback {
                    let _ = self
                        .api("answerCallbackQuery", json!({"callback_query_id":c["id"]}))
                        .await;
                }
                continue;
            }
            let text = m["text"].as_str().unwrap_or("");
            let command = text.split_whitespace().next().unwrap_or("");
            let recipient = command
                .strip_prefix('/')
                .and_then(|c| c.split_once('@'))
                .map(|(_, bot)| bot);
            if recipient.is_some_and(|bot| !bot.eq_ignore_ascii_case(&connection.username)) {
                continue;
            }
            out.push(Inbound {
                agent: self.agent.clone(),
                addressed: callback.is_some() || recipient.is_some(),
                id: if callback.is_some() {
                    format!("{}:callback:{id}", connection.name)
                } else {
                    format!(
                        "telegram:{chat}:{}",
                        m["message_id"].as_i64().context("Missing message ID")?
                    )
                },
                target,
                text: m["text"].as_str().unwrap_or("").to_owned(),
                sent_at: if callback.is_some() {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_secs()
                } else {
                    m["date"].as_u64().unwrap_or(0)
                },
                conversation_created: m.get("forum_topic_created").is_some(),
                action: callback.and_then(|c| c["data"].as_str()).map(str::to_owned),
                acknowledgement: callback.and_then(|c| c["id"].as_str()).map(str::to_owned),
                reply_to: m["reply_to_message"]["message_id"]
                    .as_i64()
                    .map(|id| id.to_string()),
            });
        }
        Ok((out, next))
    }
    async fn create_conversation(&self, space: &Conversation, title: &str) -> Result<Conversation> {
        let title: String = title.chars().take(128).collect();
        let v = self
            .api(
                "createForumTopic",
                json!({"chat_id":space.space.parse::<i64>()?,"name":title}),
            )
            .await?;
        Ok(Conversation {
            platform: "telegram".into(),
            space: space.space.clone(),
            thread: v["message_thread_id"]
                .as_i64()
                .context("Missing Topic ID")?
                .to_string(),
        })
    }
    async fn rename_conversation(
        &self,
        target: &Conversation,
        title: &str,
    ) -> std::result::Result<(), SendFailure> {
        let title: String = title.chars().take(128).collect();
        let body = json!({"chat_id":target.space,"message_thread_id":target.thread.parse::<i64>().map_err(|e|SendFailure::Permanent(e.to_string()))?,"name":title});
        let connection = self
            .connection()
            .map_err(|_| SendFailure::Retry(Duration::from_secs(1)))?;
        let (status, v) = self
            .response(&connection, "editForumTopic", body)
            .await
            .map_err(|e| SendFailure::Unknown(e.to_string()))?;
        if v["ok"] == true
            || v["description"]
                .as_str()
                .is_some_and(|s| s.contains("TOPIC_NOT_MODIFIED"))
        {
            return Ok(());
        }
        if let Some(seconds) = v["parameters"]["retry_after"].as_u64() {
            return Err(SendFailure::Retry(Duration::from_secs(seconds.max(1))));
        }
        if status.is_server_error() {
            return Err(SendFailure::Retry(Duration::from_secs(3)));
        }
        Err(SendFailure::Permanent(
            v["description"]
                .as_str()
                .unwrap_or("Topic rename failed")
                .into(),
        ))
    }
    async fn deliver(&self, d: &Delivery) -> std::result::Result<String, SendFailure> {
        let keyboard: Vec<Vec<Value>> = d
            .buttons
            .iter()
            .map(|row| {
                row.iter()
                    .map(|(label, data)| json!({"text":label,"callback_data":data}))
                    .collect()
            })
            .collect();
        let mut body = json!({"chat_id":d.target.space,"text":d.text,"link_preview_options":{"is_disabled":true},"reply_markup":{"inline_keyboard":keyboard}});
        let mut entities = vec![];
        for link in &d.links {
            let before = d
                .text
                .get(..link.start)
                .ok_or_else(|| SendFailure::Permanent("Invalid link start".into()))?;
            let label = d
                .text
                .get(link.start..link.end)
                .ok_or_else(|| SendFailure::Permanent("Invalid link end".into()))?;
            entities.push(json!({"type":"text_link","offset":before.encode_utf16().count(),"length":label.encode_utf16().count(),"url":link.url}));
        }
        body["entities"] = json!(entities);
        let method = if let Some(id) = &d.message {
            body["message_id"] = json!(
                id.parse::<i64>()
                    .map_err(|e| SendFailure::Permanent(e.to_string()))?
            );
            "editMessageText"
        } else {
            if !d.target.thread.is_empty() {
                body["message_thread_id"] = json!(
                    d.target
                        .thread
                        .parse::<i64>()
                        .map_err(|e| SendFailure::Permanent(e.to_string()))?
                );
            }
            "sendMessage"
        };
        let connection = self
            .connection()
            .map_err(|_| SendFailure::Retry(Duration::from_secs(1)))?;
        let (status, v) = self
            .response(&connection, method, body)
            .await
            .map_err(|e| {
                if d.message.is_some() {
                    SendFailure::Retry(Duration::from_secs(3))
                } else {
                    SendFailure::Unknown(e.to_string())
                }
            })?;
        if v["ok"] == true {
            return Ok(v["result"]["message_id"]
                .as_i64()
                .map(|i| i.to_string())
                .or_else(|| d.message.clone())
                .unwrap_or_default());
        }
        if v["error_code"] == 429 {
            return Err(SendFailure::Retry(Duration::from_secs(
                v["parameters"]["retry_after"].as_u64().unwrap_or(3),
            )));
        }
        let description = v["description"]
            .as_str()
            .unwrap_or("Telegram API failure")
            .to_owned();
        if description.contains("message is not modified") {
            return Ok(d.message.clone().unwrap_or_default());
        }
        if status.is_server_error() {
            return Err(if d.message.is_some() {
                SendFailure::Retry(Duration::from_secs(3))
            } else {
                SendFailure::Unknown(description)
            });
        }
        Err(SendFailure::Permanent(description))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn inline_credentials_do_not_require_an_environment_variable() -> Result<()> {
        let inline = Telegram::new(
            "supa",
            &TelegramConfig {
                token: Some("123456:inline-test-token".into()),
                token_env: None,
            },
            vec![],
        )?;
        assert_eq!(inline.token(&BTreeMap::new())?, "123456:inline-test-token");
        assert!(
            !inline.enabled(),
            "reading credentials must not activate the Bot"
        );
        let external = Telegram::new(
            "supa",
            &TelegramConfig {
                token: None,
                token_env: Some("TEST_BOT".into()),
            },
            vec![],
        )?;
        assert!(external.token(&BTreeMap::new()).is_err());
        let env = BTreeMap::from([("TEST_BOT".into(), "654321:environment-test-token".into())]);
        assert_eq!(external.token(&env)?, "654321:environment-test-token");
        Ok(())
    }

    #[tokio::test]
    async fn command_menus_continue_after_unavailable_groups() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let mut received = vec![];
            for _ in 0..8 {
                let (mut io, _) = listener.accept().await.unwrap();
                let mut data = vec![];
                let mut chunk = [0; 4096];
                let body_start = loop {
                    let n = io.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&chunk[..n]);
                    if let Some(end) = data.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        let length: usize = String::from_utf8_lossy(&data[..end])
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|value| value.trim().parse().ok())
                            })
                            .unwrap();
                        if data.len() >= end + 4 + length {
                            break end + 4;
                        }
                    }
                };
                let body: Value = serde_json::from_slice(&data[body_start..]).unwrap();
                let unavailable = body["scope"]["chat_id"] == -1001;
                let response = if unavailable {
                    json!({"ok":false,"description":"Bad Request: chat not found"})
                } else {
                    json!({"ok":true,"result":true})
                }
                .to_string();
                received.push(body);
                io.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",response.len(),response).as_bytes()).await.unwrap();
            }
            received
        });
        let adapter = Telegram {
            agent: "work".into(),
            credentials: TelegramConfig::default(),
            configs: vec![
                ("unavailable".into(), ProjectTelegram { group: -1001 }),
                ("available".into(), ProjectTelegram { group: -1002 }),
            ],
            connection: RwLock::new(Some(Arc::new(Connection {
                name: "telegram:1".into(),
                username: "WorkBot".into(),
                url: format!("http://{addr}"),
                started: 0,
                cancel: CancellationToken::new(),
            }))),
            client: reqwest::Client::builder().no_proxy().build()?,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            adapter.set_commands(&[("help", "Show help"), ("new", "Create session")]),
        )
        .await?;
        assert!(
            result.is_err(),
            "partial registration failures remain visible"
        );
        let received = tokio::time::timeout(Duration::from_secs(3), server).await??;
        assert_eq!(received.len(), 8);
        let menus: Vec<_> = received
            .iter()
            .filter(|body| body.get("commands").is_some())
            .collect();
        assert_eq!(menus.len(), 2);
        for group in [-1001, -1002] {
            assert!(menus.iter().any(|body| body["scope"]
                == json!({"type":"chat","chat_id":group})
                && body["commands"].as_array().unwrap().len() == 2));
        }
        assert!(
            received
                .iter()
                .any(|body| body["scope"]["type"] == "default" && body.get("commands").is_none())
        );
        Ok(())
    }

    #[tokio::test]
    async fn mock_api_routes_groups_without_forcing_reply_ui() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut io, _)) = listener.accept().await else {
                    break;
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut data = vec![];
                    let mut chunk = [0u8; 4096];
                    let end = loop {
                        let n = io.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        data.extend_from_slice(&chunk[..n]);
                        if let Some(end) = data.windows(4).position(|s| s == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&data[..end]);
                            let length = headers
                                .lines()
                                .find_map(|l| {
                                    l.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .and_then(|l| l.trim().parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            if data.len() >= end + 4 + length {
                                break end + 4;
                            }
                        }
                    };
                    let body: Value = serde_json::from_slice(&data[end..]).unwrap();
                    let request = String::from_utf8_lossy(&data[..end]);
                    let result = if request.contains("getUpdates") {
                        json!([
                            {"update_id":1,"message":{"message_id":7,"from":{"id":1},"chat":{"id":-1001},"message_thread_id":10,"text":"one","date":1,"reply_to_message":{"message_id":6}}},
                            {"update_id":2,"message":{"message_id":8,"from":{"id":2},"chat":{"id":-1002},"message_thread_id":20,"text":"two","date":1}},
                            {"update_id":3,"message":{"message_id":11,"from":{"id":9999},"chat":{"id":-1001},"text":"another member","date":1}},
                            {"update_id":4,"callback_query":{"id":"callback-4","from":{"id":8888},"data":"choice","message":{"chat":{"id":-1001},"message_thread_id":10}}},
                            {"update_id":5,"message":{"message_id":9,"from":{"id":1},"chat":{"id":-1001},"message_thread_id":10,"text":"/new@WorkBot /tmp","date":1}},
                            {"update_id":6,"message":{"message_id":10,"from":{"id":1},"chat":{"id":-1001},"message_thread_id":10,"text":"/new@OtherBot /tmp","date":1}},
                            {"update_id":7,"message":{"message_id":12,"from":{"id":1},"chat":{"id":-1003},"text":"outside group","date":1}},
                            {"update_id":8,"message":{"message_id":13,"from":{"id":1},"chat":{"id":1},"text":"private chat","date":1}},
                            {"update_id":9,"message":{"message_id":14,"from":{"id":3,"is_bot":true},"chat":{"id":-1001},"text":"bot message","date":1}},
                            {"update_id":10,"callback_query":{"id":"outside-callback","from":{"id":1},"data":"choice","message":{"chat":{"id":-1003},"message_thread_id":10}}},
                            {"update_id":11,"message":{"message_id":15,"from":{"id":1087968824,"is_bot":true},"sender_chat":{"id":-1001,"type":"supergroup"},"chat":{"id":-1001},"text":"/agents","date":1}},
                            {"update_id":12,"message":{"message_id":16,"from":{"id":1087968824,"is_bot":true},"sender_chat":{"id":-1001,"type":"supergroup"},"chat":{"id":-1001},"message_thread_id":10,"text":"anonymous task","date":1}},
                            {"update_id":13,"message":{"message_id":17,"sender_chat":{"id":-1009,"type":"channel"},"chat":{"id":-1001},"text":"channel post","date":1}}
                        ])
                    } else {
                        json!({"message_id":42})
                    };
                    tx.send(body).await.unwrap();
                    let response = json!({"ok":true,"result":result}).to_string();
                    io.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",response.len(),response).as_bytes()).await.unwrap();
                });
            }
        });
        let adapter = Telegram {
            agent: "work".into(),
            credentials: TelegramConfig {
                token_env: Some("UNUSED".into()),
                ..Default::default()
            },
            configs: vec![
                ("work".into(), ProjectTelegram { group: -1001 }),
                ("personal".into(), ProjectTelegram { group: -1002 }),
            ],
            connection: RwLock::new(Some(Arc::new(Connection {
                name: "telegram:1".into(),
                username: "WorkBot".into(),
                url: format!("http://{addr}/bot-test"),
                started: 0,
                cancel: CancellationToken::new(),
            }))),
            client: reqwest::Client::builder().no_proxy().build()?,
        };
        let (messages, cursor) = adapter.poll("").await?;
        assert_eq!(cursor, "14");
        assert_eq!(messages.len(), 7);
        assert_eq!(messages[2].text, "another member");
        assert_eq!(messages[3].action.as_deref(), Some("choice"));
        assert!(messages[4].addressed);
        assert!(messages[4].text.contains("@WorkBot"));
        assert_eq!(messages[5].text, "/agents");
        assert!(messages[5].target.thread.is_empty());
        assert_eq!(messages[6].text, "anonymous task");
        assert_eq!(messages[6].target.thread, "10");
        assert_eq!(messages[0].reply_to.as_deref(), Some("6"));
        assert_ne!(messages[0].target.space, messages[1].target.space);
        rx.recv().await.unwrap();
        assert_eq!(
            rx.recv().await.unwrap()["callback_query_id"],
            "outside-callback"
        );
        let other = Telegram {
            agent: "personal".into(),
            credentials: TelegramConfig {
                token_env: Some("UNUSED".into()),
                ..Default::default()
            },
            configs: adapter.configs.clone(),
            connection: RwLock::new(Some(Arc::new(Connection {
                name: "telegram:2".into(),
                username: "OtherBot".into(),
                url: adapter.connection()?.url.clone(),
                started: 0,
                cancel: CancellationToken::new(),
            }))),
            client: adapter.client.clone(),
        };
        let (other_messages, _) = other.poll("").await?;
        rx.recv().await.unwrap();
        assert_eq!(
            rx.recv().await.unwrap()["callback_query_id"],
            "outside-callback"
        );
        assert_eq!(messages[0].target, other_messages[0].target);
        assert_eq!(messages[0].id, other_messages[0].id);
        assert_ne!(messages[0].agent, other_messages[0].agent);
        assert!(other_messages[4].text.contains("@OtherBot"));
        adapter
            .acknowledge(&messages[3], Some("This question was already answered"))
            .await?;
        let feedback = rx.recv().await.unwrap();
        assert_eq!(feedback["callback_query_id"], "callback-4");
        assert_eq!(feedback["text"], "This question was already answered");
        let mut delivery = Delivery {
            agent: "work".into(),
            id: 1,
            key: "answer:test:0".into(),
            target: messages[0].target.clone(),
            text: "Reply here".into(),
            message: None,
            buttons: vec![],
            links: vec![],
        };
        adapter.deliver(&delivery).await.unwrap();
        let prompt = rx.recv().await.unwrap();
        assert!(prompt["reply_markup"].get("force_reply").is_none());
        delivery.key = "card".into();
        delivery.message = Some("42".into());
        delivery.text = "🧭 Agents\n中文 🧩 Session".into();
        delivery.links = vec![crate::model::TextLink {
            start: "🧭 Agents\n".len(),
            end: delivery.text.len(),
            url: "https://t.me/c/1/10".into(),
        }];
        delivery.buttons = vec![
            vec![("1".into(), "first".into()), ("2".into(), "second".into())],
            vec![("Submit".into(), "opaque".into())],
        ];
        adapter.deliver(&delivery).await.unwrap();
        let edit = rx.recv().await.unwrap();
        assert_eq!(edit["message_id"], 42);
        assert_eq!(
            edit["entities"],
            json!([{"type":"text_link","offset":10,"length":13,"url":"https://t.me/c/1/10"}])
        );
        assert_eq!(
            edit["reply_markup"]["inline_keyboard"][1][0]["callback_data"],
            "opaque"
        );
        assert_eq!(
            edit["reply_markup"]["inline_keyboard"][0]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert!(edit["reply_markup"].get("force_reply").is_none());
        delivery.buttons.clear();
        delivery.links.clear();
        adapter.deliver(&delivery).await.unwrap();
        let cleared = rx.recv().await.unwrap();
        assert_eq!(cleared["reply_markup"]["inline_keyboard"], json!([]));
        assert_eq!(cleared["entities"], json!([]));
        adapter.indicate_working(&messages[0].target).await?;
        let activity = rx.recv().await.unwrap();
        assert_eq!(activity["action"], "typing");
        assert_eq!(activity["message_thread_id"], 10);
        assert!(activity.get("text").is_none());
        adapter
            .set_commands(&[
                ("agents", "Manage · List sessions"),
                ("plan", "Task · Toggle Plan mode"),
            ])
            .await?;
        let mut shown = vec![];
        for _ in 0..8 {
            shown.push(rx.recv().await.unwrap());
        }
        let menus: Vec<_> = shown
            .iter()
            .filter(|body| body.get("commands").is_some())
            .collect();
        assert_eq!(menus.len(), 2);
        for group in [-1001, -1002] {
            let menu = menus
                .iter()
                .find(|body| body["scope"]["chat_id"] == group)
                .unwrap();
            assert_eq!(menu["scope"]["type"], "chat");
            assert_eq!(
                menu["commands"],
                json!([{"command":"agents","description":"Manage · List sessions"},{"command":"plan","description":"Task · Toggle Plan mode"}])
            );
        }
        assert!(
            shown
                .iter()
                .any(|body| body["scope"]["type"] == "default" && body.get("commands").is_none())
        );
        adapter.deactivate().await;
        assert!(!adapter.enabled());
        let mut cleared = vec![];
        for _ in 0..8 {
            cleared.push(rx.recv().await.unwrap());
        }
        assert!(cleared.iter().all(|body| body.get("commands").is_none()));
        for kind in [
            "default",
            "all_private_chats",
            "all_group_chats",
            "all_chat_administrators",
        ] {
            assert!(cleared.iter().any(|body| body["scope"]["type"] == kind));
        }
        for group in [-1001, -1002] {
            assert!(
                cleared
                    .iter()
                    .any(|body| body["scope"] == json!({"type":"chat","chat_id":group}))
            );
        }
        server.abort();
        Ok(())
    }
}
