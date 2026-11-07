use super::{ImAdapter, SendFailure};
use crate::{
    config::{ProjectTelegram, TelegramConfig},
    model::*,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::time::Duration;

pub struct Telegram {
    configs: Vec<(String, ProjectTelegram)>,
    name: String,
    agent: String,
    username: String,
    url: String,
    client: reqwest::Client,
}
impl Telegram {
    pub fn new(
        agent: &str,
        config: &TelegramConfig,
        configs: Vec<(String, ProjectTelegram)>,
    ) -> Result<Self> {
        let token = std::env::var(&config.token_env)
            .with_context(|| format!("Missing {}", config.token_env))?;
        anyhow::ensure!(!token.is_empty(), "Empty Telegram token");
        Ok(Self {
            name: format!(
                "telegram:{}",
                token.split(':').next().context("Invalid bot token")?
            ),
            agent: agent.into(),
            username: String::new(),
            configs,
            url: format!("https://api.telegram.org/bot{token}"),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(40))
                .build()?,
        })
    }
    async fn api(&self, method: &str, body: Value) -> Result<Value> {
        // reqwest errors include the URL (and therefore the token); strip it.
        let response = self
            .client
            .post(format!("{}/{method}", self.url))
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Telegram transport: {}", e.without_url()))?;
        let v: Value = response
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Telegram response: {}", e.without_url()))?;
        anyhow::ensure!(
            v["ok"] == true,
            "Telegram {}",
            v["description"].as_str().unwrap_or("API failure")
        );
        Ok(v["result"].clone())
    }
    pub async fn validate(&mut self) -> Result<()> {
        let me = self.api("getMe", json!({})).await?;
        anyhow::ensure!(
            self.name == format!("telegram:{}", me["id"]),
            "Bot token identity mismatch"
        );
        self.username = me["username"]
            .as_str()
            .context("Bot username unavailable")?
            .into();
        let info = self.api("getWebhookInfo", json!({})).await?;
        anyhow::ensure!(
            info["url"].as_str().unwrap_or("").is_empty(),
            "Bot has a webhook; remove it explicitly before using long polling"
        );
        Ok(())
    }
}
#[async_trait]
impl ImAdapter for Telegram {
    fn name(&self) -> &str {
        &self.name
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
        let commands: Vec<_> = commands
            .iter()
            .map(|(command, description)| json!({"command":command,"description":description}))
            .collect();
        // Telegram scopes stop at the chat/member level, not individual topics.
        // Override inherited bot menus only inside configured groups.
        for (_, config) in &self.configs {
            self.api(
                "setMyCommands",
                json!({"commands":commands,"scope":{"type":"chat","chat_id":config.group}}),
            )
            .await?;
            for user in &config.allowed_users {
                self.api("setMyCommands", json!({"commands":commands,"scope":{"type":"chat_member","chat_id":config.group,"user_id":user}})).await?;
            }
        }
        Ok(())
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
    fn authorized(&self, sender: &str, target: &Conversation) -> bool {
        sender.parse::<i64>().ok().is_some_and(|id| {
            self.configs
                .iter()
                .any(|(_, c)| c.group.to_string() == target.space && c.allowed_users.contains(&id))
        })
    }
    async fn poll(&self, cursor: &str) -> Result<(Vec<Inbound>, String)> {
        let updates=self.api("getUpdates",json!({"offset":cursor.parse::<i64>().unwrap_or(0),"timeout":25,"allowed_updates":["message","callback_query"]})).await?;
        let mut out = vec![];
        let mut next = cursor.to_owned();
        for u in updates.as_array().context("Invalid updates")? {
            let id = u["update_id"].as_i64().context("Missing update ID")?;
            next = (id + 1).to_string();
            let callback = u.get("callback_query");
            let m = callback.map(|c| &c["message"]).unwrap_or(&u["message"]);
            let sender = callback.map(|c| &c["from"]).unwrap_or(&m["from"])["id"]
                .as_i64()
                .unwrap_or(0);
            // Only the configured forum group is a control surface.
            if m["from"]["is_bot"] == true && callback.is_none() {
                continue;
            }
            let Some(chat) = m["chat"]["id"].as_i64().filter(|id| *id < 0) else {
                continue;
            };
            let target = Conversation {
                platform: "telegram".into(),
                space: chat.to_string(),
                thread: m["message_thread_id"]
                    .as_i64()
                    .map(|x| x.to_string())
                    .unwrap_or_default(),
            };
            if !self.authorized(&sender.to_string(), &target) {
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
            if recipient.is_some_and(|bot| !bot.eq_ignore_ascii_case(&self.username)) {
                continue;
            }
            out.push(Inbound {
                agent: self.agent.clone(),
                addressed: callback.is_some() || recipient.is_some(),
                id: if callback.is_some() {
                    format!("{}:callback:{id}", self.name)
                } else {
                    format!(
                        "telegram:{chat}:{}",
                        m["message_id"].as_i64().context("Missing message ID")?
                    )
                },
                sender: sender.to_string(),
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
        let response = self
            .client
            .post(format!("{}/editForumTopic", self.url))
            .json(&body)
            .send()
            .await
            .map_err(|e| SendFailure::Unknown(e.without_url().to_string()))?;
        let status = response.status();
        let v: Value = response
            .json()
            .await
            .map_err(|e| SendFailure::Unknown(e.without_url().to_string()))?;
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
        let response = self
            .client
            .post(format!("{}/{method}", self.url))
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                if d.message.is_some() {
                    SendFailure::Retry(Duration::from_secs(3))
                } else {
                    SendFailure::Unknown(e.without_url().to_string())
                }
            })?;
        let status = response.status();
        let v: Value = response
            .json()
            .await
            .map_err(|e| SendFailure::Unknown(e.without_url().to_string()))?;
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

    #[tokio::test]
    async fn mock_api_routes_groups_without_forcing_reply_ui() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
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
                            {"update_id":3,"message":{"from":{"id":2},"chat":{"id":-1001},"text":"forbidden","date":1}},
                            {"update_id":4,"callback_query":{"id":"callback-4","from":{"id":1},"data":"choice","message":{"chat":{"id":-1001},"message_thread_id":10}}},
                            {"update_id":5,"message":{"message_id":9,"from":{"id":1},"chat":{"id":-1001},"message_thread_id":10,"text":"/new@WorkBot /tmp","date":1}},
                            {"update_id":6,"message":{"message_id":10,"from":{"id":1},"chat":{"id":-1001},"message_thread_id":10,"text":"/new@OtherBot /tmp","date":1}}
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
            username: "WorkBot".into(),
            configs: vec![
                (
                    "work".into(),
                    ProjectTelegram {
                        group: -1001,
                        allowed_users: vec![1],
                    },
                ),
                (
                    "personal".into(),
                    ProjectTelegram {
                        group: -1002,
                        allowed_users: vec![2],
                    },
                ),
            ],
            name: "telegram:1".into(),
            url: format!("http://{addr}/bot-test"),
            client: reqwest::Client::builder().no_proxy().build()?,
        };
        let (messages, cursor) = adapter.poll("").await?;
        assert_eq!(cursor, "7");
        assert_eq!(messages.len(), 4);
        assert!(messages[3].addressed);
        assert!(messages[3].text.contains("@WorkBot"));
        assert_eq!(messages[0].reply_to.as_deref(), Some("6"));
        assert_ne!(messages[0].target.space, messages[1].target.space);
        rx.recv().await.unwrap();
        let other = Telegram {
            agent: "personal".into(),
            username: "OtherBot".into(),
            configs: adapter.configs.clone(),
            name: "telegram:2".into(),
            url: adapter.url.clone(),
            client: adapter.client.clone(),
        };
        let (other_messages, _) = other.poll("").await?;
        rx.recv().await.unwrap();
        assert_eq!(messages[0].target, other_messages[0].target);
        assert_eq!(messages[0].id, other_messages[0].id);
        assert_ne!(messages[0].agent, other_messages[0].agent);
        assert!(other_messages[3].text.contains("@OtherBot"));
        adapter
            .acknowledge(&messages[2], Some("This question was already answered"))
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
        for scope in [
            json!({"type":"chat","chat_id":-1001}),
            json!({"type":"chat_member","chat_id":-1001,"user_id":1}),
            json!({"type":"chat","chat_id":-1002}),
            json!({"type":"chat_member","chat_id":-1002,"user_id":2}),
        ] {
            let menu = rx.recv().await.unwrap();
            assert_eq!(menu["scope"], scope);
            assert_eq!(
                menu["commands"],
                json!([{"command":"agents","description":"Manage · List sessions"},{"command":"plan","description":"Task · Toggle Plan mode"}])
            );
        }
        server.abort();
        Ok(())
    }
}
