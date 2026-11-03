use super::{ImAdapter, SendFailure};
use crate::{config::TelegramConfig, model::*};
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::time::Duration;

pub struct Telegram {
    config: TelegramConfig,
    url: String,
    client: reqwest::Client,
}
impl Telegram {
    pub fn new(config: TelegramConfig) -> Result<Self> {
        let token = std::env::var(&config.token_env)
            .with_context(|| format!("Missing {}", config.token_env))?;
        anyhow::ensure!(!token.is_empty(), "Empty Telegram token");
        Ok(Self {
            config,
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
    pub async fn validate(&self) -> Result<()> {
        self.api("getMe", json!({})).await?;
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
        "telegram"
    }
    fn space(&self) -> Conversation {
        Conversation {
            platform: self.name().into(),
            space: self.config.group.to_string(),
            thread: String::new(),
        }
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
    fn authorized(&self, sender: &str) -> bool {
        sender
            .parse::<i64>()
            .ok()
            .is_some_and(|id| self.config.allowed_users.contains(&id))
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
            if let Some(c) = callback {
                let _ = self
                    .api("answerCallbackQuery", json!({"callback_query_id":c["id"]}))
                    .await;
            }
            // Only the configured forum group is a control surface.
            if !self.authorized(&sender.to_string())
                || m["from"]["is_bot"] == true && callback.is_none()
            {
                continue;
            }
            let Some(chat) = m["chat"]["id"].as_i64().filter(|id| *id < 0) else {
                continue;
            };
            if chat != self.config.group {
                continue;
            }
            out.push(Inbound {
                id: format!("telegram:{id}"),
                sender: sender.to_string(),
                target: Conversation {
                    platform: self.name().into(),
                    space: chat.to_string(),
                    thread: m["message_thread_id"]
                        .as_i64()
                        .map(|x| x.to_string())
                        .unwrap_or_default(),
                },
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
            platform: self.name().into(),
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
        let mut body = json!({"chat_id":d.target.space,"text":d.text,"link_preview_options":{"is_disabled":true},"reply_markup":{"inline_keyboard":d.buttons.iter().map(|(label,data)|vec![json!({"text":label,"callback_data":data})]).collect::<Vec<_>>()}});
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
