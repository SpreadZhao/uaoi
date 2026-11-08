use super::{Service, shorten};
use crate::{config::OutputMode, model::*};
use anyhow::{Context, Result};
use serde_json::json;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(super) struct Message<'a> {
    pub(super) text: &'a str,
    pub(super) buttons: Vec<ButtonRow>,
    pub(super) links: Vec<TextLink>,
}

#[derive(Default)]
pub(super) struct OutputState {
    pub(super) agent: String,
    tools: ToolOutput,
    final_message: Option<AgentEvent>,
}
#[derive(Default)]
struct ToolOutput {
    current: Option<String>,
    tools: HashMap<String, String>,
    messages: HashSet<String>,
}
impl ToolOutput {
    fn message(&mut self, key: &str) {
        // Streaming updates to the same message do not start another group.
        if self.messages.insert(key.to_owned()) {
            self.current = None;
        }
    }
    fn tool(&mut self, event: &AgentEvent) -> String {
        // A late completion still belongs to the group where that tool began.
        if let Some(key) = self.tools.get(&event.key) {
            return key.clone();
        }
        let key = self
            .current
            .get_or_insert_with(|| format!("{}:activity:{}", event.session, uuid::Uuid::new_v4()))
            .clone();
        self.tools.insert(event.key.clone(), key.clone());
        key
    }
}
impl Service {
    pub async fn notice(&self, target: &Conversation, key: &str, text: &str) -> Result<()> {
        self.output(
            target,
            key,
            Message {
                text,
                ..Default::default()
            },
        )
        .await
    }
    pub(super) async fn output(
        &self,
        target: &Conversation,
        key: &str,
        message: Message<'_>,
    ) -> Result<()> {
        let agent = if let Some(session) = self.store.bound(target).await? {
            session.agent
        } else {
            let project = self.project_name(target)?;
            self.cfg.projects[project]
                .agents
                .iter()
                .find(|name| self.im(name).is_ok_and(|im| im.enabled()))
                .context("No active agent in this project")?
                .clone()
        };
        self.output_as(&agent, target, key, message).await
    }
    pub(super) async fn output_as(
        &self,
        agent: &str,
        target: &Conversation,
        key: &str,
        message: Message<'_>,
    ) -> Result<()> {
        let Message {
            text,
            buttons,
            links,
        } = message;
        let chunks = split_output(text);
        let mut offset = 0;
        for (i, part) in chunks.into_iter().enumerate() {
            let end = offset + part.len();
            let part_links = links
                .iter()
                .filter(|link| link.start >= offset && link.end <= end)
                .map(|link| TextLink {
                    start: link.start - offset,
                    end: link.end - offset,
                    url: link.url.clone(),
                })
                .collect();
            self.store
                .enqueue(
                    agent,
                    target,
                    &format!("{key}:{i}"),
                    &part,
                    if i == 0 { buttons.clone() } else { vec![] },
                    part_links,
                )
                .await?;
            offset = end;
        }
        Ok(())
    }
    pub(crate) async fn project(&self, e: &AgentEvent, t: &Conversation) -> Result<()> {
        let lock = self.route_lock(t).await;
        let _guard = lock.lock().await;
        let Some(s) = self.store.bound(t).await?.filter(|s| s.id == e.session) else {
            return Ok(());
        };
        let mode = self.cfg.agents[&s.agent].output;
        let output_key = (e.session.clone(), t.clone());
        if matches!(e.kind, EventKind::Finished | EventKind::Offline) {
            let final_message = self
                .outputs
                .lock()
                .await
                .remove(&output_key)
                .and_then(|state| state.final_message);
            if mode == OutputMode::Minimal
                && matches!(e.kind, EventKind::Finished)
                && let Some(last) = final_message
            {
                self.output(
                    t,
                    &format!("{}:{}", last.session, last.key),
                    Message {
                        text: &last.text,
                        ..Default::default()
                    },
                )
                .await?;
            }
            return Ok(());
        }
        if matches!(e.kind, EventKind::Registered) {
            return Ok(());
        }
        if mode == OutputMode::Compact
            && matches!(e.kind, EventKind::Reasoning)
            && e.request.is_none()
        {
            return Ok(());
        }
        let activity_key = if mode == OutputMode::Compact {
            let mut states = self.outputs.lock().await;
            let state = states
                .entry(output_key.clone())
                .or_insert_with(|| OutputState {
                    agent: s.agent.clone(),
                    ..Default::default()
                });
            if matches!(e.kind, EventKind::Tool) && e.request.is_none() {
                Some(state.tools.tool(e))
            } else {
                state.tools.message(&e.key);
                None
            }
        } else {
            None
        };
        if e.request.is_none()
            && let Some(id) = e.key.strip_prefix("request:")
        {
            self.drafts.lock().await.remove(id);
            return self
                .notice(
                    t,
                    &format!("{}:{}", e.session, e.key),
                    &shorten(&e.text, 1200),
                )
                .await;
        }
        if matches!(e.kind, EventKind::User) {
            if let Some(state) = self.outputs.lock().await.get_mut(&output_key) {
                state.final_message = None;
            }
            if let Some(origin) = &e.origin
                && self.store.origin(origin).await?.as_ref() == Some(t)
            {
                return Ok(());
            }
        }
        if let Some(req) = &e.request {
            if mode == OutputMode::Minimal && req.kind != RequestKind::Plan {
                // A question can be the last assistant output of an async
                // turn. Do not publish the earlier progress as its final reply.
                if let Some(state) = self.outputs.lock().await.get_mut(&output_key) {
                    state.final_message = None;
                }
            }
            if matches!(
                req.kind,
                RequestKind::Questions | RequestKind::AsyncQuestions
            ) {
                return self.render_question(&s, t, req).await;
            }
            let mut buttons = vec![];
            for (label, value) in &req.choices {
                buttons.push(
                    self.button(
                        t,
                        label,
                        json!({"op":"decision","request":req.id,"value":value}),
                    )
                    .await?,
                );
            }
            return self
                .output(
                    t,
                    &format!("{}:{}", e.session, e.key),
                    Message {
                        text: &e.text,
                        buttons: buttons.into_iter().map(|button| vec![button]).collect(),
                        ..Default::default()
                    },
                )
                .await;
        }
        if mode == OutputMode::Minimal {
            match e.kind {
                EventKind::Text | EventKind::Plan => {
                    self.outputs
                        .lock()
                        .await
                        .entry(output_key.clone())
                        .or_insert_with(|| OutputState {
                            agent: s.agent.clone(),
                            ..Default::default()
                        })
                        .final_message = Some(e.clone());
                    return Ok(());
                }
                EventKind::Error => {
                    if let Some(state) = self.outputs.lock().await.get_mut(&output_key) {
                        state.final_message = None;
                    }
                }
                // Status events report interactions, such as an answer from
                // the CLI. Plans can be the final reply before a plan decision.
                EventKind::User | EventKind::Status => {}
                _ => return Ok(()),
            }
        }
        let (key, text) = if matches!(e.kind, EventKind::User) {
            (
                format!("{}:{}", e.session, e.key),
                format!("You · CLI\n\n{}", e.text),
            )
        } else if let Some(activity_key) = activity_key {
            (
                activity_key,
                e.text
                    .lines()
                    .next()
                    .unwrap_or("Working")
                    .chars()
                    .take(240)
                    .collect(),
            )
        } else {
            (format!("{}:{}", e.session, e.key), e.text.clone())
        };
        self.output(
            t,
            &key,
            Message {
                text: &text,
                ..Default::default()
            },
        )
        .await
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
