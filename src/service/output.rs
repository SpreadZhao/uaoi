use super::{Service, shorten};
use crate::{config::OutputMode, model::*};
use anyhow::{Context, Result};
use serde_json::json;

#[derive(Default)]
pub(super) struct Message<'a> {
    pub(super) text: &'a str,
    pub(super) buttons: Vec<ButtonRow>,
    pub(super) links: Vec<TextLink>,
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
        if self
            .store
            .bound(t)
            .await?
            .is_none_or(|session| session.id != e.session)
        {
            return Ok(());
        }
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
        if matches!(
            e.kind,
            EventKind::Registered | EventKind::Finished | EventKind::Offline
        ) {
            return Ok(());
        }

        if matches!(e.kind, EventKind::User)
            && let Some(origin) = &e.origin
            && self.store.origin(origin).await?.as_ref() == Some(t)
        {
            return Ok(());
        }
        let s = self.store.session(&e.session).await?;
        if let Some(req) = &e.request {
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
        let mode = self.cfg.agents[&s.agent].output;
        if mode == OutputMode::Compact && matches!(e.kind, EventKind::Reasoning) {
            return Ok(());
        }
        let (key, text) = if matches!(e.kind, EventKind::User) {
            (
                format!("{}:{}", e.session, e.key),
                format!("You · CLI\n\n{}", e.text),
            )
        } else if mode == OutputMode::Compact && matches!(e.kind, EventKind::Tool) {
            (
                format!("{}:activity", s.id),
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
