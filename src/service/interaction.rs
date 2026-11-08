use super::output::Message;
use super::{Service, shorten};
use crate::model::*;
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Clone, Default)]
pub(super) struct QuestionDraft {
    current: usize,
    review: bool,
    submitting: bool,
    answers: BTreeMap<String, String>,
}
impl Service {
    pub(super) async fn button(
        &self,
        target: &Conversation,
        label: &str,
        body: Value,
    ) -> Result<(String, String)> {
        Ok((label.into(), self.store.action(target, body).await?))
    }
    pub(super) async fn answer_question_reply(&self, msg: &Inbound, id: &str) -> Result<()> {
        let (_, req) = self.store.request(id).await.context(
            "This question has expired or was already answered; reply to a current question",
        )?;
        anyhow::ensure!(
            matches!(
                req.kind,
                RequestKind::Questions | RequestKind::AsyncQuestions
            ),
            "Use this card's buttons to choose a decision"
        );
        let draft = self
            .drafts
            .lock()
            .await
            .get(id)
            .cloned()
            .unwrap_or_default();
        anyhow::ensure!(
            !draft.review,
            "Select a question before replying with an answer"
        );
        let question = req
            .questions
            .get(draft.current)
            .context("Question unavailable")?;
        self.save_answer(
            msg,
            &json!({"request":id,"question":question.id}),
            &msg.text,
        )
        .await
    }
    pub(super) async fn question_action(&self, msg: &Inbound, action: &Value) -> Result<()> {
        let id = action["request"]
            .as_str()
            .context("Invalid question button")?;
        let (sid, req) = self
            .store
            .request(id)
            .await
            .context("This question expired or was already answered")?;
        let session = self
            .store
            .bound(&msg.target)
            .await?
            .context("Session unavailable")?;
        anyhow::ensure!(sid == session.id, "Question belongs to another session");
        anyhow::ensure!(
            session.agent == msg.agent,
            "This topic is controlled by agent {}",
            session.agent
        );
        match action["op"].as_str().unwrap_or("") {
            "decision" => {
                self.agent
                    .respond(
                        &session,
                        id,
                        action["value"].as_str().context("Missing decision")?,
                    )
                    .await
            }
            "choice" => {
                self.save_answer(
                    msg,
                    action,
                    action["value"].as_str().context("Missing answer")?,
                )
                .await
            }
            "navigate" | "review" => {
                let mut drafts = self.drafts.lock().await;
                let draft = drafts.entry(id.into()).or_default();
                anyhow::ensure!(
                    !draft.submitting,
                    "Answer submission is pending; check /status or /history"
                );
                if action["op"] == "navigate" {
                    let current = action["question"].as_u64().context("Missing question")? as usize;
                    anyhow::ensure!(current < req.questions.len(), "Unknown question");
                    draft.current = current;
                    draft.review = false;
                } else {
                    draft.review = true;
                }
                drop(drafts);
                self.render_question(&session, &msg.target, &req).await
            }
            "submit" => {
                let draft = self
                    .drafts
                    .lock()
                    .await
                    .get(id)
                    .cloned()
                    .unwrap_or_default();
                anyhow::ensure!(
                    req.questions
                        .iter()
                        .all(|q| draft.answers.contains_key(&q.id)),
                    "Answer every question before submitting"
                );
                self.submit_answers(msg, &session, &req, &draft.answers)
                    .await
            }
            _ => anyhow::bail!("Unknown question action"),
        }
    }
    pub(super) async fn submit_answers(
        &self,
        msg: &Inbound,
        session: &Session,
        req: &PendingRequest,
        answers: &BTreeMap<String, String>,
    ) -> Result<()> {
        {
            let mut drafts = self.drafts.lock().await;
            let draft = drafts.entry(req.id.clone()).or_default();
            anyhow::ensure!(
                !draft.submitting,
                "Answer submission is pending; check /status or /history"
            );
            draft.submitting = true;
        }
        self.render_question(session, &msg.target, req).await?;
        if let Err(error) = self
            .agent
            .respond(session, &req.id, &serde_json::to_string(answers)?)
            .await
        {
            if !error.is::<crate::agent::OutcomeUnknown>() {
                self.drafts
                    .lock()
                    .await
                    .entry(req.id.clone())
                    .or_default()
                    .submitting = false;
            }
            self.render_question(session, &msg.target, req).await?;
            return Err(error);
        }
        self.store.resolve(&req.id).await?;
        self.drafts.lock().await.remove(&req.id);
        self.notice(
            &msg.target,
            &format!("{}:request:{}", session.id, req.id),
            &answer_summary(req, answers, "Answer submitted via Telegram"),
        )
        .await
    }
    pub(super) async fn save_answer(
        &self,
        msg: &Inbound,
        action: &Value,
        text: &str,
    ) -> Result<()> {
        let id = action["request"].as_str().context("Missing request")?;
        let (sid, req) = self.store.request(id).await?;
        let session = self
            .store
            .bound(&msg.target)
            .await?
            .context("Session unavailable")?;
        anyhow::ensure!(sid == session.id, "Question belongs to another session");
        anyhow::ensure!(
            session.agent == msg.agent,
            "This topic is controlled by agent {}",
            session.agent
        );
        let q = req
            .questions
            .iter()
            .find(|q| Some(q.id.as_str()) == action["question"].as_str())
            .context("Unknown question")?;
        anyhow::ensure!(!q.secret, "Answer sensitive questions in the CLI");
        anyhow::ensure!(!text.trim().is_empty(), "Answer must not be empty");
        {
            let mut drafts = self.drafts.lock().await;
            let draft = drafts.entry(id.into()).or_default();
            anyhow::ensure!(
                !draft.submitting,
                "Answer submission is pending; check /status or /history"
            );
            draft.answers.insert(q.id.clone(), text.into());
            draft.current = req
                .questions
                .iter()
                .position(|question| question.id == q.id)
                .unwrap_or(0);
            draft.review = false;
        }
        self.render_question(&session, &msg.target, &req).await
    }
    pub(super) async fn render_question(
        &self,
        s: &Session,
        t: &Conversation,
        req: &PendingRequest,
    ) -> Result<()> {
        let draft = self
            .drafts
            .lock()
            .await
            .get(&req.id)
            .cloned()
            .unwrap_or_default();
        let status = if req.kind == RequestKind::AsyncQuestions {
            "Task continues while you answer"
        } else if req.blocking {
            "Waiting for your answer"
        } else {
            "Optional question; the agent may clear it automatically"
        };
        let count = req.questions.len();
        let answered = draft.answers.len();
        let key = format!("{}:request:{}", s.id, req.id);
        if draft.submitting {
            return self
                .notice(
                    t,
                    &key,
                    &answer_summary(
                        req,
                        &draft.answers,
                        "Answer submission pending; check /status or /history before retrying",
                    ),
                )
                .await;
        }
        anyhow::ensure!(count > 0, "No questions to answer");
        let mut rows = vec![];
        if count > 1 || draft.review {
            let mut navigation = vec![];
            for (index, q) in req.questions.iter().enumerate() {
                let label = format!(
                    "{}{} {}",
                    if !draft.review && draft.current == index {
                        "• "
                    } else {
                        ""
                    },
                    index + 1,
                    if draft.answers.contains_key(&q.id) {
                        "✓"
                    } else {
                        "○"
                    }
                );
                navigation.push(
                    self.button(
                        t,
                        &label,
                        json!({"op":"navigate","request":req.id,"question":index}),
                    )
                    .await?,
                );
            }
            rows.extend(navigation.chunks(6).map(|row| row.to_vec()));
        }
        let text = if draft.review {
            answer_summary(
                req,
                &draft.answers,
                &format!("Review answers · {answered}/{count} answered\n{status}"),
            )
        } else {
            let q = req
                .questions
                .get(draft.current)
                .context("Question unavailable")?;
            let mut text = format!(
                "Question {}/{} · {answered}/{count} answered\n{status}\n\n{}",
                draft.current + 1,
                count,
                shorten(&q.title, 1400)
            );
            if q.secret {
                text.push_str("\n\nSensitive question: answer in the CLI.");
            } else {
                for (index, (label, description)) in q.options.iter().enumerate() {
                    if !description.is_empty() {
                        text.push_str(&format!(
                            "\n{}. {} — {}",
                            index + 1,
                            shorten(label, 80),
                            shorten(description, 180)
                        ));
                    }
                    let selected = draft
                        .answers
                        .get(&q.id)
                        .is_some_and(|answer| answer == label);
                    rows.push(vec![
                        self.button(
                            t,
                            &format!("{}{}", if selected { "✓ " } else { "" }, shorten(label, 80)),
                            json!({"op":"choice","request":req.id,"question":q.id,"value":label}),
                        )
                        .await?,
                    ]);
                }
                text.push_str(&format!("\n\nCurrent answer: {}\nReply to this message to write or replace this answer.",
                    draft.answers.get(&q.id).map(|answer| shorten(answer, 600)).unwrap_or_else(|| "Not answered".into())));
            }
            text
        };
        if draft.review || count == 1 {
            if answered == count {
                rows.push(vec![
                    self.button(t, "Submit answers", json!({"op":"submit","request":req.id}))
                        .await?,
                ]);
            }
        } else {
            rows.push(vec![
                self.button(t, "Review answers", json!({"op":"review","request":req.id}))
                    .await?,
            ]);
        }
        self.output(
            t,
            &key,
            Message {
                text: &shorten(&text, 3500),
                buttons: rows,
                ..Default::default()
            },
        )
        .await
    }
}

fn answer_summary(
    req: &PendingRequest,
    answers: &BTreeMap<String, String>,
    heading: &str,
) -> String {
    let mut text = heading.to_owned();
    for (index, question) in req.questions.iter().enumerate() {
        let answer = if question.secret {
            "Answer in the CLI".into()
        } else {
            answers
                .get(&question.id)
                .map(|answer| shorten(answer, 600))
                .unwrap_or_else(|| "Not answered".into())
        };
        text.push_str(&format!(
            "\n\n{}. {}\n{}",
            index + 1,
            shorten(&question.title, 160),
            answer
        ));
    }
    shorten(&text, 3500)
}
