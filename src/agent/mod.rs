pub mod codex;
pub mod codex_cli;
pub mod rpc;
use crate::model::{LaunchPlan, Session};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait AgentAdapter: Send + Sync {
    async fn open(
        &self,
        client: &str,
        project: &str,
        cwd: &std::path::Path,
        env: &std::collections::BTreeMap<String, String>,
        args: &[std::ffi::OsString],
    ) -> Result<LaunchPlan>;
    async fn close(&self, client: &str);
    async fn disconnected(&self, client: &str);
    async fn active(&self, session: &Session) -> bool;
    async fn accepts(&self, session: &Session, sent_at: u64) -> bool;
    async fn submit(
        &self,
        session: &Session,
        text: &str,
        input_id: &str,
        steer: bool,
    ) -> Result<()>;
    async fn interrupt(&self, session: &Session) -> Result<()>;
    async fn respond(&self, session: &Session, request: &str, answer: &str) -> Result<()>;
    async fn history(&self, session: &Session) -> Result<()>;
    async fn status(&self, session: &Session) -> Result<String>;
    async fn shutdown(&self);
}

#[derive(Debug)]
pub struct OutcomeUnknown(pub String);
impl std::fmt::Display for OutcomeUnknown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}; outcome unknown, check /status and /history before retrying",
            self.0
        )
    }
}
impl std::error::Error for OutcomeUnknown {}
