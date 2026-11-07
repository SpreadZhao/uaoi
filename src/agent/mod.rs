pub mod codex;
pub mod codex_cli;
pub mod rpc;
use crate::model::{LaunchPlan, Session, SessionMode, SessionStatus, SessionSummary};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait AgentAdapter: Send + Sync {
    async fn start(&self) -> Result<()>;
    async fn describe(&self, session: &Session) -> Result<SessionSummary>;
    async fn restore(&self, agent: &str, native: &str) -> Result<Session>;
    async fn create(&self, agent: &str, cwd: &std::path::Path) -> Result<Session>;
    async fn busy(&self) -> bool;
    async fn open(
        &self,
        client: &str,
        agent: &str,
        cwd: &std::path::Path,
        env: &std::collections::BTreeMap<String, String>,
        args: &[std::ffi::OsString],
    ) -> Result<LaunchPlan>;
    async fn close(&self, client: &str);
    async fn disconnected(&self, client: &str);
    async fn active(&self, session: &Session) -> bool;
    async fn working(&self, session: &Session) -> bool;
    async fn submit(&self, session: &Session, text: &str, input_id: &str) -> Result<()>;
    async fn set_mode(&self, session: &Session, mode: SessionMode) -> Result<()>;
    async fn toggle_plan_mode(&self, session: &Session) -> Result<SessionMode>;
    async fn submit_in_mode(
        &self,
        session: &Session,
        text: &str,
        input_id: &str,
        mode: SessionMode,
    ) -> Result<()>;
    async fn interrupt(&self, session: &Session) -> Result<()>;
    async fn respond(&self, session: &Session, request: &str, answer: &str) -> Result<()>;
    async fn history(&self, session: &Session) -> Result<()>;
    async fn status(&self, session: &Session) -> Result<SessionStatus>;
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
