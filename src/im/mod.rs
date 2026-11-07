pub mod telegram;
use crate::model::{Conversation, Delivery, Inbound};
use anyhow::Result;
use async_trait::async_trait;

#[derive(Debug)]
pub enum SendFailure {
    Retry(std::time::Duration),
    Unknown(String),
    Permanent(String),
}
#[async_trait]
pub trait ImAdapter: Send + Sync {
    fn name(&self) -> &str;
    fn spaces(&self) -> Vec<Conversation>;
    fn agent(&self) -> &str;
    fn project(&self, target: &Conversation) -> Option<&str>;
    fn destination(&self, project: &str) -> Option<Conversation>;
    fn topic(&self, project: &str, id: &str) -> Result<Conversation>;
    async fn discard_backlog(&self) -> Result<String>;
    async fn poll(&self, cursor: &str) -> Result<(Vec<Inbound>, String)>;
    async fn create_conversation(&self, space: &Conversation, title: &str) -> Result<Conversation>;
    async fn rename_conversation(
        &self,
        target: &Conversation,
        title: &str,
    ) -> std::result::Result<(), SendFailure>;
    async fn deliver(&self, delivery: &Delivery) -> std::result::Result<String, SendFailure>;
    async fn indicate_working(&self, _target: &Conversation) -> Result<()> {
        Ok(())
    }
    async fn acknowledge(&self, _input: &Inbound, _feedback: Option<&str>) -> Result<()> {
        Ok(())
    }
    async fn set_commands(&self, _commands: &[(&str, &str)]) -> Result<()> {
        Ok(())
    }
    fn authorized(&self, sender: &str, target: &Conversation) -> bool;
    fn link(&self, target: &Conversation) -> String;
}
