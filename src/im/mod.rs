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
    fn space(&self) -> Conversation;
    async fn discard_backlog(&self) -> Result<String>;
    async fn poll(&self, cursor: &str) -> Result<(Vec<Inbound>, String)>;
    async fn create_conversation(&self, space: &Conversation, title: &str) -> Result<Conversation>;
    async fn rename_conversation(
        &self,
        target: &Conversation,
        title: &str,
    ) -> std::result::Result<(), SendFailure>;
    async fn deliver(&self, delivery: &Delivery) -> std::result::Result<String, SendFailure>;
    fn authorized(&self, sender: &str) -> bool;
}
