use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub project: String,
    pub agent: String,
    pub native: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Conversation {
    pub platform: String,
    pub space: String,
    pub thread: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Registered,
    Offline,
    User,
    Text,
    Reasoning,
    Tool,
    Status,
    Finished,
    Question,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentEvent {
    pub session: String,
    pub key: String,
    pub kind: EventKind,
    pub text: String,
    pub complete: bool,
    pub request: Option<PendingRequest>,
    pub origin: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingRequest {
    pub id: String,
    pub label: String,
    pub choices: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LaunchPlan {
    pub command: Vec<String>,
    pub env: std::collections::BTreeMap<String, String>,
    pub cwd: std::path::PathBuf,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delivery {
    pub id: i64,
    pub target: Conversation,
    pub text: String,
    pub message: Option<String>,
    pub buttons: Vec<(String, String)>,
}
#[derive(Clone, Debug)]
pub struct Inbound {
    pub id: String,
    pub sender: String,
    pub target: Conversation,
    pub text: String,
    pub conversation_created: bool,
    pub sent_at: u64,
    pub action: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TitleChange {
    pub target: Conversation,
    pub title: String,
}
