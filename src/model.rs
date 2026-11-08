use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
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
    Error,
    Question,
    Plan,
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
    pub choices: Vec<(String, String)>,
    pub kind: RequestKind,
    pub questions: Vec<Question>,
    pub blocking: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RequestKind {
    Approval,
    Questions,
    AsyncQuestions,
    Plan,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Question {
    pub id: String,
    pub title: String,
    pub options: Vec<(String, String)>,
    pub secret: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionSummary {
    pub native: String,
    pub title: String,
    pub cwd: std::path::PathBuf,
    pub status: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BindingSpec {
    pub project: String,
    pub agent: String,
    pub session: String,
    pub topic: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BindingsDocument {
    pub version: u32,
    pub revision: u64,
    pub bindings: Vec<BindingSpec>,
}
#[derive(Clone, Debug)]
pub struct BindingRecord {
    pub session: Session,
    pub target: Conversation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionMode {
    Default,
    Plan,
}
impl SessionMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Default => "Default",
            Self::Plan => "Plan",
        }
    }
}
#[derive(Clone, Debug)]
pub struct SessionStatus {
    pub cwd: std::path::PathBuf,
    pub state: String,
    pub mode: Option<SessionMode>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LaunchPlan {
    pub command: Vec<String>,
    pub env: std::collections::BTreeMap<String, String>,
    pub cwd: std::path::PathBuf,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delivery {
    pub agent: String,
    pub id: i64,
    pub key: String,
    pub target: Conversation,
    pub text: String,
    pub message: Option<String>,
    pub buttons: Vec<ButtonRow>,
    pub links: Vec<TextLink>,
}

pub type Button = (String, String);
pub type ButtonRow = Vec<Button>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TextLink {
    pub start: usize,
    pub end: usize,
    pub url: String,
}
#[derive(Clone, Debug)]
pub struct Inbound {
    pub agent: String,
    pub addressed: bool,
    pub id: String,
    pub target: Conversation,
    pub text: String,
    pub conversation_created: bool,
    pub sent_at: u64,
    pub action: Option<String>,
    pub acknowledgement: Option<String>,
    pub reply_to: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TitleChange {
    pub agent: String,
    pub target: Conversation,
    pub title: String,
}
