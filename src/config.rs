use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub state_dir: Option<PathBuf>,
    pub runtime_dir: Option<PathBuf>,
    #[serde(default)]
    pub agents: BTreeMap<String, AgentSpec>,
    #[serde(default)]
    pub projects: BTreeMap<String, Project>,
    #[serde(default)]
    pub im: ImConfig,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSpec {
    pub kind: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Project {
    pub workspace: PathBuf,
    pub agent: String,
}
#[derive(Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ImConfig {
    pub telegram: Option<TelegramConfig>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramConfig {
    pub token_env: String,
    pub group: i64,
    pub allowed_users: Vec<i64>,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let mut cfg: Self = toml::from_str(
            &std::fs::read_to_string(path)
                .with_context(|| format!("Read configuration {}", path.display()))?,
        )?;
        let mut workspaces = std::collections::HashSet::new();
        for (name, p) in &mut cfg.projects {
            anyhow::ensure!(
                p.workspace.is_absolute(),
                "Project {name}: workspace must be absolute"
            );
            p.workspace = std::fs::canonicalize(&p.workspace)
                .with_context(|| format!("Project {name}: workspace must exist"))?;
            anyhow::ensure!(
                workspaces.insert(p.workspace.clone()),
                "Project {name}: workspace already configured"
            );
            anyhow::ensure!(
                p.workspace.is_dir(),
                "Project {name}: workspace is not a directory"
            );
            anyhow::ensure!(
                cfg.agents.contains_key(&p.agent),
                "Unknown agent {}",
                p.agent
            );
        }
        for (name, a) in &cfg.agents {
            anyhow::ensure!(a.kind == "codex", "Agent {name}: only codex is implemented");
            anyhow::ensure!(
                !a.command.is_empty() && !a.command[0].is_empty(),
                "Agent {name}: empty command"
            );
        }
        if let Some(t) = &cfg.im.telegram {
            anyhow::ensure!(t.group < 0, "Telegram group must be a group chat ID");
            anyhow::ensure!(
                !t.allowed_users.is_empty(),
                "Telegram allowed_users must not be empty"
            );
        }
        if cfg.projects.is_empty() {
            bail!("Configure at least one project");
        }
        for path in [&cfg.state_dir, &cfg.runtime_dir].into_iter().flatten() {
            anyhow::ensure!(
                path.is_absolute(),
                "state_dir and runtime_dir must be absolute"
            );
        }
        Ok(cfg)
    }
    pub fn project(&self, cwd: &Path) -> Result<(&str, &Project)> {
        self.projects
            .iter()
            .filter(|(_, p)| cwd.starts_with(&p.workspace))
            .max_by_key(|(_, p)| p.workspace.components().count())
            .map(|(name, p)| (name.as_str(), p))
            .context("No configured project contains this directory")
    }
    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(self).expect("serialize configuration"))
        )
    }
    pub fn default_path() -> PathBuf {
        directories::ProjectDirs::from("", "", "uaoi")
            .expect("home directory")
            .config_dir()
            .join("config.toml")
    }
    pub fn state(&self) -> PathBuf {
        self.state_dir.clone().unwrap_or_else(|| {
            directories::ProjectDirs::from("", "", "uaoi")
                .expect("home directory")
                .state_dir()
                .unwrap()
                .to_owned()
        })
    }
    pub fn runtime(&self) -> PathBuf {
        self.runtime_dir
            .clone()
            .unwrap_or_else(|| self.state().join("run"))
    }
}
