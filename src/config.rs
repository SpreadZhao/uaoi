use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub state_dir: Option<PathBuf>,
    pub runtime_dir: Option<PathBuf>,
    pub agents: BTreeMap<String, AgentSpec>,
    pub projects: BTreeMap<String, ProjectSpec>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSpec {
    pub kind: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub output: OutputMode,
}
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputMode {
    Full,
    #[default]
    Compact,
    Minimal,
}
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSpec {
    pub cwd: PathBuf,
    pub agents: Vec<String>,
    pub telegram: ProjectTelegram,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectTelegram {
    pub group: i64,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let mut cfg: Self = toml::from_str(
            &std::fs::read_to_string(path)
                .with_context(|| format!("Read configuration {}", path.display()))?,
        )?;
        anyhow::ensure!(
            cfg.version == 4,
            "Configuration version 4 required; create a new configuration and state directory"
        );
        anyhow::ensure!(
            !cfg.agents.is_empty(),
            "Configure at least one agent instance"
        );
        anyhow::ensure!(!cfg.projects.is_empty(), "Configure at least one project");
        let mut credentials = std::collections::HashSet::new();
        for (name, a) in &mut cfg.agents {
            anyhow::ensure!(a.kind == "codex", "Agent {name}: only codex is implemented");
            anyhow::ensure!(
                !a.command.is_empty() && !a.command[0].is_empty(),
                "Agent {name}: empty command"
            );
            let credential = match (&a.telegram.token, &a.telegram.token_env) {
                (Some(token), None) if !token.trim().is_empty() => ("token", token),
                (None, Some(variable)) if !variable.trim().is_empty() => ("token_env", variable),
                _ => anyhow::bail!(
                    "Agent {name}: configure exactly one nonempty telegram.token or telegram.token_env"
                ),
            };
            anyhow::ensure!(
                credentials.insert((credential.0, credential.1.clone())),
                "Agent {name}: each agent requires its own Bot credential"
            );
        }
        let mut groups = std::collections::HashSet::new();
        let mut directories = std::collections::HashSet::new();
        for (name, project) in &mut cfg.projects {
            anyhow::ensure!(
                project.cwd.is_absolute(),
                "Project {name}: cwd must be absolute"
            );
            project.cwd = std::fs::canonicalize(&project.cwd)
                .with_context(|| format!("Project {name}: cwd must exist"))?;
            anyhow::ensure!(
                project.cwd.is_dir(),
                "Project {name}: cwd must be a directory"
            );
            anyhow::ensure!(
                directories.insert(project.cwd.clone()),
                "Projects must have distinct canonical directories"
            );
            anyhow::ensure!(
                project.telegram.group < 0,
                "Project {name}: Telegram group must be negative"
            );
            anyhow::ensure!(
                groups.insert(project.telegram.group),
                "Each Telegram group must belong to exactly one project"
            );
            let mut members = std::collections::HashSet::new();
            anyhow::ensure!(
                !project.agents.is_empty(),
                "Project {name}: configure available agents"
            );
            for agent in &project.agents {
                anyhow::ensure!(
                    cfg.agents.contains_key(agent),
                    "Project {name}: unknown agent {agent}"
                );
                anyhow::ensure!(
                    members.insert(agent),
                    "Project {name}: duplicate agent {agent}"
                );
            }
        }
        for path in [&cfg.state_dir, &cfg.runtime_dir].into_iter().flatten() {
            anyhow::ensure!(
                path.is_absolute(),
                "state_dir and runtime_dir must be absolute"
            );
        }
        Ok(cfg)
    }
    pub fn project_for_cwd(&self, agent: &str, cwd: &Path) -> Result<(&str, &ProjectSpec)> {
        let cwd = std::fs::canonicalize(cwd).context("Working directory must exist")?;
        anyhow::ensure!(cwd.is_dir(), "Working directory must be a directory");
        let (name, project) = self
            .projects
            .iter()
            .find(|(_, p)| p.cwd == cwd)
            .with_context(|| {
                format!(
                    "No project configured for {}; add it to projects and restart uaoi",
                    cwd.display()
                )
            })?;
        anyhow::ensure!(
            project.agents.iter().any(|a| a == agent),
            "Agent {agent} is not available in project {name}"
        );
        Ok((name, project))
    }
    pub fn select_agent<'a>(&'a self, explicit: Option<&str>) -> Result<&'a str> {
        self.agents
            .get_key_value(explicit.context("Select an agent explicitly with --agent NAME")?)
            .map(|(name, _)| name.as_str())
            .context("Unknown agent instance")
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
