use anyhow::{Context, Result};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

// Interpret only launcher routing options; Codex remains the argument validator.
pub fn invocation(args: &[OsString], base: &Path) -> Result<(bool, PathBuf)> {
    let mut cwd = base.to_owned();
    let mut i = 0;
    let mut command = None;
    let mut positional = false;
    while i < args.len() {
        let a = args[i].to_string_lossy();
        if !positional && a == "--" {
            positional = true;
            i += 1;
            continue;
        }
        if !positional && ["--help", "-h", "--version", "-V"].contains(&a.as_ref()) {
            return Ok((false, cwd));
        }
        if !positional
            && (a == "-p"
                || a == "--profile"
                || a.starts_with("--profile=")
                || a.starts_with("-p") && a.len() > 2)
        {
            anyhow::bail!(
                "Configure the profile on the agent launcher, then select it with uaoi --agent NAME"
            );
        }
        if !positional
            && (a == "--remote"
                || a.starts_with("--remote=")
                || a == "--remote-auth-token-env"
                || a.starts_with("--remote-auth-token-env=")
                || a == "--no-daemon")
        {
            anyhow::bail!("uaoi owns the remote endpoint; omit --remote");
        }
        if !positional && (a == "-C" || a == "--cd") {
            i += 1;
            cwd = base.join(args.get(i).context("Missing directory after -C/--cd")?);
        } else if !positional && (a.starts_with("--cd=") || a.starts_with("-C") && a.len() > 2) {
            cwd = base.join(
                a.strip_prefix("--cd=")
                    .unwrap_or(a[2..].strip_prefix('=').unwrap_or(&a[2..])),
            );
        } else if !positional
            && [
                "-c",
                "--config",
                "--enable",
                "--disable",
                "-i",
                "--image",
                "-m",
                "--model",
                "--local-provider",
                "-p",
                "--profile",
                "-s",
                "--sandbox",
                "--add-dir",
                "-a",
                "--ask-for-approval",
            ]
            .contains(&a.as_ref())
        {
            i += 1;
        } else if !positional && !a.starts_with('-') && command.is_none() {
            command = Some(a.to_string());
        }
        i += 1;
    }
    let native_commands = [
        "exec",
        "e",
        "review",
        "login",
        "logout",
        "mcp",
        "plugin",
        "app-server",
        "remote-control",
        "completion",
        "update",
        "doctor",
        "sandbox",
        "debug",
        "apply",
        "a",
        "queue",
        "archive",
        "delete",
        "migrate-rollouts",
        "migrate-nfs-db",
        "unarchive",
        "cloud",
        "exec-server",
        "features",
        "help",
    ];
    let interactive = !command
        .as_deref()
        .is_some_and(|c| native_commands.contains(&c));
    Ok((interactive, std::fs::canonicalize(cwd)?))
}

#[derive(Clone, Debug, Default)]
pub struct Permissions {
    pub approval: Option<String>,
    pub sandbox: Option<String>,
    pub reviewer: Option<String>,
}
impl Permissions {
    pub fn apply(&self, params: &mut serde_json::Value) {
        if let Some(policy) = &self.approval {
            params["approvalPolicy"] = policy.clone().into();
        }
        if let Some(reviewer) = &self.reviewer {
            params["approvalsReviewer"] = reviewer.clone().into();
        }
        if let Some(sandbox) = &self.sandbox {
            params["sandbox"] = sandbox.clone().into();
            params["permissions"] = serde_json::Value::Null;
            if let Some(config) = params["config"].as_object_mut() {
                for key in ["default_permissions", "permissions"] {
                    config.remove(key);
                }
            }
        }
    }
}

// Remote TUI rejects resume when these are CLI overrides. Apply them to the
// native lifecycle requests instead; never edit the user's saved configuration.
pub fn remote_arguments(args: &[OsString]) -> Result<(Vec<OsString>, Permissions)> {
    let mut tui = vec![];
    let mut permissions = Permissions::default();
    let mut i = 0;
    let mut bypass = false;
    let mut auto_review = false;
    while i < args.len() {
        let arg = args[i].to_string_lossy();
        if arg == "--" {
            tui.extend_from_slice(&args[i..]);
            break;
        }
        if arg == "--dangerously-bypass-approvals-and-sandbox" {
            bypass = true;
            i += 1;
            continue;
        }
        if arg == "--approve-for-me" {
            auto_review = true;
            i += 1;
            continue;
        }
        let config_value = if arg == "-c" || arg == "--config" {
            args.get(i + 1).map(|v| v.to_string_lossy().to_string())
        } else {
            arg.strip_prefix("--config=")
                .or_else(|| arg.strip_prefix("-c").filter(|s| !s.is_empty()))
                .map(str::to_owned)
        };
        if let Some(value) = config_value
            && let Some((key, value)) = value.split_once('=')
            && ["approval_policy", "sandbox_mode", "approvals_reviewer"].contains(&key.trim())
        {
            let value = toml::from_str::<toml::Table>(&format!("value={value}"))
                .ok()
                .and_then(|t| {
                    t.get("value")
                        .and_then(toml::Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| value.trim().to_owned());
            match key.trim() {
                "approval_policy" => {
                    anyhow::ensure!(
                        ["never", "on-request"].contains(&value.as_str()),
                        "Invalid approval policy"
                    );
                    permissions.approval = Some(value);
                }
                "sandbox_mode" => {
                    anyhow::ensure!(
                        ["read-only", "workspace-write", "danger-full-access"]
                            .contains(&value.as_str()),
                        "Invalid sandbox"
                    );
                    permissions.sandbox = Some(value);
                }
                _ => {
                    anyhow::ensure!(
                        ["user", "auto_review"].contains(&value.as_str()),
                        "Invalid approvals reviewer"
                    );
                    permissions.reviewer = Some(value);
                }
            }
            i += if arg == "-c" || arg == "--config" {
                2
            } else {
                1
            };
            continue;
        }
        let mut setting = None;
        for (short, long, key) in [
            ("-a", "--ask-for-approval", "approval"),
            ("-s", "--sandbox", "sandbox"),
        ] {
            if arg == short || arg == long {
                i += 1;
                setting = Some((
                    key,
                    args.get(i)
                        .context("Missing permission option value")?
                        .to_string_lossy()
                        .to_string(),
                ));
                break;
            }
            if let Some(value) = arg
                .strip_prefix(&format!("{long}="))
                .or_else(|| arg.strip_prefix(short).filter(|s| !s.is_empty()))
            {
                setting = Some((key, value.trim_start_matches('=').to_owned()));
                break;
            }
        }
        if let Some((key, value)) = setting {
            if key == "approval" {
                anyhow::ensure!(
                    ["never", "on-request"].contains(&value.as_str()),
                    "Invalid approval policy {value}"
                );
                permissions.approval = Some(value);
            } else {
                anyhow::ensure!(
                    ["read-only", "workspace-write", "danger-full-access"]
                        .contains(&value.as_str()),
                    "Invalid sandbox {value}"
                );
                permissions.sandbox = Some(value);
            }
        } else {
            // Preserve option values as a unit; a prompt/model beginning with -s
            // or -a is not a permission option.
            tui.push(args[i].clone());
            if [
                "-c",
                "--config",
                "--enable",
                "--disable",
                "-i",
                "--image",
                "-m",
                "--model",
                "--local-provider",
                "-p",
                "--profile",
                "-C",
                "--cd",
                "--add-dir",
            ]
            .contains(&arg.as_ref())
            {
                i += 1;
                tui.push(args.get(i).context("Missing option value")?.clone());
            }
        }
        i += 1;
    }
    anyhow::ensure!(
        !(bypass
            && (auto_review || permissions.approval.is_some() || permissions.sandbox.is_some())),
        "Bypass flag conflicts with explicit permission options"
    );
    anyhow::ensure!(
        !(auto_review && permissions.approval.is_some()),
        "--approve-for-me conflicts with --ask-for-approval"
    );
    if bypass {
        permissions.approval = Some("never".into());
        permissions.sandbox = Some("danger-full-access".into());
    }
    if auto_review {
        permissions.approval = Some("on-request".into());
        permissions.reviewer = Some("auto_review".into());
        if permissions.sandbox.is_none() {
            permissions.sandbox = Some("workspace-write".into());
        }
    }
    Ok((tui, permissions))
}
