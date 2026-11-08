use std::process::Command;

#[test]
fn cli_exposes_agent_launch_without_daemon_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_uaoi"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    for command in ["status", "check", "bindings", "recovery", "--agent"] {
        assert!(text.contains(command), "missing {command}");
    }
    for command in ["start", "serve", "stop", "codex", "__broker"] {
        assert!(
            !text
                .lines()
                .any(|line| line.trim_start().starts_with(&format!("{command} ")))
        );
    }
    for command in ["start", "serve", "stop", "codex"] {
        assert!(
            !Command::new(env!("CARGO_BIN_EXE_uaoi"))
                .arg(command)
                .output()
                .unwrap()
                .status
                .success()
        );
    }
}

#[tokio::test]
async fn bindings_editor_applies_a_versioned_document_through_the_broker() {
    use serde_json::{Value, json};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let temp = tempfile::tempdir().unwrap();
    let run = temp.path().join("run");
    std::fs::create_dir(&run).unwrap();
    let config = temp.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
version = 4
state_dir = {:?}
runtime_dir = {:?}
[agents.work]
kind = "codex"
command = ["unused"]
[agents.work.telegram]
token_env = "UNUSED"
[projects.demo]
cwd = "/tmp"
agents = ["work"]
[projects.demo.telegram]
group = -1001
"#,
            temp.path().join("state"),
            run
        ),
    )
    .unwrap();
    let script = temp.path().join("editor with spaces.sh");
    std::fs::write(
        &script,
        r#"cat > "$1" <<'TOML'
version = 3
revision = 7
[[bindings]]
project = "demo"
agent = "work"
session = "native-new"
TOML
"#,
    )
    .unwrap();
    let listener = tokio::net::UnixListener::bind(run.join("control.sock")).unwrap();
    let server = tokio::spawn(async move {
        for method in ["bindings", "bindings/apply"] {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["method"], method);
            assert!(request["fingerprint"].as_str().is_some());
            if method == "bindings/apply" {
                assert_eq!(
                    request["document"],
                    json!({"version":3,"revision":7,"bindings":[{"project":"demo","agent":"work","session":"native-new","topic":null}]})
                );
            }
            let result = json!({"document":{"version":3,"revision":7,"bindings":[]},"titles":[]});
            stream
                .get_mut()
                .write_all(format!("{}\n", json!({"result":result})).as_bytes())
                .await
                .unwrap();
        }
    });
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_uaoi"))
        .arg("--config")
        .arg(&config)
        .arg("bindings")
        .env("VISUAL", format!("sh '{}'", script.display()))
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Bindings updated."));
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn old_configuration_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("config.toml");
    std::fs::write(&path, "[projects.old]\nworkspace='/'\nagent='work'\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_uaoi"))
        .args(["--config", path.to_str().unwrap(), "check"])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

fn fixture_config(dir: &std::path::Path, command: &[String]) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(
        &path,
        format!(
            r#"
version = 4
state_dir = {:?}
runtime_dir = {:?}
[agents.work]
kind = "codex"
command = {}
[agents.work.telegram]
token_env = "UAOI_TEST_MISSING_BOT_TOKEN"
[projects.demo]
cwd = "/tmp"
agents = ["work"]
[projects.demo.telegram]
group = -1001
"#,
            dir.join("state"),
            dir.join("run"),
            serde_json::to_string(command).unwrap()
        ),
    )
    .unwrap();
    path
}

#[test]
fn native_arguments_are_passed_without_starting_the_broker() {
    let temp = tempfile::tempdir().unwrap();
    let script = temp.path().join("native.py");
    std::fs::write(&script, "import sys,json; print(json.dumps(sys.argv[1:]))").unwrap();
    let config = fixture_config(
        temp.path(),
        &["python3".into(), script.display().to_string()],
    );
    let output = Command::new(env!("CARGO_BIN_EXE_uaoi"))
        .args([
            "--config",
            config.to_str().unwrap(),
            "--agent",
            "work",
            "--",
            "--help",
            "--config",
            "model=example",
            "a prompt with spaces",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let args: Vec<String> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        args,
        [
            "--help",
            "--config",
            "model=example",
            "a prompt with spaces"
        ]
    );
    assert!(!temp.path().join("run").exists());
    assert!(!temp.path().join("state").exists());
}

#[test]
fn management_commands_never_start_a_broker() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture_config(temp.path(), &["unused".into()]);
    for command in ["check", "status", "bindings", "recovery"] {
        let output = Command::new(env!("CARGO_BIN_EXE_uaoi"))
            .args(["--config", config.to_str().unwrap(), command])
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            matches!(command, "check" | "status")
        );
    }
    let mixed = Command::new(env!("CARGO_BIN_EXE_uaoi"))
        .args([
            "--config",
            config.to_str().unwrap(),
            "--agent",
            "work",
            "status",
        ])
        .output()
        .unwrap();
    assert!(!mixed.status.success());
    assert!(!temp.path().join("run").exists());
    assert!(!temp.path().join("state").exists());
}

#[test]
fn failed_first_activation_reaps_the_spawned_broker() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture_config(temp.path(), &["unused".into()]);
    let output = Command::new(env!("CARGO_BIN_EXE_uaoi"))
        .args(["--config", config.to_str().unwrap(), "--agent", "work"])
        .env_remove("UAOI_TEST_MISSING_BOT_TOKEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Missing UAOI_TEST_MISSING_BOT_TOKEN")
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while temp.path().join("run/control.sock").exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "orphaned control socket"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(temp.path().join("state/broker.lock"))
        .unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).expect("broker still owns its lifetime lock");
}

#[test]
fn inline_token_configuration_checks_without_token_environment() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture_config(temp.path(), &["unused".into()]);
    let contents = std::fs::read_to_string(&config).unwrap().replace(
        "token_env = \"UAOI_TEST_MISSING_BOT_TOKEN\"",
        "token = \"123456:example-test-token\"",
    );
    std::fs::write(&config, contents).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_uaoi"))
        .args(["--config", config.to_str().unwrap(), "check"])
        .env_remove("UAOI_TEST_MISSING_BOT_TOKEN")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("example-test-token"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("example-test-token"));
    assert!(!temp.path().join("run").exists());
}
