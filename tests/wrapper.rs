use std::process::Command;

#[test]
fn cli_exposes_service_lifecycle_without_loading_config() {
    let output = Command::new(env!("CARGO_BIN_EXE_uaoi"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    for command in ["start", "serve", "status", "stop", "bindings", "--agent"] {
        assert!(text.contains(command), "missing {command}");
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
version = 3
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
allowed_users = [1]
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
version = 2
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
                    json!({"version":2,"revision":7,"bindings":[{"project":"demo","agent":"work","session":"native-new","topic":null}]})
                );
            }
            let result = json!({"document":{"version":2,"revision":7,"bindings":[]},"titles":[]});
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
