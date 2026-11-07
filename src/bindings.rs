use crate::{config::Config, model::BindingsDocument, wrapper};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{io::Write, path::Path};

fn render(snapshot: &Value) -> Result<String> {
    let document: BindingsDocument = serde_json::from_value(snapshot["document"].clone())?;
    let mut text = String::from(
        "# Topic/session bindings. Save and exit to apply; leave unchanged to cancel.\n\
         # Add an entry with project, agent and native session ID; omit topic to create one.\n\
         # Set topic to an existing topic ID (as a quoted string) to use it.\n\
         # Remove an entry to unbind. This never deletes sessions or topics.\n\
         # Do not edit version or revision. New sessions may be bound while editing.\n\n",
    );
    text.push_str(&format!(
        "version = {}\nrevision = {}\n",
        document.version, document.revision
    ));
    if document.bindings.is_empty() {
        text.push_str("bindings = []\n\n# To add a binding, remove the empty bindings line above and add:\n# [[bindings]]\n# project = \"example\"\n# agent = \"work\"\n# session = \"native-session-id\"\n# topic = \"123\"\n");
    }
    for (index, binding) in document.bindings.iter().enumerate() {
        if let Some(title) = snapshot["titles"][index].as_str() {
            text.push_str(&format!("\n# {}\n", title.replace(['\n', '\r'], " ")));
        }
        text.push_str("\n[[bindings]]\n");
        text.push_str(&toml::to_string(binding)?);
    }
    Ok(text)
}

pub async fn edit(cfg: &Config, config_path: &Path) -> Result<()> {
    let mut connection = wrapper::connect(cfg, config_path).await?;
    let snapshot = wrapper::request(
        &mut connection,
        json!({"method":"bindings","fingerprint":cfg.fingerprint()}),
    )
    .await?;
    let original: BindingsDocument = serde_json::from_value(snapshot["document"].clone())?;
    let mut file = tempfile::Builder::new()
        .prefix("uaoi-bindings-")
        .suffix(".toml")
        .tempfile()?;
    file.write_all(render(&snapshot)?.as_bytes())?;
    file.flush()?;
    let editor = std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "vi".into());
    let args = shlex::split(&editor).context("Invalid VISUAL/EDITOR command")?;
    let executable = args.first().context("VISUAL/EDITOR cannot be empty")?;
    let status = tokio::process::Command::new(executable)
        .args(&args[1..])
        .arg(file.path())
        .status()
        .await
        .context("Start bindings editor")?;
    anyhow::ensure!(
        status.success(),
        "Bindings editor exited unsuccessfully; no changes applied"
    );
    let result: Result<()> = async {
        let document: BindingsDocument = toml::from_str(&std::fs::read_to_string(file.path())?)?;
        if document == original {
            println!("Bindings unchanged.");
            return Ok(());
        }
        anyhow::ensure!(
            document.version == original.version && document.revision == original.revision,
            "Do not edit the binding document version or revision"
        );
        // Reconnect after editing: the broker may close idle control connections.
        let mut connection = wrapper::connect(cfg, config_path).await?;
        wrapper::request(
            &mut connection,
            json!({"method":"bindings/apply","fingerprint":cfg.fingerprint(),"document":document}),
        )
        .await?;
        println!("Bindings updated.");
        Ok(())
    }
    .await;
    if let Err(error) = result {
        let (_, path) = file.keep()?;
        return Err(error.context(format!(
            "Check the current bindings before retrying. Edited document retained at {}",
            path.display()
        )));
    }
    Ok(())
}
