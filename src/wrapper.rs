use crate::{config::Config, model::LaunchPlan};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    os::unix::process::{CommandExt, ExitStatusExt},
    path::Path,
    process::Stdio,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::Command,
};

pub fn private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}
pub async fn request(io: &mut BufReader<UnixStream>, value: Value) -> Result<Value> {
    io.get_mut()
        .write_all(format!("{value}\n").as_bytes())
        .await?;
    let mut line = String::new();
    let n = tokio::time::timeout(std::time::Duration::from_secs(120), io.read_line(&mut line))
        .await??;
    if n == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "uaoi broker disconnected",
        )
        .into());
    }
    let v: Value = serde_json::from_str(&line).context("uaoi broker disconnected")?;
    anyhow::ensure!(
        v.get("error").is_none(),
        "{}",
        v["error"].as_str().unwrap_or("Service error")
    );
    Ok(v["result"].clone())
}
/// Management connections never create a broker or register a UAOI instance.
pub async fn connect(cfg: &Config) -> Result<BufReader<UnixStream>> {
    Ok(BufReader::new(
        UnixStream::connect(cfg.runtime().join("control.sock"))
            .await
            .context("No running uaoi; open uaoi --agent NAME first")?,
    ))
}

async fn spawn_broker(
    cfg: &Config,
    path: &Path,
) -> Result<(BufReader<UnixStream>, std::os::unix::net::UnixStream)> {
    let (bootstrap, inherited) = std::os::unix::net::UnixStream::pair()?;
    let inherited: std::os::fd::OwnedFd = inherited.into();
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(cfg.state().join("broker.log"))?;
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command
        .arg("--config")
        .arg(path)
        .arg("__broker")
        .stdin(Stdio::from(inherited))
        .stdout(Stdio::null())
        .stderr(log);
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if let Ok(stream) = connect(cfg).await {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
            return Ok((stream, bootstrap));
        }
        if child.try_wait()?.is_some() || tokio::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!(
                "Could not start uaoi; inspect {}",
                cfg.state().join("broker.log").display()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn open(
    cfg: &Config,
    path: &Path,
    request_value: Value,
) -> Result<(BufReader<UnixStream>, LaunchPlan)> {
    private_dir(&cfg.state())?;
    private_dir(&cfg.runtime())?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for uaoi to finish stopping"
        );
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(cfg.runtime().join("startup.lock"))?;
        let lock = tokio::task::spawn_blocking(move || -> Result<_> {
            fs2::FileExt::lock_exclusive(&lock)?;
            Ok(lock)
        })
        .await??;
        let (mut io, bootstrap) = match connect(cfg).await {
            Ok(io) => (io, None),
            Err(_) => {
                // The previous broker removes its socket before releasing this
                // lifetime lock. Wait for it instead of spawning a competing process.
                let probe = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(cfg.runtime().join("control.lock"))?;
                if fs2::FileExt::try_lock_exclusive(&probe).is_err() {
                    drop(lock);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    continue;
                }
                drop(probe);
                let (io, bootstrap) = spawn_broker(cfg, path).await?;
                (io, Some(bootstrap))
            }
        };
        let result = request(&mut io, request_value.clone()).await;
        drop(bootstrap);
        drop(lock);
        match result {
            Ok(result) if result["retry"] == true => {}
            Ok(result) => return Ok((io, serde_json::from_value(result)?)),
            Err(error) => {
                let disconnected = error.downcast_ref::<std::io::Error>().is_some_and(|e| {
                    matches!(
                        e.kind(),
                        std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::BrokenPipe
                            | std::io::ErrorKind::ConnectionAborted
                    )
                });
                if !disconnected {
                    return Err(error);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
pub async fn run(
    cfg: &Config,
    path: &Path,
    explicit_agent: Option<&str>,
    args: Vec<OsString>,
) -> Result<i32> {
    let base = std::env::current_dir()?;
    let (interactive, cwd) = crate::agent::codex_cli::invocation(&args, &base)?;
    let agent = cfg.select_agent(explicit_agent)?;
    let spec = &cfg.agents[agent];
    if !interactive {
        let error = std::process::Command::new(&spec.command[0])
            .args(&spec.command[1..])
            .args(args)
            .envs(&spec.env)
            .exec();
        return Err(error.into());
    }
    let (mut io, plan) = open(cfg, path, json!({"method":"open","args":args,"agent":agent,"cwd":cwd,"config":path,"fingerprint":cfg.fingerprint(),"env":std::env::vars().collect::<std::collections::BTreeMap<_,_>>()})).await?;
    let mut cmd = Command::new(&plan.command[0]);
    cmd.args(&plan.command[1..])
        .env_clear()
        .envs(&plan.env)
        .current_dir(base)
        .kill_on_drop(true);
    #[cfg(target_os = "linux")]
    unsafe {
        let parent = libc::getpid();
        cmd.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::getppid() != parent {
                libc::_exit(1);
            }
            Ok(())
        });
    }
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = request(&mut io, json!({"method":"close"})).await;
            return Err(error.into());
        }
    };
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut hup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    let mut line = String::new();
    let mut deadline = None;
    let status = loop {
        tokio::select! {
            status = child.wait() => break status?,
            _ = tokio::signal::ctrl_c() => {}, // The foreground TUI receives SIGINT too.
            _ = term.recv() => {
                if let Some(pid) = child.id() { unsafe { libc::kill(pid as i32, libc::SIGTERM); } }
                deadline = Some(tokio::time::Instant::now() + std::time::Duration::from_secs(3));
            },
            _ = hup.recv() => {
                if let Some(pid) = child.id() { unsafe { libc::kill(pid as i32, libc::SIGHUP); } }
                deadline = Some(tokio::time::Instant::now() + std::time::Duration::from_secs(3));
            },
            _ = async { if let Some(at) = deadline { tokio::time::sleep_until(at).await } else { std::future::pending::<()>().await } } => {
                child.kill().await?;
                break child.wait().await?;
            },
            _ = io.read_line(&mut line) => {
                let _ = child.kill().await;
                anyhow::bail!("uaoi broker or agent runtime disconnected");
            }
        }
    };
    // The final UAOI instance waits until its agent's resources have been reaped.
    request(&mut io, json!({"method":"close"})).await?;
    Ok(status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)))
}
