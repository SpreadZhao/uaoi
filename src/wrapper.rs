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
    tokio::time::timeout(std::time::Duration::from_secs(30), io.read_line(&mut line)).await??;
    let v: Value = serde_json::from_str(&line).context("uaoi service disconnected")?;
    anyhow::ensure!(
        v.get("error").is_none(),
        "{}",
        v["error"].as_str().unwrap_or("Service error")
    );
    Ok(v["result"].clone())
}
pub async fn connect(cfg: &Config, path: &Path) -> Result<BufReader<UnixStream>> {
    private_dir(&cfg.state())?;
    private_dir(&cfg.runtime())?;
    // Separate from the broker's lifetime lock: serialize discovery and startup.
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
    let socket = cfg.runtime().join("control.sock");
    let result = if let Ok(io) = UnixStream::connect(&socket).await {
        Ok(BufReader::new(io))
    } else {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(cfg.state().join("service.log"))?;
        let mut cmd = std::process::Command::new(std::env::current_exe()?);
        cmd.arg("--config")
            .arg(path)
            .arg("serve")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log);
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = cmd.spawn()?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(25);
        loop {
            if let Ok(io) = UnixStream::connect(&socket).await {
                break Ok(BufReader::new(io));
            }
            if child.try_wait()?.is_some() || tokio::time::Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                break Err(anyhow::anyhow!(
                    "Could not start uaoi; inspect {}",
                    cfg.state().join("service.log").display()
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    };
    drop(lock);
    result
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
    let mut io = connect(cfg, path).await?;
    let plan: LaunchPlan = serde_json::from_value(request(&mut io, json!({"method":"open","args":args,"agent":agent,"cwd":cwd,"config":path,"fingerprint":cfg.fingerprint(),"env":std::env::vars().collect::<std::collections::BTreeMap<_,_>>()})).await?)?;
    let mut cmd = Command::new(&plan.command[0]);
    cmd.args(&plan.command[1..])
        .env_clear()
        .envs(&plan.env)
        .current_dir(base)
        .kill_on_drop(true);
    #[cfg(target_os = "linux")]
    unsafe {
        cmd.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = cmd.spawn()?;
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut hup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    let mut line = String::new();
    let status = loop {
        tokio::select! {
            status=child.wait()=>break status?,
            _=tokio::signal::ctrl_c()=>{}, // The foreground TUI receives SIGINT from the terminal too.
            _=term.recv()=>{ if let Some(pid)=child.id(){unsafe{libc::kill(pid as i32,libc::SIGTERM);}} },
            _=hup.recv()=>{ if let Some(pid)=child.id(){unsafe{libc::kill(pid as i32,libc::SIGHUP);}} },
            _=io.read_line(&mut line)=>{ anyhow::bail!("uaoi service disconnected"); }
        }
    };
    // Release only this terminal gateway; the shared server keeps its tasks.
    request(&mut io, json!({"method":"close"})).await?;
    Ok(status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)))
}
