use std::{io, path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    if let Err(error) = run() {
        eprintln!("roost-daemon: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let dir = match args.as_slice() {
        [flag, db] if flag == roost_daemon::guardian::ARG => {
            // The guardian reports through its lease and never logs project output.
            return roost_daemon::guardian::run(&PathBuf::from(db))
                .map_err(|_| io::Error::other("worker guardian did not complete").into());
        }
        [] => roost_ipc::state_dir()?,
        [flag, dir] if flag == "--state-dir" => PathBuf::from(dir),
        [flag] if flag == "--help" || flag == "-h" => {
            println!("Usage: roost-daemon [--state-dir ABSOLUTE_PATH]");
            return Ok(());
        }
        _ => {
            return Err(io::Error::other("usage: roost-daemon [--state-dir ABSOLUTE_PATH]").into());
        }
    };
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        let mut interrupt = signal(SignalKind::interrupt())?;
        let daemon = roost_daemon::Daemon::bind(&dir).await?;
        tracing::info!("Roost IPC is ready");
        let result = tokio::select! {
            result = daemon.run() => result,
            _ = terminate.recv() => Ok(()),
            _ = interrupt.recv() => Ok(()),
        };
        daemon.shutdown().await;
        result?;
        Ok(())
    });
    runtime.shutdown_timeout(roost_ipc::IO_TIMEOUT);
    result
}
